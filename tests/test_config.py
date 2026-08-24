from pathlib import Path

import pytest
from pydantic import ValidationError

from scam_sniffer.config import AppConfig
from scam_sniffer.domain.models import Market
from scam_sniffer.core.tasks.errors import TaskQueueError
from scam_sniffer.data.database.engine import DatabaseConfig
from scam_sniffer.core.tasks.config import AsyncTaskQueueConfig
from scam_sniffer.domain.manager.config import CandleManagerConfig
from scam_sniffer.data.api.client.config import ApiConfig, WsConfig

_MARKET_KEY = "SCAM_SNIFFER_MARKET"
_SYMBOLS_KEY = "SCAM_SNIFFER_SYMBOLS"
_DATABASE_DSN_KEY = "SCAM_SNIFFER_DATABASE_CONFIG__DSN"
_WORKER_COUNT_KEY = "SCAM_SNIFFER_ASYNC_TASK_QUEUE_CONFIG__WORKER_COUNT"

_CONFIG_ENV_KEYS = (
    _MARKET_KEY,
    _SYMBOLS_KEY,
    _DATABASE_DSN_KEY,
    _WORKER_COUNT_KEY,
    "SCAM_SNIFFER_API_CONFIG__REST_URL",
    "SCAM_SNIFFER_API_CONFIG__MAX_ATTEMPTS",
    "SCAM_SNIFFER_API_CONFIG__MAX_RETRY_DELAY",
    "SCAM_SNIFFER_API_CONFIG__TIMEOUT_SECONDS",
    "SCAM_SNIFFER_API_CONFIG__WS_CONFIG__WS_URL",
    "SCAM_SNIFFER_API_CONFIG__WS_CONFIG__WS_QUEUE_SIZE",
    "SCAM_SNIFFER_API_CONFIG__WS_CONFIG__WS_PING_TIMEOUT",
    "SCAM_SNIFFER_API_CONFIG__WS_CONFIG__WS_PING_INTERVAL",
    "SCAM_SNIFFER_API_CONFIG__WS_CONFIG__WS_CLOSE_TIMEOUT",
    "SCAM_SNIFFER_DATABASE_CONFIG__POOL_MIN_SIZE",
    "SCAM_SNIFFER_DATABASE_CONFIG__POOL_MAX_SIZE",
    "SCAM_SNIFFER_DATABASE_CONFIG__COMMAND_TIMEOUT",
    "SCAM_SNIFFER_ASYNC_TASK_QUEUE_CONFIG__QUEUE_SIZE",
    "SCAM_SNIFFER_ASYNC_TASK_QUEUE_CONFIG__DEDUPE_CACHE_SIZE",
    "SCAM_SNIFFER_CANDLE_MANAGER_CONFIG__BATCH_SIZE",
    "SCAM_SNIFFER_CANDLE_MANAGER_CONFIG__BACKFILL_SIZE",
    "SCAM_SNIFFER_CANDLE_MANAGER_CONFIG__STREAM_RETRY_DELAY",
    "SCAM_SNIFFER_CANDLE_MANAGER_CONFIG__STREAM_RETRY_MAX_DELAY",
)

@pytest.fixture(autouse=True)
def _clear_app_config_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for env_key in _CONFIG_ENV_KEYS:
        monkeypatch.delenv(env_key, raising=False)

def test_app_config_loads_nested_configs_from_conf_file(tmp_path: Path) -> None:
    config_path = tmp_path / "scam-sniffer.conf"
    config_path.write_text(
        "\n".join(
            (
                "SCAM_SNIFFER_DATABASE_CONFIG__DSN=postgresql://file/database",
                "SCAM_SNIFFER_DATABASE_CONFIG__POOL_MAX_SIZE=4",
                "SCAM_SNIFFER_MARKET=binance",
                'SCAM_SNIFFER_SYMBOLS=["BTCUSDT","ETHUSDT"]',
                "SCAM_SNIFFER_API_CONFIG__WS_CONFIG__WS_URL=wss://file/ws",
                "SCAM_SNIFFER_ASYNC_TASK_QUEUE_CONFIG__WORKER_COUNT=2",
                "SCAM_SNIFFER_CANDLE_MANAGER_CONFIG__BACKFILL_SIZE=5000",
            )
        ),
        encoding="utf-8",
    )

    config = AppConfig.load(path=config_path)

    assert config.market is Market.BINANCE
    assert config.symbols == ("BTCUSDT", "ETHUSDT")
    assert isinstance(config.api_config, ApiConfig)
    assert isinstance(config.database_config, DatabaseConfig)
    assert isinstance(config.api_config.ws_config, WsConfig)
    assert isinstance(config.async_task_queue_config, AsyncTaskQueueConfig)
    assert isinstance(config.candle_manager_config, CandleManagerConfig)
    assert config.api_config.ws_config.ws_url == "wss://file/ws"
    assert config.database_config.dsn == "postgresql://file/database"
    assert config.database_config.pool_max_size == 4
    assert config.async_task_queue_config.worker_count == 2
    assert config.candle_manager_config.backfill_size == 5_000

def test_app_config_environment_overrides_conf_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "scam-sniffer.conf"
    config_path.write_text(
        "\n".join(
            (
                "SCAM_SNIFFER_DATABASE_CONFIG__DSN=postgresql://file/database",
                "SCAM_SNIFFER_API_CONFIG__WS_CONFIG__WS_URL=wss://file/ws",
                "SCAM_SNIFFER_ASYNC_TASK_QUEUE_CONFIG__WORKER_COUNT=2",
            )
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv(_DATABASE_DSN_KEY, "postgresql://environment/database")
    monkeypatch.setenv(_SYMBOLS_KEY, '["ETHUSDT"]')
    monkeypatch.setenv(_WORKER_COUNT_KEY, "4")

    config = AppConfig.load(path=config_path)

    assert config.api_config.ws_config.ws_url == "wss://file/ws"
    assert config.symbols == ("ETHUSDT",)
    assert config.database_config.dsn == "postgresql://environment/database"
    assert config.async_task_queue_config.worker_count == 4

def test_app_config_uses_nested_defaults(tmp_path: Path) -> None:
    config_path = tmp_path / "scam-sniffer.conf"
    config_path.write_text(
        "SCAM_SNIFFER_DATABASE_CONFIG__DSN=postgresql://file/database\n",
        encoding="utf-8",
    )

    config = AppConfig.load(path=config_path)

    assert config.market is Market.BINANCE
    assert config.symbols == ("BTCUSDT",)
    assert config.api_config.rest_url == "https://fapi.binance.com"
    assert config.api_config.ws_config.ws_url == "wss://fstream.binance.com/market/ws"
    assert config.async_task_queue_config == AsyncTaskQueueConfig()
    assert config.candle_manager_config == CandleManagerConfig()

def test_app_config_uses_default_conf_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    Path("scam-sniffer.conf").write_text(
        "SCAM_SNIFFER_DATABASE_CONFIG__DSN=postgresql://file/database\n",
        encoding="utf-8",
    )

    config = AppConfig.load()

    assert config.database_config.dsn == "postgresql://file/database"

def test_app_config_preserves_nested_config_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(_DATABASE_DSN_KEY, "postgresql://environment/database")
    monkeypatch.setenv(_WORKER_COUNT_KEY, "0")

    with pytest.raises(TaskQueueError):
        AppConfig.load(path=tmp_path / "missing.conf")

def test_app_config_rejects_missing_database_config(tmp_path: Path) -> None:
    with pytest.raises(ValidationError):
        AppConfig.load(path=tmp_path / "missing.conf")
