"""Application configuration assembled from environment and file values."""

from typing import Self
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from scam_sniffer.data.api.client.config import ApiConfig, WsConfig
from scam_sniffer.data.database.engine import DatabaseConfig
from scam_sniffer.domain.tasks.config import AsyncTaskQueueConfig
from scam_sniffer.domain.manager.config import CandleManagerConfig

_CONFIG_PATH = Path("scam-sniffer.conf")
_DEFAULT_API_CONFIG = ApiConfig(
    rest_url="https://fapi.binance.com",
    ws_config=WsConfig(ws_url="wss://fstream.binance.com/ws"),
)

class AppConfig(BaseSettings):
    """Hold every subsystem configuration used by the application.

    Environment variables have priority over values loaded from the dotenv-compatible
    ``scam-sniffer.conf`` file. Nested fields use a double underscore delimiter.

    Attributes:
        api_config: Shared HTTP and WebSocket transport configuration.
        database_config: PostgreSQL connection pool configuration.
        candle_manager_config: Candle synchronization manager configuration.
        async_task_queue_config: Asynchronous task queue configuration.
    """

    model_config = SettingsConfigDict(
        extra="ignore",
        frozen=True,
        case_sensitive=False,
        nested_model_default_partial_update=True,
        env_file=_CONFIG_PATH,
        env_prefix="SCAM_SNIFFER_",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
    )

    api_config: ApiConfig = Field(default=_DEFAULT_API_CONFIG)
    database_config: DatabaseConfig
    candle_manager_config: CandleManagerConfig = Field(default_factory=CandleManagerConfig)
    async_task_queue_config: AsyncTaskQueueConfig = Field(default_factory=AsyncTaskQueueConfig)

    @classmethod
    def load(cls, path: Path = _CONFIG_PATH) -> Self:
        """Load application configuration using environment-over-file priority.

        Args:
            path: Dotenv-compatible fallback configuration file.

        Returns:
            Validated application configuration.

        Raises:
            ValueError: If a required value is missing or cannot be parsed.
            ScamError: If a nested subsystem configuration is invalid.
        """
        return cls(_env_file=path)
