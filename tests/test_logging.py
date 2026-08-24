from io import StringIO
from typing import cast
from datetime import datetime
from collections.abc import Iterator

import json
import logging

import pytest
import structlog

from scam_sniffer.core.log.errors import LogError
from scam_sniffer.core.log.config import LogLevel, LogRenderer, LogConfig
from scam_sniffer.core.log.logger import get_logger, configure_logging

class LoggingProbe:
    def emit(self) -> None:
        get_logger().info(
            "Candles %s",
            "synchronized",
            symbol="BTCUSDT",
            timeframes=("5m", "15m"),
        )

@pytest.fixture(autouse=True)
def _restore_logging() -> Iterator[None]:
    root_logger = logging.getLogger()
    root_level = root_logger.level
    root_handlers = tuple(root_logger.handlers)

    yield

    for handler in root_logger.handlers:
        if handler not in root_handlers:
            handler.close()
    root_logger.handlers.clear()
    for handler in root_handlers:
        root_logger.addHandler(handler)
    root_logger.setLevel(root_level)
    logging.captureWarnings(False)
    structlog.reset_defaults()

def test_logging_config_rejects_invalid_values() -> None:
    with pytest.raises(LogError) as level_error_info:
        LogConfig(level=cast(LogLevel, "verbose"))
    with pytest.raises(LogError) as renderer_error_info:
        LogConfig(renderer=cast(LogRenderer, "xml"))

    assert not hasattr(level_error_info.value, "reason")
    assert not hasattr(level_error_info.value, "operation")
    assert not hasattr(level_error_info.value, "root_cause")
    assert not hasattr(renderer_error_info.value, "reason")
    assert not hasattr(renderer_error_info.value, "operation")
    assert not hasattr(renderer_error_info.value, "root_cause")

def test_console_logging_uses_application_format() -> None:
    stream = StringIO()
    configure_logging(
        config=LogConfig(level=LogLevel.DEBUG),
        stream=stream,
    )

    LoggingProbe().emit()

    datetime_value, message = stream.getvalue().strip().split(" ", maxsplit=1)
    assert datetime.fromisoformat(datetime_value.replace("Z", "+00:00")).tzinfo is not None
    assert message == (
        "INFO LoggingProbe|test_logging.py::emit "
        'Candles synchronized: symbol=BTCUSDT timeframes=["5m","15m"]'
    )

def test_json_logging_uses_the_same_event_contract() -> None:
    stream = StringIO()
    configure_logging(
        config=LogConfig(renderer=LogRenderer.JSON),
        stream=stream,
    )

    LoggingProbe().emit()

    event = json.loads(stream.getvalue())
    assert event["level"] == "info"
    assert event["class"] == "LoggingProbe"
    assert event["file"] == "test_logging.py"
    assert event["function"] == "emit"
    assert event["message"] == "Candles synchronized"
    assert event["symbol"] == "BTCUSDT"
    assert event["timeframes"] == ["5m", "15m"]

def test_logging_filters_events_below_configured_level() -> None:
    stream = StringIO()
    configure_logging(
        config=LogConfig(level=LogLevel.WARNING),
        stream=stream,
    )
    logger = get_logger()

    logger.info("Filtered event")
    logger.warning("Visible event")

    assert "Filtered event" not in stream.getvalue()
    assert "Visible event" in stream.getvalue()
