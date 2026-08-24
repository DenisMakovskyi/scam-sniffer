"""Structured application log setup and rendering."""

from enum import StrEnum
from typing import Any, cast, TextIO
from datetime import UTC, date, datetime

import sys
import json
import logging

import structlog
from structlog.typing import EventDict, WrappedLogger

from scam_sniffer.core.log.errors import LogError
from scam_sniffer.core.log.config import LogLevel, LogRenderer, LogConfig

_LOG_LEVELS = {
    LogLevel.INFO: logging.INFO,
    LogLevel.DEBUG: logging.DEBUG,
    LogLevel.ERROR: logging.ERROR,
    LogLevel.WARNING: logging.WARNING,
    LogLevel.CRITICAL: logging.CRITICAL,
}

class _ConsoleRenderer:
    """Render one application event in the agreed human-readable format."""

    def __call__(
        self,
        logger: WrappedLogger,
        method: str,
        events: EventDict,
    ) -> str:
        """Build a console line and append exception details when present.

        Args:
            logger: Wrapped logger that emitted the event.
            method: Logging method used for the event.
            events: Processed structured event fields.

        Returns:
            Human-readable log entry.
        """
        del logger, method

        level = str(events.pop("level")).upper()
        message = events.pop("message")
        exception = events.pop("exception", None)
        file_name = events.pop("file")
        class_name = events.pop("class")
        function_name = events.pop("function")
        datetime_value = events.pop("datetime")

        context = " ".join(
            f"{key}={_render_value(value)}"
            for key, value in events.items()
        )
        log_line = (
            f"{datetime_value} {level} "
            f"{class_name}|{file_name}::{function_name} {message}"
        )
        if context:
            log_line = f"{log_line}: {context}"
        if exception:
            log_line = f"{log_line}\n{exception}"
        return log_line

def get_logger() -> structlog.stdlib.BoundLogger:
    """Return a lazily configured structured application logger."""
    return cast(structlog.stdlib.BoundLogger, structlog.get_logger())

def configure_logging(
    config: LogConfig,
    stream: TextIO | None = None,
) -> None:
    """Configure structured and standard-library log once per process.

    Args:
        config: Validated log severity and renderer configuration.
        stream: Optional output stream used mainly by focused tests.

    Raises:
        LogError: If log setup fails.
    """
    try:
        renderer: structlog.types.Processor = (
            structlog.processors.JSONRenderer(serializer=_json_dumps)
            if config.renderer is LogRenderer.JSON
            else _ConsoleRenderer()
        )
        log_level = _LOG_LEVELS[config.level]
        shared_processors: list[structlog.types.Processor] = [
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            _add_datetime,
            structlog.processors.CallsiteParameterAdder(
                {
                    structlog.processors.CallsiteParameter.MODULE,
                    structlog.processors.CallsiteParameter.FILENAME,
                    structlog.processors.CallsiteParameter.FUNC_NAME,
                    structlog.processors.CallsiteParameter.QUAL_NAME,
                }
            ),
        ]

        # noinspection bad-argument-type
        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                *shared_processors,
                structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
            ],
            context_class=dict,
            wrapper_class=structlog.stdlib.BoundLogger,
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use=True,
        )

        formatter = structlog.stdlib.ProcessorFormatter(
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                _normalize_event,
                renderer,
            ],
            foreign_pre_chain=shared_processors,
        )

        handler = logging.StreamHandler(stream=stream or sys.stdout)
        handler.setFormatter(formatter)

        root_logger = logging.getLogger()
        root_logger.handlers.clear()
        root_logger.addHandler(handler)
        root_logger.setLevel(log_level)
        logging.captureWarnings(True)
    except (KeyError, TypeError, ValueError) as error:
        raise LogError(message="Application log setup failed") from error

def _render_value(value: Any) -> str:
    """Render one context value without losing machine-readable structure."""
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (dict, list, tuple, bool)) or value is None:
        return _json_dumps(value)
    return str(value)

def _add_datetime(
    logger: WrappedLogger,
    method: str,
    events: EventDict,
) -> EventDict:
    """Add a millisecond-precision UTC timestamp to a log event."""
    del logger, method
    current_time = datetime.now(tz=UTC)
    events["datetime"] = current_time.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    return events

def _normalize_event(
    logger: WrappedLogger,
    method: str,
    events: EventDict,
) -> EventDict:
    """Normalize processor fields into the application log contract."""
    del logger, method

    module_name = str(events.pop("module", "unknown"))
    function_name = str(events.pop("func_name", "unknown"))
    qualified_name = str(events.pop("qual_name", function_name))

    class_name = _resolve_class_name(
        module_name=module_name,
        function_name=function_name,
        qualified_name=qualified_name,
    )

    normalized_event: EventDict = {
        "datetime": events.pop("datetime", "unknown"),
        "level": events.pop("level", "info"),
        "class": class_name,
        "file": events.pop("filename", "unknown.py"),
        "function": function_name,
        "message": events.pop("event", ""),
    }
    normalized_event.update(events)
    return normalized_event

def _resolve_class_name(
    module_name: str,
    function_name: str,
    qualified_name: str,
) -> str:
    """Resolve a class name or fall back to the emitting module name."""
    name_parts = qualified_name.split(".")
    if len(name_parts) > 1 and name_parts[-2] != "<locals>":
        return name_parts[-2]
    if module_name:
        return module_name.rsplit(".", maxsplit=1)[-1]
    return function_name

def _json_dumps(value: Any, **kwargs: Any) -> str:
    """Serialize log values into compact UTF-8 JSON."""
    kwargs["default"] = str
    kwargs["ensure_ascii"] = False
    kwargs["separators"] = (",", ":")
    return json.dumps(value, **kwargs)
