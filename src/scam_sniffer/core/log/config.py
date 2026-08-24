"""Configuration models for application log."""

from enum import StrEnum
from dataclasses import dataclass

from scam_sniffer.core.log.errors import LogError

class LogLevel(StrEnum):
    """Define supported application log severity thresholds."""

    INFO = "info"
    DEBUG = "debug"
    ERROR = "error"
    WARNING = "warning"
    CRITICAL = "critical"

class LogRenderer(StrEnum):
    """Define supported application log output formats."""

    JSON = "json"
    CONSOLE = "console"

@dataclass(frozen=True, slots=True)
class LogConfig:
    """Configure application log filtering and rendering.

    Attributes:
        level: Minimum severity emitted by application log.
        renderer: Output renderer used by the log pipeline.
    """

    level: LogLevel = LogLevel.INFO
    renderer: LogRenderer = LogRenderer.CONSOLE

    def __post_init__(self) -> None:
        """Validate the log level and renderer.

        Raises:
            LogError: If a log option is not supported.
        """
        if not isinstance(self.level, LogLevel):
            raise LogError(message="Log level is invalid")
        if not isinstance(self.renderer, LogRenderer):
            raise LogError(message="Log renderer is invalid")
