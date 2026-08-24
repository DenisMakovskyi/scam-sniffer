"""Logging configuration errors."""

from scam_sniffer.errors import AppError

class LogError(AppError):
    """Represent a log configuration or setup failure."""

    def __init__(
        self,
        *,
        message: str,
    ) -> None:
        """Initialize a log failure with a human-readable message.

        Args:
            message: Human-readable failure description.
        """
        RuntimeError.__init__(self, message)
