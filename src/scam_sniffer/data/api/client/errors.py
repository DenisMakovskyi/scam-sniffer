"""Failures raised by shared remote API transports."""

from __future__ import annotations

from enum import StrEnum

from scam_sniffer.errors import AppError

class ApiError(AppError):
    """Represent an HTTP or WebSocket transport failure."""

    def __init__(
        self,
        reason: ApiErrorReason,
        message: str,
        operation: str,
    ) -> None:
        """Initialize an API transport failure.

        Args:
            reason: Transport failure category.
            message: Human-readable failure description.
            operation: Transport operation active during the failure.
        """
        super().__init__(
            reason=reason,
            message=message,
            operation=operation,
        )

class ApiErrorReason(StrEnum):
    """Categorize failures produced by the shared API client."""

    CONF = "conf"
    CONNECTION = "connection"
    RATE_LIMIT = "rate_limit"
    NEGOTIATION = "negotiation"
