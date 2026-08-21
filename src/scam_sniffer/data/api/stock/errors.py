"""Failures raised by exchange-specific market-data sources."""

from __future__ import annotations

from enum import StrEnum

from scam_sniffer.errors import ScamError

class StockError(ScamError):
    """Represent an exchange integration failure."""

    def __init__(
        self,
        reason: StockErrorReason,
        message: str,
        operation: str,
    ) -> None:
        """Initialize an exchange integration failure.

        Args:
            reason: Exchange failure category.
            message: Human-readable failure description.
            operation: Exchange operation active during the failure.
        """
        super().__init__(
            reason=reason,
            message=message,
            operation=operation,
        )

class StockErrorReason(StrEnum):
    """Categorize failures produced by an exchange integration."""

    API_ERROR = "api_error"
    INVALID_LIMIT = "invalid_limit"
    INVALID_RANGE = "invalid_range"
