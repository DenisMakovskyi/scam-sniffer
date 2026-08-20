"""Failures raised by database infrastructure."""

from __future__ import annotations

from enum import StrEnum

from scam_sniffer.errors import ScamError

class DatabaseError(ScamError):
    """Represent a database configuration or operation failure."""

    def __init__(
        self,
        reason: DatabaseErrorReason,
        message: str,
        operation: str,
        root_cause: Exception | None = None,
    ) -> None:
        """Initialize a database failure.

        Args:
            reason: Database failure category.
            message: Human-readable failure description.
            operation: Database operation active during the failure.
            root_cause: Lower-level database exception, if available.
        """
        super().__init__(
            reason=reason,
            message=message,
            operation=operation,
            root_cause=root_cause,
        )

class DatabaseErrorReason(StrEnum):
    """Categorize failures produced by database infrastructure."""

    CONF = "conf"
    QUERY = "query"
    MIGRATION = "migration"
    CONNECTION = "connection"
