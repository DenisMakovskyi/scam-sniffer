from __future__ import annotations

from enum import StrEnum

from scam_sniffer.domain.errors import ScamError

class DatabaseError(ScamError):
    def __init__(
        self,
        reason: DatabaseErrorReason,
        message: str,
        operation: str,
        root_cause: Exception | None = None,
    ) -> None:
        super().__init__(
            reason=reason,
            message=message,
            operation=operation,
            root_cause=root_cause,
        )

class DatabaseErrorReason(StrEnum):
    QUERY = "query"
    MIGRATION = "migration"
    CONNECTION = "connection"
    NOT_CONNECTED = "not_connected"
    INVALID_CONFIG = "invalid_config"
