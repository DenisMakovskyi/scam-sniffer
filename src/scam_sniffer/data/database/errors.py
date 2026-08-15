from __future__ import annotations

from enum import StrEnum

class DatabaseError(RuntimeError):
    def __init__(
        self,
        reason: DatabaseErrorReason,
        message: str,
        operation: str,
        root_cause: Exception | None = None,
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.operation = operation
        self.root_cause = root_cause

class DatabaseErrorReason(StrEnum):
    QUERY = "query"
    MIGRATION = "migration"
    CONNECTION = "connection"
    NOT_CONNECTED = "not_connected"
    INVALID_CONFIG = "invalid_config"
