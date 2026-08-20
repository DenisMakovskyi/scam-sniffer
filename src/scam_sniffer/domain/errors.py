from __future__ import annotations

from enum import StrEnum

class ScamError(RuntimeError):
    def __init__(
        self,
        reason: StrEnum,
        message: str,
        operation: str,
        root_cause: Exception | None = None,
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.operation = operation
        self.root_cause = root_cause

class ScamErrorReason(StrEnum):
    INVALID_CANDLE = "invalid_candle"
