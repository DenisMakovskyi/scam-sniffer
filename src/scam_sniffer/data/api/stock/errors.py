from __future__ import annotations

from enum import StrEnum

class StockError(RuntimeError):
    def __init__(
        self,
        reason: StockErrorReason,
        message: str,
        operation: str,
        root_cause: Exception | None = None,
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.operation = operation
        self.root_cause = root_cause

class StockErrorReason(StrEnum):
    API_ERROR = "api_error"
    INVALID_LIMIT = "invalid_limit"
    INVALID_RANGE = "invalid_range"
    INVALID_CANDLE = "invalid_candle"
