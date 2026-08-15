from __future__ import annotations

from enum import StrEnum

class ApiError(RuntimeError):
    def __init__(
        self,
        reason: ApiErrorReason,
        message: str,
        operation: str,
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.operation = operation

class ApiErrorReason(StrEnum):
    CONNECTION = "connection"
    RATE_LIMIT = "rate_limit"
    INVALID_CONFIG = "invalid_config"
    INVALID_RESPONSE = "invalid_response"
