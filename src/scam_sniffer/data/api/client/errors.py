from __future__ import annotations

from enum import StrEnum

from scam_sniffer.domain.errors import ScamError

class ApiError(ScamError):
    def __init__(
        self,
        reason: ApiErrorReason,
        message: str,
        operation: str,
    ) -> None:
        super().__init__(
            reason=reason,
            message=message,
            operation=operation,
        )

class ApiErrorReason(StrEnum):
    CONNECTION = "connection"
    RATE_LIMIT = "rate_limit"
    INVALID_CONFIG = "invalid_config"
    INVALID_RESPONSE = "invalid_response"
