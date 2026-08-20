from __future__ import annotations

from enum import StrEnum

from scam_sniffer.domain.errors import ScamError

class RepoError(ScamError):
    def __init__(
        self,
        reason: RepoErrorReason,
        message: str,
        operation: str,
        root_cause: Exception,
    ) -> None:
        super().__init__(
            reason=reason,
            message=message,
            operation=operation,
            root_cause=root_cause,
        )

class RepoErrorReason(StrEnum):
    REMOTE = "remote"
    MAPPING = "mapping"
    STORAGE = "storage"
