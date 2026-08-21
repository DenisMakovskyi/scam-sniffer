"""Failures raised while coordinating application workflows."""

from __future__ import annotations

from enum import StrEnum

from scam_sniffer.errors import ScamError

class ManagerError(ScamError):
    """Represent a manager configuration or workflow failure."""

    def __init__(
        self,
        reason: ManagerErrorReason,
        message: str,
        operation: str,
        root_cause: Exception | None = None,
    ) -> None:
        """Initialize an application manager failure.

        Args:
            reason: Manager failure category.
            message: Human-readable failure description.
            operation: Manager operation active during the failure.
            root_cause: Lower-level domain failure, if available.
        """
        super().__init__(
            reason=reason,
            message=message,
            operation=operation,
            root_cause=root_cause,
        )

class ManagerErrorReason(StrEnum):
    """Categorize failures produced by application managers."""

    REPO = "repo"
    PARAMS = "params"
    LIFECYCLE = "lifecycle"
    CONTINUITY = "continuity"
