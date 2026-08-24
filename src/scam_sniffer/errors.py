"""Root error type shared by every application layer."""

from __future__ import annotations

from enum import StrEnum

class AppError(RuntimeError):
    """Represent a categorized application failure with its root cause."""

    def __init__(
        self,
        reason: StrEnum,
        message: str,
        operation: str,
        root_cause: Exception | None = None,
    ) -> None:
        """Initialize an application failure.

        Args:
            reason: Layer-specific category describing the failure.
            message: Human-readable failure description.
            operation: Operation active when the failure occurred.
            root_cause: Lower-level exception that caused the failure, if available.
        """
        super().__init__(message)
        self.reason = reason
        self.operation = operation
        self.root_cause = root_cause

class AppErrorReason(StrEnum):
    """Categorize failures produced by the application runner."""

    LIFECYCLE = "lifecycle"
