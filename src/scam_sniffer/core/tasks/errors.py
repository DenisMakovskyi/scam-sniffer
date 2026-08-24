"""Failures exposed by core asynchronous task queues."""

from __future__ import annotations

from enum import StrEnum

from scam_sniffer.errors import AppError

class TaskQueueError(AppError):
    """Represent a task queue configuration or lifecycle failure."""

    def __init__(
        self,
        reason: TaskQueueErrorReason,
        message: str,
        operation: str,
        root_cause: Exception | None = None,
    ) -> None:
        """Initialize a task queue failure.

        Args:
            reason: Task queue failure category.
            message: Human-readable failure description.
            operation: Queue operation active during the failure.
            root_cause: Lower-level task failure, if available.
        """
        super().__init__(
            reason=reason,
            message=message,
            operation=operation,
            root_cause=root_cause,
        )

class TaskQueueErrorReason(StrEnum):
    """Categorize failures produced by asynchronous task queues."""

    CONF = "conf"
    PARAMS = "params"
    CAPACITY = "capacity"
    EXECUTION = "execution"
    LIFECYCLE = "lifecycle"
