"""Typed contracts for asynchronous task execution."""

from typing import Protocol
from collections.abc import Hashable

class QueueTask[TKey: Hashable](Protocol):
    """Expose a stable deduplication key and asynchronous task body."""

    @property
    def key(self) -> TKey:
        """Return the stable key identifying equivalent task executions."""
        ...

    async def execute(self) -> None:
        """Execute the task body.

        Raises:
            Exception: If task execution fails.
        """
        ...

class TaskQueue[TKey: Hashable](Protocol):
    """Schedule typed tasks for bounded asynchronous execution."""

    def start(self) -> bool:
        """Start queue workers.

        Returns:
            Whether workers were started. Repeated starts return false.

        Raises:
            TaskQueueError: If no event loop is running.
        """
        ...

    def submit(self, task: QueueTask[TKey]) -> bool:
        """Submit a task unless its key is already known.

        Args:
            task: Typed asynchronous task to schedule.

        Returns:
            Whether the task was accepted. Duplicate keys return false.

        Raises:
            TaskQueueError: If workers are stopped, the key is invalid, or the queue is full.
        """
        ...

    async def wait_until_idle(self) -> None:
        """Wait until submitted tasks finish and report execution failures.

        Raises:
            TaskQueueError: If one or more tasks failed.
        """
        ...

    async def shutdown_gracefully(self) -> None:
        """Finish submitted tasks and stop every worker.

        Raises:
            TaskQueueError: If one or more tasks failed before shutdown.
        """
        ...
