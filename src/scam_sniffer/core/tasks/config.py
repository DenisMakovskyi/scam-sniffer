"""Configuration models for asynchronous task queues."""

from dataclasses import dataclass

from scam_sniffer.core.tasks.errors import TaskQueueError, TaskQueueErrorReason

@dataclass(frozen=True, slots=True)
class AsyncTaskQueueConfig:
    """Configure queue capacity, workers, and completed-key retention.

    Attributes:
        queue_size: Maximum number of tasks waiting for execution.
        worker_count: Number of concurrent in-memory task workers.
        dedupe_cache_size: Maximum number of completed keys retained for deduplication.
    """

    queue_size: int = 1_000
    worker_count: int = 1
    dedupe_cache_size: int = 10_000

    def __post_init__(self) -> None:
        """Validate queue limits.

        Raises:
            TaskQueueError: If a queue limit is invalid.
        """
        if self.queue_size < 1:
            raise TaskQueueError(
                reason=TaskQueueErrorReason.CONF,
                message="Task queue size must be positive",
                operation="init",
            )
        if self.worker_count < 1:
            raise TaskQueueError(
                reason=TaskQueueErrorReason.CONF,
                message="Task queue worker count must be positive",
                operation="init",
            )
        if self.dedupe_cache_size < 0:
            raise TaskQueueError(
                reason=TaskQueueErrorReason.CONF,
                message="Task queue dedupe cache size cannot be negative",
                operation="init",
            )
