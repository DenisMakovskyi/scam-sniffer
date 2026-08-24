"""Bounded in-memory task queue with deduplication."""

from __future__ import annotations

from enum import StrEnum
from typing import override
from collections.abc import Hashable

import asyncio

from scam_sniffer.core.tasks.proto import QueueTask, TaskQueue
from scam_sniffer.core.tasks.config import AsyncTaskQueueConfig
from scam_sniffer.core.tasks.errors import TaskQueueError, TaskQueueErrorReason

class AsyncTaskQueue[TKey: Hashable](TaskQueue[TKey]):
    """Execute bounded asynchronous tasks while suppressing duplicate keys."""

    def __init__(self, config: AsyncTaskQueueConfig) -> None:
        """Initialize a stopped queue with empty deduplication state.

        Args:
            config: Queue capacity, worker count, and completed-key retention.
        """
        self._state = _TaskQueueState.STOPPED
        self._queue = asyncio.Queue[QueueTask[TKey]](maxsize=config.queue_size)
        self._config = config
        self._active_keys: set[TKey] = set()
        self._worker_tasks: list[asyncio.Task[None]] = []
        self._completed_keys: dict[TKey, None] = {}
        self._execution_errors: list[Exception] = []

    @override
    def start(self) -> bool:
        """Start configured workers in the active event loop.

        Returns:
            Whether workers were started. Repeated starts return false.

        Raises:
            TaskQueueError: If no event loop is running.
        """
        if self._state is _TaskQueueState.RUNNING:
            return False
        if self._state is _TaskQueueState.STOPPING:
            raise TaskQueueError(
                reason=TaskQueueErrorReason.LIFECYCLE,
                message="Task queue cannot start during shutdown",
                operation="start",
            )
        try:
            event_loop = asyncio.get_running_loop()
        except RuntimeError as error:
            raise TaskQueueError(
                reason=TaskQueueErrorReason.LIFECYCLE,
                message="Task queue requires a running event loop",
                operation="start",
                root_cause=error,
            ) from error

        self._worker_tasks = [
            event_loop.create_task(
                coro=self.__worker_loop(),
                name=f"task_queue_worker:{worker_index}",
            )
            for worker_index in range(self._config.worker_count)
        ]
        self._state = _TaskQueueState.RUNNING
        return True

    @override
    def submit(self, task: QueueTask[TKey]) -> bool:
        """Submit a task unless its key is active or recently completed.

        Args:
            task: Typed asynchronous task to schedule.

        Returns:
            Whether the task was accepted. Duplicate keys return false.

        Raises:
            TaskQueueError: If workers are stopped, the key is invalid, or the queue is full.
        """
        if self._state is not _TaskQueueState.RUNNING:
            raise TaskQueueError(
                reason=TaskQueueErrorReason.LIFECYCLE,
                message="Task queue must be started before submission",
                operation="submit",
            )

        try:
            task_key = task.key
            is_duplicate = task_key in self._active_keys or task_key in self._completed_keys
        except (AttributeError, TypeError) as error:
            raise TaskQueueError(
                reason=TaskQueueErrorReason.PARAMS,
                message="Task queue key must be hashable",
                operation="submit",
                root_cause=error,
            ) from error
        if is_duplicate:
            return False

        try:
            self._queue.put_nowait(task)
        except asyncio.QueueFull as error:
            raise TaskQueueError(
                reason=TaskQueueErrorReason.CAPACITY,
                message="Task queue capacity has been reached",
                operation="submit",
                root_cause=error,
            ) from error
        self._active_keys.add(task_key)
        return True

    @override
    async def wait_until_idle(self) -> None:
        """Wait until submitted tasks finish and report execution failures.

        Raises:
            TaskQueueError: If one or more tasks failed.
        """
        if self._state is _TaskQueueState.STOPPED:
            raise TaskQueueError(
                reason=TaskQueueErrorReason.LIFECYCLE,
                message="Task queue must be running while waiting for idle",
                operation="wait_until_idle",
            )
        await self._queue.join()
        self.__raise_execution_error(operation="wait_until_idle")

    @override
    async def shutdown_gracefully(self) -> None:
        """Finish submitted tasks, stop workers, and report failures.

        Raises:
            TaskQueueError: If one or more tasks failed before shutdown.
        """
        if self._state is _TaskQueueState.STOPPED:
            return
        if self._state is _TaskQueueState.STOPPING:
            raise TaskQueueError(
                reason=TaskQueueErrorReason.LIFECYCLE,
                message="Task queue shutdown is already in progress",
                operation="shutdown_gracefully",
            )

        self._state = _TaskQueueState.STOPPING
        worker_tasks = self._worker_tasks
        self._worker_tasks = []

        execution_error: TaskQueueError | None = None
        try:
            await self._queue.join()
            self.__raise_execution_error(operation="shutdown_gracefully")
        except TaskQueueError as error:
            execution_error = error
        finally:
            for worker_task in worker_tasks:
                worker_task.cancel()
            await asyncio.gather(*worker_tasks, return_exceptions=True)
            self._state = _TaskQueueState.STOPPED

        if execution_error is not None:
            raise execution_error

    async def __worker_loop(self) -> None:
        """Execute queued tasks until the worker is cancelled."""
        while True:
            task = await self._queue.get()
            task_key = task.key
            try:
                await task.execute()
            except asyncio.CancelledError:
                self._active_keys.discard(task_key)
                raise
            except Exception as error:
                self._active_keys.discard(task_key)
                self._execution_errors.append(error)
            else:
                self._active_keys.discard(task_key)
                self.__cache_completed_key(task_key)
            finally:
                self._queue.task_done()

    def __cache_completed_key(self, task_key: TKey) -> None:
        """Retain a completed key inside the configured bounded cache.

        Args:
            task_key: Successfully executed task identity.
        """
        if not self._config.dedupe_cache_size:
            return
        self._completed_keys[task_key] = None
        while len(self._completed_keys) > self._config.dedupe_cache_size:
            oldest_key = next(iter(self._completed_keys))
            self._completed_keys.pop(oldest_key)

    def __raise_execution_error(self, operation: str) -> None:
        """Raise and clear errors collected by task workers.

        Args:
            operation: Public queue operation reporting the failures.

        Raises:
            TaskQueueError: If one or more task executions failed.
        """
        if not self._execution_errors:
            return

        execution_errors = self._execution_errors
        self._execution_errors = []
        root_cause: Exception = (
            execution_errors[0]
            if len(execution_errors) == 1
            else ExceptionGroup("Multiple task executions failed", execution_errors)
        )
        raise TaskQueueError(
            reason=TaskQueueErrorReason.EXECUTION,
            message="Task queue execution failed",
            operation=operation,
            root_cause=root_cause,
        ) from root_cause

class _TaskQueueState(StrEnum):
    """Represent the internal lifecycle of an in-memory task queue."""

    RUNNING = "running"
    STOPPED = "stopped"
    STOPPING = "stopping"
