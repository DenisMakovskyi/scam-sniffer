import asyncio
from typing import cast
from collections.abc import Hashable

import pytest

from scam_sniffer.domain.tasks.config import TaskQueueConfig
from scam_sniffer.domain.errors import TaskQueueError, TaskQueueErrorReason
from scam_sniffer.domain.tasks.queue import MemoTaskQueue

class FakeQueueTask:
    def __init__(
        self,
        key: str,
        calls: list[str],
        error: Exception | None = None,
        start_event: asyncio.Event | None = None,
        release_event: asyncio.Event | None = None,
    ) -> None:
        self._key = key
        self._error = error
        self._calls = calls
        self._start_event = start_event
        self._release_event = release_event

    @property
    def key(self) -> str:
        return self._key

    async def execute(self) -> None:
        self._calls.append(self._key)
        if self._start_event is not None:
            self._start_event.set()
        if self._release_event is not None:
            await self._release_event.wait()
        if self._error is not None:
            raise self._error

class InvalidQueueTask:
    @property
    def key(self) -> Hashable:
        return cast(Hashable, ["invalid"])

    async def execute(self) -> None:
        raise AssertionError("Invalid task must not be executed")

def test_task_queue_config_rejects_invalid_values() -> None:
    with pytest.raises(TaskQueueError):
        TaskQueueConfig(queue_size=0)
    with pytest.raises(TaskQueueError):
        TaskQueueConfig(worker_count=0)
    with pytest.raises(TaskQueueError):
        TaskQueueConfig(dedupe_cache_size=-1)

def test_task_queue_start_requires_running_event_loop() -> None:
    task_queue = MemoTaskQueue[str](config=TaskQueueConfig())

    with pytest.raises(TaskQueueError) as error_info:
        task_queue.start()

    error = error_info.value
    assert error.reason is TaskQueueErrorReason.LIFECYCLE
    assert error.operation == "start"
    assert isinstance(error.root_cause, RuntimeError)

@pytest.mark.asyncio
async def test_task_queue_starts_once_and_rejects_stopped_submission() -> None:
    calls: list[str] = []
    task_queue = MemoTaskQueue[str](config=TaskQueueConfig())

    with pytest.raises(TaskQueueError) as error_info:
        task_queue.submit(FakeQueueTask(key="first", calls=calls))

    assert error_info.value.reason is TaskQueueErrorReason.LIFECYCLE
    assert task_queue.start()
    assert not task_queue.start()

    await task_queue.shutdown_gracefully()

    with pytest.raises(TaskQueueError) as error_info:
        task_queue.submit(FakeQueueTask(key="second", calls=calls))

    assert error_info.value.reason is TaskQueueErrorReason.LIFECYCLE

@pytest.mark.asyncio
async def test_task_queue_rejects_start_and_submission_during_shutdown() -> None:
    calls: list[str] = []
    start_event = asyncio.Event()
    release_event = asyncio.Event()
    task_queue = MemoTaskQueue[str](config=TaskQueueConfig())
    task_queue.start()

    task_queue.submit(
        FakeQueueTask(
            key="running",
            calls=calls,
            start_event=start_event,
            release_event=release_event,
        ),
    )
    await asyncio.wait_for(start_event.wait(), timeout=1)
    shutdown_task = asyncio.create_task(task_queue.shutdown_gracefully())
    await asyncio.sleep(0)

    with pytest.raises(TaskQueueError) as start_error_info:
        task_queue.start()
    with pytest.raises(TaskQueueError) as submit_error_info:
        task_queue.submit(FakeQueueTask(key="rejected", calls=calls))

    assert start_error_info.value.reason is TaskQueueErrorReason.LIFECYCLE
    assert submit_error_info.value.reason is TaskQueueErrorReason.LIFECYCLE

    release_event.set()
    await shutdown_task

    assert calls == ["running"]

@pytest.mark.asyncio
async def test_task_queue_deduplicates_active_and_completed_tasks() -> None:
    calls: list[str] = []
    start_event = asyncio.Event()
    release_event = asyncio.Event()
    task_queue = MemoTaskQueue[str](config=TaskQueueConfig())
    task_queue.start()

    first_task = FakeQueueTask(
        key="first",
        calls=calls,
        start_event=start_event,
        release_event=release_event,
    )
    duplicate_task = FakeQueueTask(key="first", calls=calls)

    assert task_queue.submit(first_task)
    assert not task_queue.submit(duplicate_task)
    await asyncio.wait_for(start_event.wait(), timeout=1)
    assert not task_queue.submit(duplicate_task)

    release_event.set()
    await task_queue.wait_until_idle()

    assert not task_queue.submit(duplicate_task)
    assert calls == ["first"]

    await task_queue.shutdown_gracefully()

@pytest.mark.asyncio
async def test_task_queue_preserves_order_with_one_worker() -> None:
    calls: list[str] = []
    task_queue = MemoTaskQueue[str](config=TaskQueueConfig(worker_count=1))
    task_queue.start()

    assert task_queue.submit(FakeQueueTask(key="first", calls=calls))
    assert task_queue.submit(FakeQueueTask(key="second", calls=calls))
    assert task_queue.submit(FakeQueueTask(key="third", calls=calls))

    await task_queue.wait_until_idle()

    assert calls == ["first", "second", "third"]

    await task_queue.shutdown_gracefully()

@pytest.mark.asyncio
async def test_task_queue_executes_tasks_with_configured_workers() -> None:
    calls: list[str] = []
    first_start_event = asyncio.Event()
    second_start_event = asyncio.Event()
    release_event = asyncio.Event()
    task_queue = MemoTaskQueue[str](config=TaskQueueConfig(worker_count=2))
    task_queue.start()

    task_queue.submit(
        FakeQueueTask(
            key="first",
            calls=calls,
            start_event=first_start_event,
            release_event=release_event,
        ),
    )
    task_queue.submit(
        FakeQueueTask(
            key="second",
            calls=calls,
            start_event=second_start_event,
            release_event=release_event,
        ),
    )

    await asyncio.wait_for(first_start_event.wait(), timeout=1)
    await asyncio.wait_for(second_start_event.wait(), timeout=1)

    release_event.set()
    await task_queue.wait_until_idle()

    assert set(calls) == {"first", "second"}

    await task_queue.shutdown_gracefully()

@pytest.mark.asyncio
async def test_task_queue_releases_evicted_completed_key() -> None:
    calls: list[str] = []
    task_queue = MemoTaskQueue[str](
        config=TaskQueueConfig(dedupe_cache_size=1),
    )
    task_queue.start()

    assert task_queue.submit(FakeQueueTask(key="first", calls=calls))
    await task_queue.wait_until_idle()
    assert task_queue.submit(FakeQueueTask(key="second", calls=calls))
    await task_queue.wait_until_idle()
    assert task_queue.submit(FakeQueueTask(key="first", calls=calls))
    await task_queue.wait_until_idle()

    assert calls == ["first", "second", "first"]

    await task_queue.shutdown_gracefully()

@pytest.mark.asyncio
async def test_task_queue_releases_failed_key_and_keeps_worker_alive() -> None:
    calls: list[str] = []
    task_error = RuntimeError("Task failed")
    task_queue = MemoTaskQueue[str](config=TaskQueueConfig())
    task_queue.start()

    assert task_queue.submit(
        FakeQueueTask(key="failed", calls=calls, error=task_error),
    )
    assert task_queue.submit(FakeQueueTask(key="completed", calls=calls))

    with pytest.raises(TaskQueueError) as error_info:
        await task_queue.wait_until_idle()

    error = error_info.value
    assert error.reason is TaskQueueErrorReason.EXECUTION
    assert error.operation == "wait_until_idle"
    assert error.root_cause is task_error
    assert error.__cause__ is task_error

    assert task_queue.submit(FakeQueueTask(key="failed", calls=calls))
    await task_queue.wait_until_idle()

    assert calls == ["failed", "completed", "failed"]

    await task_queue.shutdown_gracefully()

@pytest.mark.asyncio
async def test_task_queue_shutdown_reports_execution_failure_and_stops_workers() -> None:
    calls: list[str] = []
    task_error = RuntimeError("Task failed")
    task_queue = MemoTaskQueue[str](config=TaskQueueConfig())
    task_queue.start()
    task_queue.submit(FakeQueueTask(key="failed", calls=calls, error=task_error))

    with pytest.raises(TaskQueueError) as error_info:
        await task_queue.shutdown_gracefully()

    error = error_info.value
    assert error.reason is TaskQueueErrorReason.EXECUTION
    assert error.operation == "shutdown_gracefully"
    assert error.root_cause is task_error
    assert task_queue.start()

    await task_queue.shutdown_gracefully()

@pytest.mark.asyncio
async def test_task_queue_rejects_submission_when_capacity_is_reached() -> None:
    calls: list[str] = []
    start_event = asyncio.Event()
    release_event = asyncio.Event()
    task_queue = MemoTaskQueue[str](config=TaskQueueConfig(queue_size=1))
    task_queue.start()

    assert task_queue.submit(
        FakeQueueTask(
            key="running",
            calls=calls,
            start_event=start_event,
            release_event=release_event,
        ),
    )
    await asyncio.wait_for(start_event.wait(), timeout=1)
    assert task_queue.submit(FakeQueueTask(key="queued", calls=calls))

    with pytest.raises(TaskQueueError) as error_info:
        task_queue.submit(FakeQueueTask(key="rejected", calls=calls))

    assert error_info.value.reason is TaskQueueErrorReason.FULL
    release_event.set()
    await task_queue.wait_until_idle()

    assert calls == ["running", "queued"]

    await task_queue.shutdown_gracefully()

@pytest.mark.asyncio
async def test_task_queue_rejects_unhashable_task_key() -> None:
    task_queue = MemoTaskQueue[Hashable](config=TaskQueueConfig())
    task_queue.start()

    with pytest.raises(TaskQueueError) as error_info:
        task_queue.submit(InvalidQueueTask())

    error = error_info.value
    assert error.reason is TaskQueueErrorReason.PARAMS
    assert isinstance(error.root_cause, TypeError)

    await task_queue.shutdown_gracefully()
