import asyncio
from typing import Any

import pytest

from scam_sniffer.app import Application
from scam_sniffer.config import AppConfig
from scam_sniffer.domain.models import Market
from scam_sniffer.errors import AppError, AppErrorReason
from scam_sniffer.data.database.engine import DatabaseConfig

class ApplicationHarness:
    def __init__(
        self,
        fail_migration: bool = False,
        fail_use_case: bool = False,
        fail_task_queue_shutdown: bool = False,
    ) -> None:
        self.calls: list[str] = []
        self.market: Market | None = None
        self.symbols: tuple[str, ...] = ()
        self.use_case_started = asyncio.Event()
        self.fail_migration = fail_migration
        self.fail_use_case = fail_use_case
        self.fail_task_queue_shutdown = fail_task_queue_shutdown

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        harness = self

        class FakeEventBus:
            def __init__(self) -> None:
                harness.calls.append("event_bus:init")

        class FakeDatabaseEngine:
            def __init__(self, config: object) -> None:
                self.pool = object()
                harness.calls.append("database:init")

            async def connect(self) -> None:
                harness.calls.append("database:connect")

            async def migrate(self) -> None:
                harness.calls.append("database:migrate")
                if harness.fail_migration:
                    raise RuntimeError("Migration failed")

            async def close(self) -> None:
                harness.calls.append("database:close")

        class FakeTaskQueue:
            @classmethod
            def __class_getitem__(cls, _: object) -> type[Any]:
                return cls

            def __init__(self, config: object) -> None:
                harness.calls.append("task_queue:init")

            def start(self) -> bool:
                harness.calls.append("task_queue:start")
                return True

            async def shutdown_gracefully(self) -> None:
                harness.calls.append("task_queue:shutdown")
                if harness.fail_task_queue_shutdown:
                    raise RuntimeError("Task queue shutdown failed")

        class FakeStock:
            def __init__(self, config: object) -> None:
                harness.calls.append("stock:init")

            async def close(self) -> None:
                harness.calls.append("stock:close")

        class FakeCandleDao:
            def __init__(self, pool: object) -> None:
                harness.calls.append("dao:init")

        class FakeRepository:
            def __init__(self, dao: object, stock: object) -> None:
                harness.calls.append("repository:init")

        class FakeManager:
            def __init__(
                self,
                config: object,
                publisher: object,
                repository: object,
            ) -> None:
                harness.calls.append("manager:init")

        class FakeUseCase:
            def __init__(
                self,
                market: Market,
                manager: object,
                symbols: tuple[str, ...],
            ) -> None:
                harness.market = market
                harness.symbols = symbols
                harness.calls.append("use_case:init")

            async def execute(self) -> None:
                harness.calls.append("use_case:execute")
                harness.use_case_started.set()
                try:
                    if harness.fail_use_case:
                        raise RuntimeError("Use case failed")
                    await asyncio.Event().wait()
                finally:
                    harness.calls.append("use_case:finish")

        monkeypatch.setattr("scam_sniffer.app.MemoEventBus", FakeEventBus)
        monkeypatch.setattr("scam_sniffer.app.DatabaseEngine", FakeDatabaseEngine)
        monkeypatch.setattr("scam_sniffer.app.AsyncTaskQueue", FakeTaskQueue)
        monkeypatch.setattr("scam_sniffer.app.BinanceStock", FakeStock)
        monkeypatch.setattr("scam_sniffer.app.CandleDao", FakeCandleDao)
        monkeypatch.setattr("scam_sniffer.app.CandleManager", FakeManager)
        monkeypatch.setattr("scam_sniffer.app.CandleRepositoryImpl", FakeRepository)
        monkeypatch.setattr("scam_sniffer.app.CandleUseCase", FakeUseCase)

@pytest.mark.asyncio
async def test_application_runs_use_cases_and_closes_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = ApplicationHarness()
    harness.install(monkeypatch)
    stop_event = asyncio.Event()
    application = Application(config=_app_config())

    async def stop_application() -> None:
        await harness.use_case_started.wait()
        stop_event.set()

    stop_async_task = asyncio.create_task(stop_application())
    await application.run(stop_event=stop_event)
    await stop_async_task

    assert harness.market is Market.BINANCE
    assert harness.symbols == ("BTCUSDT",)
    assert harness.calls[:11] == [
        "event_bus:init",
        "stock:init",
        "database:init",
        "task_queue:init",
        "database:connect",
        "database:migrate",
        "dao:init",
        "repository:init",
        "manager:init",
        "use_case:init",
        "task_queue:start",
    ]
    assert harness.calls[-4:] == [
        "use_case:finish",
        "task_queue:shutdown",
        "stock:close",
        "database:close",
    ]

@pytest.mark.asyncio
async def test_application_start_is_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = ApplicationHarness()
    harness.install(monkeypatch)
    application = Application(config=_app_config())

    assert await application.start()
    call_count = len(harness.calls)
    assert not await application.start()
    assert len(harness.calls) == call_count

    await application.shutdown()

@pytest.mark.asyncio
async def test_application_rejects_restart_after_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = ApplicationHarness()
    harness.install(monkeypatch)
    application = Application(config=_app_config())

    assert await application.start()
    await application.shutdown()

    with pytest.raises(AppError) as error_info:
        await application.start()

    error = error_info.value
    assert error.reason is AppErrorReason.LIFECYCLE
    assert error.operation == "start"

@pytest.mark.asyncio
async def test_application_closes_resources_after_startup_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = ApplicationHarness(fail_migration=True)
    harness.install(monkeypatch)
    application = Application(config=_app_config())

    with pytest.raises(RuntimeError, match="Migration failed"):
        await application.run(stop_event=asyncio.Event())

    assert harness.calls[-3:] == [
        "task_queue:shutdown",
        "stock:close",
        "database:close",
    ]

@pytest.mark.asyncio
async def test_application_wraps_unexpected_use_case_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = ApplicationHarness(fail_use_case=True)
    harness.install(monkeypatch)
    application = Application(config=_app_config())

    with pytest.raises(AppError) as error_info:
        await application.run(stop_event=asyncio.Event())

    error = error_info.value
    assert error.reason is AppErrorReason.LIFECYCLE
    assert isinstance(error.root_cause, RuntimeError)
    assert harness.calls[-3:] == [
        "task_queue:shutdown",
        "stock:close",
        "database:close",
    ]

@pytest.mark.asyncio
async def test_application_continues_shutdown_after_resource_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = ApplicationHarness(fail_task_queue_shutdown=True)
    harness.install(monkeypatch)
    stop_event = asyncio.Event()
    application = Application(config=_app_config())

    async def stop_application() -> None:
        await harness.use_case_started.wait()
        stop_event.set()

    stop_async_task = asyncio.create_task(stop_application())
    with pytest.raises(RuntimeError, match="Task queue shutdown failed"):
        await application.run(stop_event=stop_event)
    await stop_async_task

    assert harness.calls[-3:] == [
        "task_queue:shutdown",
        "stock:close",
        "database:close",
    ]

def _app_config() -> AppConfig:
    return AppConfig(
        database_config=DatabaseConfig(
            dsn="postgresql://scam_sniffer:scam_sniffer@localhost/scam_sniffer",
        ),
    )
