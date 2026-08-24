"""Application dependency assembly and lifecycle."""

from collections.abc import Hashable

import signal
import asyncio

from scam_sniffer.errors import AppError, AppErrorReason
from scam_sniffer.config import AppConfig

from scam_sniffer.core.tasks.proto import TaskQueue
from scam_sniffer.core.tasks.queue import AsyncTaskQueue
from scam_sniffer.core.events.bus import MemoEventBus

from scam_sniffer.data.api.stock.base import AbsStock
from scam_sniffer.data.api.stock.binance import BinanceStock
from scam_sniffer.data.database.engine import DatabaseEngine
from scam_sniffer.data.database.dao.candle import CandleDao
from scam_sniffer.data.repository.candle import CandleRepositoryImpl

from scam_sniffer.domain.models import Market
from scam_sniffer.domain.usecase.proto import UseCase
from scam_sniffer.domain.usecase.candle import CandleUseCase
from scam_sniffer.domain.manager.candle import CandleManager

class Application:
    """Own application dependencies and coordinate their lifecycle."""

    def __init__(self, config: AppConfig) -> None:
        """Initialize disconnected application resources.

        Args:
            config: Validated configuration for every application subsystem.
        """
        self._started = False
        self._shutdown = False

        self._config = config
        self._event_bus = MemoEventBus()
        self._stock_abs = self.__build_stock(market=config.market)
        self._database_engine = DatabaseEngine(config=config.database_config)

        self._task_queue: TaskQueue[Hashable] = AsyncTaskQueue[Hashable](
            config=config.async_task_queue_config,
        )

        self._use_cases: list[UseCase] = []
        self._use_case_tasks: list[asyncio.Task[None]] = []
        self._use_case_observed_tasks: set[asyncio.Task[None]] = set()

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        """Start the application and wait for an external shutdown request.

        Args:
            stop_event: Optional externally managed event used mainly for controlled runs.

        Raises:
            Exception: If startup, use case execution, or shutdown fails.
        """
        try:
            await self.start()
            await self.__wait_for_shutdown(stop_event)
        finally:
            await self.shutdown()

    async def start(self) -> bool:
        """Assemble dependencies and start every application use case.

        Returns:
            Whether the application was started. Repeated starts return false.

        Raises:
            AppError: If the application was already shut down.
            Exception: If a dependency or use case cannot start.
        """
        if self._started:
            return False
        if self._shutdown:
            raise AppError(
                reason=AppErrorReason.LIFECYCLE,
                message="Application cannot restart after shutdown",
                operation="start",
            )

        await self._database_engine.connect()
        await self._database_engine.migrate()

        self._use_cases = self.__build_use_cases()
        self._task_queue.start()
        self.__execute_use_cases()

        self._started = True
        return True

    async def shutdown(self) -> None:
        """Stop live work and close every initialized resource in reverse order.

        Raises:
            Exception: If one resource fails to stop. Remaining resources are still closed.
            ExceptionGroup: If multiple resources fail to stop.
        """
        if self._shutdown:
            return

        errors: list[Exception] = []
        use_case_tasks = self._use_case_tasks
        use_case_observed_tasks = self._use_case_observed_tasks

        self._started = False
        self._shutdown = True

        self._use_cases = []
        self._use_case_tasks = []
        self._use_case_observed_tasks = set()

        for use_case_task in use_case_tasks:
            use_case_task.cancel()
        task_results = await asyncio.gather(*use_case_tasks, return_exceptions=True)
        for use_case_task, task_result in zip(use_case_tasks, task_results, strict=True):
            if use_case_task in use_case_observed_tasks:
                continue
            if isinstance(task_result, Exception):
                errors.append(task_result)

        try:
            await self._task_queue.shutdown_gracefully()
        except Exception as error:
            errors.append(error)

        try:
            await self._stock_abs.close()
        except Exception as error:
            errors.append(error)

        try:
            await self._database_engine.close()
        except Exception as error:
            errors.append(error)

        if len(errors) == 1:
            raise errors[0]
        if errors:
            raise ExceptionGroup("Application shutdown failed", errors)

    def __build_stock(self, market: Market) -> AbsStock:
        """Build the configured market-data source implementation.

        Args:
            market: Market-data provider selected by application configuration.

        Returns:
            Exchange-specific market-data source behind the shared abstraction.

        Raises:
            AppError: If the configured market has no source implementation.
        """
        if market is Market.BINANCE:
            return BinanceStock(config=self._config.api_config)

    # noinspection bad-argument-type
    def __build_use_cases(self) -> list[UseCase]:
        """Assemble application use cases and their dependency graphs.

        Returns:
            Use cases ready for asynchronous execution.
        """
        candle_db_dao = CandleDao(pool=self._database_engine.pool)
        candle_repository = CandleRepositoryImpl(
            dao=candle_db_dao,
            stock=self._stock_abs,
        )
        candle_workflow_manager = CandleManager(
            config=self._config.candle_manager_config,
            publisher=self._event_bus,
            repository=candle_repository,
        )
        return [
            CandleUseCase(
                market=self._config.market,
                symbols=self._config.symbols,
                manager=candle_workflow_manager,
            ),
        ]

    def __execute_use_cases(self) -> None:
        """Start every assembled use case in its own asynchronous task."""
        event_loop = asyncio.get_running_loop()
        self._use_case_tasks = [
            event_loop.create_task(
                coro=use_case.execute(),
                name=f"use_case:{index}:{type(use_case).__name__}",
            )
            for index, use_case in enumerate(self._use_cases)
        ]

    async def __wait_for_shutdown(self, stop_event: asyncio.Event | None) -> None:
        """Wait until shutdown is requested or a critical use case stops.

        Args:
            stop_event: Optional externally controlled shutdown event.

        Raises:
            ScamError: If a use case fails.
            AppError: If a use case terminates unexpectedly.
        """
        shutdown_task = asyncio.create_task(
            coro=self.__wait_for_shutdown_coro(stop_event),
            name="application_shutdown_waiter",
        )
        try:
            completed_tasks, _ = await asyncio.wait(
                fs=[shutdown_task, *self._use_case_tasks],
                return_when=asyncio.FIRST_COMPLETED,
            )
            if shutdown_task in completed_tasks:
                await shutdown_task
                return

            for use_case_task in completed_tasks:
                self._use_case_observed_tasks.add(use_case_task)
                if use_case_task.cancelled():
                    raise AppError(
                        reason=AppErrorReason.LIFECYCLE,
                        message="Application use case cancelled unexpectedly",
                        operation="run",
                    )
                error = use_case_task.exception()
                if isinstance(error, AppError):
                    raise error
                if isinstance(error, Exception):
                    raise AppError(
                        reason=AppErrorReason.LIFECYCLE,
                        message="Application use case failed",
                        operation="run",
                        root_cause=error,
                    ) from error
            raise AppError(
                reason=AppErrorReason.LIFECYCLE,
                message="Application use case stopped unexpectedly",
                operation="run",
            )
        finally:
            shutdown_task.cancel()
            await asyncio.gather(shutdown_task, return_exceptions=True)

    @staticmethod
    async def __wait_for_shutdown_coro(event: asyncio.Event | None) -> None:
        """Wait for a supplied event or an operating-system shutdown signal.

        Args:
            event: Optional externally controlled shutdown event.
        """
        if event is not None:
            await event.wait()
            return

        event = asyncio.Event()
        event_loop = asyncio.get_running_loop()
        registered_signals: list[signal.Signals] = []

        for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
            try:
                event_loop.add_signal_handler(shutdown_signal, event.set)
            except (NotImplementedError, RuntimeError):
                continue
            registered_signals.append(shutdown_signal)
        try:
            await event.wait()
        finally:
            for shutdown_signal in registered_signals:
                event_loop.remove_signal_handler(shutdown_signal)
