"""Candle synchronization and streaming use case."""

import asyncio
from typing import override

from scam_sniffer.domain.errors import ManagerError, ManagerErrorReason
from scam_sniffer.domain.models import Market, Timeframe
from scam_sniffer.domain.usecase.proto import UseCase
from scam_sniffer.domain.manager.candle import CandleManager

_TIMEFRAMES = (
    Timeframe.M5,
    Timeframe.M15,
    Timeframe.H1,
)

class CandleUseCase(UseCase):
    """Synchronize configured candle series and own their live streams."""

    def __init__(
        self,
        market: Market,
        manager: CandleManager,
        symbols: tuple[str, ...],
    ) -> None:
        """Initialize the use case with its candle workflow manager.

        Args:
            market: Market-data provider that owns every configured series.
            manager: Candle manager used for synchronization and streaming.
            symbols: Trading pair symbols synchronized by the use case.
        """
        self._market = market
        self._manager = manager
        self._symbols = symbols

    @override
    async def execute(self) -> None:
        """Synchronize candles and run their streams until failure or cancellation.

        Raises:
            ManagerError: If synchronization or a stream task fails.
        """
        try:
            await self.__sync_candles()
            await self.__await_stream_tasks(self.__start_stream_tasks())
        finally:
            await self._manager.gather_stream_tasks()

    async def __sync_candles(self) -> None:
        """Recover every configured candle series before live streaming."""
        for symbol in self._symbols:
            for timeframe in _TIMEFRAMES:
                await self._manager.backfill(
                    market=self._market,
                    symbol=symbol,
                    timeframe=timeframe,
                )
                await self._manager.batch_audit(
                    market=self._market,
                    symbol=symbol,
                    timeframe=timeframe,
                )
                await self._manager.tail_gap_sync(
                    market=self._market,
                    symbol=symbol,
                    timeframe=timeframe,
                )

    def __start_stream_tasks(self) -> list[asyncio.Task[None]]:
        """Start and return one managed stream task for every configured series."""
        return [
            self._manager.start_stream_task(
                symbol=symbol,
                timeframe=timeframe,
            )
            for symbol in self._symbols
            for timeframe in _TIMEFRAMES
        ]

    @staticmethod
    async def __await_stream_tasks(
        stream_async_tasks: list[asyncio.Task[None]],
    ) -> None:
        """Wait until a stream task stops and translate unexpected failures.

        Args:
            stream_async_tasks: Managed candle stream tasks.

        Raises:
            ManagerError: If a stream task fails or terminates unexpectedly.
        """
        completed_tasks, _ = await asyncio.wait(
            fs=stream_async_tasks,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in completed_tasks:
            if task.cancelled():
                raise ManagerError(
                    reason=ManagerErrorReason.LIFECYCLE,
                    message="Candle stream task was cancelled unexpectedly",
                    operation="execute",
                )
            error = task.exception()
            if isinstance(error, ManagerError):
                raise error
            if isinstance(error, Exception):
                raise ManagerError(
                    reason=ManagerErrorReason.LIFECYCLE,
                    message="Candle stream task failed",
                    operation="execute",
                    root_cause=error,
                ) from error
        raise ManagerError(
            reason=ManagerErrorReason.LIFECYCLE,
            message="Candle stream task stopped unexpectedly",
            operation="execute",
        )
