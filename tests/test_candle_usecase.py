import asyncio

import pytest

from scam_sniffer.domain.models import Market, Timeframe
from scam_sniffer.domain.usecase.candle import CandleUseCase
from scam_sniffer.domain.errors import ManagerError, ManagerErrorReason

class FakeCandleManager:
    def __init__(
        self,
        fail_backfill: bool = False,
        fail_stream_task: bool = False,
    ) -> None:
        self.fail_backfill = fail_backfill
        self.fail_stream_task = fail_stream_task
        self.calls: list[str] = []
        self.markets: list[Market] = []
        self.streams_started = asyncio.Event()
        self.stream_async_tasks: list[asyncio.Task[None]] = []

    async def backfill(
        self,
        market: Market,
        symbol: str,
        timeframe: Timeframe,
    ) -> int:
        self.markets.append(market)
        self.calls.append(f"backfill:{symbol}:{timeframe.value}")
        if self.fail_backfill:
            raise ManagerError(
                reason=ManagerErrorReason.REPOSITORY,
                message="Backfill failed",
                operation="backfill",
            )
        return 0

    async def batch_audit(
        self,
        market: Market,
        symbol: str,
        timeframe: Timeframe,
    ) -> int:
        self.markets.append(market)
        self.calls.append(f"batch_audit:{symbol}:{timeframe.value}")
        return 0

    async def tail_gap_sync(
        self,
        market: Market,
        symbol: str,
        timeframe: Timeframe,
    ) -> int:
        self.markets.append(market)
        self.calls.append(f"tail_gap_sync:{symbol}:{timeframe.value}")
        return 0

    def start_stream_task(
        self,
        symbol: str,
        timeframe: Timeframe,
    ) -> asyncio.Task[None]:
        async def stream() -> None:
            if self.fail_stream_task and timeframe is Timeframe.M5:
                raise RuntimeError("Stream failed")
            await asyncio.Event().wait()

        self.calls.append(f"stream:{symbol}:{timeframe.value}")
        stream_async_task = asyncio.create_task(stream())
        self.stream_async_tasks.append(stream_async_task)
        if len(self.stream_async_tasks) == 3:
            self.streams_started.set()
        return stream_async_task

    async def gather_stream_tasks(self) -> None:
        self.calls.append("gather_stream_tasks")
        for stream_async_task in self.stream_async_tasks:
            stream_async_task.cancel()
        await asyncio.gather(*self.stream_async_tasks, return_exceptions=True)

@pytest.mark.asyncio
async def test_candle_use_case_synchronizes_before_streaming() -> None:
    manager = FakeCandleManager()
    use_case = CandleUseCase(
        market=Market.BINANCE,
        manager=manager,
        symbols=("ETHUSDT",),
    )
    use_case_async_task = asyncio.create_task(use_case.execute())

    await manager.streams_started.wait()
    use_case_async_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await use_case_async_task

    assert manager.calls == [
        "backfill:ETHUSDT:5m",
        "batch_audit:ETHUSDT:5m",
        "tail_gap_sync:ETHUSDT:5m",
        "backfill:ETHUSDT:15m",
        "batch_audit:ETHUSDT:15m",
        "tail_gap_sync:ETHUSDT:15m",
        "backfill:ETHUSDT:1h",
        "batch_audit:ETHUSDT:1h",
        "tail_gap_sync:ETHUSDT:1h",
        "stream:ETHUSDT:5m",
        "stream:ETHUSDT:15m",
        "stream:ETHUSDT:1h",
        "gather_stream_tasks",
    ]
    assert set(manager.markets) == {Market.BINANCE}

@pytest.mark.asyncio
async def test_candle_use_case_cleans_streams_after_sync_failure() -> None:
    manager = FakeCandleManager(fail_backfill=True)
    use_case = CandleUseCase(
        market=Market.BINANCE,
        manager=manager,
        symbols=("BTCUSDT",),
    )

    with pytest.raises(ManagerError) as error_info:
        await use_case.execute()

    assert error_info.value.reason is ManagerErrorReason.REPOSITORY
    assert manager.calls == [
        "backfill:BTCUSDT:5m",
        "gather_stream_tasks",
    ]

@pytest.mark.asyncio
async def test_candle_use_case_wraps_unexpected_stream_failure() -> None:
    manager = FakeCandleManager(fail_stream_task=True)
    use_case = CandleUseCase(
        market=Market.BINANCE,
        manager=manager,
        symbols=("BTCUSDT",),
    )

    with pytest.raises(ManagerError) as error_info:
        await use_case.execute()

    error = error_info.value
    assert error.reason is ManagerErrorReason.LIFECYCLE
    assert isinstance(error.root_cause, RuntimeError)
    assert manager.calls[-1] == "gather_stream_tasks"
