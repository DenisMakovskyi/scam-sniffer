from typing import cast
from decimal import Decimal
from datetime import UTC, datetime

import pytest

from scam_sniffer.manager.candle import CandleManager
from scam_sniffer.domain.models import Candle, Market, Timeframe
from scam_sniffer.domain.repository.candle import CandleRepository
from scam_sniffer.domain.errors import DomainError, DomainErrorReason
from scam_sniffer.manager.errors import ManagerError, ManagerErrorReason

_CURRENT_TIME = datetime(2025, 1, 1, 12, 3, tzinfo=UTC)

class FakeCandleRepository:
    def __init__(
        self,
        error: DomainError | None = None,
        fill_batches: bool = False,
        latest_candle: Candle | None = None,
    ) -> None:
        self.error = error
        self.fill_batches = fill_batches
        self.latest_candle = latest_candle
        self.calls: list[tuple[str, int, Timeframe, datetime, datetime]] = []
        self.latest_calls: list[tuple[Market, str, Timeframe]] = []

    async def fetch_candles(
        self,
        symbol: str,
        k_limit: int,
        timeframe: Timeframe,
        start_time: datetime,
        finish_time: datetime,
    ) -> list[Candle]:
        self.calls.append((symbol, k_limit, timeframe, start_time, finish_time))
        if self.error is not None:
            raise self.error
        return [_candle()] * k_limit if self.fill_batches else []

    async def select_latest_closed_candle(
        self,
        market: Market,
        symbol: str,
        timeframe: Timeframe,
    ) -> Candle | None:
        self.latest_calls.append((market, symbol, timeframe))
        if self.error is not None:
            raise self.error
        return self.latest_candle

@pytest.mark.asyncio
async def test_batch_sync_delegates_to_repository() -> None:
    repository = FakeCandleRepository()
    manager = _manager(repository)
    start_time = datetime(2025, 1, 1, tzinfo=UTC)
    finish_time = datetime(2025, 1, 2, tzinfo=UTC)

    candles = await manager.batch_sync(
        symbol="BTCUSDT",
        k_limit=1500,
        timeframe=Timeframe.M5,
        start_time=start_time,
        finish_time=finish_time,
    )

    assert candles == []
    assert repository.calls == [
        ("BTCUSDT", 1500, Timeframe.M5, start_time, finish_time)
    ]

@pytest.mark.asyncio
async def test_batch_sync_absorbs_repository_error() -> None:
    root_cause = RuntimeError("Remote request failed")
    repository_error = DomainError(
        reason=DomainErrorReason.REMOTE,
        message="Candle remote fetch failed",
        operation="fetch_candles",
        root_cause=root_cause,
    )
    manager = _manager(FakeCandleRepository(error=repository_error))

    with pytest.raises(ManagerError) as error_info:
        await manager.batch_sync(
            symbol="BTCUSDT",
            k_limit=1500,
            timeframe=Timeframe.M5,
            start_time=datetime(2025, 1, 1, tzinfo=UTC),
            finish_time=datetime(2025, 1, 2, tzinfo=UTC),
        )

    error = error_info.value
    assert error.reason is ManagerErrorReason.REPO
    assert error.root_cause is repository_error
    assert error.__cause__ is repository_error

@pytest.mark.asyncio
async def test_initial_backfill_skips_non_empty_series() -> None:
    repository = FakeCandleRepository(latest_candle=_candle())
    manager = _manager(repository)

    synchronized_count = await manager.initial_backfill(
        market=Market.BINANCE,
        symbol="BTCUSDT",
        timeframe=Timeframe.M5,
    )

    assert synchronized_count == 0
    assert repository.calls == []

@pytest.mark.parametrize("has_latest", [False, True])
@pytest.mark.asyncio
async def test_tail_gap_recovery_skips_complete_and_empty_series(
    has_latest: bool,
) -> None:
    finish_time = datetime(2025, 1, 1, 12, tzinfo=UTC)
    latest_candle = (
        _candle(open_time=finish_time - Timeframe.M5.duration)
        if has_latest
        else None
    )
    repository = FakeCandleRepository(latest_candle=latest_candle)
    manager = _manager(repository)

    synchronized_count = await manager.tail_gap_recovery(
        market=Market.BINANCE,
        symbol="BTCUSDT",
        timeframe=Timeframe.M5,
    )

    assert synchronized_count == 0
    assert repository.calls == []

@pytest.mark.parametrize(
    "timeframe",
    [Timeframe.M5, Timeframe.M15, Timeframe.H1],
)
@pytest.mark.asyncio
async def test_initial_backfill_requests_ten_thousand_closed_candles(
    timeframe: Timeframe,
) -> None:
    repository = FakeCandleRepository(fill_batches=True)
    manager = _manager(repository)
    finish_time = datetime(2025, 1, 1, 12, tzinfo=UTC)
    start_time = finish_time - timeframe.duration * 10_000

    synchronized_count = await manager.initial_backfill(
        market=Market.BINANCE,
        symbol="BTCUSDT",
        timeframe=timeframe,
    )

    assert synchronized_count == 10_000
    assert repository.latest_calls == [(Market.BINANCE, "BTCUSDT", timeframe)]
    assert len(repository.calls) == 7
    assert sum(call[1] for call in repository.calls) == 10_000
    assert repository.calls[0][3] == start_time
    assert repository.calls[-1][1] == 1_000
    assert repository.calls[-1][4] == finish_time
    assert all(
        previous_call[4] == current_call[3]
        for previous_call, current_call in zip(
            repository.calls,
            repository.calls[1:],
            strict=False,
        )
    )

@pytest.mark.asyncio
async def test_tail_gap_recovery_absorbs_repository_failure() -> None:
    root_cause = RuntimeError("Database query failed")
    repository_error = DomainError(
        reason=DomainErrorReason.STORAGE,
        message="Latest closed candle selection failed",
        operation="select_latest_closed_candle",
        root_cause=root_cause,
    )
    manager = _manager(FakeCandleRepository(error=repository_error))

    with pytest.raises(ManagerError) as error_info:
        await manager.tail_gap_recovery(
            market=Market.BINANCE,
            symbol="BTCUSDT",
            timeframe=Timeframe.M5,
        )

    error = error_info.value
    assert error.reason is ManagerErrorReason.REPO
    assert error.operation == "tail_gap_recovery"
    assert error.root_cause is repository_error
    assert error.__cause__ is repository_error

@pytest.mark.parametrize(
    "timeframe",
    [Timeframe.M5, Timeframe.M15, Timeframe.H1],
)
@pytest.mark.asyncio
async def test_tail_gap_recovery_requests_only_missing_closed_candles(
    timeframe: Timeframe,
) -> None:
    finish_time = datetime(2025, 1, 1, 12, tzinfo=UTC)
    latest_open_time = finish_time - timeframe.duration * 3
    repository = FakeCandleRepository(
        fill_batches=True,
        latest_candle=_candle(
            timeframe=timeframe,
            open_time=latest_open_time,
        ),
    )
    manager = _manager(repository)

    synchronized_count = await manager.tail_gap_recovery(
        market=Market.BINANCE,
        symbol="BTCUSDT",
        timeframe=timeframe,
    )

    assert synchronized_count == 2
    assert repository.latest_calls == [(Market.BINANCE, "BTCUSDT", timeframe)]
    assert repository.calls == [
        (
            "BTCUSDT",
            2,
            timeframe,
            latest_open_time + timeframe.duration,
            finish_time,
        )
    ]

def _candle(
    timeframe: Timeframe = Timeframe.M5,
    open_time: datetime | None = None,
) -> Candle:
    open_time = open_time or datetime(2025, 1, 1, tzinfo=UTC)
    return Candle(
        market=Market.BINANCE,
        symbol="BTCUSDT",
        is_closed=True,
        timeframe=timeframe,
        open_time=open_time,
        close_time=open_time + timeframe.duration,
        event_time=open_time + timeframe.duration,
        open_price=Decimal("100.0"),
        close_price=Decimal("102.0"),
        lowest_price=Decimal("99.0"),
        highest_price=Decimal("103.0"),
        trade_count=42,
        trade_volume=Decimal("12.5"),
        volume_quote=Decimal("1250.0"),
    )

def _manager(repository: FakeCandleRepository) -> CandleManager:
    return CandleManager(
        repository=cast(CandleRepository, repository),
        time_provider=lambda: _CURRENT_TIME,
    )
