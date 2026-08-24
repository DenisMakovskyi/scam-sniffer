import asyncio

from typing import cast
from decimal import Decimal
from datetime import UTC, datetime
from collections.abc import AsyncIterator, Awaitable, Callable

import pytest

from scam_sniffer.domain.manager.candle import CandleManager
from scam_sniffer.domain.manager.config import CandleManagerConfig
from scam_sniffer.domain.manager.events import (
    CandleClosed,
    CandleEvent,
    CandlesSynchronized,
)
from scam_sniffer.domain.models import Candle, Market, Timeframe
from scam_sniffer.core.events import EventPublisher
from scam_sniffer.domain.repository.candle import CandleRepository
from scam_sniffer.domain.errors import (
    DomainError,
    ManagerError,
    DomainErrorReason,
    ManagerErrorReason
)

_CURRENT_TIME = datetime(2025, 1, 1, 12, 3, tzinfo=UTC)

class FakeCandleRepository:
    def __init__(
        self,
        error: DomainError | None = None,
        range_error: DomainError | None = None,
        fill_batches: bool = False,
        latest_candle: Candle | None = None,
        stream_outcomes: list[DomainError | list[Candle] | None] | None = None,
        selected_candles: list[Candle] | None = None,
    ) -> None:
        self.error = error
        self.range_error = range_error
        self.fill_batches = fill_batches
        self.latest_candle = latest_candle
        self.stream_event = asyncio.Event()
        self.stream_outcomes = list(stream_outcomes or [None])
        self.selected_candles = selected_candles or []
        self.calls: list[tuple[str, int, Timeframe, datetime, datetime]] = []
        self.range_calls: list[tuple[Market, str, Timeframe, datetime, datetime]] = []
        self.latest_calls: list[tuple[Market, str, Timeframe]] = []
        self.stream_calls: list[tuple[str, Timeframe]] = []

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

    async def stream_candles(
        self,
        symbol: str,
        timeframe: Timeframe,
    ) -> AsyncIterator[Candle]:
        self.stream_calls.append((symbol, timeframe))
        outcome = self.stream_outcomes.pop(0) if self.stream_outcomes else None

        if isinstance(outcome, DomainError):
            raise outcome
        if outcome is None:
            await self.stream_event.wait()
            return
        for candle in outcome:
            yield candle

    async def select_candles(
        self,
        market: Market,
        symbol: str,
        timeframe: Timeframe,
        start_time: datetime,
        finish_time: datetime,
    ) -> list[Candle]:
        self.range_calls.append((market, symbol, timeframe, start_time, finish_time))
        if self.range_error is not None:
            raise self.range_error
        return self.selected_candles

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

class FakeEventPublisher:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.events: list[CandleEvent] = []

    async def publish(self, event: CandleEvent) -> None:
        if self.error is not None:
            raise self.error
        self.events.append(event)

def test_candle_manager_config_rejects_invalid_values() -> None:
    with pytest.raises(ManagerError):
        CandleManagerConfig(batch_size=0)
    with pytest.raises(ManagerError):
        CandleManagerConfig(batch_size=1_501)
    with pytest.raises(ManagerError):
        CandleManagerConfig(backfill_size=0)
    with pytest.raises(ManagerError):
        CandleManagerConfig(stream_retry_delay=0)
    with pytest.raises(ManagerError):
        CandleManagerConfig(stream_retry_delay=2, stream_retry_max_delay=1)

@pytest.mark.asyncio
async def test_batch_sync_delegates_to_repository() -> None:
    repository = FakeCandleRepository()
    manager = _manager(repository)
    start_time = datetime(2025, 1, 1, tzinfo=UTC)
    finish_time = datetime(2025, 1, 2, tzinfo=UTC)

    candles = await manager.batch_fetch(
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
        await manager.batch_fetch(
            symbol="BTCUSDT",
            k_limit=1500,
            timeframe=Timeframe.M5,
            start_time=datetime(2025, 1, 1, tzinfo=UTC),
            finish_time=datetime(2025, 1, 2, tzinfo=UTC),
        )

    error = error_info.value
    assert error.reason is ManagerErrorReason.REPOSITORY
    assert error.root_cause is repository_error
    assert error.__cause__ is repository_error

@pytest.mark.asyncio
async def test_initial_backfill_skips_non_empty_series() -> None:
    repository = FakeCandleRepository(latest_candle=_candle())
    manager = _manager(repository)

    synchronized_count = await manager.backfill(
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

    synchronized_count = await manager.tail_gap_sync(
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

    synchronized_count = await manager.backfill(
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
async def test_backfill_uses_manager_config_limits() -> None:
    repository = FakeCandleRepository(fill_batches=True)
    event_publisher = FakeEventPublisher()
    manager = _manager(
        repository=repository,
        manager_config=CandleManagerConfig(batch_size=2, backfill_size=5),
        event_publisher=event_publisher,
    )

    synchronized_count = await manager.backfill(
        market=Market.BINANCE,
        symbol="BTCUSDT",
        timeframe=Timeframe.M5,
    )

    assert synchronized_count == 5
    assert [call[1] for call in repository.calls] == [2, 2, 1]
    assert event_publisher.events == [
        CandlesSynchronized(
            market=Market.BINANCE,
            symbol="BTCUSDT",
            timeframe=Timeframe.M5,
            start_time=datetime(2025, 1, 1, 11, 35, tzinfo=UTC),
            finish_time=datetime(2025, 1, 1, 12, tzinfo=UTC),
            synchronized_count=5,
        )
    ]

@pytest.mark.asyncio
async def test_backfill_absorbs_event_publication_failure_after_sync() -> None:
    publish_error = RuntimeError("Event publication failed")
    repository = FakeCandleRepository(fill_batches=True)
    manager = _manager(
        repository=repository,
        manager_config=CandleManagerConfig(batch_size=2, backfill_size=2),
        event_publisher=FakeEventPublisher(error=publish_error),
    )

    with pytest.raises(ManagerError) as error_info:
        await manager.backfill(
            market=Market.BINANCE,
            symbol="BTCUSDT",
            timeframe=Timeframe.M5,
        )

    error = error_info.value
    assert len(repository.calls) == 1
    assert error.reason is ManagerErrorReason.PUBLISHER
    assert error.operation == "backfill"
    assert error.root_cause is publish_error
    assert error.__cause__ is publish_error

@pytest.mark.asyncio
async def test_continuity_audit_skips_empty_series() -> None:
    repository = FakeCandleRepository()
    manager = _manager(repository)

    synchronized_count = await manager.batch_audit(
        market=Market.BINANCE,
        symbol="BTCUSDT",
        timeframe=Timeframe.M5,
    )

    assert synchronized_count == 0
    assert repository.calls == []
    assert repository.range_calls == []

@pytest.mark.asyncio
async def test_continuity_audit_skips_continuous_series() -> None:
    finish_time = datetime(2025, 1, 1, 12, tzinfo=UTC)
    latest_candle = _candle(open_time=finish_time - Timeframe.M5.duration)
    repository = FakeCandleRepository(
        latest_candle=latest_candle,
        selected_candles=[
            _candle(open_time=latest_candle.open_time - Timeframe.M5.duration),
            latest_candle,
        ],
    )
    manager = _manager(repository)

    synchronized_count = await manager.batch_audit(
        market=Market.BINANCE,
        symbol="BTCUSDT",
        timeframe=Timeframe.M5,
    )

    assert synchronized_count == 0
    assert repository.calls == []
    assert repository.range_calls == [
        (
            Market.BINANCE,
            "BTCUSDT",
            Timeframe.M5,
            finish_time - Timeframe.M5.duration * 10_000,
            finish_time,
        )
    ]

@pytest.mark.asyncio
async def test_continuity_audit_absorbs_range_failure() -> None:
    repository_error = DomainError(
        reason=DomainErrorReason.STORAGE,
        message="Candle range selection failed",
        operation="select_candles",
    )
    manager = _manager(
        FakeCandleRepository(
            range_error=repository_error,
            latest_candle=_candle(),
        )
    )

    with pytest.raises(ManagerError) as error_info:
        await manager.batch_audit(
            market=Market.BINANCE,
            symbol="BTCUSDT",
            timeframe=Timeframe.M5,
        )

    error = error_info.value
    assert error.reason is ManagerErrorReason.REPOSITORY
    assert error.operation == "batch_audit"
    assert error.root_cause is repository_error
    assert error.__cause__ is repository_error

@pytest.mark.parametrize(
    "timeframe",
    [Timeframe.M5, Timeframe.M15, Timeframe.H1],
)
@pytest.mark.asyncio
async def test_continuity_audit_recovers_internal_gaps(
    timeframe: Timeframe,
) -> None:
    start_time = datetime(2025, 1, 1, tzinfo=UTC)
    latest_candle = _candle(
        timeframe=timeframe,
        open_time=start_time + timeframe.duration * 6,
    )
    repository = FakeCandleRepository(
        fill_batches=True,
        latest_candle=latest_candle,
        selected_candles=[
            _candle(timeframe=timeframe, open_time=start_time),
            _candle(timeframe=timeframe, open_time=start_time + timeframe.duration),
            _candle(
                timeframe=timeframe,
                open_time=start_time + timeframe.duration * 2,
                is_closed=False,
            ),
            _candle(
                timeframe=timeframe,
                open_time=start_time + timeframe.duration * 4,
            ),
            latest_candle,
        ],
    )
    manager = _manager(repository)

    synchronized_count = await manager.batch_audit(
        market=Market.BINANCE,
        symbol="BTCUSDT",
        timeframe=timeframe,
    )

    assert synchronized_count == 3
    assert repository.calls == [
        (
            "BTCUSDT",
            2,
            timeframe,
            start_time + timeframe.duration * 2,
            start_time + timeframe.duration * 4,
        ),
        (
            "BTCUSDT",
            1,
            timeframe,
            start_time + timeframe.duration * 5,
            start_time + timeframe.duration * 6,
        ),
    ]

@pytest.mark.asyncio
async def test_continuity_audit_rejects_misaligned_sequence() -> None:
    start_time = datetime(2025, 1, 1, tzinfo=UTC)
    latest_candle = _candle(open_time=start_time + Timeframe.M5.duration * 2.5)
    repository = FakeCandleRepository(
        latest_candle=latest_candle,
        selected_candles=[
            _candle(open_time=start_time),
            latest_candle,
        ],
    )
    manager = _manager(repository)

    with pytest.raises(ManagerError) as error_info:
        await manager.batch_audit(
            market=Market.BINANCE,
            symbol="BTCUSDT",
            timeframe=Timeframe.M5,
        )

    error = error_info.value
    assert error.reason is ManagerErrorReason.CONTINUITY
    assert error.operation == "batch_audit"
    assert repository.calls == []

def test_start_stream_rejects_empty_symbol() -> None:
    manager = _manager(FakeCandleRepository())

    with pytest.raises(ManagerError) as error_info:
        manager.start_stream_task(symbol=" ", timeframe=Timeframe.M5)

    error = error_info.value
    assert error.reason is ManagerErrorReason.PARAMS
    assert error.operation == "start_stream_task"

def test_start_stream_requires_running_event_loop() -> None:
    manager = _manager(FakeCandleRepository())

    with pytest.raises(ManagerError) as error_info:
        manager.start_stream_task(symbol="BTCUSDT", timeframe=Timeframe.M5)

    error = error_info.value
    assert error.reason is ManagerErrorReason.LIFECYCLE
    assert error.operation == "start_stream_task"
    assert isinstance(error.root_cause, RuntimeError)

@pytest.mark.asyncio
async def test_start_stream_reuses_and_stops_active_tasks() -> None:
    repository = FakeCandleRepository(stream_outcomes=[None, None])
    manager = _manager(repository)

    first_task = manager.start_stream_task(symbol="btcusdt", timeframe=Timeframe.M5)
    reused_task = manager.start_stream_task(symbol=" BTCUSDT ", timeframe=Timeframe.M5)
    second_task = manager.start_stream_task(symbol="BTCUSDT", timeframe=Timeframe.M15)
    await _wait_for_stream_calls(repository=repository, expected_count=2)

    assert first_task is reused_task
    assert first_task is not second_task
    assert repository.stream_calls == [
        ("BTCUSDT", Timeframe.M5),
        ("BTCUSDT", Timeframe.M15),
    ]

    await manager.gather_stream_tasks()

    assert first_task.cancelled()
    assert second_task.cancelled()

@pytest.mark.asyncio
async def test_stream_reconnect_uses_bounded_backoff() -> None:
    retry_delays: list[float] = []
    remote_error = DomainError(
        reason=DomainErrorReason.REMOTE,
        message="Candle remote stream failed",
        operation="stream_candles",
    )

    async def sleep_provider(delay: float) -> None:
        retry_delays.append(delay)

    repository = FakeCandleRepository(
        stream_outcomes=[remote_error] * 6 + [None],
    )
    manager = _manager(repository=repository, sleep_provider=sleep_provider)

    manager.start_stream_task(symbol="BTCUSDT", timeframe=Timeframe.M5)
    await _wait_for_stream_calls(repository=repository, expected_count=7)

    assert retry_delays == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0]

    await manager.gather_stream_tasks()

@pytest.mark.asyncio
async def test_stream_reconnect_resets_backoff_after_update() -> None:
    retry_delays: list[float] = []
    remote_error = DomainError(
        reason=DomainErrorReason.REMOTE,
        message="Candle remote stream failed",
        operation="stream_candles",
    )

    async def sleep_provider(delay: float) -> None:
        retry_delays.append(delay)

    repository = FakeCandleRepository(
        stream_outcomes=[remote_error, [_candle()], remote_error, None],
    )
    manager = _manager(repository=repository, sleep_provider=sleep_provider)

    manager.start_stream_task(symbol="BTCUSDT", timeframe=Timeframe.M5)
    await _wait_for_stream_calls(repository=repository, expected_count=4)

    assert retry_delays == [1.0, 1.0, 1.0]

    await manager.gather_stream_tasks()

@pytest.mark.parametrize("is_closed", [False, True])
@pytest.mark.asyncio
async def test_stream_publishes_only_closed_persisted_candles(
    is_closed: bool,
) -> None:
    async def sleep_provider(_: float) -> None:
        await asyncio.sleep(0)

    candle = _candle(is_closed=is_closed)
    repository = FakeCandleRepository(stream_outcomes=[[candle], None])
    event_publisher = FakeEventPublisher()
    manager = _manager(
        repository=repository,
        sleep_provider=sleep_provider,
        event_publisher=event_publisher,
    )

    manager.start_stream_task(symbol="BTCUSDT", timeframe=Timeframe.M5)
    await _wait_for_stream_calls(repository=repository, expected_count=2)

    expected_events = [CandleClosed(candle=candle)] if is_closed else []
    assert event_publisher.events == expected_events

    await manager.gather_stream_tasks()

@pytest.mark.parametrize(
    ("is_closed", "expected_count"),
    [(True, 2), (False, 3)],
)
@pytest.mark.asyncio
async def test_stream_reconnect_recovers_missed_candles(
    is_closed: bool,
    expected_count: int,
) -> None:
    start_time = datetime(2025, 1, 1, tzinfo=UTC)
    retry_delays: list[float] = []

    async def sleep_provider(delay: float) -> None:
        retry_delays.append(delay)

    repository = FakeCandleRepository(
        fill_batches=True,
        stream_outcomes=[
            [_candle(open_time=start_time, is_closed=is_closed)],
            [_candle(open_time=start_time + Timeframe.M5.duration * 3)],
            None,
        ],
    )
    event_publisher = FakeEventPublisher()
    manager = _manager(
        repository=repository,
        sleep_provider=sleep_provider,
        event_publisher=event_publisher,
    )

    manager.start_stream_task(symbol="BTCUSDT", timeframe=Timeframe.M5)
    await _wait_for_stream_calls(repository=repository, expected_count=3)

    expected_start_time = (
        start_time + Timeframe.M5.duration
        if is_closed
        else start_time
    )
    assert repository.calls == [
        (
            "BTCUSDT",
            expected_count,
            Timeframe.M5,
            expected_start_time,
            start_time + Timeframe.M5.duration * 3,
        )
    ]
    synchronized_events = [
        event
        for event in event_publisher.events
        if isinstance(event, CandlesSynchronized)
    ]
    assert synchronized_events == [
        CandlesSynchronized(
            market=Market.BINANCE,
            symbol="BTCUSDT",
            timeframe=Timeframe.M5,
            start_time=expected_start_time,
            finish_time=start_time + Timeframe.M5.duration * 3,
            synchronized_count=expected_count,
        )
    ]

    await manager.gather_stream_tasks()

@pytest.mark.parametrize(
    "reason",
    [DomainErrorReason.MAPPING, DomainErrorReason.STORAGE],
)
@pytest.mark.asyncio
async def test_stream_lifecycle_rejects_non_remote_failure(
    reason: DomainErrorReason,
) -> None:
    repository_error = DomainError(
        reason=reason,
        message="Candle stream processing failed",
        operation="stream_candles",
    )
    repository = FakeCandleRepository(stream_outcomes=[repository_error])
    manager = _manager(repository)

    stream_task = manager.start_stream_task(symbol="BTCUSDT", timeframe=Timeframe.M5)
    await asyncio.wait({stream_task})

    with pytest.raises(ManagerError) as error_info:
        await manager.gather_stream_tasks()

    error = error_info.value
    assert error.reason is ManagerErrorReason.REPOSITORY
    assert error.operation == "streaming_loop"
    assert error.root_cause is repository_error
    assert error.__cause__ is repository_error

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
        await manager.tail_gap_sync(
            market=Market.BINANCE,
            symbol="BTCUSDT",
            timeframe=Timeframe.M5,
        )

    error = error_info.value
    assert error.reason is ManagerErrorReason.REPOSITORY
    assert error.operation == "tail_gap_sync"
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
    event_publisher = FakeEventPublisher()
    manager = _manager(
        repository=repository,
        event_publisher=event_publisher,
    )

    synchronized_count = await manager.tail_gap_sync(
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
    assert event_publisher.events == [
        CandlesSynchronized(
            market=Market.BINANCE,
            symbol="BTCUSDT",
            timeframe=timeframe,
            start_time=latest_open_time + timeframe.duration,
            finish_time=finish_time,
            synchronized_count=2,
        )
    ]

def _candle(
    timeframe: Timeframe = Timeframe.M5,
    open_time: datetime | None = None,
    is_closed: bool = True,
) -> Candle:
    open_time = open_time or datetime(2025, 1, 1, tzinfo=UTC)
    return Candle(
        market=Market.BINANCE,
        symbol="BTCUSDT",
        is_closed=is_closed,
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

async def _wait_for_stream_calls(
    repository: FakeCandleRepository,
    expected_count: int,
) -> None:
    for _ in range(100):
        if len(repository.stream_calls) >= expected_count:
            return
        await asyncio.sleep(0)
    raise AssertionError(f"Expected {expected_count} stream calls")

def _manager(
    repository: FakeCandleRepository,
    manager_config: CandleManagerConfig | None = None,
    sleep_provider: Callable[[float], Awaitable[None]] | None = None,
    event_publisher: FakeEventPublisher | None = None,
) -> CandleManager:
    return CandleManager(
        config=manager_config or CandleManagerConfig(),
        publisher=cast(
            EventPublisher[CandleEvent],
            event_publisher or FakeEventPublisher(),
        ),
        repository=cast(CandleRepository, repository),
        time_provider=lambda: _CURRENT_TIME,
        sleep_provider=sleep_provider,
    )
