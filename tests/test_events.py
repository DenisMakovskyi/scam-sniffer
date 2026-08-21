from decimal import Decimal
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone

import pytest

from scam_sniffer.domain.models import Candle, Market, Timeframe
from scam_sniffer.domain.manager.events import CandleClosed, CandlesSynchronized

def test_candle_closed_accepts_closed_candle() -> None:
    candle = _candle()

    event = CandleClosed(candle=candle)

    assert event.candle is candle

def test_candle_closed_rejects_unclosed_candle() -> None:
    with pytest.raises(ValueError):
        CandleClosed(candle=replace(_candle(), is_closed=False))

def test_candles_synchronized_normalizes_values() -> None:
    local_timezone = timezone(timedelta(hours=2))
    start_time = datetime(2025, 1, 1, tzinfo=local_timezone)
    finish_time = start_time + Timeframe.M5.duration * 3

    event = CandlesSynchronized(
        market=Market.BINANCE,
        symbol=" btcusdt ",
        timeframe=Timeframe.M5,
        start_time=start_time,
        finish_time=finish_time,
        synchronized_count=3,
    )

    assert event.symbol == "BTCUSDT"
    assert event.start_time.tzinfo is UTC
    assert event.finish_time.tzinfo is UTC

def test_candles_synchronized_rejects_invalid_values() -> None:
    start_time = datetime(2025, 1, 1, tzinfo=UTC)
    finish_time = start_time + Timeframe.M5.duration

    with pytest.raises(ValueError):
        _sync_event(symbol=" ")
    with pytest.raises(ValueError):
        _sync_event(synchronized_count=0)
    with pytest.raises(ValueError):
        _sync_event(start_time=start_time.replace(tzinfo=None))
    with pytest.raises(ValueError):
        _sync_event(start_time=finish_time, finish_time=start_time)

def _candle() -> Candle:
    open_time = datetime(2025, 1, 1, tzinfo=UTC)
    return Candle(
        market=Market.BINANCE,
        symbol="BTCUSDT",
        is_closed=True,
        timeframe=Timeframe.M5,
        open_time=open_time,
        close_time=open_time + Timeframe.M5.duration,
        event_time=open_time + Timeframe.M5.duration,
        open_price=Decimal("100.0"),
        close_price=Decimal("102.0"),
        lowest_price=Decimal("99.0"),
        highest_price=Decimal("103.0"),
        trade_count=42,
        trade_volume=Decimal("12.5"),
        volume_quote=Decimal("1250.0"),
    )

def _sync_event(
    symbol: str = "BTCUSDT",
    synchronized_count: int = 1,
    start_time: datetime | None = None,
    finish_time: datetime | None = None,
) -> CandlesSynchronized:
    start_time = start_time or datetime(2025, 1, 1, tzinfo=UTC)
    finish_time = finish_time or start_time + Timeframe.M5.duration
    return CandlesSynchronized(
        market=Market.BINANCE,
        symbol=symbol,
        timeframe=Timeframe.M5,
        start_time=start_time,
        finish_time=finish_time,
        synchronized_count=synchronized_count,
    )
