"""Persistence entities stored by database access objects."""

from __future__ import annotations

from dataclasses import dataclass

from decimal import Decimal
from datetime import datetime

@dataclass(frozen=True, slots=True)
class CandleEntity:
    """Represent one candle row in database-compatible types.

    Attributes:
        market: Persisted exchange identifier.
        symbol: Persisted trading pair symbol.
        is_closed: Whether the exchange finalized the candle.
        timeframe: Persisted candle interval identifier.
        open_time: Inclusive candle boundary.
        close_time: Exclusive candle boundary.
        event_time: Latest exchange event time, if available.
        open_price: First traded price in the interval.
        close_price: Last traded price in the interval.
        lowest_price: Lowest traded price in the interval.
        highest_price: Highest traded price in the interval.
        trade_count: Number of trades in the interval, if available.
        trade_volume: Base-asset volume traded in the interval.
        volume_quote: Quote-asset volume traded in the interval, if available.
    """

    market: str
    symbol: str
    is_closed: bool
    timeframe: str
    open_time: datetime
    close_time: datetime
    event_time: datetime | None
    open_price: Decimal
    close_price: Decimal
    lowest_price: Decimal
    highest_price: Decimal
    trade_count: int | None
    trade_volume: Decimal
    volume_quote: Decimal | None
