"""Exchange-independent domain models for candle workflows."""

from __future__ import annotations

from enum import StrEnum
from dataclasses import dataclass

from decimal import Decimal
from datetime import UTC, datetime, timedelta

class Market(StrEnum):
    """Identify a supported market-data provider."""

    BINANCE = "binance"

class Timeframe(StrEnum):
    """Represent a supported domain candle interval."""

    M5 = "5m"
    M15 = "15m"
    H1 = "1h"

    @property
    def duration(self) -> timedelta:
        """Return the wall-clock duration represented by the interval."""
        return {
            Timeframe.M5: timedelta(minutes=5),
            Timeframe.M15: timedelta(minutes=15),
            Timeframe.H1: timedelta(hours=1),
        }[self]

    @property
    def component_count(self) -> int:
        """Return the number of five-minute candles in the interval."""
        return {
            Timeframe.M5: 1,
            Timeframe.M15: 3,
            Timeframe.H1: 12,
        }[self]

@dataclass(frozen=True, slots=True)
class Candle:
    """Represent a validated exchange-independent OHLCV candle.

    Attributes:
        market: Market-data provider that owns the series.
        symbol: Uppercase trading pair symbol.
        is_closed: Whether the exchange finalized the candle.
        timeframe: Candle aggregation interval.
        open_time: Inclusive timezone-aware candle boundary.
        close_time: Exclusive timezone-aware candle boundary.
        event_time: Latest exchange event time, if available.
        open_price: First traded price in the interval.
        close_price: Last traded price in the interval.
        lowest_price: Lowest traded price in the interval.
        highest_price: Highest traded price in the interval.
        trade_count: Number of trades in the interval, if available.
        trade_volume: Base-asset volume traded in the interval.
        volume_quote: Quote-asset volume traded in the interval, if available.
    """

    market: Market
    symbol: str
    is_closed: bool
    timeframe: Timeframe
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

    def __post_init__(self) -> None:
        """Normalize enums, symbol, and timestamps and validate OHLCV values.

        Raises:
            ValueError: If an enum, symbol, timestamp, price, or volume is invalid.
        """
        try:
            if not isinstance(self.market, Market):
                object.__setattr__(self, "market", Market(self.market))
            if not isinstance(self.timeframe, Timeframe):
                object.__setattr__(self, "timeframe", Timeframe(self.timeframe))
        except ValueError as error:
            raise ValueError("Market and Timeframe must be supported") from error

        symbol = self.symbol.upper().strip()
        object.__setattr__(self, "symbol", symbol)
        if not symbol:
            raise ValueError("Symbol cannot be empty")

        if self.open_time.tzinfo is None or self.close_time.tzinfo is None:
            raise ValueError("Candle timestamps must be timezone-aware")
        object.__setattr__(self, "open_time", self.open_time.astimezone(UTC))
        object.__setattr__(self, "close_time", self.close_time.astimezone(UTC))
        if self.close_time <= self.open_time:
            raise ValueError("Close time must be after open time")
        if self.event_time is not None:
            if self.event_time.tzinfo is None:
                raise ValueError("Event time must be timezone-aware")
            object.__setattr__(self, "event_time", self.event_time.astimezone(UTC))

        if self.highest_price < max(self.open_price, self.close_price, self.lowest_price):
            raise ValueError(
                "Highest price must be greater than or equal to OHLC values"
            )
        if self.lowest_price > min(self.open_price, self.close_price, self.highest_price):
            raise ValueError("Lowest price must be less than or equal to OHLC values")

        if self.trade_count is not None and self.trade_count < 0:
            raise ValueError("Trade count cannot be negative")
        if self.trade_volume < 0:
            raise ValueError("Trade volume cannot be negative")
        if self.volume_quote is not None and self.volume_quote < 0:
            raise ValueError("Volume quote cannot be negative")
