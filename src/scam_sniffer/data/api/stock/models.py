"""Transport models returned by exchange market-data sources."""

from __future__ import annotations

from enum import StrEnum
from dataclasses import dataclass

from decimal import Decimal
from datetime import UTC, datetime, timedelta

class MarketDto(StrEnum):
    """Identify the exchange that produced a transport model."""

    BINANCE = "binance"

class TransportDto(StrEnum):
    """Identify the transport that delivered remote market data."""

    WS = "ws"
    REST = "rest"

class TimeframeResponse(StrEnum):
    """Represent candle intervals supported by the remote API."""

    M5 = "5m"
    M15 = "15m"
    H1 = "1h"

    @property
    def duration(self) -> timedelta:
        """Return the wall-clock duration represented by the interval."""
        return {
            TimeframeResponse.M5: timedelta(minutes=5),
            TimeframeResponse.M15: timedelta(minutes=15),
            TimeframeResponse.H1: timedelta(hours=1),
        }[self]

    @property
    def component_count(self) -> int:
        """Return the number of five-minute candles in the interval."""
        return {
            TimeframeResponse.M5: 1,
            TimeframeResponse.M15: 3,
            TimeframeResponse.H1: 12,
        }[self]

@dataclass(frozen=True, slots=True)
class CandleResponse:
    """Represent an OHLCV candle received from an exchange.

    Attributes:
        market: Exchange that produced the candle.
        source: Transport that delivered the candle.
        symbol: Uppercase trading pair symbol.
        is_closed: Whether the exchange finalized the candle.
        timeframe: Candle aggregation interval.
        open_time: Inclusive timezone-aware candle boundary.
        close_time: Exclusive timezone-aware candle boundary.
        event_time: Exchange event time for streamed updates, if available.
        open_price: First traded price in the interval.
        close_price: Last traded price in the interval.
        lowest_price: Lowest traded price in the interval.
        highest_price: Highest traded price in the interval.
        trade_count: Number of trades in the interval, if available.
        trade_volume: Base-asset volume traded in the interval.
        volume_quote: Quote-asset volume traded in the interval, if available.
    """

    market: MarketDto
    source: TransportDto
    symbol: str
    is_closed: bool
    timeframe: TimeframeResponse
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
            if not isinstance(self.market, MarketDto):
                object.__setattr__(self, "market", MarketDto(self.market))
            if not isinstance(self.source, TransportDto):
                object.__setattr__(self, "source", TransportDto(self.source))
            if not isinstance(self.timeframe, TimeframeResponse):
                object.__setattr__(self, "timeframe", TimeframeResponse(self.timeframe))
        except ValueError as error:
            raise ValueError("Market, Source and Timeframe must be supported") from error

        symbol = self.symbol.upper().strip()
        object.__setattr__(self, "symbol", symbol)
        if not symbol:
            raise ValueError("Symbol cannot be empty")

        if self.open_time.tzinfo is None or self.close_time.tzinfo is None:
            raise ValueError("Candle timestamps must be timezone-aware")
        object.__setattr__(self, "open_time", self.open_time.astimezone(UTC))
        object.__setattr__(self, "close_time", self.close_time.astimezone(UTC))
        if self.close_time <= self.open_time:
            raise ValueError("close_time must be after open_time")
        if self.event_time is not None:
            if self.event_time.tzinfo is None:
                raise ValueError("Event time must be timezone-aware")
            object.__setattr__(
                self,
                "event_time",
                self.event_time.astimezone(UTC),
            )

        if self.highest_price < max(self.open_price, self.close_price, self.lowest_price):
            raise ValueError("highest_price must be greater than or equal to OHLC values")
        if self.lowest_price > min(self.open_price, self.close_price, self.highest_price):
            raise ValueError("lowest_price must be less than or equal to OHLC values")

        if self.trade_count is not None and self.trade_count < 0:
            raise ValueError("trade_count cannot be negative")
        if self.trade_volume < 0:
            raise ValueError("trade_volume cannot be negative")
        if self.volume_quote is not None and self.volume_quote < 0:
            raise ValueError("volume_quote cannot be negative")
