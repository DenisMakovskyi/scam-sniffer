from __future__ import annotations

from enum import StrEnum
from dataclasses import dataclass

from decimal import Decimal
from datetime import UTC, datetime, timedelta

from scam_sniffer.domain.errors import ScamError, ScamErrorReason

class Market(StrEnum):
    BINANCE = "binance"

class Timeframe(StrEnum):
    M5 = "5m"
    M15 = "15m"
    H1 = "1h"

    @property
    def duration(self) -> timedelta:
        return {
            Timeframe.M5: timedelta(minutes=5),
            Timeframe.M15: timedelta(minutes=15),
            Timeframe.H1: timedelta(hours=1),
        }[self]

    @property
    def component_count(self) -> int:
        return {
            Timeframe.M5: 1,
            Timeframe.M15: 3,
            Timeframe.H1: 12,
        }[self]

@dataclass(frozen=True, slots=True)
class Candle:
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
        try:
            if not isinstance(self.market, Market):
                object.__setattr__(self, "market", Market(self.market))
            if not isinstance(self.timeframe, Timeframe):
                object.__setattr__(self, "timeframe", Timeframe(self.timeframe))
        except ValueError as error:
            raise _candle_error(
                message="Market and Timeframe must be supported",
                root_cause=error,
            ) from error

        symbol = self.symbol.upper().strip()
        object.__setattr__(self, "symbol", symbol)
        if not symbol:
            raise _candle_error("Symbol cannot be empty")

        if self.open_time.tzinfo is None or self.close_time.tzinfo is None:
            raise _candle_error("Candle timestamps must be timezone-aware")
        object.__setattr__(self, "open_time", self.open_time.astimezone(UTC))
        object.__setattr__(self, "close_time", self.close_time.astimezone(UTC))
        if self.close_time <= self.open_time:
            raise _candle_error("Close time must be after open time")
        if self.event_time is not None:
            if self.event_time.tzinfo is None:
                raise _candle_error("Event time must be timezone-aware")
            object.__setattr__(self, "event_time", self.event_time.astimezone(UTC))

        if self.highest_price < max(self.open_price, self.close_price, self.lowest_price):
            raise _candle_error(
                "Highest price must be greater than or equal to OHLC values"
            )
        if self.lowest_price > min(self.open_price, self.close_price, self.highest_price):
            raise _candle_error("Lowest price must be less than or equal to OHLC values")

        if self.trade_count is not None and self.trade_count < 0:
            raise _candle_error("Trade count cannot be negative")
        if self.trade_volume < 0:
            raise _candle_error("Trade volume cannot be negative")
        if self.volume_quote is not None and self.volume_quote < 0:
            raise _candle_error("Volume quote cannot be negative")

def _candle_error(
    message: str,
    root_cause: Exception | None = None,
) -> ScamError:
    return ScamError(
        reason=ScamErrorReason.INVALID_CANDLE,
        message=message,
        operation="validate_candle",
        root_cause=root_cause,
    )
