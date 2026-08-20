from __future__ import annotations

from enum import StrEnum
from dataclasses import dataclass

from decimal import Decimal
from datetime import UTC, datetime, timedelta

from scam_sniffer.data.api.stock.errors import StockError, StockErrorReason

class MarketDto(StrEnum):
    BINANCE = "binance"

class TransportDto(StrEnum):
    WS = "ws"
    REST = "rest"

class TimeframeResponse(StrEnum):
    M5 = "5m"
    M15 = "15m"
    H1 = "1h"

    @property
    def duration(self) -> timedelta:
        return {
            TimeframeResponse.M5: timedelta(minutes=5),
            TimeframeResponse.M15: timedelta(minutes=15),
            TimeframeResponse.H1: timedelta(hours=1),
        }[self]

    @property
    def component_count(self) -> int:
        return {
            TimeframeResponse.M5: 1,
            TimeframeResponse.M15: 3,
            TimeframeResponse.H1: 12,
        }[self]

@dataclass(frozen=True, slots=True)
class CandleResponse:
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
        try:
            if not isinstance(self.market, MarketDto):
                object.__setattr__(self, "market", MarketDto(self.market))
            if not isinstance(self.source, TransportDto):
                object.__setattr__(self, "source", TransportDto(self.source))
            if not isinstance(self.timeframe, TimeframeResponse):
                object.__setattr__(self, "timeframe", TimeframeResponse(self.timeframe))
        except ValueError as error:
            raise _candle_err("Market, Source and Timeframe must be supported") from error

        symbol = self.symbol.upper().strip()
        object.__setattr__(self, "symbol", symbol)
        if not symbol:
            raise _candle_err("symbol cannot be empty")

        if self.open_time.tzinfo is None or self.close_time.tzinfo is None:
            raise _candle_err("candle timestamps must be timezone-aware")
        object.__setattr__(self, "open_time", self.open_time.astimezone(UTC))
        object.__setattr__(self, "close_time", self.close_time.astimezone(UTC))
        if self.close_time <= self.open_time:
            raise _candle_err("close_time must be after open_time")
        if self.event_time is not None:
            if self.event_time.tzinfo is None:
                raise _candle_err("event_time time must be timezone-aware")
            object.__setattr__(
                self,
                "event_time",
                self.event_time.astimezone(UTC),
            )

        if self.highest_price < max(self.open_price, self.close_price, self.lowest_price):
            raise _candle_err("highest_price must be greater than or equal to OHLC values")
        if self.lowest_price > min(self.open_price, self.close_price, self.highest_price):
            raise _candle_err("lowest_price must be less than or equal to OHLC values")

        if self.trade_count is not None and self.trade_count < 0:
            raise _candle_err("trade_count cannot be negative")
        if self.trade_volume < 0:
            raise _candle_err("trade_volume cannot be negative")
        if self.volume_quote is not None and self.volume_quote < 0:
            raise _candle_err("volume_quote cannot be negative")

def _candle_err(message: str) -> StockError:
    return StockError(
        reason=StockErrorReason.INVALID_CANDLE,
        message=message,
        operation="validate_candle",
    )
