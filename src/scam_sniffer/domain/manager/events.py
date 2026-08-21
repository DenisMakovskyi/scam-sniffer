"""Events produced by candle synchronization workflows."""

from datetime import UTC, datetime
from dataclasses import dataclass

from scam_sniffer.domain.models import Candle, Market, Timeframe

@dataclass(frozen=True, slots=True)
class CandleClosed:
    """Report that a finalized candle was persisted successfully.

    Attributes:
        candle: Finalized domain candle available to downstream consumers.
    """

    candle: Candle

    def __post_init__(self) -> None:
        """Validate that the event contains a finalized candle.

        Raises:
            ValueError: If the candle is not closed.
        """
        if not self.candle.is_closed:
            raise ValueError("CandleClosed event requires a closed candle")

@dataclass(frozen=True, slots=True)
class CandlesSynchronized:
    """Report that a closed-candle range was persisted successfully.

    Attributes:
        market: Market-data provider that owns the synchronized series.
        symbol: Uppercase trading pair symbol.
        timeframe: Candle aggregation interval.
        start_time: Inclusive synchronized range boundary.
        finish_time: Exclusive synchronized range boundary.
        synchronized_count: Number of candles persisted in the range.
    """

    market: Market
    symbol: str
    timeframe: Timeframe
    start_time: datetime
    finish_time: datetime
    synchronized_count: int

    def __post_init__(self) -> None:
        """Normalize event values and validate the synchronized range.

        Raises:
            ValueError: If an enum, symbol, count, or range boundary is invalid.
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

        if self.start_time.tzinfo is None or self.finish_time.tzinfo is None:
            raise ValueError("Synchronization timestamps must be timezone-aware")
        object.__setattr__(self, "start_time", self.start_time.astimezone(UTC))
        object.__setattr__(self, "finish_time", self.finish_time.astimezone(UTC))
        if self.finish_time <= self.start_time:
            raise ValueError("Finish time must be after start time")

        if self.synchronized_count < 1:
            raise ValueError("Synchronized candle count must be positive")

type CandleEvent = CandleClosed | CandlesSynchronized
