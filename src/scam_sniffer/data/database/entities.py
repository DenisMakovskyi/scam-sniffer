from __future__ import annotations

from dataclasses import dataclass

from decimal import Decimal
from datetime import datetime

@dataclass(frozen=True, slots=True)
class CandleEntity:
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
