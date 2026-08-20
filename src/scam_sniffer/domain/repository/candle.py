from __future__ import annotations

from typing import Protocol
from datetime import datetime
from collections.abc import AsyncIterator

from scam_sniffer.domain.models import Candle, Market, Timeframe

class CandleRepository(Protocol):
    async def fetch_candles(
        self,
        symbol: str,
        k_limit: int,
        timeframe: Timeframe,
        start_time: datetime,
        finish_time: datetime,
    ) -> list[Candle]: ...

    async def select_candles(
        self,
        market: Market,
        symbol: str,
        timeframe: Timeframe,
        start_time: datetime,
        finish_time: datetime,
    ) -> list[Candle]: ...

    def stream_candles(
        self,
        symbol: str,
        timeframe: Timeframe,
    ) -> AsyncIterator[Candle]: ...
