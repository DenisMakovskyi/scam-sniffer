"""Domain-owned candle repository contract."""

from __future__ import annotations

from typing import Protocol
from datetime import datetime
from collections.abc import AsyncIterator

from scam_sniffer.domain.models import Candle, Market, Timeframe

class CandleRepository(Protocol):
    """Define remote synchronization and local candle reads."""

    async def fetch_candles(
        self,
        symbol: str,
        k_limit: int,
        timeframe: Timeframe,
        start_time: datetime,
        finish_time: datetime,
    ) -> list[Candle]:
        """Fetch and persist candles inside a half-open time range.

        Args:
            symbol: Exchange trading pair symbol.
            k_limit: Maximum number of candles to fetch.
            timeframe: Domain candle interval.
            start_time: Inclusive range boundary.
            finish_time: Exclusive range boundary.

        Returns:
            Persisted candles in remote response order.

        Raises:
            DomainError: If remote retrieval, mapping, or persistence fails.
        """
        ...

    def stream_candles(
        self,
        symbol: str,
        timeframe: Timeframe,
    ) -> AsyncIterator[Candle]:
        """Persist and stream live candle snapshots.

        Args:
            symbol: Exchange trading pair symbol.
            timeframe: Domain candle interval.

        Yields:
            Candle snapshots after each successful persistence operation.

        Raises:
            DomainError: If remote streaming, mapping, or persistence fails.
        """
        ...

    async def select_candles(
        self,
        market: Market,
        symbol: str,
        timeframe: Timeframe,
        start_time: datetime,
        finish_time: datetime,
    ) -> list[Candle]:
        """Read locally persisted candles inside a half-open time range.

        Args:
            market: Market-data provider that owns the series.
            symbol: Trading pair symbol.
            timeframe: Domain candle interval.
            start_time: Inclusive range boundary.
            finish_time: Exclusive range boundary.

        Returns:
            Persisted candles ordered by open time.

        Raises:
            DomainError: If storage access or entity mapping fails.
        """
        ...

    async def select_latest_closed_candle(
        self,
        market: Market,
        symbol: str,
        timeframe: Timeframe,
    ) -> Candle | None:
        """Read the latest locally persisted finalized candle.

        Args:
            market: Market-data provider that owns the series.
            symbol: Trading pair symbol.
            timeframe: Domain candle interval.

        Returns:
            Latest closed candle, or ``None`` when none is stored.

        Raises:
            DomainError: If storage access or entity mapping fails.
        """
        ...
