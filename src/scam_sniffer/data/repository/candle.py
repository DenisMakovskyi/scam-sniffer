"""Candle repository impl. combining remote and database data sources."""

from __future__ import annotations

from typing import override
from datetime import datetime
from collections.abc import AsyncIterator

from scam_sniffer.data.api.stock.base import AbsStock
from scam_sniffer.data.api.stock.errors import StockError
from scam_sniffer.data.api.stock.models import TimeframeResponse
from scam_sniffer.data.database.errors import DatabaseError
from scam_sniffer.data.database.dao.candle import CandleDao
from scam_sniffer.data.mappings.candle import (
    dto_to_candle,
    entity_to_candle,
    candle_to_entity,
)

from scam_sniffer.domain.errors import DomainError, DomainErrorReason
from scam_sniffer.domain.models import Candle, Market, Timeframe
from scam_sniffer.domain.repository.candle import CandleRepository

class CandleRepositoryImpl(CandleRepository):
    """Synchronize exchange candles into storage and expose domain models."""

    def __init__(
        self,
        dao: CandleDao,
        stock: AbsStock,
    ) -> None:
        """Initialize the repository with its local and remote data sources.

        Args:
            dao: Local candle database access object.
            stock: Remote exchange market-data source.
        """
        self._dao = dao
        self._stock = stock

    @override
    async def fetch_candles(
        self,
        symbol: str,
        k_limit: int,
        timeframe: Timeframe,
        start_time: datetime,
        finish_time: datetime,
    ) -> list[Candle]:
        """Fetch, map, and persist candles inside a half-open time range.

        Args:
            symbol: Exchange trading pair symbol.
            k_limit: Maximum number of candles to fetch.
            timeframe: Domain candle interval.
            start_time: Inclusive range boundary.
            finish_time: Exclusive range boundary.

        Returns:
            Persisted domain candles in remote response order.

        Raises:
            DomainError: If remote retrieval, mapping, or persistence fails.
        """
        try:
            timeframe_response = TimeframeResponse(timeframe.value)
        except ValueError as error:
            raise DomainError(
                reason=DomainErrorReason.MAPPING,
                message="Candle timeframe mapping failed",
                operation="fetch_candles",
                root_cause=error,
            ) from error

        try:
            responses = await self._stock.get_candles(
                symbol=symbol,
                k_limit=k_limit,
                timeframe=timeframe_response,
                start_time=start_time,
                finish_time=finish_time,
            )
        except StockError as error:
            raise DomainError(
                reason=DomainErrorReason.REMOTE,
                message="Candle remote fetch failed",
                operation="fetch_candles",
                root_cause=error,
            ) from error

        try:
            candles = [dto_to_candle(response) for response in responses]
        except ValueError as error:
            raise DomainError(
                reason=DomainErrorReason.MAPPING,
                message="Candle mapping failed",
                operation="fetch_candles",
                root_cause=error,
            ) from error

        try:
            await self._dao.upsert_many([candle_to_entity(candle) for candle in candles])
        except ValueError as error:
            raise DomainError(
                reason=DomainErrorReason.MAPPING,
                message="Candle entity mapping failed",
                operation="fetch_candles",
                root_cause=error,
            ) from error
        except DatabaseError as error:
            raise DomainError(
                reason=DomainErrorReason.STORAGE,
                message="Candle batch persistence failed",
                operation="fetch_candles",
                root_cause=error,
            ) from error
        return candles

    @override
    async def stream_candles(
        self,
        symbol: str,
        timeframe: Timeframe,
    ) -> AsyncIterator[Candle]:
        """Map, persist, and stream live candle snapshots.

        Args:
            symbol: Exchange trading pair symbol.
            timeframe: Domain candle interval.

        Yields:
            Domain candles after each successful persistence operation.

        Raises:
            DomainError: If remote streaming, mapping, or persistence fails.
        """
        try:
            async for response in self._stock.stream_candles(
                symbol=symbol,
                timeframe=TimeframeResponse(timeframe.value),
            ):
                candle = dto_to_candle(response)
                is_persisted = await self._dao.upsert_one(candle_to_entity(candle))
                if is_persisted:
                    yield candle
        except StockError as error:
            raise DomainError(
                reason=DomainErrorReason.REMOTE,
                message="Candle remote stream failed",
                operation="stream_candles",
                root_cause=error,
            ) from error
        except ValueError as error:
            raise DomainError(
                reason=DomainErrorReason.MAPPING,
                message="Candle mapping failed",
                operation="stream_candles",
                root_cause=error,
            ) from error
        except DatabaseError as error:
            raise DomainError(
                reason=DomainErrorReason.STORAGE,
                message="Candle stream persistence failed",
                operation="stream_candles",
                root_cause=error,
            ) from error

    @override
    async def select_candles(
        self,
        market: Market,
        symbol: str,
        timeframe: Timeframe,
        start_time: datetime,
        finish_time: datetime,
    ) -> list[Candle]:
        """Read domain candles from a half-open local time range.

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
        try:
            entities = await self._dao.select_range(
                market=market.value,
                symbol=symbol,
                timeframe=timeframe.value,
                start_time=start_time,
                finish_time=finish_time,
            )
        except DatabaseError as error:
            raise DomainError(
                reason=DomainErrorReason.STORAGE,
                message="Candle range selection failed",
                operation="select_candles",
                root_cause=error,
            ) from error

        try:
            return [entity_to_candle(entity) for entity in entities]
        except ValueError as error:
            raise DomainError(
                reason=DomainErrorReason.MAPPING,
                message="Candle mapping failed",
                operation="select_candles",
                root_cause=error,
            ) from error

    @override
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
        try:
            entity = await self._dao.select_latest_closed(
                market=market.value,
                symbol=symbol,
                timeframe=timeframe.value,
            )
        except DatabaseError as error:
            raise DomainError(
                reason=DomainErrorReason.STORAGE,
                message="Latest closed candle selection failed",
                operation="select_latest_closed_candle",
                root_cause=error,
            ) from error

        if entity is None:
            return None
        try:
            return entity_to_candle(entity)
        except ValueError as error:
            raise DomainError(
                reason=DomainErrorReason.MAPPING,
                message="Candle mapping failed",
                operation="select_latest_closed_candle",
                root_cause=error,
            ) from error
