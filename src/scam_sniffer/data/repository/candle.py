from __future__ import annotations

from typing import override
from datetime import datetime
from collections.abc import AsyncIterator

from scam_sniffer.domain.errors import ScamError
from scam_sniffer.data.api.stock.base import AbsStock
from scam_sniffer.data.api.stock.errors import StockError
from scam_sniffer.data.api.stock.models import TimeframeResponse
from scam_sniffer.data.database.errors import DatabaseError
from scam_sniffer.data.database.dao.candle import CandleDao
from scam_sniffer.data.repository.mapping.candle import (
    dto_to_candle,
    entity_to_candle,
    candle_to_entity,
)

from scam_sniffer.domain.models import Candle, Market, Timeframe
from scam_sniffer.domain.repository.errors import RepoError, RepoErrorReason
from scam_sniffer.domain.repository.candle import CandleRepository

class CandleRepositoryImpl(CandleRepository):
    def __init__(
        self,
        dao: CandleDao,
        stock: AbsStock,
    ) -> None:
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
        try:
            timeframe_response = TimeframeResponse(timeframe.value)
        except ValueError as error:
            raise RepoError(
                reason=RepoErrorReason.MAPPING,
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
            raise RepoError(
                reason=RepoErrorReason.REMOTE,
                message="Candle remote fetch failed",
                operation="fetch_candles",
                root_cause=error,
            ) from error

        try:
            candles = [dto_to_candle(response) for response in responses]
        except (ValueError, ScamError) as error:
            raise RepoError(
                reason=RepoErrorReason.MAPPING,
                message="Candle response mapping failed",
                operation="fetch_candles",
                root_cause=error,
            ) from error

        try:
            await self._dao.upsert_many([candle_to_entity(candle) for candle in candles])
        except DatabaseError as error:
            raise RepoError(
                reason=RepoErrorReason.STORAGE,
                message="Candle batch persistence failed",
                operation="fetch_candles",
                root_cause=error,
            ) from error
        return candles

    @override
    async def select_candles(
        self,
        market: Market,
        symbol: str,
        timeframe: Timeframe,
        start_time: datetime,
        finish_time: datetime,
    ) -> list[Candle]:
        try:
            entities = await self._dao.select_range(
                market=market.value,
                symbol=symbol,
                timeframe=timeframe.value,
                start_time=start_time,
                finish_time=finish_time,
            )
        except DatabaseError as error:
            raise RepoError(
                reason=RepoErrorReason.STORAGE,
                message="Candle range selection failed",
                operation="select_candles",
                root_cause=error,
            ) from error

        try:
            return [entity_to_candle(entity) for entity in entities]
        except (ValueError, ScamError) as error:
            raise RepoError(
                reason=RepoErrorReason.MAPPING,
                message="Candle entity mapping failed",
                operation="select_candles",
                root_cause=error,
            ) from error

    @override
    async def stream_candles(
        self,
        symbol: str,
        timeframe: Timeframe,
    ) -> AsyncIterator[Candle]:
        try:
            async for response in self._stock.stream_candles(
                symbol=symbol,
                timeframe=TimeframeResponse(timeframe.value),
            ):
                candle = dto_to_candle(response)
                await self._dao.upsert_one(candle_to_entity(candle))
                yield candle
        except StockError as error:
            raise RepoError(
                reason=RepoErrorReason.REMOTE,
                message="Candle remote stream failed",
                operation="stream_candles",
                root_cause=error,
            ) from error
        except (ValueError, ScamError) as error:
            raise RepoError(
                reason=RepoErrorReason.MAPPING,
                message="Candle response mapping failed",
                operation="stream_candles",
                root_cause=error,
            ) from error
        except DatabaseError as error:
            raise RepoError(
                reason=RepoErrorReason.STORAGE,
                message="Candle stream persistence failed",
                operation="stream_candles",
                root_cause=error,
            ) from error

