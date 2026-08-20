from __future__ import annotations

from typing import Any
from collections.abc import Mapping, Sequence

from datetime import datetime

import asyncpg

from scam_sniffer.data.database.errors import DatabaseError, DatabaseErrorReason
from scam_sniffer.data.database.entities import CandleEntity
from scam_sniffer.data.database.schema.candle import (
    CANDLE_SELECT,
    CANDLE_CREATE,
    CANDLE_DELETE,
    CANDLE_UPDATE,
    CANDLE_UPSERT,
    CANDLE_SELECT_RANGE,
    CANDLE_SELECT_LATEST,
)

class CandleDao:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    # Select region

    async def select(
        self,
        market: str,
        symbol: str,
        timeframe: str,
        open_time: datetime,
    ) -> CandleEntity | None:
        try:
            row = await self._pool.fetchrow(
                CANDLE_SELECT,
                market,
                symbol,
                timeframe,
                open_time,
            )
        except (asyncpg.PostgresError, asyncpg.InterfaceError) as error:
            raise _db_query_error(operation="select", root_cause=error) from error
        return _build_entity(row) if row is not None else None

    async def select_range(
        self,
        market: str,
        symbol: str,
        timeframe: str,
        start_time: datetime,
        finish_time: datetime,
    ) -> list[CandleEntity]:
        try:
            rows = await self._pool.fetch(
                CANDLE_SELECT_RANGE,
                market,
                symbol,
                timeframe,
                start_time,
                finish_time,
            )
        except (asyncpg.PostgresError, asyncpg.InterfaceError) as error:
            raise _db_query_error(operation="select_range", root_cause=error) from error
        return [_build_entity(row) for row in rows]

    async def select_latest(
        self,
        market: str,
        symbol: str,
        timeframe: str,
    ) -> CandleEntity | None:
        try:
            row = await self._pool.fetchrow(
                CANDLE_SELECT_LATEST,
                market,
                symbol,
                timeframe,
            )
        except (asyncpg.PostgresError, asyncpg.InterfaceError) as error:
            raise _db_query_error(operation="select_latest", root_cause=error) from error
        return _build_entity(row) if row is not None else None

    # Create region

    async def create(self, entity: CandleEntity) -> None:
        try:
            await self._pool.execute(CANDLE_CREATE, *_entity_args(entity))
        except (asyncpg.PostgresError, asyncpg.InterfaceError) as error:
            raise _db_query_error(operation="create", root_cause=error) from error

    # Upsert region

    async def upsert_one(self, entity: CandleEntity) -> bool:
        try:
            status = await self._pool.execute(CANDLE_UPSERT, *_entity_args(entity))
        except (asyncpg.PostgresError, asyncpg.InterfaceError) as error:
            raise _db_query_error(operation="upsert_one", root_cause=error) from error
        return _rows_count(status) == 1

    async def upsert_many(self, entities: Sequence[CandleEntity]) -> None:
        if not entities:
            return
        try:
            await self._pool.executemany(
                CANDLE_UPSERT,
                [_entity_args(entity) for entity in entities],
            )
        except (asyncpg.PostgresError, asyncpg.InterfaceError) as error:
            raise _db_query_error(operation="upsert_many", root_cause=error) from error

    # Update region

    async def update(self, entity: CandleEntity) -> bool:
        try:
            status = await self._pool.execute(CANDLE_UPDATE, *_entity_args(entity))
        except (asyncpg.PostgresError, asyncpg.InterfaceError) as error:
            raise _db_query_error(operation="update", root_cause=error) from error
        return _rows_count(status) == 1

    # Delete region

    async def delete(
        self,
        market: str,
        symbol: str,
        timeframe: str,
        open_time: datetime,
    ) -> bool:
        try:
            status = await self._pool.execute(
                CANDLE_DELETE,
                market,
                symbol,
                timeframe,
                open_time,
            )
        except (asyncpg.PostgresError, asyncpg.InterfaceError) as error:
            raise _db_query_error(operation="delete", root_cause=error) from error
        return _rows_count(status) == 1

def _rows_count(status: str) -> int:
    try:
        return int(status.rsplit(maxsplit=1)[-1])
    except (IndexError, ValueError):
        return 0

def _entity_args(entity: CandleEntity) -> tuple[Any, ...]:
    return (
        entity.market,
        entity.symbol,
        entity.is_closed,
        entity.timeframe,
        entity.open_time,
        entity.close_time,
        entity.event_time,
        entity.open_price,
        entity.close_price,
        entity.lowest_price,
        entity.highest_price,
        entity.trade_count,
        entity.trade_volume,
        entity.volume_quote,
    )

def _build_entity(row: Mapping[str, Any]) -> CandleEntity:
    return CandleEntity(
        market=row["market"],
        symbol=row["symbol"],
        is_closed=row["is_closed"],
        timeframe=row["timeframe"],
        open_time=row["open_time"],
        close_time=row["close_time"],
        event_time=row["event_time"],
        open_price=row["open_price"],
        close_price=row["close_price"],
        lowest_price=row["lowest_price"],
        highest_price=row["highest_price"],
        trade_count=row["trade_count"],
        trade_volume=row["trade_volume"],
        volume_quote=row["volume_quote"],
    )

def _db_query_error(operation: str, root_cause: Exception) -> DatabaseError:
    return DatabaseError(
        reason=DatabaseErrorReason.QUERY,
        message="Database query failed",
        operation=operation,
        root_cause=root_cause,
    )
