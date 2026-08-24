"""Database access object for persisted candles."""

from __future__ import annotations

from typing import Any
from datetime import datetime
from collections.abc import Mapping, Sequence

import asyncpg

from scam_sniffer.data.database.schema.candle import (
    CANDLE_SELECT,
    CANDLE_CREATE,
    CANDLE_DELETE,
    CANDLE_UPDATE,
    CANDLE_UPSERT,
    CANDLE_SELECT_RANGE,
    CANDLE_SELECT_LATEST,
    CANDLE_SELECT_LATEST_CLOSED,
)
from scam_sniffer.data.database.errors import DatabaseError, DatabaseErrorReason
from scam_sniffer.data.database.entities import CandleEntity

class CandleDao:
    """Execute candle CRUD operations against PostgreSQL."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        """Initialize the DAO with an active PostgreSQL pool.

        Args:
            pool: Connection pool used for every candle query.
        """
        self._pool = pool

    # Select region

    async def select(
        self,
        market: str,
        symbol: str,
        timeframe: str,
        open_time: datetime,
    ) -> CandleEntity | None:
        """Select a candle by its composite identity.

        Args:
            market: Persisted exchange identifier.
            symbol: Persisted trading pair symbol.
            timeframe: Persisted candle interval identifier.
            open_time: Inclusive candle boundary identifying the row.

        Returns:
            Matching candle entity, or ``None`` when no row exists.

        Raises:
            DatabaseError: If the query fails.
        """
        try:
            row = await self._pool.fetchrow(
                CANDLE_SELECT,
                market,
                symbol,
                timeframe,
                open_time,
            )
        except (asyncpg.PostgresError, asyncpg.InterfaceError) as error:
            raise _raise_db_query_error("select") from error
        return _build_entity(row) if row is not None else None

    async def select_range(
        self,
        market: str,
        symbol: str,
        timeframe: str,
        start_time: datetime,
        finish_time: datetime,
    ) -> list[CandleEntity]:
        """Select candles inside a half-open time range.

        Args:
            market: Persisted exchange identifier.
            symbol: Persisted trading pair symbol.
            timeframe: Persisted candle interval identifier.
            start_time: Inclusive range boundary.
            finish_time: Exclusive range boundary.

        Returns:
            Matching entities ordered by open time.

        Raises:
            DatabaseError: If the query fails.
        """
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
            raise _raise_db_query_error("select_range") from error
        return [_build_entity(row) for row in rows]

    async def select_latest(
        self,
        market: str,
        symbol: str,
        timeframe: str,
    ) -> CandleEntity | None:
        """Select the most recent candle in a series.

        Args:
            market: Persisted exchange identifier.
            symbol: Persisted trading pair symbol.
            timeframe: Persisted candle interval identifier.

        Returns:
            Latest candle entity, or ``None`` when the series is empty.

        Raises:
            DatabaseError: If the query fails.
        """
        try:
            row = await self._pool.fetchrow(
                CANDLE_SELECT_LATEST,
                market,
                symbol,
                timeframe,
            )
        except (asyncpg.PostgresError, asyncpg.InterfaceError) as error:
            raise _raise_db_query_error("select_latest") from error
        return _build_entity(row) if row is not None else None

    async def select_latest_closed(
        self,
        market: str,
        symbol: str,
        timeframe: str,
    ) -> CandleEntity | None:
        """Select the most recent finalized candle in a series.

        Args:
            market: Persisted exchange identifier.
            symbol: Persisted trading pair symbol.
            timeframe: Persisted candle interval identifier.

        Returns:
            Latest closed candle entity, or ``None`` when none exists.

        Raises:
            DatabaseError: If the query fails.
        """
        try:
            row = await self._pool.fetchrow(
                CANDLE_SELECT_LATEST_CLOSED,
                market,
                symbol,
                timeframe,
            )
        except (asyncpg.PostgresError, asyncpg.InterfaceError) as error:
            raise _raise_db_query_error("select_latest_closed") from error
        return _build_entity(row) if row is not None else None

    # Create region

    async def create(self, entity: CandleEntity) -> None:
        """Insert a candle without conflict resolution.

        Args:
            entity: Candle row to insert.

        Raises:
            DatabaseError: If the insert fails.
        """
        try:
            await self._pool.execute(CANDLE_CREATE, *_entity_args(entity))
        except (asyncpg.PostgresError, asyncpg.InterfaceError) as error:
            raise _raise_db_query_error("create") from error

    # Upsert region

    async def upsert_one(self, entity: CandleEntity) -> bool:
        """Insert one candle or update a newer non-final snapshot.

        Args:
            entity: Candle row to persist.

        Returns:
            Whether PostgreSQL inserted or updated the row.

        Raises:
            DatabaseError: If the upsert fails.
        """
        try:
            status = await self._pool.execute(CANDLE_UPSERT, *_entity_args(entity))
        except (asyncpg.PostgresError, asyncpg.InterfaceError) as error:
            raise _raise_db_query_error("upsert_one") from error
        return _rows_count(status) == 1

    async def upsert_many(self, entities: Sequence[CandleEntity]) -> None:
        """Upsert multiple candles while preserving input order.

        Args:
            entities: Candle rows to persist.

        Raises:
            DatabaseError: If any upsert fails.
        """
        if not entities:
            return
        try:
            await self._pool.executemany(
                CANDLE_UPSERT,
                [_entity_args(entity) for entity in entities],
            )
        except (asyncpg.PostgresError, asyncpg.InterfaceError) as error:
            raise _raise_db_query_error("upsert_many") from error

    # Update region

    async def update(self, entity: CandleEntity) -> bool:
        """Update a candle selected by its composite identity.

        Args:
            entity: Candle row containing identity and replacement values.

        Returns:
            Whether PostgreSQL updated one row.

        Raises:
            DatabaseError: If the update fails.
        """
        try:
            status = await self._pool.execute(CANDLE_UPDATE, *_entity_args(entity))
        except (asyncpg.PostgresError, asyncpg.InterfaceError) as error:
            raise _raise_db_query_error("update") from error
        return _rows_count(status) == 1

    # Delete region

    async def delete(
        self,
        market: str,
        symbol: str,
        timeframe: str,
        open_time: datetime,
    ) -> bool:
        """Delete a candle by its composite identity.

        Args:
            market: Persisted exchange identifier.
            symbol: Persisted trading pair symbol.
            timeframe: Persisted candle interval identifier.
            open_time: Inclusive candle boundary identifying the row.

        Returns:
            Whether PostgreSQL deleted one row.

        Raises:
            DatabaseError: If the delete fails.
        """
        try:
            status = await self._pool.execute(
                CANDLE_DELETE,
                market,
                symbol,
                timeframe,
                open_time,
            )
        except (asyncpg.PostgresError, asyncpg.InterfaceError) as error:
            raise _raise_db_query_error("delete") from error
        return _rows_count(status) == 1

def _rows_count(status: str) -> int:
    """Extract the affected-row count from a PostgreSQL command status.

    Args:
        status: Status string returned by ``asyncpg``.

    Returns:
        Parsed row count, or zero when the status is malformed.
    """
    try:
        return int(status.rsplit(maxsplit=1)[-1])
    except (IndexError, ValueError):
        return 0

def _entity_args(entity: CandleEntity) -> tuple[Any, ...]:
    """Convert a candle entity to SQL arguments in schema order.

    Args:
        entity: Candle row to serialize.

    Returns:
        Values ordered exactly like the candle SQL columns.
    """
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
    """Build a candle entity from a database row mapping.

    Args:
        row: Database values keyed by candle column name.

    Returns:
        Candle persistence entity.
    """
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

def _raise_db_query_error(operation: str) -> DatabaseError:
    """Wrap a driver exception as a categorized database error.

    Args:
        operation: DAO operation active during the failure.

    Returns:
        Categorized database query error.
    """
    return DatabaseError(
        reason=DatabaseErrorReason.QUERY,
        message="Database query failed",
        operation=operation,
    )
