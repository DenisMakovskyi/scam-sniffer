from __future__ import annotations

from typing import Any, cast
from dataclasses import fields

from pathlib import Path
from decimal import Decimal
from datetime import UTC, datetime, timedelta

import pytest
import asyncpg

from scam_sniffer.data.api.stock.models import CandleResponse
from scam_sniffer.domain.models import Candle
from scam_sniffer.data.database.errors import DatabaseError, DatabaseErrorReason
from scam_sniffer.data.database.engine import DatabaseConfig, DatabaseEngine
from scam_sniffer.data.database.entities import CandleEntity
from scam_sniffer.data.database.dao.candle import CandleDao
from scam_sniffer.data.database.schema.candle import (
    CANDLE_UPSERT,
    COLUMNS_CANDLE,
    CANDLE_SELECT_LATEST_CLOSED,
)

_MIGRATION_PATH = (
    Path(__file__).parents[1]
    / "src/scam_sniffer/data/database/migration/0001_create_candles.sql"
)

class FakePool:
    def __init__(self) -> None:
        self.status = "INSERT 0 1"
        self.row: dict[str, Any] | None = None
        self.rows: list[dict[str, Any]] = []
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    async def execute(self, query: str, *args: Any) -> str:
        self.calls.append((query, args))
        return self.status

    async def executemany(self, query: str, args: list[tuple[Any, ...]]) -> None:
        self.calls.extend((query, values) for values in args)

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        self.calls.append((query, args))
        return self.row

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.calls.append((query, args))
        return self.rows

class BrokenPool(FakePool):
    async def execute(self, query: str, *args: Any) -> str:
        raise asyncpg.InterfaceError("Database connection is unavailable")

def test_database_config_rejects_invalid_pool_size() -> None:
    with pytest.raises(DatabaseError) as error_info:
        DatabaseConfig(
            dsn="postgresql://localhost/scam_sniffer",
            pool_min_size=2,
            pool_max_size=1,
        )

    assert error_info.value.reason is DatabaseErrorReason.CONF

def test_engine_requires_connection_before_pool_access() -> None:
    engine = DatabaseEngine(DatabaseConfig(dsn="postgresql://localhost/scam_sniffer"))

    with pytest.raises(DatabaseError) as error_info:
        _ = engine.pool

    assert error_info.value.reason is DatabaseErrorReason.CONNECTION

@pytest.mark.asyncio
async def test_dao_crud_uses_candle_entity_values() -> None:
    pool = FakePool()
    entity = _entity()
    dao = CandleDao(pool=cast(asyncpg.Pool, pool))

    await dao.create(entity)
    assert await dao.update(entity) is True
    assert await dao.delete(
        market=entity.market,
        symbol=entity.symbol,
        timeframe=entity.timeframe,
        open_time=entity.open_time,
    ) is True

    assert pool.calls[0][1][0:4] == (
        entity.market,
        entity.symbol,
        entity.is_closed,
        entity.timeframe,
    )

@pytest.mark.asyncio
async def test_dao_reads_entities_and_upserts_batches() -> None:
    pool = FakePool()
    entity = _entity()
    pool.row = _entity_row(entity)
    pool.rows = [_entity_row(entity)]
    dao = CandleDao(pool=cast(asyncpg.Pool, pool))

    stored = await dao.select(
        market=entity.market,
        symbol=entity.symbol,
        timeframe=entity.timeframe,
        open_time=entity.open_time,
    )
    latest = await dao.select_latest(
        market=entity.market,
        symbol=entity.symbol,
        timeframe=entity.timeframe,
    )
    latest_closed = await dao.select_latest_closed(
        market=entity.market,
        symbol=entity.symbol,
        timeframe=entity.timeframe,
    )
    candles = await dao.select_range(
        market=entity.market,
        symbol=entity.symbol,
        timeframe=entity.timeframe,
        start_time=entity.open_time,
        finish_time=entity.close_time,
    )
    await dao.upsert_many([entity, entity])

    assert stored == entity
    assert latest == entity
    assert latest_closed == entity
    assert candles == [entity]
    assert pool.calls[-2][0] == CANDLE_UPSERT
    assert pool.calls[-1][0] == CANDLE_UPSERT

def test_upsert_protects_closed_and_newer_candles() -> None:
    assert "WHERE NOT candles.is_closed" in CANDLE_UPSERT
    assert "EXCLUDED.event_time >= candles.event_time" in CANDLE_UPSERT

def test_latest_query_selects_only_closed_candles() -> None:
    assert "AND is_closed" in CANDLE_SELECT_LATEST_CLOSED

def test_related_candle_fields_keep_the_same_order() -> None:
    domain_fields = [field.name for field in fields(Candle)]
    entity_fields = [field.name for field in fields(CandleEntity)]
    response_fields = [field.name for field in fields(CandleResponse)]
    common_response_fields = [name for name in response_fields if name in entity_fields]
    schema_fields = [line.strip().rstrip(",") for line in COLUMNS_CANDLE.strip().splitlines()]

    migration = _MIGRATION_PATH.read_text(encoding="utf-8")
    migration_positions = [migration.index(f"    {name} ") for name in entity_fields]

    assert domain_fields == entity_fields
    assert entity_fields == common_response_fields
    assert schema_fields == entity_fields
    assert migration_positions == sorted(migration_positions)

@pytest.mark.asyncio
async def test_dao_wraps_driver_error_as_root_cause() -> None:
    dao = CandleDao(pool=cast(asyncpg.Pool, BrokenPool()))

    with pytest.raises(DatabaseError) as error_info:
        await dao.create(_entity())

    error = error_info.value
    assert error.reason is DatabaseErrorReason.QUERY
    assert isinstance(error.root_cause, asyncpg.InterfaceError)
    assert error.__cause__ is error.root_cause

def _entity() -> CandleEntity:
    open_time = datetime(2025, 1, 1, tzinfo=UTC)
    return CandleEntity(
        market="binance",
        symbol="BTCUSDT",
        is_closed=True,
        timeframe="5m",
        open_time=open_time,
        close_time=open_time + timedelta(minutes=5),
        event_time=open_time + timedelta(minutes=5),
        open_price=Decimal("100.0"),
        close_price=Decimal("102.0"),
        lowest_price=Decimal("99.0"),
        highest_price=Decimal("103.0"),
        trade_count=42,
        trade_volume=Decimal("12.5"),
        volume_quote=Decimal("1250.0"),
    )

def _entity_row(entity: CandleEntity) -> dict[str, Any]:
    return {
        "market": entity.market,
        "symbol": entity.symbol,
        "is_closed": entity.is_closed,
        "timeframe": entity.timeframe,
        "open_time": entity.open_time,
        "close_time": entity.close_time,
        "event_time": entity.event_time,
        "open_price": entity.open_price,
        "close_price": entity.close_price,
        "lowest_price": entity.lowest_price,
        "highest_price": entity.highest_price,
        "trade_count": entity.trade_count,
        "trade_volume": entity.trade_volume,
        "volume_quote": entity.volume_quote,
    }
