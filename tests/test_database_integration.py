from __future__ import annotations

import os

from decimal import Decimal
from datetime import UTC, datetime, timedelta

import pytest

from scam_sniffer.data.database.engine import DatabaseConfig, DatabaseEngine
from scam_sniffer.data.database.entities import CandleEntity
from scam_sniffer.data.database.dao.candle import CandleDao

_DATABASE_URL_KEY = "SCAM_SNIFFER_TEST_DATABASE_URL"

@pytest.mark.integration
@pytest.mark.asyncio
async def test_timescale_migration_and_candle_upsert() -> None:
    database_url = os.environ.get(_DATABASE_URL_KEY)
    if database_url is None:
        pytest.skip(f"{_DATABASE_URL_KEY} is not configured")

    engine = DatabaseEngine(DatabaseConfig(dsn=database_url))
    await engine.connect()
    dao = CandleDao(pool=engine.pool)

    open_time = datetime(2025, 1, 1, tzinfo=UTC)
    open_candle = _entity(
        open_time=open_time,
        close_price=Decimal("101.0"),
        event_time=open_time + timedelta(minutes=1),
        is_closed=False,
    )
    closed_candle = _entity(
        open_time=open_time,
        close_price=Decimal("102.0"),
        event_time=open_time + timedelta(minutes=5),
        is_closed=True,
    )

    is_migrated = False
    try:
        await engine.migrate()
        is_migrated = True
        is_hypertable = await engine.pool.fetchval(
            """
            SELECT EXISTS (
                SELECT 1
                FROM timescaledb_information.hypertables
                WHERE hypertable_name = 'candles'
            )
            """
        )
        await dao.delete(
            market=open_candle.market,
            symbol=open_candle.symbol,
            timeframe=open_candle.timeframe,
            open_time=open_candle.open_time,
        )
        assert await dao.upsert_one(open_candle) is True
        assert await dao.upsert_one(closed_candle) is True
        assert await dao.upsert_one(open_candle) is False

        stored = await dao.select(
            market=closed_candle.market,
            symbol=closed_candle.symbol,
            timeframe=closed_candle.timeframe,
            open_time=closed_candle.open_time,
        )

        assert is_hypertable is True
        assert stored == closed_candle
    finally:
        if is_migrated:
            await dao.delete(
                market=closed_candle.market,
                symbol=closed_candle.symbol,
                timeframe=closed_candle.timeframe,
                open_time=closed_candle.open_time,
            )
        await engine.close()

def _entity(
    open_time: datetime,
    close_price: Decimal,
    event_time: datetime,
    is_closed: bool,
) -> CandleEntity:
    return CandleEntity(
        market="binance",
        symbol="TESTBTCUSDT",
        is_closed=is_closed,
        timeframe="5m",
        open_time=open_time,
        close_time=open_time + timedelta(minutes=5),
        event_time=event_time,
        open_price=Decimal("100.0"),
        close_price=close_price,
        lowest_price=Decimal("99.0"),
        highest_price=Decimal("103.0"),
        trade_count=42,
        trade_volume=Decimal("12.5"),
        volume_quote=Decimal("1250.0"),
    )
