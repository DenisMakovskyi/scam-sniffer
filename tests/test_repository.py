from __future__ import annotations

from typing import cast
from decimal import Decimal
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from collections.abc import AsyncIterator, Sequence

import pytest

from scam_sniffer.data.api.stock.models import (
    MarketDto,
    TransportDto,
    CandleResponse,
    TimeframeResponse,
)
from scam_sniffer.data.api.stock.base import AbsStock
from scam_sniffer.data.database.dao.candle import CandleDao
from scam_sniffer.data.database.entities import CandleEntity
from scam_sniffer.domain.models import Candle, Market, Timeframe
from scam_sniffer.domain.repository.candle import CandleRepository
from scam_sniffer.data.repository.candle import CandleRepositoryImpl
from scam_sniffer.data.api.stock.errors import StockError, StockErrorReason
from scam_sniffer.domain.errors import DomainError, DomainErrorReason
from scam_sniffer.data.database.errors import DatabaseError, DatabaseErrorReason

class FakeStock:
    def __init__(
        self,
        responses: list[CandleResponse],
        error: StockError | None = None,
    ) -> None:
        self.error = error
        self.responses = responses
        self.calls: list[tuple[str, TimeframeResponse]] = []

    async def get_candles(
        self,
        symbol: str,
        k_limit: int,
        timeframe: TimeframeResponse,
        start_time: datetime,
        finish_time: datetime,
    ) -> list[CandleResponse]:
        self.calls.append((symbol, timeframe))
        if self.error is not None:
            raise self.error
        return self.responses

    async def stream_candles(
        self,
        symbol: str,
        timeframe: TimeframeResponse,
    ) -> AsyncIterator[CandleResponse]:
        self.calls.append((symbol, timeframe))
        if self.error is not None:
            raise self.error
        for response in self.responses:
            yield response

class FakeCandleDao:
    def __init__(
        self,
        entities: list[CandleEntity] | None = None,
        error: DatabaseError | None = None,
        is_persisted: bool = True,
    ) -> None:
        self.error = error
        self.entities = entities or []
        self.is_persisted = is_persisted
        self.stored: list[CandleEntity] = []

    async def select_range(
        self,
        market: str,
        symbol: str,
        timeframe: str,
        start_time: datetime,
        finish_time: datetime,
    ) -> list[CandleEntity]:
        if self.error is not None:
            raise self.error
        return self.entities

    async def select_latest_closed(
        self,
        market: str,
        symbol: str,
        timeframe: str,
    ) -> CandleEntity | None:
        if self.error is not None:
            raise self.error
        return self.entities[-1] if self.entities else None

    async def upsert_one(self, entity: CandleEntity) -> bool:
        if self.error is not None:
            raise self.error
        if not self.is_persisted:
            return False
        self.stored.append(entity)
        return True

    async def upsert_many(self, entities: Sequence[CandleEntity]) -> None:
        if self.error is not None:
            raise self.error
        self.stored.extend(entities)

@pytest.mark.asyncio
async def test_fetch_candles_maps_response_and_persists_entity() -> None:
    stock = FakeStock(responses=[_response(source=TransportDto.REST)])
    dao = FakeCandleDao()
    repository = _repository(stock=stock, dao=dao)

    candles = await repository.fetch_candles(
        symbol="BTCUSDT",
        k_limit=100,
        timeframe=Timeframe.M5,
        start_time=datetime(2025, 1, 1, tzinfo=UTC),
        finish_time=datetime(2025, 1, 2, tzinfo=UTC),
    )

    assert candles == [_candle()]
    assert dao.stored == [_entity()]
    assert stock.calls == [("BTCUSDT", TimeframeResponse.M5)]

@pytest.mark.asyncio
async def test_select_candles_maps_entities_to_domain() -> None:
    stock = FakeStock(responses=[])
    dao = FakeCandleDao(entities=[_entity()])
    repository = _repository(stock=stock, dao=dao)

    candles = await repository.select_candles(
        market=Market.BINANCE,
        symbol="BTCUSDT",
        timeframe=Timeframe.M5,
        start_time=datetime(2025, 1, 1, tzinfo=UTC),
        finish_time=datetime(2025, 1, 2, tzinfo=UTC),
    )
    latest_candle = await repository.select_latest_closed_candle(
        market=Market.BINANCE,
        symbol="BTCUSDT",
        timeframe=Timeframe.M5,
    )

    assert candles == [_candle()]
    assert latest_candle == _candle()

@pytest.mark.asyncio
async def test_stream_candles_persists_every_update() -> None:
    stock = FakeStock(responses=[_response(source=TransportDto.WS)])
    dao = FakeCandleDao()
    repository = _repository(stock=stock, dao=dao)

    candles = [
        candle
        async for candle in repository.stream_candles(
            symbol="BTCUSDT",
            timeframe=Timeframe.M5,
        )
    ]

    assert candles == [_candle()]
    assert dao.stored == [_entity()]

@pytest.mark.asyncio
async def test_stream_candles_skips_unpersisted_update() -> None:
    stock = FakeStock(responses=[_response(source=TransportDto.WS)])
    dao = FakeCandleDao(is_persisted=False)
    repository = _repository(stock=stock, dao=dao)

    candles = [
        candle
        async for candle in repository.stream_candles(
            symbol="BTCUSDT",
            timeframe=Timeframe.M5,
        )
    ]

    assert candles == []
    assert dao.stored == []

@pytest.mark.asyncio
async def test_fetch_candles_absorbs_stock_error() -> None:
    stock_error = StockError(
        reason=StockErrorReason.INVALID_RANGE,
        message="Invalid range",
        operation="get_candles",
    )
    repository = _repository(
        stock=FakeStock(responses=[], error=stock_error),
        dao=FakeCandleDao(),
    )

    with pytest.raises(DomainError) as error_info:
        await repository.fetch_candles(
            symbol="BTCUSDT",
            k_limit=100,
            timeframe=Timeframe.M5,
            start_time=datetime(2025, 1, 1, tzinfo=UTC),
            finish_time=datetime(2025, 1, 2, tzinfo=UTC),
        )

    error = error_info.value
    assert error.reason is DomainErrorReason.REMOTE
    assert error.root_cause is stock_error
    assert error.__cause__ is stock_error

@pytest.mark.asyncio
async def test_fetch_candles_absorbs_database_error() -> None:
    database_error = DatabaseError(
        reason=DatabaseErrorReason.QUERY,
        message="Query failed",
        operation="upsert_many",
    )
    repository = _repository(
        stock=FakeStock(responses=[_response(source=TransportDto.REST)]),
        dao=FakeCandleDao(error=database_error),
    )

    with pytest.raises(DomainError) as error_info:
        await repository.fetch_candles(
            symbol="BTCUSDT",
            k_limit=100,
            timeframe=Timeframe.M5,
            start_time=datetime(2025, 1, 1, tzinfo=UTC),
            finish_time=datetime(2025, 1, 2, tzinfo=UTC),
        )

    error = error_info.value
    assert error.reason is DomainErrorReason.STORAGE
    assert error.root_cause is database_error
    assert error.__cause__ is database_error

@pytest.mark.asyncio
async def test_select_candles_absorbs_mapping_error() -> None:
    invalid_entity = replace(_entity(), market="unknown")
    repository = _repository(
        stock=FakeStock(responses=[]),
        dao=FakeCandleDao(entities=[invalid_entity]),
    )

    with pytest.raises(DomainError) as error_info:
        await repository.select_candles(
            market=Market.BINANCE,
            symbol="BTCUSDT",
            timeframe=Timeframe.M5,
            start_time=datetime(2025, 1, 1, tzinfo=UTC),
            finish_time=datetime(2025, 1, 2, tzinfo=UTC),
        )

    error = error_info.value
    assert error.reason is DomainErrorReason.MAPPING
    assert isinstance(error.root_cause, ValueError)
    assert error.__cause__ is error.root_cause

def _candle() -> Candle:
    open_time = datetime(2025, 1, 1, tzinfo=UTC)
    return Candle(
        market=Market.BINANCE,
        symbol="BTCUSDT",
        is_closed=True,
        timeframe=Timeframe.M5,
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

def _entity() -> CandleEntity:
    candle = _candle()
    return CandleEntity(
        market=candle.market.value,
        symbol=candle.symbol,
        is_closed=candle.is_closed,
        timeframe=candle.timeframe.value,
        open_time=candle.open_time,
        close_time=candle.close_time,
        event_time=candle.event_time,
        open_price=candle.open_price,
        close_price=candle.close_price,
        lowest_price=candle.lowest_price,
        highest_price=candle.highest_price,
        trade_count=candle.trade_count,
        trade_volume=candle.trade_volume,
        volume_quote=candle.volume_quote,
    )

def _response(source: TransportDto) -> CandleResponse:
    candle = _candle()
    return CandleResponse(
        market=MarketDto(candle.market.value),
        source=source,
        symbol=candle.symbol,
        is_closed=candle.is_closed,
        timeframe=TimeframeResponse(candle.timeframe.value),
        open_time=candle.open_time,
        close_time=candle.close_time,
        event_time=candle.event_time,
        open_price=candle.open_price,
        close_price=candle.close_price,
        lowest_price=candle.lowest_price,
        highest_price=candle.highest_price,
        trade_count=candle.trade_count,
        trade_volume=candle.trade_volume,
        volume_quote=candle.volume_quote,
    )

def _repository(stock: FakeStock, dao: FakeCandleDao) -> CandleRepository:
    return CandleRepositoryImpl(
        dao=cast(CandleDao, dao),
        stock=cast(AbsStock, stock),
    )
