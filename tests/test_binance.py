from __future__ import annotations

import json
from typing import Any
from http import HTTPStatus
from datetime import UTC, datetime

import httpx
import pytest

from scam_sniffer.data.api.stock.binance import BinanceStock
from scam_sniffer.data.api.client.config import ApiConfig, WsConfig
from scam_sniffer.data.api.client.errors import ApiError, ApiErrorReason
from scam_sniffer.data.api.stock.errors import StockError, StockErrorReason
from scam_sniffer.data.api.stock.models import TransportDto, TimeframeResponse

_API_CONFIG = ApiConfig(
    rest_url="https://fapi.binance.com",
    ws_config=WsConfig(ws_url="wss://fstream.binance.com/market/ws"),
    max_attempts=1,
)

class FakeWebSocket:
    def __init__(self, messages: list[str]) -> None:
        self._messages = iter(messages)

    async def __aenter__(self) -> FakeWebSocket:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    def __aiter__(self) -> FakeWebSocket:
        return self

    async def __anext__(self) -> str:
        try:
            return next(self._messages)
        except StopIteration as error:
            raise StopAsyncIteration from error

def test_init_absorbs_api_client_error_as_root_cause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def async_client(**_: Any) -> httpx.AsyncClient:
        raise ValueError("Invalid HTTP client configuration")

    monkeypatch.setattr("scam_sniffer.data.api.client.client.httpx.AsyncClient", async_client)

    with pytest.raises(StockError) as error_info:
        BinanceStock(config=_API_CONFIG)

    error = error_info.value
    assert error.reason is StockErrorReason.API
    assert isinstance(error.root_cause, ApiError)
    assert error.root_cause.reason is ApiErrorReason.CONF
    assert error.__cause__ is error.root_cause

@pytest.mark.asyncio
async def test_init_builds_valid_user_agent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    original_async_client = httpx.AsyncClient

    def async_client(**kwargs: Any) -> httpx.AsyncClient:
        captured.update(kwargs)
        return original_async_client()

    monkeypatch.setattr("scam_sniffer.data.api.client.client.httpx.AsyncClient", async_client)
    stock = BinanceStock(config=_API_CONFIG)
    await stock.close()

    headers = captured["headers"]
    user_agent = headers["User-Agent"]
    assert user_agent == user_agent.strip()
    assert user_agent.startswith("Mozilla/5.0")
    assert httpx.Headers(headers)["User-Agent"] == user_agent

@pytest.mark.asyncio
async def test_ws_config_reaches_api_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    event = {
        "e": "kline",
        "E": 1_735_689_899_999,
        "k": {
            "s": "BTCUSDT",
            "x": True,
            "i": "5m",
            "t": 1_735_689_600_000,
            "T": 1_735_689_899_999,
            "o": "100.0",
            "c": "102.0",
            "l": "99.0",
            "h": "103.0",
            "n": 42,
            "v": "12.5",
            "q": "1250.0",
        },
    }

    def connect(**kwargs: Any) -> FakeWebSocket:
        captured.update(kwargs)
        return FakeWebSocket([json.dumps(event)])

    monkeypatch.setattr("scam_sniffer.data.api.client.client.websockets.connect", connect)
    client = httpx.AsyncClient(base_url="https://fapi.binance.com")
    stock = BinanceStock(
        config=ApiConfig(
            rest_url="https://fapi.binance.com",
            ws_config=WsConfig(
                ws_url="wss://fstream.binance.com/market/ws",
                ws_queue_size=7,
                ws_ping_timeout=8.0,
                ws_ping_interval=9.0,
                ws_close_timeout=10.0,
            ),
            max_attempts=1,
        ),
        client=client,
    )
    try:
        candle = await anext(stock.stream_candles(symbol="BTCUSDT", timeframe=TimeframeResponse.M5))
    finally:
        await stock.close()

    assert candle.source is TransportDto.WS
    assert captured == {
        "uri": "wss://fstream.binance.com/market/ws/btcusdt@kline_5m",
        "max_queue": 7,
        "ping_timeout": 8.0,
        "close_timeout": 10.0,
        "ping_interval": 9.0,
    }

@pytest.mark.asyncio
async def test_stream_klines_absorbs_invalid_response_as_api_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def connect(**_: Any) -> FakeWebSocket:
        return FakeWebSocket([json.dumps({"e": "unexpected"})])

    monkeypatch.setattr("scam_sniffer.data.api.client.client.websockets.connect", connect)
    client = httpx.AsyncClient(base_url="https://fapi.binance.com")
    stock = BinanceStock(config=_API_CONFIG, client=client)
    try:
        with pytest.raises(StockError) as error_info:
            await anext(stock.stream_candles(symbol="BTCUSDT", timeframe=TimeframeResponse.M5))
    finally:
        await stock.close()

    error = error_info.value
    assert error.reason is StockErrorReason.API
    assert isinstance(error.root_cause, ApiError)
    assert error.root_cause.reason is ApiErrorReason.NEGOTIATION
    assert error.__cause__ is error.root_cause

@pytest.mark.asyncio
async def test_get_klines_maps_binance_response() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["symbol"] == "BTCUSDT"
        assert request.url.params["interval"] == "5m"
        assert request.url.params["limit"] == "1"
        return httpx.Response(
            status_code=HTTPStatus.OK,
            json=[
                [
                    1_735_689_600_000,
                    "100.0",
                    "103.0",
                    "99.0",
                    "102.0",
                    "12.5",
                    1_735_689_899_999,
                    "1_250.0",
                    42,
                    "0",
                    "0",
                ]
            ],
        )

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://fapi.binance.com",
    )
    stock = BinanceStock(config=_API_CONFIG, client=client)
    candles = await stock.get_candles(
        symbol="btcusdt",
        k_limit=1,
        timeframe=TimeframeResponse.M5,
        start_time=datetime(2025, 1, 1, tzinfo=UTC),
        finish_time=datetime(2025, 1, 2, tzinfo=UTC),
    )
    await stock.close()

    assert len(candles) == 1
    assert candles[0].symbol == "BTCUSDT"
    assert candles[0].source is TransportDto.REST
    assert candles[0].trade_count == 42
    assert candles[0].is_closed is True

@pytest.mark.asyncio
async def test_get_klines_absorbs_api_error_as_root_cause() -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code=HTTPStatus.TOO_MANY_REQUESTS)

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://fapi.binance.com",
    )
    stock = BinanceStock(config=_API_CONFIG, client=client)
    try:
        with pytest.raises(StockError) as error_info:
            await stock.get_candles(
                symbol="BTCUSDT",
                k_limit=1,
                timeframe=TimeframeResponse.M5,
                start_time=datetime(2025, 1, 1, tzinfo=UTC),
                finish_time=datetime(2025, 1, 2, tzinfo=UTC),
            )
    finally:
        await stock.close()

    error = error_info.value
    assert error.reason is StockErrorReason.API
    assert isinstance(error.root_cause, ApiError)
    assert error.root_cause.reason is ApiErrorReason.RATE_LIMIT
    assert error.__cause__ is error.root_cause

@pytest.mark.asyncio
async def test_get_klines_absorbs_invalid_response_as_api_error() -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code=HTTPStatus.OK, json={"invalid": "payload"})

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://fapi.binance.com",
    )
    stock = BinanceStock(config=_API_CONFIG, client=client)
    try:
        with pytest.raises(StockError) as error_info:
            await stock.get_candles(
                symbol="BTCUSDT",
                k_limit=1,
                timeframe=TimeframeResponse.M5,
                start_time=datetime(2025, 1, 1, tzinfo=UTC),
                finish_time=datetime(2025, 1, 2, tzinfo=UTC),
            )
    finally:
        await stock.close()

    error = error_info.value
    assert error.reason is StockErrorReason.API
    assert isinstance(error.root_cause, ApiError)
    assert error.root_cause.reason is ApiErrorReason.NEGOTIATION
    assert error.__cause__ is error.root_cause
