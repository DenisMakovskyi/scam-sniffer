from __future__ import annotations

from typing import Any

from http import HTTPStatus

import json
import httpx
import pytest

from scam_sniffer.data.api.client.errors import ApiError, ApiErrorReason
from scam_sniffer.data.api.client.config import WsConfig
from scam_sniffer.data.api.client.client import ApiClient

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

@pytest.mark.asyncio
async def test_get_returns_decoded_json() -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code=HTTPStatus.OK, json={"status": "ok"})

    http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://example.com",
    )
    api_client = _api_client(http_client)
    try:
        payload = await api_client.http_get(path="/status", params={})
    finally:
        await api_client.close()

    assert payload == {"status": "ok"}

@pytest.mark.asyncio
async def test_stream_rejects_invalid_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def connect(**_: Any) -> FakeWebSocket:
        return FakeWebSocket(["invalid-json"])

    monkeypatch.setattr("scam_sniffer.data.api.client.client.websockets.connect", connect)
    http_client = httpx.AsyncClient(base_url="https://example.com")
    api_client = _api_client(http_client)
    try:
        with pytest.raises(ApiError) as error_info:
            await anext(
                api_client.ws_stream(
                    stream_name="btcusdt@kline_5m",
                    event_parser=lambda event: event,
                )
            )
    finally:
        await api_client.close()

    assert error_info.value.reason is ApiErrorReason.INVALID_RESPONSE

@pytest.mark.asyncio
async def test_rate_limit_raises_typed_error() -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code=HTTPStatus.TOO_MANY_REQUESTS)

    http_client = httpx.AsyncClient(
        base_url="https://example.com",
        transport=httpx.MockTransport(handler),
    )
    api_client = _api_client(http_client)
    try:
        with pytest.raises(ApiError) as error_info:
            await api_client.http_get(path="/status", params={})
    finally:
        await api_client.close()

    assert error_info.value.reason is ApiErrorReason.RATE_LIMIT

@pytest.mark.asyncio
async def test_stream_delivers_config_and_parsed_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def connect(**kwargs: Any) -> FakeWebSocket:
        captured.update(kwargs)
        return FakeWebSocket([json.dumps({"value": 7})])

    monkeypatch.setattr("scam_sniffer.data.api.client.client.websockets.connect", connect)
    http_client = httpx.AsyncClient(base_url="https://example.com")
    api_client = _api_client(http_client)
    try:
        values = [
            value
            async for value in api_client.ws_stream(
                stream_name="btcusdt@kline_5m",
                event_parser=lambda event: int(event["value"]),
            )
        ]
    finally:
        await api_client.close()

    assert values == [7]
    assert captured == {
        "uri": "wss://example.com/ws/btcusdt@kline_5m",
        "max_queue": 1024,
        "ping_timeout": 20.0,
        "close_timeout": 10.0,
        "ping_interval": 20.0,
    }

def _api_client(client: httpx.AsyncClient) -> ApiClient:
    return ApiClient(
        client=client,
        ws_config=WsConfig(ws_url="wss://example.com/ws"),
        max_attempts=1,
        rate_limit_codes=frozenset({HTTPStatus.TOO_MANY_REQUESTS}),
    )
