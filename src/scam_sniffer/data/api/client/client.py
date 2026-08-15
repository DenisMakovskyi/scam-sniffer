from __future__ import annotations

from typing import Any, TypeVar
from collections.abc import AsyncIterator, Callable

import json
import httpx
import random
import asyncio
import websockets

from http import HTTPStatus

from scam_sniffer.data.api.client.errors import ApiError, ApiErrorReason
from scam_sniffer.data.api.client.config import WsConfig

T = TypeVar("T")

class ApiClient:
    __MAX_RETRY_DELAY_SECONDS = 30.0

    def __init__(
        self,
        client: httpx.AsyncClient,
        ws_config: WsConfig,
        max_attempts: int,
        rate_limit_codes: frozenset[HTTPStatus],
    ) -> None:
        if max_attempts < 1:
            raise ApiError(
                reason=ApiErrorReason.INVALID_CONFIG,
                message="API retry count must be positive",
                operation="init",
            )

        self._client = client
        self._ws_config = ws_config
        self._max_attempts = max_attempts
        self._rate_limit_codes = rate_limit_codes

    async def close(self) -> None:
        await self._client.aclose()

    async def http_get(self, path: str, params: dict[str, Any]) -> Any:
        last_error: Exception | None = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                response = await self._client.get(url=path, params=params)
                if response.status_code in self._rate_limit_codes:
                    if attempt == self._max_attempts:
                        raise ApiError(
                            reason=ApiErrorReason.RATE_LIMIT,
                            message="API rate limit remained active after bounded retries",
                            operation="get",
                        )
                    await asyncio.sleep(_retry_delay(response=response, attempt=attempt))
                    continue
                response.raise_for_status()
                return response.json()
            except ApiError:
                raise
            except ValueError as error:
                raise ApiError(
                    reason=ApiErrorReason.INVALID_RESPONSE,
                    message="API returned invalid response metadata or JSON",
                    operation="get",
                ) from error
            except (httpx.TimeoutException, httpx.NetworkError, httpx.HTTPStatusError) as error:
                last_error = error
                if attempt < self._max_attempts:
                    delay = min(self.__MAX_RETRY_DELAY_SECONDS, 2 ** (attempt - 1))
                    await asyncio.sleep(delay + random.uniform(0, 0.25))

        raise ApiError(
            reason=ApiErrorReason.CONNECTION,
            message="API request failed after bounded retries",
            operation="get",
        ) from last_error

    async def ws_stream(
        self,
        stream_name: str,
        event_parser: Callable[[Any], T],
    ) -> AsyncIterator[T]:
        try:
            async with websockets.connect(
                uri=f"{self._ws_config.ws_url.rstrip('/')}/{stream_name}",
                max_queue=self._ws_config.ws_queue_size,
                ping_timeout=self._ws_config.ws_ping_timeout,
                close_timeout=self._ws_config.ws_close_timeout,
                ping_interval=self._ws_config.ws_ping_interval,
            ) as websocket:
                async for message in websocket:
                    try:
                        event = json.loads(message)
                    except (TypeError, json.JSONDecodeError) as error:
                        raise ApiError(
                            reason=ApiErrorReason.INVALID_RESPONSE,
                            message="API returned an invalid WebSocket message",
                            operation="stream",
                        ) from error
                    yield event_parser(event)
        except asyncio.CancelledError:
            raise
        except ApiError:
            raise
        except (OSError, websockets.WebSocketException) as error:
            raise ApiError(
                reason=ApiErrorReason.CONNECTION,
                message="API WebSocket connection failed",
                operation="stream",
            ) from error

def _retry_delay(response: httpx.Response, attempt: int) -> float:
    retry_after = response.headers.get("Retry-After")
    if retry_after is None:
        return float(attempt)
    try:
        return float(retry_after)
    except ValueError:
        return float(attempt)
