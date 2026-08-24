"""Shared asynchronous HTTP and WebSocket client."""

from __future__ import annotations

from typing import Any, TypeVar
from collections.abc import AsyncIterator, Callable

from http import HTTPStatus

import random
import asyncio

import json
import httpx
import websockets

from scam_sniffer.core.log.logger import get_logger

from scam_sniffer.data.api.client.config import ApiConfig
from scam_sniffer.data.api.client.errors import ApiError, ApiErrorReason

T = TypeVar("T")
_LOGGER = get_logger()

class ApiClient:
    """Execute remote transport operations with bounded HTTP retries."""

    def __init__(
        self,
        config: ApiConfig,
        client: httpx.AsyncClient | None,
        headers: dict[str, str],
        rate_limit_codes: frozenset[HTTPStatus],
    ) -> None:
        """Initialize the shared transport client.

        Args:
            config: HTTP and WebSocket transport configuration.
            client: Optional preconfigured asynchronous HTTP client.
            headers: HTTP headers used when building the client.
            rate_limit_codes: HTTP statuses that trigger rate-limit retries.

        Raises:
            ApiError: If the asynchronous HTTP client cannot be initialized.
        """
        try:
            self._client = client or httpx.AsyncClient(
                headers=headers,
                timeout=config.timeout_seconds,
                base_url=config.rest_url.rstrip("/"),
            )
        except (TypeError, ValueError) as error:
            raise ApiError(
                reason=ApiErrorReason.CONF,
                message="API client configuration is invalid",
                operation="init",
            ) from error

        self._ws_config = config.ws_config
        self._max_attempts = config.max_attempts
        self._max_retry_delay = config.max_retry_delay
        self._rate_limit_codes = rate_limit_codes

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        await self._client.aclose()
        _LOGGER.debug("API client closed")

    async def http_get(self, path: str, params: dict[str, Any]) -> Any:
        """Execute a JSON HTTP GET request with bounded retries.

        Args:
            path: Relative or absolute request path.
            params: Query parameters sent with the request.

        Returns:
            Decoded JSON response payload.

        Raises:
            ApiError: If the response is invalid, rate limited, or unavailable.
        """
        last_error: Exception | None = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                response = await self._client.get(url=path, params=params)
                if response.status_code in self._rate_limit_codes:
                    if attempt == self._max_attempts:
                        raise ApiError(
                            reason=ApiErrorReason.RATE_LIMIT,
                            message="API rate limit is exceeded after bounded retries",
                            operation="get",
                        )
                    retry_delay = _retry_delay(response=response, attempt=attempt)
                    _LOGGER.warning(
                        "API rate limit retry scheduled",
                        path=path,
                        delay=retry_delay,
                        attempt=attempt,
                        max_attempts=self._max_attempts,
                    )
                    await asyncio.sleep(retry_delay)
                    continue
                response.raise_for_status()
                return response.json()
            except ApiError:
                raise
            except ValueError as error:
                raise ApiError(
                    reason=ApiErrorReason.NEGOTIATION,
                    message="API returned invalid response",
                    operation="get",
                ) from error
            except (httpx.TimeoutException, httpx.NetworkError, httpx.HTTPStatusError) as error:
                last_error = error
                if attempt < self._max_attempts:
                    delay = min(self._max_retry_delay, 2 ** (attempt - 1))
                    retry_delay = delay + random.uniform(0, 0.25)
                    _LOGGER.warning(
                        "API request retry scheduled",
                        path=path,
                        delay=retry_delay,
                        attempt=attempt,
                        max_attempts=self._max_attempts,
                    )
                    await asyncio.sleep(retry_delay)

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
        """Stream parsed events from a WebSocket endpoint.

        Args:
            stream_name: Endpoint-specific WebSocket stream identifier.
            event_parser: Function that converts decoded JSON into a transport model.

        Yields:
            Parsed events in their original stream order.

        Raises:
            ApiError: If connection, decoding, or event parsing fails.
        """
        try:
            async with websockets.connect(
                uri=f"{self._ws_config.ws_url.rstrip('/')}/{stream_name}",
                max_queue=self._ws_config.ws_queue_size,
                ping_timeout=self._ws_config.ws_ping_timeout,
                close_timeout=self._ws_config.ws_close_timeout,
                ping_interval=self._ws_config.ws_ping_interval,
            ) as websocket:
                _LOGGER.info("API WebSocket connected", stream_name=stream_name)
                async for message in websocket:
                    try:
                        event = json.loads(message)
                    except (TypeError, json.JSONDecodeError) as error:
                        raise ApiError(
                            reason=ApiErrorReason.NEGOTIATION,
                            message="API returned an invalid WebSocket message",
                            operation="stream",
                        ) from error
                    yield event_parser(event)
                _LOGGER.warning("API WebSocket ended", stream_name=stream_name)
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
    """Return the server retry delay or an attempt-based fallback.

    Args:
        response: Rate-limited HTTP response.
        attempt: Current one-based attempt number.

    Returns:
        Delay in seconds before the next request attempt.
    """
    retry_after = response.headers.get("Retry-After")
    if retry_after is None:
        return float(attempt)
    try:
        return float(retry_after)
    except ValueError:
        return float(attempt)
