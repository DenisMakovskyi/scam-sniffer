"""Abstract contract shared by exchange market-data sources."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

import httpx

from http import HTTPStatus
from datetime import datetime

from scam_sniffer.data.api.client.errors import ApiError
from scam_sniffer.data.api.client.config import WsConfig
from scam_sniffer.data.api.client.client import ApiClient
from scam_sniffer.data.api.stock.errors import StockError, StockErrorReason
from scam_sniffer.data.api.stock.models import CandleResponse, TimeframeResponse

class AbsStock(ABC):
    """Expose exchange-independent candle retrieval and streaming operations."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        ws_config: WsConfig,
        max_attempts: int,
        rate_limit_codes: frozenset[HTTPStatus],
    ) -> None:
        """Initialize an exchange source around the shared API client.

        Args:
            client: Configured asynchronous HTTP client.
            ws_config: WebSocket transport configuration.
            max_attempts: Maximum number of HTTP attempts per request.
            rate_limit_codes: HTTP statuses that trigger rate-limit retries.

        Raises:
            StockError: If the shared API client cannot be initialized.
        """
        try:
            self._api_client = ApiClient(
                client=client,
                ws_config=ws_config,
                max_attempts=max_attempts,
                rate_limit_codes=rate_limit_codes,
            )
        except ApiError as error:
            raise StockError(
                reason=StockErrorReason.API_ERROR,
                message="Stock API client init failed",
                operation="init",
            ) from error

    async def close(self) -> None:
        """Close the exchange transport resources.

        Raises:
            StockError: If transport shutdown fails.
        """
        try:
            await self._api_client.close()
        except ApiError as error:
            raise StockError(
                reason=StockErrorReason.API_ERROR,
                message="Stock API client shutdown failed",
                operation="close",
            ) from error

    @abstractmethod
    async def get_candles(
        self,
        symbol: str,
        k_limit: int,
        timeframe: TimeframeResponse,
        start_time: datetime,
        finish_time: datetime,
    ) -> list[CandleResponse]:
        """Fetch candles inside a half-open time range.

        Args:
            symbol: Exchange trading pair symbol.
            k_limit: Maximum number of candles to return.
            timeframe: Exchange candle interval.
            start_time: Inclusive range boundary.
            finish_time: Exclusive range boundary.

        Returns:
            Candles returned by the exchange in chronological order.

        Raises:
            StockError: If request validation or remote retrieval fails.
        """
        ...

    @abstractmethod
    def stream_candles(
        self,
        symbol: str,
        timeframe: TimeframeResponse,
    ) -> AsyncIterator[CandleResponse]:
        """Stream live candle updates for a symbol and timeframe.

        Args:
            symbol: Exchange trading pair symbol.
            timeframe: Exchange candle interval.

        Yields:
            Candle snapshots in their original stream order.

        Raises:
            StockError: If the remote stream fails.
        """
        ...
