"""Abstract contract shared by exchange market-data sources."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from http import HTTPStatus
from datetime import datetime

import httpx

from scam_sniffer.data.api.client.errors import ApiError
from scam_sniffer.data.api.client.config import ApiConfig
from scam_sniffer.data.api.client.client import ApiClient
from scam_sniffer.data.api.stock.errors import StockError, StockErrorReason
from scam_sniffer.data.api.stock.models import CandleResponse, TimeframeResponse

class AbsStock(ABC):
    """Expose exchange-independent candle retrieval and streaming operations."""

    def __init__(
        self,
        config: ApiConfig,
        client: httpx.AsyncClient | None,
        headers: dict[str, str],
        rate_limit_codes: frozenset[HTTPStatus],
    ) -> None:
        """Initialize an exchange source around the shared API client.

        Args:
            config: Shared HTTP and WebSocket transport configuration.
            client: Optional preconfigured asynchronous HTTP client.
            headers: Exchange-specific HTTP headers.
            rate_limit_codes: HTTP statuses that trigger rate-limit retries.

        Raises:
            StockError: If the shared API client cannot be initialized.
        """
        try:
            self._api_client = ApiClient(
                config=config,
                client=client,
                headers=headers,
                rate_limit_codes=rate_limit_codes,
            )
        except ApiError as error:
            raise StockError(
                reason=StockErrorReason.API,
                message="Stock API client init failed",
                operation="init",
                root_cause=error,
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
                reason=StockErrorReason.API,
                message="Stock API client shutdown failed",
                operation="close",
                root_cause=error,
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
