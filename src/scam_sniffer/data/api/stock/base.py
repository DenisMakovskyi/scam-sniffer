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
    def __init__(
        self,
        client: httpx.AsyncClient,
        ws_config: WsConfig,
        max_attempts: int,
        rate_limit_codes: frozenset[HTTPStatus],
    ) -> None:
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
                message="Stock API client initialization failed",
                operation="init",
                root_cause=error,
            ) from error

    async def close(self) -> None:
        try:
            await self._api_client.close()
        except ApiError as error:
            raise StockError(
                reason=StockErrorReason.API_ERROR,
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
    ) -> list[CandleResponse]: ...

    @abstractmethod
    def stream_candles(
        self,
        symbol: str,
        timeframe: TimeframeResponse,
    ) -> AsyncIterator[CandleResponse]: ...
