"""Binance Futures market-data source implementation."""

from __future__ import annotations

from typing import Any, override
from dataclasses import replace

from decimal import Decimal, InvalidOperation
from collections.abc import AsyncIterator

import httpx

from http import HTTPStatus
from datetime import UTC, datetime

from scam_sniffer.data.api.client.errors import ApiError, ApiErrorReason
from scam_sniffer.data.api.client.config import WsConfig
from scam_sniffer.data.api.stock.base import AbsStock
from scam_sniffer.data.api.stock.errors import StockError, StockErrorReason
from scam_sniffer.data.api.stock.models import (
    MarketDto,
    TransportDto,
    CandleResponse,
    TimeframeResponse,
)
from scam_sniffer.data.api.stock.mapping import BinanceMappingKey, BinanceMappingIndex

from scam_sniffer.utils.datetime import ms_to_sec

class BinanceStock(AbsStock):
    """Read historical and live USD-M futures candles from Binance."""

    __WS_URL = "wss://fstream.binance.com/ws"
    __REST_URL = "https://fapi.binance.com"
    __WS_CONFIG = WsConfig(ws_url=__WS_URL)
    __HEADER_USER_AGENT = "User-Agent"

    __API_PATH_KLINES = "/fapi/v1/klines"

    __PARAM_SYMBOL = "symbol"
    __PARAM_K_LIMIT = "limit"
    __PARAM_INTERVAL = "interval"
    __PARAM_START_TIME = "startTime"
    __PARAM_FINISH_TIME = "endTime"

    __USER_AGENT = (
        "Chrome/131.0.0.0 Safari/537.36"
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
    )
    __RATE_LIMIT_CODES = frozenset(
        {
            HTTPStatus.IM_A_TEAPOT,
            HTTPStatus.TOO_MANY_REQUESTS,
        }
    )

    def __init__(
        self,
        client: httpx.AsyncClient | None = None,
        rest_url: str = __REST_URL,
        ws_config: WsConfig = __WS_CONFIG,
        max_attempts: int = 5,
        timeout_seconds: float = 15.0,
    ) -> None:
        """Initialize a Binance Futures market-data source.

        Args:
            client: Optional preconfigured HTTP client used mainly for injection.
            rest_url: Binance Futures REST base URL.
            ws_config: Binance Futures WebSocket configuration.
            max_attempts: Maximum number of HTTP attempts per request.
            timeout_seconds: HTTP request timeout in seconds.

        Raises:
            StockError: If transport configuration or initialization fails.
        """
        if timeout_seconds <= 0:
            api_error = ApiError(
                reason=ApiErrorReason.CONF,
                message="API request timeout must be positive",
                operation="init",
            )
            raise StockError(
                reason=StockErrorReason.API_ERROR,
                message="Binance API client configuration is invalid",
                operation="init",
                root_cause=api_error,
            ) from api_error

        super().__init__(
            client=client or httpx.AsyncClient(
                headers={self.__HEADER_USER_AGENT: self.__USER_AGENT},
                timeout=timeout_seconds,
                base_url=rest_url.rstrip("/"),
            ),
            ws_config=ws_config,
            max_attempts=max_attempts,
            rate_limit_codes=self.__RATE_LIMIT_CODES,
        )

    @override
    async def get_candles(
        self,
        symbol: str,
        k_limit: int,
        timeframe: TimeframeResponse,
        start_time: datetime,
        finish_time: datetime,
    ) -> list[CandleResponse]:
        """Fetch Binance Futures candles inside a half-open time range.

        Args:
            symbol: Binance trading pair symbol.
            k_limit: Maximum number of candles, between one and 1,500.
            timeframe: Binance candle interval.
            start_time: Inclusive timezone-aware range boundary.
            finish_time: Exclusive timezone-aware range boundary.

        Returns:
            Parsed candles in Binance response order.

        Raises:
            StockError: If validation or remote retrieval fails.
        """
        _validate_klines_request(
            limit=k_limit,
            start_time=start_time,
            finish_time=finish_time,
        )

        try:
            start_time_ms = int(start_time.astimezone(UTC).timestamp() * 1000)
            finish_time_ms = int(finish_time.astimezone(UTC).timestamp() * 1000) - 1

            payload = await self._api_client.http_get(
                path=self.__API_PATH_KLINES,
                params={
                    self.__PARAM_SYMBOL: symbol.upper(),
                    self.__PARAM_K_LIMIT: k_limit,
                    self.__PARAM_INTERVAL: timeframe.value,
                    self.__PARAM_START_TIME: start_time_ms,
                    self.__PARAM_FINISH_TIME: finish_time_ms,
                },
            )
            return _rest_build_candles(
                rows=payload,
                symbol=symbol,
                timeframe=timeframe,
            )
        except ApiError as error:
            raise StockError(
                reason=StockErrorReason.API_ERROR,
                message="Binance kline request failed",
                operation="get_klines",
                root_cause=error,
            ) from error

    @override
    async def stream_candles(
        self,
        symbol: str,
        timeframe: TimeframeResponse,
    ) -> AsyncIterator[CandleResponse]:
        """Stream Binance Futures candle updates.

        Args:
            symbol: Binance trading pair symbol.
            timeframe: Binance candle interval.

        Yields:
            Parsed candle snapshots in stream order.

        Raises:
            StockError: If the Binance WebSocket stream fails.
        """
        stream_name = f"{symbol.lower()}@kline_{timeframe.value}"
        try:
            async for candle in self._api_client.ws_stream(
                stream_name=stream_name,
                event_parser=_ws_build_candle,
            ):
                yield candle
        except ApiError as error:
            raise StockError(
                reason=StockErrorReason.API_ERROR,
                message="Binance kline stream failed",
                operation="stream_klines",
                root_cause=error,
            ) from error

def _validate_klines_request(
    limit: int,
    start_time: datetime,
    finish_time: datetime,
) -> None:
    """Validate Binance kline request boundaries and result limit.

    Args:
        limit: Maximum number of requested candles.
        start_time: Inclusive range boundary.
        finish_time: Exclusive range boundary.

    Raises:
        StockError: If the limit or time range is invalid.
    """
    if not 1 <= limit <= 1500:
        raise StockError(
            reason=StockErrorReason.INVALID_LIMIT,
            message="Binance kline limit must be between 1 and 1500",
            operation="get_klines",
        )
    if start_time.tzinfo is None or finish_time.tzinfo is None:
        raise StockError(
            reason=StockErrorReason.INVALID_RANGE,
            message="Start and finish times must be timezone-aware",
            operation="get_klines",
        )
    if finish_time <= start_time:
        raise StockError(
            reason=StockErrorReason.INVALID_RANGE,
            message="Finish time must be after start time",
            operation="get_klines",
        )

def _ws_build_candle(event: dict[str, Any]) -> CandleResponse:
    """Build a candle response from a Binance WebSocket event.

    Args:
        event: Decoded Binance WebSocket event.

    Returns:
        Validated candle transport model.

    Raises:
        ApiError: If the event shape or a field value is invalid.
    """
    try:
        if event.get("e") != "kline":
            raise ValueError("Event is not a Binance kline event")
        kline = event["k"]
        return CandleResponse(
            market=MarketDto.BINANCE,
            source=TransportDto.WS,
            symbol=str(kline[BinanceMappingKey.SYMBOL]),
            is_closed=bool(kline[BinanceMappingKey.IS_CLOSED]),
            timeframe=TimeframeResponse(str(kline[BinanceMappingKey.TIMEFRAME])),
            open_time=datetime.fromtimestamp(
                tz=UTC,
                timestamp=ms_to_sec(kline[BinanceMappingKey.OPEN_TIME]),
            ),
            close_time=datetime.fromtimestamp(
                tz=UTC,
                timestamp=ms_to_sec(kline[BinanceMappingKey.CLOSE_TIME]) + 1,
            ),
            event_time=datetime.fromtimestamp(
                tz=UTC,
                timestamp=ms_to_sec(event[BinanceMappingKey.EVENT_TIME]),
            ),
            open_price=Decimal(str(kline[BinanceMappingKey.OPEN_PRICE])),
            close_price=Decimal(str(kline[BinanceMappingKey.CLOSE_PRICE])),
            lowest_price=Decimal(str(kline[BinanceMappingKey.LOWEST_PRICE])),
            highest_price=Decimal(str(kline[BinanceMappingKey.HIGHEST_PRICE])),
            trade_count=int(kline[BinanceMappingKey.TRADE_COUNT]),
            trade_volume=Decimal(str(kline[BinanceMappingKey.TRADE_VOLUME])),
            volume_quote=Decimal(str(kline[BinanceMappingKey.VOLUME_QUOTE])),
        )
    except (KeyError, TypeError, ValueError, InvalidOperation) as error:
        raise ApiError(
            reason=ApiErrorReason.NEGOTIATION,
            message="Binance returned an invalid kline event",
            operation="stream_klines",
        ) from error

def _rest_build_candle(
    row: list[Any],
    symbol: str,
    timeframe: TimeframeResponse,
) -> CandleResponse:
    """Build one candle response from a Binance REST row.

    Args:
        row: Positional Binance kline values.
        symbol: Requested Binance trading pair symbol.
        timeframe: Requested Binance candle interval.

    Returns:
        Validated candle transport model.
    """
    close_time = _rest_close_datetime(row)
    return CandleResponse(
        market=MarketDto.BINANCE,
        source=TransportDto.REST,
        symbol=symbol,
        is_closed=True,
        timeframe=timeframe,
        open_time=datetime.fromtimestamp(
            tz=UTC,
            timestamp=ms_to_sec(row[BinanceMappingIndex.OPEN_TIME]),
        ),
        close_time=close_time,
        event_time=None,
        open_price=Decimal(str(row[BinanceMappingIndex.OPEN_PRICE])),
        close_price=Decimal(str(row[BinanceMappingIndex.CLOSE_PRICE])),
        lowest_price=Decimal(str(row[BinanceMappingIndex.LOWEST_PRICE])),
        highest_price=Decimal(str(row[BinanceMappingIndex.HIGHEST_PRICE])),
        trade_count=int(row[BinanceMappingIndex.TRADE_COUNT]),
        trade_volume=Decimal(str(row[BinanceMappingIndex.TRADE_VOLUME])),
        volume_quote=Decimal(str(row[BinanceMappingIndex.VOLUME_QUOTE])),
    )

def _rest_build_candles(
    rows: Any,
    symbol: str,
    timeframe: TimeframeResponse,
) -> list[CandleResponse]:
    """Build candle responses from a Binance REST payload.

    Args:
        rows: Decoded Binance kline response.
        symbol: Requested Binance trading pair symbol.
        timeframe: Requested Binance candle interval.

    Returns:
        Validated candles in response order.

    Raises:
        ApiError: If the payload shape or a field value is invalid.
    """
    try:
        if not isinstance(rows, list):
            raise TypeError("Binance returned a non-list kline response")

        current_time = datetime.now(tz=UTC)
        return [
            replace(
                _rest_build_candle(row=row, symbol=symbol, timeframe=timeframe),
                is_closed=_rest_close_datetime(row) <= current_time,
            )
            for row in rows
        ]
    except (KeyError, TypeError, ValueError, InvalidOperation) as error:
        raise ApiError(
            reason=ApiErrorReason.NEGOTIATION,
            message="Binance returned an invalid kline payload",
            operation="get_klines",
        ) from error

def _rest_close_datetime(row: list[Any]) -> datetime:
    """Convert a Binance REST row close time to an exclusive UTC boundary.

    Args:
        row: Positional Binance kline values.

    Returns:
        Exclusive timezone-aware candle close boundary.

    Raises:
        ValueError: If the row contains too few fields.
    """
    if len(row) < 11:
        raise ValueError(f"Expected at least 11 kline fields, received {len(row)}")
    return datetime.fromtimestamp(
        tz=UTC,
        timestamp=ms_to_sec(row[BinanceMappingIndex.CLOSE_TIME]) + 1,
    )
