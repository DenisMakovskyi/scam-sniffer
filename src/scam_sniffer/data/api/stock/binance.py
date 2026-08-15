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
from scam_sniffer.data.api.stock.models import CandleResponse, MarketDto, TransportDto, TimeframeResponse
from scam_sniffer.data.api.stock.mapping import BinanceMappingKey, BinanceMappingIndex
from scam_sniffer.utils.datetime import ms_to_sec

class BinanceStock(AbsStock):
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
        if timeout_seconds <= 0:
            api_error = ApiError(
                reason=ApiErrorReason.INVALID_CONFIG,
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
                payload=payload,
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
            reason=ApiErrorReason.INVALID_RESPONSE,
            message="Binance returned an invalid kline event",
            operation="stream_klines",
        ) from error

def _rest_build_candles(
    payload: Any,
    symbol: str,
    timeframe: TimeframeResponse,
) -> list[CandleResponse]:
    try:
        if not isinstance(payload, list):
            raise TypeError("Binance returned a non-list kline response")

        current_time = datetime.now(tz=UTC)
        return [
            replace(
                _rest_build_candle(row=row, symbol=symbol, timeframe=timeframe),
                is_closed=_rest_close_time(row) <= current_time,
            )
            for row in payload
        ]
    except (KeyError, TypeError, ValueError, InvalidOperation) as error:
        raise ApiError(
            reason=ApiErrorReason.INVALID_RESPONSE,
            message="Binance returned an invalid kline payload",
            operation="get_klines",
        ) from error

def _rest_close_time(row: list[Any]) -> datetime:
    if len(row) < 11:
        raise ValueError(f"Expected at least 11 kline fields, received {len(row)}")
    return datetime.fromtimestamp(
        tz=UTC,
        timestamp=ms_to_sec(row[BinanceMappingIndex.CLOSE_TIME]) + 1,
    )

def _rest_build_candle(row: list[Any], *, symbol: str, timeframe: TimeframeResponse) -> CandleResponse:
    close_time = _rest_close_time(row)
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
