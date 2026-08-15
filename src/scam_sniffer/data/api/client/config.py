from __future__ import annotations

from dataclasses import dataclass

from scam_sniffer.data.api.client.errors import ApiError, ApiErrorReason

@dataclass(frozen=True, slots=True)
class WsConfig:
    ws_url: str
    ws_queue_size: int = 1024
    ws_ping_timeout: float = 20.0
    ws_ping_interval: float = 20.0
    ws_close_timeout: float = 10.0

    def __post_init__(self) -> None:
        if not self.ws_url.strip():
            raise ApiError(
                reason=ApiErrorReason.INVALID_CONFIG,
                message="WebSocket URL cannot be empty",
                operation="init",
            )
        if self.ws_queue_size < 1:
            raise ApiError(
                reason=ApiErrorReason.INVALID_CONFIG,
                message="WebSocket queue size must be positive",
                operation="init",
            )
        if min(self.ws_ping_timeout, self.ws_ping_interval, self.ws_close_timeout) <= 0:
            raise ApiError(
                reason=ApiErrorReason.INVALID_CONFIG,
                message="WebSocket timeouts must be positive",
                operation="init",
            )
