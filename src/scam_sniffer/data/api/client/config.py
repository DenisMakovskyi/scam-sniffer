"""Configuration models for shared API transports."""

from __future__ import annotations

from dataclasses import dataclass

from scam_sniffer.data.api.client.errors import ApiError, ApiErrorReason

@dataclass(frozen=True, slots=True)
class WsConfig:
    """Configure WebSocket connection limits and heartbeat timing.

    Attributes:
        ws_url: Base WebSocket endpoint URL.
        ws_queue_size: Maximum number of frames buffered by the client.
        ws_ping_timeout: Seconds to wait for a pong response.
        ws_ping_interval: Seconds between heartbeat pings.
        ws_close_timeout: Seconds to wait for graceful connection closure.
    """

    ws_url: str
    ws_queue_size: int = 1024
    ws_ping_timeout: float = 20.0
    ws_ping_interval: float = 20.0
    ws_close_timeout: float = 10.0

    def __post_init__(self) -> None:
        """Validate WebSocket endpoint and timing values.

        Raises:
            ApiError: If the URL is empty or a numeric setting is not positive.
        """
        if not self.ws_url.strip():
            raise ApiError(
                reason=ApiErrorReason.CONF,
                message="WebSocket URL cannot be empty",
                operation="init",
            )
        if self.ws_queue_size < 1:
            raise ApiError(
                reason=ApiErrorReason.CONF,
                message="WebSocket queue size must be positive",
                operation="init",
            )
        if min(self.ws_ping_timeout, self.ws_ping_interval, self.ws_close_timeout) <= 0:
            raise ApiError(
                reason=ApiErrorReason.CONF,
                message="WebSocket timeouts must be positive",
                operation="init",
            )
