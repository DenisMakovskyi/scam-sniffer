"""Configuration models for candle manager workflows."""

from dataclasses import dataclass

from scam_sniffer.domain.errors import ManagerError, ManagerErrorReason

@dataclass(frozen=True, slots=True)
class CandleManagerConfig:
    """Configure candle synchronization limits and stream reconnect delays.

    Attributes:
        batch_size: Maximum number of candles fetched in one request.
        backfill_size: Number of candles loaded into an empty series.
        stream_retry_delay: Initial stream reconnect delay in seconds.
        stream_retry_max_delay: Maximum stream reconnect delay in seconds.
    """

    batch_size: int = 1_500
    backfill_size: int = 10_000
    stream_retry_delay: float = 1.0
    stream_retry_max_delay: float = 30.0

    def __post_init__(self) -> None:
        """Validate synchronization limits and reconnect delays.

        Raises:
            ManagerError: If a limit or reconnect delay is outside its valid range.
        """
        if not 1 <= self.batch_size <= 1_500:
            raise ManagerError(
                reason=ManagerErrorReason.CONF,
                message=f"Batch size must be between 1 and 1500",
                operation="init",
            )
        if self.backfill_size < 1:
            raise ManagerError(
                reason=ManagerErrorReason.CONF,
                message=f"Backfill size must be positive",
                operation="init",
            )
        if self.stream_retry_delay <= 0:
            raise ManagerError(
                reason=ManagerErrorReason.CONF,
                message=f"Stream retry delay must be positive",
                operation="init",
            )
        if self.stream_retry_max_delay < self.stream_retry_delay:
            raise ManagerError(
                reason=ManagerErrorReason.CONF,
                message=f"Stream maximum retry delay cannot be less than retry delay",
                operation="init",
            )
