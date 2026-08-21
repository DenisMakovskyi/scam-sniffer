"""Configuration models for candle manager workflows."""

from dataclasses import dataclass

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
            ValueError: If a limit or reconnect delay is outside its valid range.
        """
        if not 1 <= self.batch_size <= 1_500:
            raise ValueError("Batch size must be between 1 and 1500")
        if self.backfill_size < 1:
            raise ValueError("Backfill size must be positive")
        if self.stream_retry_delay <= 0:
            raise ValueError("Stream retry delay must be positive")
        if self.stream_retry_max_delay < self.stream_retry_delay:
            raise ValueError("Stream maximum retry delay cannot be less than retry delay")
