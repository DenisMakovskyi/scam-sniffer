"""Datetime and timestamp conversion utilities."""

from decimal import Decimal
from datetime import UTC, datetime

_MS_PER_SEC = 1000

def now_utc() -> datetime:
    """Return the current timezone-aware UTC datetime."""
    return datetime.now(tz=UTC)

def ms_to_sec(millis: int | float | Decimal) -> float:
    """Convert milliseconds to floating-point seconds.

    Args:
        millis: Timestamp or duration expressed in milliseconds.

    Returns:
        Equivalent number of seconds.
    """
    return float(millis) / _MS_PER_SEC

def from_timestamp(timestamp: int | float | Decimal) -> datetime:
    """Convert a millisecond timestamp to a timezone-aware UTC datetime.

    Args:
        timestamp: Timestamp expressed in milliseconds since the epoch.
    """
    return datetime.fromtimestamp(tz=UTC, timestamp=ms_to_sec(timestamp))
