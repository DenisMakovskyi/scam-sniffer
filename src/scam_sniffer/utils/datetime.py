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
