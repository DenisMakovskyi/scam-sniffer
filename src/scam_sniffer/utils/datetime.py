from decimal import Decimal

_MS_PER_SEC = 1000

def ms_to_sec(millis: int | float | Decimal) -> float:
    return float(millis) / _MS_PER_SEC
