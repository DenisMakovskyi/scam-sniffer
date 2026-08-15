from enum import IntEnum, StrEnum

class BinanceMappingKey(StrEnum):
    SYMBOL = "s"
    IS_CLOSED = "x"
    TIMEFRAME = "i"
    OPEN_TIME = "t"
    CLOSE_TIME = "T"
    EVENT_TIME = "E"
    OPEN_PRICE = "o"
    CLOSE_PRICE = "c"
    LOWEST_PRICE = "l"
    HIGHEST_PRICE = "h"
    TRADE_COUNT = "n"
    TRADE_VOLUME = "v"
    VOLUME_QUOTE = "q"

class BinanceMappingIndex(IntEnum):
    OPEN_TIME = 0
    CLOSE_TIME = 6
    OPEN_PRICE = 1
    CLOSE_PRICE = 4
    LOWEST_PRICE = 3
    HIGHEST_PRICE = 2
    TRADE_COUNT = 8
    TRADE_VOLUME = 5
    VOLUME_QUOTE = 7
