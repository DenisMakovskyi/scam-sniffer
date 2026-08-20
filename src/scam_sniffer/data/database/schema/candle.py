"""SQL statements for candle persistence and retrieval."""

from __future__ import annotations

TABLE_CANDLE = "candles"
COLUMNS_CANDLE = """
market,
symbol,
is_closed,
timeframe,
open_time,
close_time,
event_time,
open_price,
close_price,
lowest_price,
highest_price,
trade_count,
trade_volume,
volume_quote
"""

CANDLE_SELECT = (f"\n"
                 f"SELECT {COLUMNS_CANDLE}\n"
                 f"FROM {TABLE_CANDLE}\n"
                 f"WHERE market = $1\n"
                 f"  AND symbol = $2\n"
                 f"  AND timeframe = $3\n"
                 f"  AND open_time = $4\n")

CANDLE_SELECT_RANGE = (f"\n"
                       f"SELECT {COLUMNS_CANDLE}\n"
                       f"FROM {TABLE_CANDLE}\n"
                       f"WHERE market = $1\n"
                       f"  AND symbol = $2\n"
                       f"  AND timeframe = $3\n"
                       f"  AND open_time >= $4\n"
                       f"  AND open_time < $5\n"
                       f"ORDER BY open_time\n")

CANDLE_SELECT_LATEST = (f"\n"
                        f"SELECT {COLUMNS_CANDLE}\n"
                        f"FROM {TABLE_CANDLE}\n"
                        f"WHERE market = $1\n"
                        f"  AND symbol = $2\n"
                        f"  AND timeframe = $3\n"
                        f"ORDER BY open_time DESC\n"
                        f"LIMIT 1\n")

CANDLE_SELECT_LATEST_CLOSED = (f"\n"
                               f"SELECT {COLUMNS_CANDLE}\n"
                               f"FROM {TABLE_CANDLE}\n"
                               f"WHERE market = $1\n"
                               f"  AND symbol = $2\n"
                               f"  AND timeframe = $3\n"
                               f"  AND is_closed\n"
                               f"ORDER BY open_time DESC\n"
                               f"LIMIT 1\n")

CANDLE_CREATE = (f"\n"
                 f"INSERT INTO {TABLE_CANDLE} (\n"
                 f"    {COLUMNS_CANDLE}\n"
                 f")\n"
                 f"VALUES (\n"
                 f"    $1, $2, $3, $4, $5, $6, $7,\n"
                 f"    $8, $9, $10, $11, $12, $13, $14\n"
                 f")\n")

CANDLE_UPSERT = (f"\n"
                 f"{CANDLE_CREATE}\n"
                 f"ON CONFLICT (market, symbol, timeframe, open_time)\n"
                 f"DO UPDATE SET\n"
                 f"    is_closed = EXCLUDED.is_closed,\n"
                 f"    close_time = EXCLUDED.close_time,\n"
                 f"    event_time = EXCLUDED.event_time,\n"
                 f"    open_price = EXCLUDED.open_price,\n"
                 f"    close_price = EXCLUDED.close_price,\n"
                 f"    lowest_price = EXCLUDED.lowest_price,\n"
                 f"    highest_price = EXCLUDED.highest_price,\n"
                 f"    trade_count = EXCLUDED.trade_count,\n"
                 f"    trade_volume = EXCLUDED.trade_volume,\n"
                 f"    volume_quote = EXCLUDED.volume_quote\n"
                 f"WHERE NOT {TABLE_CANDLE}.is_closed\n"
                 f"  AND (\n"
                 f"      EXCLUDED.is_closed\n"
                 f"      OR (\n"
                 f"          EXCLUDED.event_time IS NOT NULL\n"
                 f"          AND (\n"
                 f"              {TABLE_CANDLE}.event_time IS NULL\n"
                 f"              OR EXCLUDED.event_time >= {TABLE_CANDLE}.event_time\n"
                 f"          )\n"
                 f"      )\n"
                 f"  )\n")

CANDLE_UPDATE = (f"\n"
                 f"UPDATE {TABLE_CANDLE}\n"
                 f"SET is_closed = $3,\n"
                 f"    close_time = $6,\n"
                 f"    event_time = $7,\n"
                 f"    open_price = $8,\n"
                 f"    close_price = $9,\n"
                 f"    lowest_price = $10,\n"
                 f"    highest_price = $11,\n"
                 f"    trade_count = $12,\n"
                 f"    trade_volume = $13,\n"
                 f"    volume_quote = $14\n"
                 f"WHERE market = $1\n"
                 f"  AND symbol = $2\n"
                 f"  AND timeframe = $4\n"
                 f"  AND open_time = $5\n")

CANDLE_DELETE = (f"\n"
                 f"DELETE FROM {TABLE_CANDLE}\n"
                 f"WHERE market = $1\n"
                 f"  AND symbol = $2\n"
                 f"  AND timeframe = $3\n"
                 f"  AND open_time = $4\n")
