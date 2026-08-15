CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE candles (
    market TEXT NOT NULL,
    symbol TEXT NOT NULL,
    is_closed BOOLEAN NOT NULL,
    timeframe TEXT NOT NULL,
    open_time TIMESTAMPTZ NOT NULL,
    close_time TIMESTAMPTZ NOT NULL,
    event_time TIMESTAMPTZ,
    open_price NUMERIC NOT NULL,
    close_price NUMERIC NOT NULL,
    lowest_price NUMERIC NOT NULL,
    highest_price NUMERIC NOT NULL,
    trade_count BIGINT,
    trade_volume NUMERIC NOT NULL,
    volume_quote NUMERIC,
    PRIMARY KEY (market, symbol, timeframe, open_time),
    CONSTRAINT candles_symbol_not_empty CHECK (BTRIM(symbol) <> ''),
    CONSTRAINT candles_time_range_valid CHECK (close_time > open_time),
    CONSTRAINT candles_highest_price_valid CHECK (
        highest_price >= GREATEST(open_price, close_price, lowest_price)
    ),
    CONSTRAINT candles_lowest_price_valid CHECK (
        lowest_price <= LEAST(open_price, close_price, highest_price)
    ),
    CONSTRAINT candles_trade_volume_valid CHECK (trade_volume >= 0),
    CONSTRAINT candles_volume_quote_valid CHECK (volume_quote IS NULL OR volume_quote >= 0),
    CONSTRAINT candles_trade_count_valid CHECK (trade_count IS NULL OR trade_count >= 0)
);

SELECT create_hypertable(
    'candles',
    by_range('open_time', INTERVAL '7 days'),
    if_not_exists => TRUE
);
