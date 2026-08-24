UPDATE candles AS candle
SET close_time = candle.open_time + candle_timeframe.duration
FROM (
    VALUES
        ('5m', INTERVAL '5 minutes'),
        ('15m', INTERVAL '15 minutes'),
        ('1h', INTERVAL '1 hour')
) AS candle_timeframe(name, duration)
WHERE candle.timeframe = candle_timeframe.name AND candle.close_time <> candle.open_time + candle_timeframe.duration;
