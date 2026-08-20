from scam_sniffer.data.api.stock.models import CandleResponse
from scam_sniffer.data.database.entities import CandleEntity
from scam_sniffer.domain.models import Candle, Market, Timeframe

def dto_to_candle(response: CandleResponse) -> Candle:
    return Candle(
        market=Market(response.market.value),
        symbol=response.symbol,
        is_closed=response.is_closed,
        timeframe=Timeframe(response.timeframe.value),
        open_time=response.open_time,
        close_time=response.close_time,
        event_time=response.event_time,
        open_price=response.open_price,
        close_price=response.close_price,
        lowest_price=response.lowest_price,
        highest_price=response.highest_price,
        trade_count=response.trade_count,
        trade_volume=response.trade_volume,
        volume_quote=response.volume_quote,
    )

def entity_to_candle(entity: CandleEntity) -> Candle:
    return Candle(
        market=Market(entity.market),
        symbol=entity.symbol,
        is_closed=entity.is_closed,
        timeframe=Timeframe(entity.timeframe),
        open_time=entity.open_time,
        close_time=entity.close_time,
        event_time=entity.event_time,
        open_price=entity.open_price,
        close_price=entity.close_price,
        lowest_price=entity.lowest_price,
        highest_price=entity.highest_price,
        trade_count=entity.trade_count,
        trade_volume=entity.trade_volume,
        volume_quote=entity.volume_quote,
    )

def candle_to_entity(candle: Candle) -> CandleEntity:
    return CandleEntity(
        market=candle.market.value,
        symbol=candle.symbol,
        is_closed=candle.is_closed,
        timeframe=candle.timeframe.value,
        open_time=candle.open_time,
        close_time=candle.close_time,
        event_time=candle.event_time,
        open_price=candle.open_price,
        close_price=candle.close_price,
        lowest_price=candle.lowest_price,
        highest_price=candle.highest_price,
        trade_count=candle.trade_count,
        trade_volume=candle.trade_volume,
        volume_quote=candle.volume_quote,
    )
