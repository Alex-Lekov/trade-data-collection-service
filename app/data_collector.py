import asyncio
import sys
from loguru import logger
from cryptofeed import FeedHandler
from decimal import Decimal
from cryptofeed.exchanges import Binance
from cryptofeed.defines import CANDLES
from datetime import datetime
import time

import aioch
from aioch import Client as AIOClickHouseClient
import clickhouse_driver
import yaml
#from asynch import connect


logger.remove()
logger.add(
    sys.stderr,
    colorize=True,
    format="<green>{time:HH:mm:ss:ms}</green> | <level>{message}</level>",
    level=10,
)

logger.add("./logs/data_collector.log", rotation="1 MB", level="DEBUG", compression="zip")


def create_database_if_not_exists() -> None:
    '''Creates the binance_data database if it doesn't exist.'''
    with clickhouse_driver.Client(host='clickhouse', port=9000) as ch:
        ch.execute('CREATE DATABASE IF NOT EXISTS binance_data')
        logger.info('CREATE DATABASE IF NOT EXISTS binance_data')

def create_table_if_not_exists() -> None:
    '''Creates the binance_data.candles table if it doesn't exist.'''
    query = '''
        CREATE TABLE IF NOT EXISTS binance_data.candles (
            exchange String,
            symbol String,
            start DateTime,
            stop DateTime,
            close_unixtime Float32,
            interval String,
            trades Int32,
            open Float32,
            close Float32,
            high Float32,
            low Float32,
            volume Float64,
            timestamp DateTime,
            receipt_timestamp DateTime
        ) ENGINE = ReplacingMergeTree(receipt_timestamp)
        ORDER BY (symbol, interval, start)
    '''
    with clickhouse_driver.Client(host='clickhouse', port=9000) as ch:
        ch.execute(query)
        logger.info('CREATE TABLE IF NOT EXISTS binance_data.candles')

# -------- Sharding helpers and symbols watcher (hot-add only; removal via shard-restart to be added) --------
def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


async def symbols_watcher(handler: FeedHandler, symbol_type: str, timeframe: str, known_symbols: set, interval_sec: int = 60):
    """
    Periodically polls available symbols and hot-adds newly listed ones as shard feeds to the existing FeedHandler.
    De-listing is only logged here; safe removal will be done via partial shard restart (not implemented in this watcher).

    Args:
        handler: Active FeedHandler instance.
        symbol_type: Filter substring for symbols (e.g. '-USDT-PERP').
        timeframe: Candle interval (e.g. '1m').
        known_symbols: Mutable set of currently subscribed symbols, will be updated in-place.
        interval_sec: Poll interval in seconds.
    """
    callbacks = {CANDLES: candle_callback}
    while True:
        try:
            current = Binance.symbols()
            current = [s for s in current if symbol_type in s]

            current_set = set(current)
            added = list(current_set.difference(known_symbols))
            removed = list(known_symbols.difference(current_set))

            if added:
                logger.info(f'Watcher: detected {len(added)} newly listed symbols. Hot-adding shards.')
                # Add in shards up to 100 symbols per feed
                for idx, chunk in enumerate(chunks(sorted(added), 100)):
                    logger.info(f'Watcher: add shard with {len(chunk)} symbols')
                    handler.add_feed(Binance(symbols=chunk, channels=[CANDLES], callbacks=callbacks, candle_interval=timeframe, candle_closed_only=True))
                known_symbols.update(added)

            if removed:
                # We only log here; actual unsubscribe is not uniformly supported.
                # Plan: partial restart of affected shard(s) will handle removals.
                logger.warning(f'Watcher: detected {len(removed)} delisted symbols. Will require partial shard restart to remove.')
                # Update local view to avoid repeated logging; stream may still receive data until shard restart occurs.
                for s in removed:
                    if s in known_symbols:
                        known_symbols.remove(s)
        except Exception as e:
            logger.error(f'Watcher error: {e}')

        await asyncio.sleep(interval_sec)


async def candle_callback(candle, receipt_timestamp) -> None:
    """Callback function that stores candle data into ClickHouse.

    Args:
        candle: The candle data.
        receipt_timestamp: The receipt timestamp.

    """
    #logger.info(candle)

    exchange = candle.exchange
    symbol = candle.symbol
    start = datetime.fromtimestamp(candle.start)
    stop = datetime.fromtimestamp(candle.stop)
    interval = candle.interval
    trades = candle.trades
    open_price = Decimal(candle.open)
    close_price = Decimal(candle.close)
    high_price = Decimal(candle.high)
    low_price = Decimal(candle.low)
    volume = Decimal(candle.volume)
    #closed = (candle.closed)
    timestamp = datetime.fromtimestamp(candle.timestamp)
    #receipt_timestamp = receipt_timestamp
    
    query = '''
        INSERT INTO binance_data.candles 
        (exchange, symbol, start, stop, close_unixtime, interval, trades, open, close, high, low, volume, timestamp, receipt_timestamp)
        VALUES 
        (%(exchange)s, %(symbol)s, %(start)s, %(stop)s, %(close_unixtime)s, %(interval)s, %(trades)s, %(open)s, %(close)s, %(high)s, %(low)s, %(volume)s, %(timestamp)s, %(receipt_timestamp)s)
    '''

    ch = AIOClickHouseClient(host='clickhouse', port=9000,)
    await ch.execute(query, {'exchange': exchange, 'symbol': symbol, 'start': start, 'stop': stop, 'close_unixtime': candle.stop, 'interval': interval, 'trades': trades, 'open': open_price, 'close': close_price, 'high': high_price, 'low': low_price, 'volume': volume, 'timestamp': timestamp, 'receipt_timestamp': receipt_timestamp})


# symbols_callback removed: dynamic symbol management will be handled by a dedicated watcher/manager


if __name__ == '__main__':
    logger.info(f'Delay start by 10sec so that BD are ready')
    time.sleep(10)
    logger.info(f'start')

    ####### LOAD CONFIG #########################################################
    with open("config.yaml", 'r') as ymlfile:
        config = yaml.load(ymlfile, Loader=yaml.SafeLoader)

    SYMBOLS_TYPE = config['SYMBOLS_TYPE']
    TIMEFRAME = config['TIMEFRAME']

    # Set up the FeedHandler
    while True:
        logger.info(f'Start new loop')
        create_database_if_not_exists()
        create_table_if_not_exists()

        symbols = Binance.symbols()
        symbols = [symbol for symbol in symbols if SYMBOLS_TYPE in symbol]
        logger.info(f'Add symbols: {len(symbols)}')
        
        callbacks = {CANDLES: candle_callback}
        #binance = Binance(symbols=symbols, channels=[CANDLES,], callbacks=callbacks)

        f = FeedHandler()
        loop = asyncio.get_event_loop()
        #f.add_feed(binance)

        # Sharded subscription: batch symbols into groups of up to 100 symbols per feed
        for idx, chunk in enumerate(chunks(symbols, 100)):
            logger.info(f'ADD shard {idx} feed with {len(chunk)} symbols')
            f.add_feed(Binance(symbols=chunk, channels=[CANDLES], callbacks=callbacks, candle_interval=TIMEFRAME, candle_closed_only=True))

        # Start the data collection
        f.run(start_loop=False)

        # Start watcher task to hot-add new symbols every 60s; removals will be handled via partial shard restart
        known_symbols = set(symbols)
        loop.create_task(symbols_watcher(f, SYMBOLS_TYPE, TIMEFRAME, known_symbols, interval_sec=60))

        loop.run_forever()
        time.sleep(5)
