import time
from loguru import logger
import sys
import datetime
import yaml
from tqdm import tqdm
from decimal import Decimal

from cryptofeed.exchanges import BinanceFutures
import clickhouse_driver

####### Logger Configuration #########################################################

# Remove the default sink and add a new sink to stderr with a custom format and log level
logger.remove()
logger.add(
    sys.stderr,
    colorize=True,
    format="<green>{time:HH:mm:ss:ms}</green> | <level>{message}</level>",
    level=10,
)

# Add a file sink with DEBUG level and compression
logger.add("log.log", rotation="1 MB", level="DEBUG", compression="zip")

####### Load Config #########################################################

with open("config.yaml", 'r') as ymlfile:
    config = yaml.load(ymlfile, Loader=yaml.SafeLoader)

START_DATE: str = config['START_DATE']
LOAD_HISTORY: bool = config['LOAD_HISTORY']


####### Functions #########################################################

def candle_save(candle, receipt_timestamp: datetime.datetime) -> None:
    """Inserts the given candle data into ClickHouse.

    Args:
        candle: A dictionary containing candle data.
        receipt_timestamp: A datetime object representing the receipt timestamp.

    """
    exchange = candle.exchange
    symbol = candle.symbol
    start = datetime.datetime.fromtimestamp(candle.start)
    stop = datetime.datetime.fromtimestamp(candle.stop)
    interval = candle.interval
    trades = candle.trades
    open_price = Decimal(candle.open)
    close_price = Decimal(candle.close)
    high_price = Decimal(candle.high)
    low_price = Decimal(candle.low)
    volume = Decimal(candle.volume)
    #closed = (candle.closed)
    timestamp = datetime.datetime.fromtimestamp(candle.timestamp)
    
    query = '''
        INSERT INTO binance_data.candles 
        (exchange, symbol, start, stop, interval, trades, open, close, high, low, volume, timestamp, receipt_timestamp)
        VALUES 
        (%(exchange)s, %(symbol)s, %(start)s, %(stop)s, %(interval)s, %(trades)s, %(open)s, %(close)s, %(high)s, %(low)s, %(volume)s, %(timestamp)s, %(receipt_timestamp)s)
    '''

    with clickhouse_driver.Client(host='clickhouse', port=9000) as ch:
        ch.execute(query, {'exchange': exchange, 'symbol': symbol, 'start': start, 'stop': stop, 'interval': interval, 'trades': trades, 'open': open_price, 'close': close_price, 'high': high_price, 'low': low_price, 'volume': volume, 'timestamp': timestamp, 'receipt_timestamp': receipt_timestamp})


def get_symbols_last_date() -> dict:
    """Returns a dictionary containing the last date for each symbol in the ClickHouse database.

    Returns:
        A dictionary containing symbol names as keys and the corresponding last date as values.

    """
    with clickhouse_driver.Client(host='clickhouse', port=9000) as ch:
        result = ch.execute('SELECT symbol, MIN(stop) as min_date FROM binance_data.candles GROUP BY symbol')
    
    symbol_last_data_dict = {}
    for res in result:
        symbol_last_data_dict[res[0]] = res[1]
    return(symbol_last_data_dict)


####### Main #########################################################

if __name__ == '__main__':
    if LOAD_HISTORY:
        logger.info(f'Delay start by 120sec so that BD are ready')
        time.sleep(120)
        logger.info(f'START HISTORY LOAD')

        # Get the last stop date for each symbol
        symbol_last_data_dict = get_symbols_last_date()

        # Loop through each symbol and load its history
        for symbol, last_date in tqdm(symbol_last_data_dict.items()):
            logger.info(f'Load history: {symbol}')
            for data in tqdm(BinanceFutures().candles_sync(symbol, start=START_DATE, end=last_date)):
                for row in data:
                    candle_save(row, datetime.datetime.now())
            logger.info(f'Finish load history: {symbol}')

        logger.info(f'ALL history is loaded!')

    else:
        logger.info(f'No history load')
