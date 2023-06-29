import time
from loguru import logger
import sys
import datetime
import yaml
from progressbar import progressbar
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
logger.add("./logs/load_history.log", rotation="1 MB", level="DEBUG", compression="zip")

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
        (exchange, symbol, start, stop, close_unixtime, interval, trades, open, close, high, low, volume, timestamp, receipt_timestamp)
        VALUES 
        (%(exchange)s, %(symbol)s, %(start)s, %(stop)s, %(close_unixtime)s, %(interval)s, %(trades)s, %(open)s, %(close)s, %(high)s, %(low)s, %(volume)s, %(timestamp)s, %(receipt_timestamp)s)
    '''

    with clickhouse_driver.Client(host='clickhouse', port=9000) as ch:
        ch.execute(query, {'exchange': exchange, 'symbol': symbol, 'start': start, 'stop': stop, 'close_unixtime': candle.stop, 'interval': interval, 'trades': trades, 'open': open_price, 'close': close_price, 'high': high_price, 'low': low_price, 'volume': volume, 'timestamp': timestamp, 'receipt_timestamp': receipt_timestamp})


def get_symbols_last_date() -> dict:
    """Returns a dictionary containing the last date for each symbol in the ClickHouse database.

    Returns:
        A dictionary containing symbol names as keys and the corresponding last date as values.

    """
    with clickhouse_driver.Client(host='clickhouse', port=9000) as ch:
        result = ch.execute('SELECT symbol, MIN(stop) as min_date FROM binance_data.candles FINAL GROUP BY symbol')
    
    symbol_last_data_dict = {}
    for res in result:
        symbol_last_data_dict[res[0]] = res[1]
    return(symbol_last_data_dict)


####### Main #########################################################

if __name__ == '__main__':
    if LOAD_HISTORY:
        logger.info(f'Delay start by 2000sec so that all are ready')
        time.sleep(2000)
        logger.info(f'START HISTORY LOAD')

        # start load top10 symbols
        SYMBOLS_TYPE = '-USDT-PERP'
        SYMBOLS = [
            "BTC",
            "ETH",
            "BNB",
            "ADA",
            "XRP",
            "MATIC",
            "LTC",
            "TRX",
            "DOT",
            "AVAX",
            "LINK",
            "ATOM",
            "UNI",
        ]
        for symbol in SYMBOLS:
            logger.info(f'Load top10 SYMBOLS history: {symbol} from {START_DATE}')
            # Loop with unknown finite number of iterations
            for data in BinanceFutures().candles_sync(symbol+SYMBOLS_TYPE, start=START_DATE):
                for row in data:
                    candle_save(row, datetime.datetime.now())
            logger.info(f'Finish load top10 history: {symbol}')

        # Get the last stop date for each symbol
        symbol_last_data_dict = get_symbols_last_date()

        # Loop through each symbol and load its history
        for symbol, last_date in progressbar(symbol_last_data_dict.items(), redirect_stdout=True):
            logger.info(f'Load history: {symbol} from {START_DATE} to {last_date}')
            # Loop with unknown finite number of iterations
            try:
                for data in BinanceFutures().candles_sync(symbol, start=START_DATE, end=last_date):
                    for row in data:
                        candle_save(row, datetime.datetime.now())
                logger.info(f'Finish load history: {symbol}')
            except Exception as e:
                logger.error(f'Error: {e}')
                logger.error(f'Error: {symbol} from {START_DATE} to {last_date}')
        logger.info(f'ALL history is loaded!')

    else:
        logger.info(f'No history load')
