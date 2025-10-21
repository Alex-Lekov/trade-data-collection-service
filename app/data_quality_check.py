import clickhouse_driver
from loguru import logger
import sys
import datetime
import time
import yaml
import pandas as pd
from data_collector import DEFAULT_CLICKHOUSE_HOST, DEFAULT_CLICKHOUSE_PORT
from load_history import candle_save
from progressbar import progressbar
from telegram_notifier import TelegramNotifier

from exchange_factory import get_exchange_class


logger.remove()
logger.add(
    sys.stderr,
    colorize=True,
    format="<green>{time:HH:mm:ss:ms}</green> | <level>{message}</level>",
    level=10,
)

logger.add("./logs/data_quality_check.log", rotation="1 MB", level="DEBUG", compression="zip")


####### LOAD CONFIG #########################################################
with open("config.yaml", 'r') as ymlfile:
    config = yaml.load(ymlfile, Loader=yaml.SafeLoader)

CLICKHOUSE_HOST = config.get('CLICKHOUSE_HOST', DEFAULT_CLICKHOUSE_HOST)
CLICKHOUSE_PORT = int(config.get('CLICKHOUSE_PORT', DEFAULT_CLICKHOUSE_PORT))
TIMEFRAME = config.get('TIMEFRAME', '1m')
EXCHANGE_NAME = (
    config.get('DATA_QUALITY_EXCHANGE')
    or config.get('HISTORY_EXCHANGE')
    or config.get('EXCHANGE')
    or 'binance'
)

exchange_class = get_exchange_class(EXCHANGE_NAME)
exchange_instance = exchange_class()

notifier = TelegramNotifier.from_config(config)

# Configurable startup delay for data_quality_check (seconds)
DEFAULT_STARTUP_DELAY_SEC = 120
STARTUP_DELAY_SEC = int(config.get('DATA_QUALITY_STARTUP_DELAY_SEC', DEFAULT_STARTUP_DELAY_SEC))

####### FUNC #################################################################

def timeframe_to_pandas_freq(timeframe: str) -> str:
    """Convert application timeframe (e.g., ``1m``) into a pandas frequency."""
    timeframe = (timeframe or '').strip().lower()
    match = None
    if timeframe:
        import re

        match = re.match(r'^(\d+)([smhdw])$', timeframe)
    if not match:
        raise ValueError(f'Unsupported timeframe value: {timeframe!r}')

    value, unit = match.groups()
    mapping = {
        's': 'S',
        'm': 'min',  # minutes
        'h': 'H',
        'd': 'D',
        'w': 'W',
    }
    if unit not in mapping:
        raise ValueError(f'Unsupported timeframe unit: {unit!r}')
    return f"{value}{mapping[unit]}"


RESAMPLE_FREQUENCY = timeframe_to_pandas_freq(TIMEFRAME)

def data_to_df(data: list) -> pd.DataFrame:
    """
    Converts a list to a Pandas DataFrame.

    Args:
    data: A list containing the data.

    Returns:
    A Pandas DataFrame.
    """
    columns = [
        'exchange',
        'symbol',
        'interval',
        'start',
        'stop',
        'close_unixtime',
        'trades',
        'open',
        'high',
        'low',
        'close',
        'volume',
        'timestamp',
        'receipt_timestamp',
    ]
    df = pd.DataFrame(data, columns=columns)
    df.sort_values(by='stop', ascending=False, inplace=True)
    return(df)

def check_last_data_recording(ch: clickhouse_driver.Client) -> None:
    """
    Checks the last recorded data in ClickHouse database.

    Args:
    ch: A ClickHouse client object.
    """
    query = f'SELECT * FROM binance_data.candles FINAL ORDER BY timestamp DESC LIMIT 400'
    result = ch.execute(query)
    if not result:
        logger.error(f'No result from clickhouse!')
        return

    df = data_to_df(result)
    df.drop_duplicates(subset='symbol', inplace=True)
    
    current_time = datetime.datetime.now(datetime.timezone.utc)
    oldest_stop = df.stop.min()
    time_diff = current_time - oldest_stop
    if time_diff > datetime.timedelta(minutes=2):
        message_lines = [
            'Data freshness alert',
            '--------------------',
            f'Current time : {current_time}',
            f'Last candle  : {oldest_stop}',
            f'Lag          : {time_diff}',
        ]
        message = '\n'.join(message_lines)
        if notifier:
            notifier.send(message)
        logger.error(message)

def load_missing_data(missing_dates: list, symbol: str) -> None:
    """
    Loads the missing data into the database.

    Args:
    missing_dates: A list of missing dates to be loaded.
    symbol: A string symbol to be loaded.
    """
    start_date = (missing_dates[0] - datetime.timedelta(minutes=5)).to_pydatetime()
    end_date = (missing_dates[-1] + datetime.timedelta(minutes=5)).to_pydatetime()

    message_lines = [
        'Missing candles detected',
        '------------------------',
        f'Symbol : {symbol}',
        f'First  : {missing_dates[0]}',
        f'Last   : {missing_dates[-1]}',
        f'Count  : {len(missing_dates)}',
    ]
    message = '\n'.join(message_lines)
    if notifier:
        notifier.send(message)
    logger.error(message)

    logger.info('Start load Missing values')

    try:
        with clickhouse_driver.Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT) as ch:
            for data in exchange_instance.candles_sync(
                symbol,
                start=start_date,
                end=end_date,
                interval=TIMEFRAME,
            ):
                for row in data:
                    candle_save(
                        row,
                        datetime.datetime.now(datetime.timezone.utc),
                        CLICKHOUSE_HOST,
                        CLICKHOUSE_PORT,
                        client=ch,
                    )
    except Exception as exc:
        logger.error(f'Failed to load missing data for {symbol}: {exc}')
        if notifier:
            notifier.send(
                '\n'.join(
                    [
                        'Missing candles reload failed',
                        '-----------------------------',
                        f'Symbol : {symbol}',
                        f'Window : {start_date} -> {end_date}',
                        f'Error  : {exc}',
                    ]
                )
            )
        return
            
    message = f'Finish load Missing values in {symbol}'
    # if notifier:
    #     notifier.send(message)
    logger.info(message)

def find_missing_dates(df: pd.DataFrame, symbol: str, resample_freq: str) -> list:
    """
    Finds missing dates for a specific symbol in a DataFrame.
    
    Args:
        df: The DataFrame to search for missing dates.
        symbol: The symbol for which to search for missing dates.
    
    Returns:
        A list of missing dates as strings.
    """
    data_tmp = df[df.symbol == symbol].copy()
    data_tmp.sort_values(by='start', ascending=True, inplace=True)

    if data_tmp.duplicated(['start',], keep=False).sum() > 0:
        logger.info(f'duplicates found on {symbol}; dropping in-memory copy (table optimize handled separately)')
        data_tmp.drop_duplicates(subset='start', inplace=True)

    data_tmp = data_tmp.set_index('start', drop=False)
    # Reindex the data by time
    data_tmp_rs = data_tmp.resample(resample_freq).asfreq()
    # Output the dates that are missing in the data
    missing_dates = list(data_tmp_rs[data_tmp_rs.isna().any(axis=1)].index)
    return missing_dates

def check_missing_last_data(ch: clickhouse_driver.Client, depth: int = 3000) -> None:
    """
    Checks for missing data for the last `depth` records in ClickHouse.
    
    Args:
        ch: A ClickHouse client object.
        depth: The number of records to check for missing data.
    """
    query = f'SELECT * FROM binance_data.candles FINAL ORDER BY timestamp DESC LIMIT {depth}'
    result = ch.execute(query)
    df = data_to_df(result)

    for symbol in df.symbol.unique():
        missing_dates = find_missing_dates(df, symbol, RESAMPLE_FREQUENCY)
        if len(missing_dates) > 0:
            logger.info(f'Found missing data for {symbol}! Total: {len(missing_dates)}')
            load_missing_data(missing_dates, symbol)

def check_missing_full_data() -> None:
    """
    Checks for missing data in the full data set in ClickHouse.
    """
    with clickhouse_driver.Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT) as ch:
        query = f'SELECT * FROM binance_data.candles FINAL ORDER BY timestamp DESC LIMIT 4000'
        result = ch.execute(query)
        df = data_to_df(result)

        for symbol in progressbar(df.symbol.unique()):
            query = '''SELECT * FROM binance_data.candles FINAL WHERE symbol = %(symbol)s'''
            result = ch.execute(query, {'symbol': symbol,})

            df = data_to_df(result)
            missing_dates = find_missing_dates(df, symbol, RESAMPLE_FREQUENCY)

            if len(missing_dates) > 0:
                load_missing_data(missing_dates, symbol)

def main():
    """
    The main function that runs the script in a loop.
    """
    while True:
        with clickhouse_driver.Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT) as ch:
            check_last_data_recording(ch)
            check_missing_last_data(ch)
        time.sleep(120)

####### Main #################################################################
if __name__ == '__main__':
    logger.info(f'Delay start by {STARTUP_DELAY_SEC} sec so that DB are ready')
    time.sleep(STARTUP_DELAY_SEC)

    logger.info(f'Start check all data')
    check_missing_full_data()
    logger.info(f'All data good!')

    logger.info(f'Start main loop')
    main()
