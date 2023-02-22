import clickhouse_driver
import requests
from loguru import logger
import sys
import datetime
import time
import yaml
import pandas as pd
from cryptofeed.exchanges import BinanceFutures
from load_history import candle_save
from progressbar import progressbar


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

TELEGRAM_TOKEN = config['TELEGRAM_TOKEN']
CHAT_ID = config['CHAT_ID']

####### FUNC #################################################################

def bot_send_msg(message: str) -> None:
    """
    Sends a message via Telegram API.

    Args:
    message: A string message to be sent to the user.
    """
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage?chat_id={CHAT_ID}&text={message}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            logger.info("Telegram message sent successfully.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Telegram message failed to send. Error: {e}")

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
        'start',
        'stop',
        'close_unixtime',
        'interval',
        'trades',
        'open',
        'close',
        'high',
        'low',
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

    df = data_to_df(result)
    df.drop_duplicates(subset='symbol', inplace=True)
    
    time_diff = datetime.datetime.now() - df.stop.min()
    if time_diff > datetime.timedelta(minutes=2):
        message = f"!!! Error !!! The last recording in the ClickHouse database is more than 2 minutes old, now: {datetime.datetime.now()} data: {df.stop.min()}"
        bot_send_msg(message)
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

    message = f'Missing values found! symbol: {symbol}, dates start:{missing_dates[0]}, end:{missing_dates[-1]}'
    bot_send_msg(message)
    logger.error(message)

    logger.info('Start load Missing values')

    for data in BinanceFutures().candles_sync(symbol, start=start_date, end=end_date):
        for row in data:
            candle_save(row, datetime.datetime.now())
            
    message = f'Finish load Missing values in {symbol}'
    #bot_send_msg(message)
    logger.info(message)

def find_missing_dates(df: pd.DataFrame, symbol: str) -> list:
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
        logger.info(f'duplicates found on {symbol}')

        with clickhouse_driver.Client(host='clickhouse', port=9000) as ch:
            query = f'OPTIMIZE TABLE binance_data.candles FINAL DEDUPLICATE'
            _ = ch.execute(query)

        data_tmp.drop_duplicates(subset='start', inplace=True)

    data_tmp = data_tmp.set_index('start', drop=False)
    # Reindex the data by time
    data_tmp_rs = data_tmp.resample('1min').asfreq()
    # Output the dates that are missing in the data
    missing_dates = list(data_tmp_rs[data_tmp_rs.isna().any(axis=1)].index)
    return missing_dates

def check_missing_last_data(ch: clickhouse_driver.Client, depth: int = 1000) -> None:
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
        missing_dates = find_missing_dates(df, symbol)
        if len(missing_dates) > 0:
            load_missing_data(missing_dates, symbol)

def check_missing_full_data() -> None:
    """
    Checks for missing data in the full data set in ClickHouse.
    """
    with clickhouse_driver.Client(host='clickhouse', port=9000) as ch:
        query = f'SELECT * FROM binance_data.candles FINAL ORDER BY timestamp DESC LIMIT 4000'
        result = ch.execute(query)
        df = data_to_df(result)

        for symbol in progressbar(df.symbol.unique()):
            query = '''SELECT * FROM binance_data.candles FINAL WHERE symbol = %(symbol)s'''
            result = ch.execute(query, {'symbol': symbol,})

            df = data_to_df(result)
            missing_dates = find_missing_dates(df, symbol)

            if len(missing_dates) > 0:
                load_missing_data(missing_dates, symbol)

def main():
    """
    The main function that runs the script in a loop.
    """
    while True:
        with clickhouse_driver.Client(host='clickhouse', port=9000) as ch:
            check_last_data_recording(ch)
            check_missing_last_data(ch)
        time.sleep(90)

####### Main #################################################################
if __name__ == '__main__':
    logger.info(f'Delay start by 120sec so that BD are ready')
    time.sleep(120)

    logger.info(f'Start check all data')
    check_missing_full_data()
    logger.info(f'All data good!')

    logger.info(f'Start main loop')
    main()
