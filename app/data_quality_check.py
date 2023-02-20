import clickhouse_driver
import requests
from loguru import logger
import sys
import datetime
import time
import yaml
import pandas as pd


logger.remove()
logger.add(
    sys.stderr,
    colorize=True,
    format="<green>{time:HH:mm:ss:ms}</green> | <level>{message}</level>",
    level=10,
)

logger.add("log.log", rotation="1 MB", level="DEBUG", compression="zip")


####### LOAD CONFIG #########################################################
with open("config.yaml", 'r') as ymlfile:
    config = yaml.load(ymlfile, Loader=yaml.SafeLoader)

TELEGRAM_TOKEN = config['TELEGRAM_TOKEN']
CHAT_ID = config['CHAT_ID']

####### FUNC #################################################################

def bot_send_msg(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage?chat_id={CHAT_ID}&text={message}"
    response = requests.get(url)
    if response.status_code == 200:
        logger.info("Telegram message sent successfully.")
    else:
        logger.error("Telegram message failed to send.")

####### Main #################################################################
if __name__ == '__main__':
    logger.info(f'Delay start by 90sec so that BD are ready')
    time.sleep(90)

    while True:
        #logger.info(f'Start new loop')

        with clickhouse_driver.Client(host='clickhouse', port=9000) as ch:
            #result = ch.execute('SELECT max(timestamp) FROM binance_data.candles')
            query = f'SELECT * FROM binance_data.candles ORDER BY timestamp DESC LIMIT 400'
            result = ch.execute(query)
            if not result:
                logger.error(f'No result from clickhouse!')

            columns = [
                'exchange',
                'symbol',
                'start',
                'stop',
                'interval',
                'trades',
                'open',
                'close',
                'high',
                'low',
                'volume',
                'timestamp',
                ]
            df = pd.DataFrame(result, columns=columns)
            df.sort_values(by='stop', ascending=False, inplace=True)
            df.drop_duplicates(subset='symbol', inplace=True)
            
            time_diff = datetime.datetime.now() - df.stop.min()
            if time_diff > datetime.timedelta(minutes=2):
                message = f"!!! Error !!! The last recording in the ClickHouse database is more than 2 minutes old, now: {datetime.datetime.now()} data: {df.stop.min()}"
                bot_send_msg(message)
                logger.info(message)

        time.sleep(90)
