import datetime
import sys
import time
from typing import List, Sequence, Tuple

import clickhouse_driver
import pandas as pd
import yaml
from loguru import logger
from data_collector import DEFAULT_CLICKHOUSE_HOST, DEFAULT_CLICKHOUSE_PORT
from load_history import candle_save
from progressbar import progressbar
from telegram_notifier import TelegramNotifier

from clickhouse_schema import ROLLUP_SPECS, BASE_INTERVAL, RollupSpec
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

def find_missing_dates(
    df: pd.DataFrame,
    symbol: str,
    resample_freq: str,
    *,
    time_column: str = 'start',
) -> list:
    """
    Finds missing dates for a specific symbol in a DataFrame.
    
    Args:
        df: The DataFrame to search for missing dates.
        symbol: The symbol for which to search for missing dates.
        time_column: The name of the datetime column to analyse.

    Returns:
        A list of missing pandas timestamps representing gaps.
    """
    data_tmp = df[df.symbol == symbol].copy()
    if data_tmp.empty:
        return []

    data_tmp.sort_values(by=time_column, ascending=True, inplace=True)

    if data_tmp.duplicated([time_column], keep=False).sum() > 0:
        logger.info(f'duplicates found on {symbol}; dropping in-memory copy (table optimize handled separately)')
        data_tmp.drop_duplicates(subset=time_column, inplace=True)

    data_tmp = data_tmp.set_index(time_column, drop=False)
    # Reindex the data by time
    data_tmp_rs = data_tmp.resample(resample_freq).asfreq()
    # Output the dates that are missing in the data
    missing_dates = list(data_tmp_rs[data_tmp_rs.isna().any(axis=1)].index)
    return missing_dates


def rollup_data_to_df(data: Sequence[Sequence[object]]) -> pd.DataFrame:
    """Convert raw ClickHouse rows from rollup tables into a DataFrame."""

    columns = ['exchange', 'symbol', 'candle_start']
    df = pd.DataFrame(data, columns=columns)
    if not df.empty:
        df.sort_values(by='candle_start', ascending=False, inplace=True)
    return df


def group_missing_ranges(
    missing_dates: Sequence[pd.Timestamp],
    step: datetime.timedelta,
) -> List[Tuple[datetime.datetime, datetime.datetime]]:
    """Collapse missing timestamps into inclusive ranges."""

    if not missing_dates:
        return []

    sorted_dates = sorted(missing_dates)
    ranges: List[Tuple[datetime.datetime, datetime.datetime]] = []
    previous = sorted_dates[0].to_pydatetime()
    current_start = previous

    for ts in sorted_dates[1:]:
        current = ts.to_pydatetime()
        if current - previous <= step:
            previous = current
            continue
        ranges.append((current_start, previous))
        current_start = current
        previous = current

    ranges.append((current_start, previous))
    return ranges


def resolve_exchange_for_symbol(
    ch: clickhouse_driver.Client,
    symbol: str,
    existing_exchange: str | None = None,
) -> str | None:
    """Determine exchange identifier for ``symbol`` using cached or ClickHouse data."""

    if existing_exchange:
        return existing_exchange
    result = ch.execute(
        'SELECT exchange FROM binance_data.candles WHERE symbol = %(symbol)s AND interval = %(interval)s ORDER BY start DESC LIMIT 1',
        {'symbol': symbol, 'interval': BASE_INTERVAL},
    )
    if not result:
        return None
    return result[0][0]


def floor_to_freq(value: datetime.datetime, freq: str) -> datetime.datetime:
    return pd.Timestamp(value).floor(freq).to_pydatetime()


def notify_rollup_gap(
    spec: RollupSpec,
    symbol: str,
    first: datetime.datetime,
    last: datetime.datetime,
    count: int,
) -> None:
    message_lines = [
        f'Materialized view gap detected ({spec.table_full})',
        '------------------------------',
        f'Symbol : {symbol}',
        f'First  : {first}',
        f'Last   : {last}',
        f'Count  : {count}',
    ]
    message = '\n'.join(message_lines)
    if notifier:
        notifier.send(message)
    logger.error(message)


def build_rollup_backfill_query(spec: RollupSpec) -> str:
    interval_expr = spec.interval_expr()
    if spec.uses_merge_states:
        open_expr = 'argMinMergeState(open)'
        high_expr = 'maxMergeState(high)'
        low_expr = 'minMergeState(low)'
        close_expr = 'argMaxMergeState(close)'
        volume_expr = 'sumMergeState(volume)'
        trades_expr = 'sumMergeState(trades)'
        source_filters = [
            'exchange = %(exchange)s',
            'symbol = %(symbol)s',
            f"{spec.source_time_column} >= %(start)s",
            f"{spec.source_time_column} < %(end)s",
        ]
    else:
        open_expr = f'argMinState(open, {spec.source_time_column})'
        high_expr = 'maxState(high)'
        low_expr = 'minState(low)'
        close_expr = f'argMaxState(close, {spec.source_time_column})'
        volume_expr = 'sumState(volume)'
        trades_expr = 'sumState(toUInt64(trades))'
        source_filters = [
            'exchange = %(exchange)s',
            'symbol = %(symbol)s',
            f"{spec.source_time_column} >= %(start)s",
            f"{spec.source_time_column} < %(end)s",
            f"interval = '{BASE_INTERVAL}'",
        ]

    where_clause = ' AND '.join(source_filters)

    return f'''
        INSERT INTO {spec.table_full}
        SELECT agg.*
        FROM (
            SELECT
                exchange,
                symbol,
                {interval_expr} AS candle_start,
                {open_expr} AS open,
                {high_expr} AS high,
                {low_expr} AS low,
                {close_expr} AS close,
                {volume_expr} AS volume,
                {trades_expr} AS trades
            FROM {spec.source_table_full}
            WHERE {where_clause}
            GROUP BY exchange, symbol, candle_start
        ) AS agg
        LEFT JOIN (
            SELECT exchange, symbol, candle_start
            FROM {spec.table_full}
            WHERE exchange = %(exchange)s
              AND symbol = %(symbol)s
              AND candle_start >= %(start)s
              AND candle_start < %(end)s
        ) AS existing
        ON agg.exchange = existing.exchange
           AND agg.symbol = existing.symbol
           AND agg.candle_start = existing.candle_start
        WHERE existing.exchange IS NULL
    '''
def backfill_rollup_range(
    ch: clickhouse_driver.Client,
    spec: RollupSpec,
    exchange: str,
    symbol: str,
    window_start: datetime.datetime,
    window_end_exclusive: datetime.datetime,
    expected_intervals: int,
) -> None:
    query = build_rollup_backfill_query(spec)
    params = {
        'exchange': exchange,
        'symbol': symbol,
        'start': window_start,
        'end': window_end_exclusive,
    }
    try:
        ch.execute(query, params)
    except Exception as exc:
        logger.error(
            'Failed to recalc rollup %s for %s (%s -> %s): %s',
            spec.table_full,
            symbol,
            window_start,
            window_end_exclusive,
            exc,
        )
        if notifier:
            notifier.send(
                '\n'.join(
                    [
                        f'Rollup recalculation failed ({spec.table_full})',
                        '------------------------------',
                        f'Symbol : {symbol}',
                        f'From   : {window_start}',
                        f'To     : {window_end_exclusive}',
                        f'Error  : {exc}',
                    ]
                )
            )
        return

    logger.info(
        'Recalculated %s for %s: %s intervals [%s, %s)',
        spec.table_full,
        symbol,
        expected_intervals,
        window_start,
        window_end_exclusive,
    )


def backfill_rollup_gaps(
    ch: clickhouse_driver.Client,
    spec: RollupSpec,
    exchange: str,
    symbol: str,
    missing_dates: Sequence[pd.Timestamp],
) -> None:
    step = datetime.timedelta(minutes=spec.minutes)
    ranges = group_missing_ranges(missing_dates, step)
    for range_start, range_end in ranges:
        window_end = range_end + step
        expected = int((window_end - range_start) / step)
        backfill_rollup_range(ch, spec, exchange, symbol, range_start, window_end, expected)


def compute_rollup_source_window(
    ch: clickhouse_driver.Client,
    symbol: str,
    spec: RollupSpec,
    freq: str,
) -> Tuple[datetime.datetime, datetime.datetime] | None:
    """Determine the full time window available in the source data for ``symbol``."""

    result = ch.execute(
        'SELECT min(start), max(start) FROM binance_data.candles WHERE symbol = %(symbol)s AND interval = %(interval)s',
        {'symbol': symbol, 'interval': BASE_INTERVAL},
    )
    if not result:
        return None
    start_raw, end_raw = result[0]
    if start_raw is None or end_raw is None:
        return None

    step = datetime.timedelta(minutes=spec.minutes)
    start_aligned = floor_to_freq(start_raw, freq)
    end_aligned = floor_to_freq(end_raw, freq) + step
    if end_aligned <= start_aligned:
        return None
    return start_aligned, end_aligned


def process_rollup_symbol(
    ch: clickhouse_driver.Client,
    spec: RollupSpec,
    df: pd.DataFrame,
    symbol: str,
    freq: str,
) -> None:
    symbol_rows = df[df.symbol == symbol]
    existing_exchange = None
    if not symbol_rows.empty:
        exchange_values = symbol_rows.exchange.dropna()
        if not exchange_values.empty:
            existing_exchange = exchange_values.iloc[0]

    symbol_missing = find_missing_dates(df, symbol, freq, time_column='candle_start')
    symbol_exchange = resolve_exchange_for_symbol(
        ch,
        symbol,
        existing_exchange=existing_exchange,
    )

    if symbol_missing:
        first = symbol_missing[0].to_pydatetime()
        last = symbol_missing[-1].to_pydatetime()
        notify_rollup_gap(spec, symbol, first, last, len(symbol_missing))
        if not symbol_exchange:
            logger.error('Cannot infer exchange for %s; skip rollup backfill %s', symbol, spec.table_full)
            return
        backfill_rollup_gaps(ch, spec, symbol_exchange, symbol, symbol_missing)
        return

    if symbol_rows.empty:
        source_window = compute_rollup_source_window(ch, symbol, spec, freq)
        if not source_window:
            return
        start_aligned, end_aligned = source_window
        step = datetime.timedelta(minutes=spec.minutes)
        count = int((end_aligned - start_aligned) / step)
        if count <= 0:
            return
        notify_rollup_gap(spec, symbol, start_aligned, end_aligned - step, count)
        if not symbol_exchange:
            logger.error('Cannot infer exchange for %s; skip rollup backfill %s', symbol, spec.table_full)
            return
        backfill_rollup_range(
            ch,
            spec,
            symbol_exchange,
            symbol,
            start_aligned,
            end_aligned,
            count,
        )


def check_rollup_last_data(ch: clickhouse_driver.Client, depth: int = 2000) -> None:
    """Inspect recent rows in rollup tables for gaps and backfill when needed."""

    for spec in ROLLUP_SPECS:
        query = f'''
            SELECT exchange, symbol, candle_start
            FROM {spec.table_full}
            ORDER BY candle_start DESC
            LIMIT {depth}
        '''
        result = ch.execute(query)
        if not result:
            continue
        df = rollup_data_to_df(result)
        freq = timeframe_to_pandas_freq(spec.label)
        for symbol in df.symbol.unique():
            process_rollup_symbol(ch, spec, df, symbol, freq)


def fetch_all_symbols(ch: clickhouse_driver.Client) -> List[str]:
    rows = ch.execute('SELECT DISTINCT symbol FROM binance_data.candles')
    return [row[0] for row in rows if row and row[0]]


def check_rollup_full_data() -> None:
    with clickhouse_driver.Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT) as ch:
        symbols = fetch_all_symbols(ch)
        if not symbols:
            logger.info('No symbols found in base table; skipping rollup validation')
            return

        for spec in ROLLUP_SPECS:
            logger.info('Validate materialized view %s', spec.table_full)
            freq = timeframe_to_pandas_freq(spec.label)
            for symbol in progressbar(symbols):
                result = ch.execute(
                    f'''
                        SELECT exchange, symbol, candle_start
                        FROM {spec.table_full}
                        WHERE symbol = %(symbol)s
                        ORDER BY candle_start ASC
                    ''',
                    {'symbol': symbol},
                )
                df = rollup_data_to_df(result)
                process_rollup_symbol(ch, spec, df, symbol, freq)

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
            check_rollup_last_data(ch)
        time.sleep(120)

####### Main #################################################################
if __name__ == '__main__':
    logger.info(f'Delay start by {STARTUP_DELAY_SEC} sec so that DB are ready')
    time.sleep(STARTUP_DELAY_SEC)

    logger.info(f'Start check all data')
    check_missing_full_data()
    logger.info('Base candles table validated')

    logger.info('Start check materialized views')
    check_rollup_full_data()
    logger.info('Materialized views consistent')

    logger.info(f'Start main loop')
    main()
