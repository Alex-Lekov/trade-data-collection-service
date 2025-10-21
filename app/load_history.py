"""Backfill historical Binance candles into ClickHouse."""

import asyncio
import datetime
import sys
import time
from decimal import Decimal
from typing import Dict, List, Optional

import clickhouse_driver
from loguru import logger
from progressbar import progressbar

from data_collector import (
    DEFAULT_CLICKHOUSE_HOST,
    DEFAULT_CLICKHOUSE_PORT,
    CONFIG_PATH,
    filter_symbols,
    load_config,
)
from clickhouse_schema import INSERT_CANDLES_QUERY
from exchange_factory import get_exchange_class
from telegram_notifier import TelegramNotifier

HISTORY_STARTUP_DELAY_SEC = 200
DEFAULT_HISTORY_CHUNK_SIZE = 1000


def candle_save(
    candle,
    receipt_timestamp: datetime.datetime,
    host: str,
    port: int,
    *,
    client: Optional[clickhouse_driver.Client] = None,
) -> None:
    """Insert Binance candle data into ClickHouse.

    When a reusable ClickHouse ``client`` is provided, it is used directly to avoid
    opening a fresh TCP connection per candle. Otherwise a short-lived client is
    created for the duration of the insert.
    """
    exchange = candle.exchange
    symbol = candle.symbol
    start = datetime.datetime.fromtimestamp(candle.start, tz=datetime.timezone.utc)
    stop = datetime.datetime.fromtimestamp(candle.stop, tz=datetime.timezone.utc)
    interval = candle.interval
    trades = int(candle.trades)
    open_price = float(Decimal(candle.open))
    close_price = float(Decimal(candle.close))
    high_price = float(Decimal(candle.high))
    low_price = float(Decimal(candle.low))
    volume = float(Decimal(candle.volume))
    timestamp = datetime.datetime.fromtimestamp(candle.timestamp, tz=datetime.timezone.utc)

    receipt_ts = (
        receipt_timestamp.astimezone(datetime.timezone.utc)
        if receipt_timestamp.tzinfo
        else receipt_timestamp.replace(tzinfo=datetime.timezone.utc)
    )

    if client is None:
        with clickhouse_driver.Client(host=host, port=port) as executor:
            executor.execute(
                INSERT_CANDLES_QUERY,
                {
                    'exchange': exchange,
                    'symbol': symbol,
                    'interval': interval,
                    'start': start,
                    'stop': stop,
                    'close_unixtime': int(candle.stop),
                    'trades': trades,
                    'open': open_price,
                    'high': high_price,
                    'low': low_price,
                    'close': close_price,
                    'volume': volume,
                    'timestamp': timestamp,
                    'receipt_timestamp': receipt_ts,
                },
            )
    else:
        client.execute(
            INSERT_CANDLES_QUERY,
            {
                'exchange': exchange,
                'symbol': symbol,
                'interval': interval,
                'start': start,
                'stop': stop,
                'close_unixtime': int(candle.stop),
                'trades': trades,
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'volume': volume,
                'timestamp': timestamp,
                'receipt_timestamp': receipt_ts,
            },
        )


def parse_timeframe_delta(timeframe: str) -> datetime.timedelta:
    """Translate timeframe string (e.g. ``1m``) into a ``timedelta``."""
    value = timeframe.strip().lower()
    match = None
    if value:
        import re

        match = re.match(r'^(\d+)([smhdw])$', value)
    if not match:
        raise ValueError(f'Unsupported timeframe value: {timeframe!r}')

    amount, unit = match.groups()
    amount_int = int(amount)
    mapping = {
        's': datetime.timedelta(seconds=amount_int),
        'm': datetime.timedelta(minutes=amount_int),
        'h': datetime.timedelta(hours=amount_int),
        'd': datetime.timedelta(days=amount_int),
        'w': datetime.timedelta(weeks=amount_int),
    }
    if unit not in mapping:
        raise ValueError(f'Unsupported timeframe unit: {unit!r}')
    return mapping[unit]


def to_utc(dt: datetime.datetime) -> datetime.datetime:
    """Normalize naive datetimes to UTC."""
    if dt.tzinfo:
        return dt.astimezone(datetime.timezone.utc)
    return dt.replace(tzinfo=datetime.timezone.utc)

def format_ch_time(dt: datetime.datetime) -> str:
    """
    ClickHouse/Binance backfill expects 'YYYY-MM-DD HH:MM:SS' (UTC) without 'T' or offset.
    """
    dt_utc = to_utc(dt)
    return dt_utc.strftime('%Y-%m-%d %H:%M:%S')


def fetch_symbol_earliest_start(
    host: str,
    port: int,
    exchange_id: str,
    interval: str,
) -> Dict[str, datetime.datetime]:
    """Fetch earliest recorded candle start per symbol for the given exchange/interval."""

    query = (
        'SELECT symbol, MIN(start) '
        'FROM binance_data.candles FINAL '
        'WHERE exchange = %(exchange)s AND interval = %(interval)s '
        'GROUP BY symbol'
    )

    with clickhouse_driver.Client(host=host, port=port) as ch:
        rows = ch.execute(query, {'exchange': exchange_id, 'interval': interval})

    return {symbol: to_utc(start) for symbol, start in rows}


def main() -> None:
    """Entry point for historical backfill."""
    logger.remove()
    logger.add(
        sys.stderr,
        colorize=True,
        format="<green>{time:HH:mm:ss:ms}</green> | <level>{message}</level>",
        level=10,
    )
    logger.add("./logs/load_history.log", rotation="1 MB", level="DEBUG", compression="zip")

    config = load_config(CONFIG_PATH)
    if not config.get('LOAD_HISTORY'):
        logger.info('No history load')
        return

    start_date_raw = config['START_DATE']
    start_date_dt = to_utc(datetime.datetime.fromisoformat(start_date_raw))
    start_date_iso = start_date_dt.isoformat()
    symbol_type = config['SYMBOLS_TYPE']
    timeframe = config['TIMEFRAME']
    whitelist = config.get('SYMBOLS_WHITELIST') or []
    blacklist = config.get('SYMBOLS_BLACKLIST') or []
    clickhouse_host = config.get('CLICKHOUSE_HOST', DEFAULT_CLICKHOUSE_HOST)
    clickhouse_port = int(config.get('CLICKHOUSE_PORT', DEFAULT_CLICKHOUSE_PORT))
    timeframe_delta = parse_timeframe_delta(timeframe)
    history_chunk_size = int(config.get('HISTORY_CHUNK_SIZE', DEFAULT_HISTORY_CHUNK_SIZE))
    if history_chunk_size <= 0:
        raise ValueError('HISTORY_CHUNK_SIZE must be a positive integer')
    chunk_span = timeframe_delta * history_chunk_size

    exchange_name = (
        config.get('HISTORY_EXCHANGE')
        or config.get('EXCHANGE')
        or 'binance'
    )
    exchange_class = get_exchange_class(exchange_name)

    logger.info(f'Delay start by {HISTORY_STARTUP_DELAY_SEC} sec so that all are ready')
    time.sleep(HISTORY_STARTUP_DELAY_SEC)
    logger.info('START HISTORY LOAD')

    notifier = TelegramNotifier.from_config(config)

    exchange = exchange_class()
    exchange_id = getattr(exchange, 'id', exchange_name.upper())

    target_symbols = filter_symbols(exchange_class.symbols(), symbol_type, whitelist, blacklist)
    if not target_symbols:
        logger.warning('No symbols matched whitelist/blacklist filters; skipping history load')
        return
    successful_symbols: List[str] = []
    failed_details: List[str] = []

    safe_now = datetime.datetime.now(datetime.timezone.utc) - timeframe_delta
    if safe_now <= start_date_dt:
        logger.info(
            'Backfill skipped: start date {} is not earlier than safe horizon {}',
            start_date_iso,
            safe_now,
        )
        return

    earliest_starts = fetch_symbol_earliest_start(
        clickhouse_host,
        clickhouse_port,
        exchange_id,
        timeframe,
    )

    try:
        for symbol in progressbar(target_symbols, redirect_stdout=True):
            logger.info(
                'Load history for {} from {} down to {} (chunk={} candles)',
                symbol,
                safe_now,
                start_date_iso,
                history_chunk_size,
            )

            earliest_existing = earliest_starts.get(symbol)
            if earliest_existing:
                end_cursor = min(safe_now, earliest_existing - timeframe_delta)
            else:
                end_cursor = safe_now

            if end_cursor <= start_date_dt:
                logger.info(
                    'Skip %s: existing history (%s) covers requested range starting at %s',
                    symbol,
                    earliest_existing,
                    start_date_iso,
                )
                continue

            chunk_counter = 0
            if notifier:
                message_lines = [
                    'History load started',
                    '--------------------',
                f'Symbol          : {symbol}',
                f'Timeframe       : {timeframe}',
                f'Requested range : {start_date_iso} -> {end_cursor.isoformat()}',
                f'Chunks          : size {history_chunk_size} candles',
                f'Exchange        : {exchange_name}',
            ]
            notifier.send('\n'.join(message_lines))

            try:
                with clickhouse_driver.Client(host=clickhouse_host, port=clickhouse_port) as ch:
                    while end_cursor > start_date_dt:
                        chunk_start = max(start_date_dt, end_cursor - chunk_span)
                        params = dict(
                            start=format_ch_time(chunk_start),
                            end=format_ch_time(end_cursor),
                            interval=timeframe,
                        )
                        logger.debug(
                            'Symbol {} chunk #{}: {} -> {}',
                            symbol,
                            chunk_counter + 1,
                            params['start'],
                            params['end'],
                        )
                        for data in exchange.candles_sync(symbol, **params):
                            for row in data:
                                candle_save(
                                    row,
                                    datetime.datetime.now(datetime.timezone.utc),
                                    clickhouse_host,
                                    clickhouse_port,
                                    client=ch,
                                )
                        chunk_counter += 1
                        if chunk_start == start_date_dt:
                            break
                        end_cursor = chunk_start

                logger.info(f'Finish load history: {symbol}')
                successful_symbols.append(symbol)
                if notifier:
                    message_lines = [
                        'History load finished',
                        '---------------------',
                        f'Symbol    : {symbol}',
                        f'Chunks    : {chunk_counter}',
                        f'Range     : {start_date_iso} -> {safe_now.isoformat()}',
                        'Status    : completed successfully.',
                    ]
                    notifier.send('\n'.join(message_lines))
            except Exception as exc:
                logger.error(f'Error: {exc}')
                logger.error(
                    'Error processing {} chunking from {} to {}',
                    symbol,
                    start_date_iso,
                    safe_now.isoformat(),
                )
                failed_details.append(f'{symbol}: {exc}')
                if notifier:
                    message_lines = [
                        'History load failed',
                        '-------------------',
                        f'Symbol    : {symbol}',
                        f'Range     : {start_date_iso} -> {safe_now.isoformat()}',
                        f'Error     : {exc}',
                    ]
                    notifier.send('\n'.join(message_lines))
    finally:
        closer = getattr(exchange, 'close', None)
        if callable(closer):
            result = closer()
            if asyncio.iscoroutine(result):
                asyncio.run(result)

    logger.info('ALL history is loaded!')
    if notifier:
        summary_lines = [
            'History load summary',
            '--------------------',
            f'Total symbols : {len(target_symbols)}',
            f'Successful    : {len(successful_symbols)}',
            f'Failed        : {len(failed_details)}',
            f'Chunk size     : {history_chunk_size} candles',
            f'Exchange       : {exchange_name}',
        ]
        if failed_details:
            summary_lines.append('')
            summary_lines.append('Failures:')
            for detail in failed_details[:5]:
                summary_lines.append(f'- {detail}')
            extras = len(failed_details) - 5
            if extras > 0:
                summary_lines.append(f'  (+{extras} more)')
        notifier.send('\n'.join(summary_lines))


if __name__ == '__main__':
    main()
