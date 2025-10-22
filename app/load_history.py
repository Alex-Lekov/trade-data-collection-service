"""Backfill historical Binance candles into ClickHouse."""

import asyncio
import contextlib
import datetime
import sys
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, List, Optional, Sequence

import clickhouse_driver
from aioch import Client as AIOClickHouseClient
from loguru import logger

try:
    from progressbar import ProgressBar
except ImportError:  # pragma: no cover - optional dependency
    ProgressBar = None  # type: ignore

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


@dataclass(frozen=True)
class ChunkJob:
    """Work item describing a single REST backfill request."""

    symbol: str
    index: int
    start: datetime.datetime
    end: datetime.datetime
    total: int


@dataclass
class SymbolProgress:
    """Track per-symbol progress and failures."""

    total_chunks: int
    completed_chunks: int = 0
    errors: List[str] = field(default_factory=list)

    def mark_success(self) -> None:
        self.completed_chunks += 1

    def mark_failure(self, error: str) -> None:
        self.completed_chunks += 1
        self.errors.append(error)

    @property
    def is_done(self) -> bool:
        return self.completed_chunks >= self.total_chunks

    @property
    def succeeded(self) -> bool:
        return self.is_done and not self.errors


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
    payload = build_candle_payload(candle, receipt_timestamp)

    if client is None:
        with clickhouse_driver.Client(host=host, port=port) as executor:
            executor.execute(INSERT_CANDLES_QUERY, payload)
    else:
        client.execute(INSERT_CANDLES_QUERY, payload)


def build_candle_payload(
    candle,
    receipt_timestamp: datetime.datetime,
) -> Dict[str, object]:
    """Serialize a candle object into ClickHouse insert payload."""
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
    return {
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
    }


class ProgressTracker:
    """Lightweight progress bar wrapper with graceful degradation."""

    def __init__(self, total: int) -> None:
        self.total = total
        self.current = 0
        self._bar = None
        self._last_log_percent = -5.0
        if ProgressBar and total > 0:
            self._bar = ProgressBar(max_value=total)
            self._bar.start()

    def update(self, value: int) -> None:
        self.current = value
        if self._bar:
            self._bar.update(value)
        else:
            if self.total:
                percent = (value / self.total) * 100
                if percent - self._last_log_percent >= 5 or value == self.total:
                    self._last_log_percent = percent
                    logger.info('History progress: %.2f%% (%d/%d)', percent, value, self.total)

    def finish(self) -> None:
        if self._bar:
            self._bar.finish()


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

    exchange_id = getattr(exchange_class, 'id', exchange_name.upper())

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

    symbol_jobs: Dict[str, List[ChunkJob]] = {}
    symbol_requested_end: Dict[str, datetime.datetime] = {}

    for symbol in target_symbols:
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

        requested_end = end_cursor
        chunk_spans: List[tuple[datetime.datetime, datetime.datetime]] = []
        while end_cursor > start_date_dt:
            chunk_start = max(start_date_dt, end_cursor - chunk_span)
            chunk_spans.append((chunk_start, end_cursor))
            if chunk_start == start_date_dt:
                break
            end_cursor = chunk_start

        if not chunk_spans:
            continue

        total_chunks = len(chunk_spans)
        jobs_for_symbol = [
            ChunkJob(symbol=symbol, index=idx, start=start, end=end, total=total_chunks)
            for idx, (start, end) in enumerate(chunk_spans, start=1)
        ]
        symbol_jobs[symbol] = jobs_for_symbol
        symbol_requested_end[symbol] = requested_end

    total_chunks = sum(len(jobs) for jobs in symbol_jobs.values())
    if total_chunks == 0:
        logger.info('No historical gaps detected; nothing to backfill.')
        return

    history_workers = int(config.get('HISTORY_WORKERS', 3))
    if history_workers <= 0:
        raise ValueError('HISTORY_WORKERS must be a positive integer')
    fetch_retries = int(config.get('HISTORY_FETCH_RETRIES', 3))
    fetch_retry_delay = float(config.get('HISTORY_FETCH_RETRY_DELAY_SEC', 5))
    insert_retries = int(config.get('HISTORY_INSERT_RETRIES', 3))
    insert_retry_delay = float(config.get('HISTORY_INSERT_RETRY_DELAY_SEC', 2))

    logger.info(
        'Scheduling %d history chunks across %d symbols (workers=%d)',
        total_chunks,
        len(symbol_jobs),
        history_workers,
    )

    successful_symbols, failed_details = asyncio.run(
        run_history_backfill(
            exchange_class=exchange_class,
            timeframe=timeframe,
            jobs=symbol_jobs,
            requested_end_map=symbol_requested_end,
            clickhouse_host=clickhouse_host,
            clickhouse_port=clickhouse_port,
            notifier=notifier,
            start_date_iso=start_date_iso,
            safe_now=safe_now,
            history_chunk_size=history_chunk_size,
            exchange_name=exchange_name,
            worker_count=history_workers,
            fetch_retries=fetch_retries,
            fetch_retry_delay=fetch_retry_delay,
            insert_retries=insert_retries,
            insert_retry_delay=insert_retry_delay,
        )
    )

    logger.info('ALL history is loaded!')
    if notifier:
        summary_lines = [
            'History load summary',
            '--------------------',
            f'Total symbols : {len(target_symbols)}',
            f'Successful    : {len(successful_symbols)}',
            f'Failed        : {len(failed_details)}',
            f'Chunk size    : {history_chunk_size} candles',
            f'Exchange      : {exchange_name}',
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


async def run_history_backfill(
    exchange_class,
    timeframe: str,
    jobs: Dict[str, Sequence[ChunkJob]],
    requested_end_map: Dict[str, datetime.datetime],
    clickhouse_host: str,
    clickhouse_port: int,
    notifier: Optional[TelegramNotifier],
    start_date_iso: str,
    safe_now: datetime.datetime,
    history_chunk_size: int,
    exchange_name: str,
    worker_count: int,
    fetch_retries: int,
    fetch_retry_delay: float,
    insert_retries: int,
    insert_retry_delay: float,
) -> tuple[List[str], List[str]]:
    """Execute backfill workload concurrently and return success/failure summaries."""

    total_chunks = sum(len(items) for items in jobs.values())
    progress = ProgressTracker(total_chunks)
    symbol_states: Dict[str, SymbolProgress] = {
        symbol: SymbolProgress(total_chunks=len(items))
        for symbol, items in jobs.items()
    }
    finished_symbols: set[str] = set()
    successful_symbols: List[str] = []
    failed_details: List[str] = []

    queue: asyncio.Queue[Optional[ChunkJob]] = asyncio.Queue()
    for items in jobs.values():
        for job in items:
            queue.put_nowait(job)
    for _ in range(worker_count):
        queue.put_nowait(None)

    if notifier:
        for symbol, items in jobs.items():
            requested_end = requested_end_map.get(symbol)
            message_lines = [
                'History load started',
                '--------------------',
                f'Symbol          : {symbol}',
                f'Timeframe       : {timeframe}',
                f"Requested range : {start_date_iso} -> {requested_end.isoformat() if requested_end else '?'}",
                f'Chunks          : {len(items)} (size {history_chunk_size} candles)',
                f'Exchange        : {exchange_name}',
            ]
            notifier.send('\n'.join(message_lines))

    client = AIOClickHouseClient(host=clickhouse_host, port=clickhouse_port)
    insert_lock = asyncio.Lock()
    completed_chunks = 0
    completed_lock = asyncio.Lock()

    async def finalize_symbol(symbol: str) -> None:
        if symbol in finished_symbols:
            return
        state = symbol_states[symbol]
        if not state.is_done:
            return
        finished_symbols.add(symbol)
        requested_end = requested_end_map.get(symbol)
        if state.errors:
            error_summary = state.errors[-1]
            failed_details.append(f'{symbol}: {error_summary}')
            logger.error(
                'History load failed for %s (%d/%d chunks complete): %s',
                symbol,
                state.completed_chunks,
                state.total_chunks,
                error_summary,
            )
            if notifier:
                message_lines = [
                    'History load failed',
                    '-------------------',
                    f'Symbol    : {symbol}',
                    f"Range     : {start_date_iso} -> {requested_end.isoformat() if requested_end else '?'}",
                    f'Chunks    : {state.total_chunks}',
                    f'Errors    : {len(state.errors)} (last: {error_summary})',
                ]
                notifier.send('\n'.join(message_lines))
        else:
            successful_symbols.append(symbol)
            logger.info('Finish load history: %s', symbol)
            if notifier:
                message_lines = [
                    'History load finished',
                    '---------------------',
                    f'Symbol    : {symbol}',
                    f'Chunks    : {state.total_chunks}',
                    f"Range     : {start_date_iso} -> {requested_end.isoformat() if requested_end else safe_now.isoformat()}",
                    'Status    : completed successfully.',
                ]
                notifier.send('\n'.join(message_lines))

    async def worker(worker_id: int) -> None:
        nonlocal completed_chunks
        exchange = exchange_class()
        try:
            while True:
                job = await queue.get()
                if job is None:
                    queue.task_done()
                    break

                params = dict(
                    start=format_ch_time(job.start),
                    end=format_ch_time(job.end),
                    interval=timeframe,
                )
                logger.debug(
                    'Worker %d: symbol %s chunk #%d/%d (%s -> %s)',
                    worker_id,
                    job.symbol,
                    job.index,
                    job.total,
                    params['start'],
                    params['end'],
                )

                last_error: Optional[BaseException] = None
                rows: List[Dict[str, object]] = []
                for attempt in range(1, fetch_retries + 1):
                    try:
                        rows = await fetch_chunk_rows(exchange, job.symbol, params)
                        break
                    except Exception as exc:
                        last_error = exc
                        logger.warning(
                            'Worker %d: fetch failed for %s chunk #%d (attempt %d/%d): %s',
                            worker_id,
                            job.symbol,
                            job.index,
                            attempt,
                            fetch_retries,
                            exc,
                        )
                        if attempt < fetch_retries:
                            await asyncio.sleep(fetch_retry_delay * attempt)
                else:
                    error_text = f'fetch failed after {fetch_retries} attempts: {last_error}'
                    symbol_states[job.symbol].mark_failure(error_text)
                    await finalize_symbol(job.symbol)
                    async with completed_lock:
                        completed_chunks += 1
                        progress.update(completed_chunks)
                    queue.task_done()
                    continue

                try:
                    await insert_rows(client, insert_lock, rows, insert_retries, insert_retry_delay)
                    symbol_states[job.symbol].mark_success()
                except Exception as exc:
                    error_text = f'insert failed: {exc}'
                    logger.error(
                        'Worker %d: insert failed for %s chunk #%d: %s',
                        worker_id,
                        job.symbol,
                        job.index,
                        exc,
                    )
                    symbol_states[job.symbol].mark_failure(error_text)

                await finalize_symbol(job.symbol)
                async with completed_lock:
                    completed_chunks += 1
                    progress.update(completed_chunks)

                queue.task_done()
        finally:
            closer = getattr(exchange, 'close', None)
            if callable(closer):
                result = closer()
                if asyncio.iscoroutine(result):
                    await result

    workers = [asyncio.create_task(worker(idx + 1)) for idx in range(worker_count)]
    await queue.join()

    for task in workers:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    progress.finish()

    disconnect = getattr(client, 'disconnect', None)
    if callable(disconnect):
        result = disconnect()
        if asyncio.iscoroutine(result):
            await result

    # Ensure any symbols that never reached finalize are accounted for.
    for symbol in jobs.keys():
        await finalize_symbol(symbol)

    return successful_symbols, failed_details


async def fetch_chunk_rows(
    exchange,
    symbol: str,
    params: Dict[str, str],
) -> List[Dict[str, object]]:
    """Download a chunk of candles and convert them into ClickHouse payloads."""
    receipt_ts = datetime.datetime.now(datetime.timezone.utc)
    rows: List[Dict[str, object]] = []
    async for batch in exchange.candles(symbol, **params):
        for row in batch:
            rows.append(build_candle_payload(row, receipt_ts))
    return rows


async def insert_rows(
    client: AIOClickHouseClient,
    lock: asyncio.Lock,
    rows: Sequence[Dict[str, object]],
    insert_retries: int,
    insert_retry_delay: float,
) -> None:
    """Insert rows into ClickHouse with retries."""
    if not rows:
        return

    last_error: Optional[BaseException] = None
    attempts = max(insert_retries, 1)
    for attempt in range(1, attempts + 1):
        try:
            async with lock:
                await client.execute(INSERT_CANDLES_QUERY, rows)
            return
        except Exception as exc:
            last_error = exc
            logger.warning(
                'ClickHouse insert failed (attempt %d/%d): %s',
                attempt,
                attempts,
                exc,
            )
            await asyncio.sleep(insert_retry_delay * attempt)

    assert last_error is not None
    raise last_error


if __name__ == '__main__':
    main()
