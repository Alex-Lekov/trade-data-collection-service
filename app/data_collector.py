import asyncio
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Awaitable, Callable, Iterator, List, Optional, Sequence, Set

import yaml
from aioch import Client as AIOClickHouseClient
from cryptofeed import FeedHandler
from cryptofeed.defines import CANDLES
from loguru import logger
#from asynch import connect

from clickhouse_schema import INSERT_CANDLES_QUERY, ensure_schema
from exchange_factory import create_exchange, get_exchange_class
from telegram_notifier import TelegramNotifier


logger.remove()
logger.add(
    sys.stderr,
    colorize=True,
    format="<green>{time:HH:mm:ss:ms}</green> | <level>{message}</level>",
    level=10,
)

logger.add("./logs/data_collector.log", rotation="1 MB", level="DEBUG", compression="zip")

DEFAULT_CLICKHOUSE_HOST = 'clickhouse'
DEFAULT_CLICKHOUSE_PORT = 9000
CONFIG_PATH = "config.yaml"
DEFAULT_SHARD_SIZE = 100
DEFAULT_WATCHER_INTERVAL_SEC = 60
DEFAULT_STARTUP_DELAY_SEC = 10

def chunks(lst: Sequence[str], chunk_size: int) -> Iterator[List[str]]:
    """Yield successive chunks from ``lst`` with at most ``chunk_size`` items.

    Args:
        lst (Sequence[str]): Source collection to split.
        chunk_size (int): Upper bound for symbols per chunk.

    Yields:
        list[str]: Subsequent slices of the original list.

    Raises:
        ValueError: If ``chunk_size`` is zero or negative.
    """
    if chunk_size <= 0:
        raise ValueError('chunk_size must be greater than zero')

    for i in range(0, len(lst), chunk_size):
        yield list(lst[i:i + chunk_size])

def filter_symbols(
    all_symbols: Sequence[str],
    symbol_type: str,
    whitelist: Optional[Sequence[str]] = None,
    blacklist: Optional[Sequence[str]] = None,
    *,
    log_missing: bool = True,
) -> List[str]:
    """Filter symbols using optional whitelist/blacklist overlays.

    Args:
        all_symbols (Sequence[str]): Raw universe returned by the exchange.
        symbol_type (str): Substring filter applied when whitelist is empty.
        whitelist (Sequence[str] | None): Explicit symbols to keep (takes priority).
        blacklist (Sequence[str] | None): Symbols to exclude from the result.
        log_missing (bool): When True, emits warnings for whitelist entries not on the exchange.

    Returns:
        list[str]: Sorted collection of symbols that pass configured filters.
    """
    whitelist = list(whitelist or [])
    blacklist = set(blacklist or [])

    if whitelist:
        available = set(all_symbols)
        if log_missing:
            missing = [symbol for symbol in whitelist if symbol not in available]
            if missing:
                logger.warning(f'Whitelist symbols not available on exchange: {missing}')
        filtered = [symbol for symbol in whitelist if symbol in available]
    else:
        filtered = [symbol for symbol in all_symbols if symbol_type in symbol]

    filtered = [symbol for symbol in filtered if symbol not in blacklist]

    return sorted(filtered)


async def symbols_watcher(
    handler: FeedHandler,
    exchange_name: str,
    exchange_class: type,
    symbol_type: str,
    timeframe: str,
    known_symbols: Set[str],
    whitelist: Sequence[str],
    blacklist: Sequence[str],
    candle_handler: Callable[..., Awaitable[None]],
    *,
    shard_size: int,
    interval_sec: int = DEFAULT_WATCHER_INTERVAL_SEC,
) -> None:
    """Monitor Binance listings and hot-add new symbols.

    Args:
        handler (FeedHandler): Active feed handler to augment with new shards.
        exchange_name (str): Configured exchange identifier used for new feed instances.
        exchange_class (type): Exchange class providing symbol discovery.
        symbol_type (str): Substring filter used when whitelist is empty.
        timeframe (str): Candle interval requested from Binance.
        known_symbols (set[str]): Mutable set of symbols already subscribed.
        whitelist (Sequence[str]): Explicit list of allowed symbols (may be empty).
        blacklist (Sequence[str]): Symbols to skip even if they pass other filters.
        candle_handler: Callback used to store executed candles.
        shard_size (int): Chunk size applied when hot-adding new feeds.
        interval_sec (int): Poll interval to refresh listings.

    Returns:
        None: The coroutine runs indefinitely until cancelled.
    """
    callbacks = {CANDLES: candle_handler}
    while True:
        try:
            current = exchange_class.symbols()
            current = filter_symbols(current, symbol_type, whitelist, blacklist, log_missing=False)

            current_set = set(current)
            added = list(current_set.difference(known_symbols))
            removed = list(known_symbols.difference(current_set))

            if added:
                logger.info(f'Watcher: detected {len(added)} newly listed symbols. Hot-adding shards.')
                # Add shards sized identically to the initial bootstrap.
                for idx, chunk in enumerate(chunks(sorted(added), shard_size)):
                    logger.info(f'Watcher: add shard with {len(chunk)} symbols')
                    handler.add_feed(
                        create_exchange(
                            exchange_name,
                            symbols=chunk,
                            channels=[CANDLES],
                            callbacks=callbacks,
                            candle_interval=timeframe,
                            candle_closed_only=True,
                        )
                    )
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


def load_config(path: str = CONFIG_PATH) -> dict:
    """Load runtime configuration from YAML.

    Args:
        path (str): Path to the configuration file.

    Returns:
        dict: Parsed configuration dictionary (empty when file is blank).
    """
    with open(path, 'r') as ymlfile:
        return yaml.load(ymlfile, Loader=yaml.SafeLoader) or {}


def make_candle_callback(host: str, port: int) -> Callable[..., Awaitable[None]]:
    """Build an async candle handler bound to the given ClickHouse endpoint.

    Args:
        host (str): ClickHouse host to connect to.
        port (int): ClickHouse port to connect to.

    Returns:
        Callable[..., Awaitable[None]]: Coroutine that persists candles.
    """

    # Reuse a single ClickHouse client per callback to avoid leaking new TCP sessions.
    client_holder: dict[str, Optional[AIOClickHouseClient]] = {'client': None}
    lock = asyncio.Lock()

    async def get_client() -> AIOClickHouseClient:
        client = client_holder['client']
        if client is None:
            client = AIOClickHouseClient(host=host, port=port)
            client_holder['client'] = client
        return client

    async def reset_client() -> None:
        client = client_holder['client']
        if not client:
            return
        close = getattr(client, 'close', None)
        if callable(close):
            result = close()
            if asyncio.iscoroutine(result):
                await result
        else:
            disconnect = getattr(client, 'disconnect', None)
            if callable(disconnect):
                result = disconnect()
                if asyncio.iscoroutine(result):
                    await result
        client_holder['client'] = None

    async def candle_callback(candle, receipt_timestamp) -> None:
        """Persist processed candle data into ClickHouse.

        Args:
            candle: Candle payload provided by cryptofeed.
            receipt_timestamp: Timestamp recorded by cryptofeed when the candle was received.

        Returns:
            None: The coroutine completes once the candle is stored.
        """
        exchange = candle.exchange
        symbol = candle.symbol
        start = datetime.fromtimestamp(candle.start, tz=timezone.utc)
        stop = datetime.fromtimestamp(candle.stop, tz=timezone.utc)
        interval = candle.interval
        trades = int(candle.trades)
        open_price = float(Decimal(candle.open))
        close_price = float(Decimal(candle.close))
        high_price = float(Decimal(candle.high))
        low_price = float(Decimal(candle.low))
        volume = float(Decimal(candle.volume))
        timestamp = datetime.fromtimestamp(candle.timestamp, tz=timezone.utc)

        if isinstance(receipt_timestamp, datetime):
            receipt_ts = receipt_timestamp.astimezone(timezone.utc) if receipt_timestamp.tzinfo else receipt_timestamp.replace(tzinfo=timezone.utc)
        else:
            receipt_ts = datetime.fromtimestamp(receipt_timestamp, tz=timezone.utc)

        async with lock:
            last_error: Exception | None = None
            for attempt in range(3):
                client = await get_client()
                try:
                    await client.execute(
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
                    break
                except Exception as exc:
                    last_error = exc
                    logger.warning(f'ClickHouse write failed (attempt {attempt + 1}/3): {exc}')
                    await reset_client()
            else:
                assert last_error is not None
                raise last_error

    return candle_callback


# symbols_callback removed: dynamic symbol management will be handled by a dedicated watcher/manager


def main() -> None:
    """Run the Binance candle collector."""
    config = load_config(CONFIG_PATH)
    # Merge host/port overrides before any ClickHouse interaction.

    shard_size = int(config.get('SHARD_SIZE', DEFAULT_SHARD_SIZE))
    watcher_interval_sec = int(config.get('WATCHER_INTERVAL_SEC', DEFAULT_WATCHER_INTERVAL_SEC))
    startup_delay_sec = int(config.get('STARTUP_DELAY_SEC', DEFAULT_STARTUP_DELAY_SEC))

    logger.info(f'Delay start by {startup_delay_sec} sec so that DB are ready')
    time.sleep(startup_delay_sec)
    logger.info('start')

    symbol_type = config['SYMBOLS_TYPE']
    timeframe = config['TIMEFRAME']
    symbols_whitelist = config.get('SYMBOLS_WHITELIST') or []
    symbols_blacklist = config.get('SYMBOLS_BLACKLIST') or []

    clickhouse_host = config.get('CLICKHOUSE_HOST', DEFAULT_CLICKHOUSE_HOST)
    clickhouse_port = int(config.get('CLICKHOUSE_PORT', DEFAULT_CLICKHOUSE_PORT))

    exchange_name = (
        config.get('COLLECTOR_EXCHANGE')
        or config.get('EXCHANGE')
        or 'binance'
    )
    exchange_class = get_exchange_class(exchange_name)

    def format_preview(collection: Sequence[str], limit: int, *, empty_label: str) -> str:
        """Return a short textual preview for Telegram notifications."""
        if not collection:
            return empty_label
        preview = ', '.join(collection[:limit])
        extras = len(collection) - limit
        if extras > 0:
            preview += f' (+{extras} more)'
        return preview

    candle_handler = make_candle_callback(clickhouse_host, clickhouse_port)
    notifier = TelegramNotifier.from_config(config)
    startup_notified = False

    # Restart the capture loop whenever the handler stops (e.g., network hiccups).
    while True:
        logger.info('Start new loop')
        ensure_schema(clickhouse_host, clickhouse_port)
        logger.info('ClickHouse schema ensured')

        symbols = filter_symbols(
            exchange_class.symbols(),
            symbol_type,
            symbols_whitelist,
            symbols_blacklist,
        )
        logger.info(f'Add symbols: {len(symbols)}')

        if notifier and not startup_notified:
            whitelist_preview = format_preview(symbols_whitelist, 5, empty_label='auto')
            blacklist_preview = format_preview(symbols_blacklist, 5, empty_label='[]')
            sample_symbols = format_preview(symbols, 10, empty_label='none')

            message_lines = [
                'Collector started',
                '-----------------',
                f'Timeframe        : {timeframe}',
                f'Exchange         : {exchange_name}',
                f'Symbol filter    : {symbol_type}',
                f'Total symbols    : {len(symbols)}',
                f'Shard size       : {shard_size}',
                f'ClickHouse       : {clickhouse_host}:{clickhouse_port}',
                '',
                f'Whitelist preview: {whitelist_preview}',
                f'Blacklist preview: {blacklist_preview}',
                #f'Symbol sample    : {sample_symbols}',
            ]
            notifier.send('\n'.join(message_lines))
            startup_notified = True

        callbacks = {CANDLES: candle_handler}
        feed_handler = FeedHandler()
        # Python 3.12 + uvloop: в главном потоке по умолчанию нет текущего event loop.
        # Создаём и устанавливаем новый цикл явно.
        event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(event_loop)

        for idx, chunk in enumerate(chunks(symbols, shard_size)):
            logger.info(f'ADD shard {idx} feed with {len(chunk)} symbols')
            # Keep shard size within Binance limits to avoid subscription drops.
            feed_handler.add_feed(
                create_exchange(
                    exchange_name,
                    symbols=chunk,
                    channels=[CANDLES],
                    callbacks=callbacks,
                    candle_interval=timeframe,
                    candle_closed_only=True,
                )
            )

        feed_handler.run(start_loop=False)

        known_symbols = set(symbols)
        # Keep subscriptions fresh by tracking newly listed symbols in the background.
        # Явно используем созданный выше цикл для фоновой задачи
        event_loop.create_task(
            symbols_watcher(
                feed_handler,
                exchange_name,
                exchange_class,
                symbol_type,
                timeframe,
                known_symbols,
                symbols_whitelist,
                symbols_blacklist,
                candle_handler,
                shard_size=shard_size,
                interval_sec=watcher_interval_sec,
            )
        )

        event_loop.run_forever()
        # Brief pause prevents a tight restart loop on repeated failures.
        time.sleep(5)


if __name__ == '__main__':
    main()
