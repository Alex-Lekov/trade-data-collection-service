"""Utilities for managing ClickHouse schema used by the collectors."""

from __future__ import annotations

from dataclasses import dataclass

import clickhouse_driver
import os
import yaml

def _load_config() -> dict:
    """Load config once at import time to parameterize DB and base table names."""
    try:
        config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
        with open(config_path, 'r') as fh:
            return yaml.safe_load(fh) or {}
    except FileNotFoundError:
        return {}

_CONFIG = _load_config()
# Use EXCHANGE value as the ClickHouse database name (e.g., 'binance_futures')
DATABASE_NAME = str(_CONFIG.get('EXCHANGE', 'binance_futures'))
# Base table name is derived from TIMEFRAME value (e.g., '1m' -> 'candles_1m')
TIMEFRAME = str(_CONFIG.get('TIMEFRAME', '1m')).lower()
CANDLES_TABLE = f'candles_{TIMEFRAME}'
CANDLES_TABLE_FULL = f'{DATABASE_NAME}.{CANDLES_TABLE}'
# Use TIMEFRAME as the base interval for rollups and views
BASE_INTERVAL = TIMEFRAME
ROLLUP_MINUTES = (5, 15, 30, 60, 120, 240, 1440)


@dataclass(frozen=True)
class RollupSpec:
    """Definition for a materialized rollup timeframe."""

    minutes: int
    parent: 'RollupSpec | None' = None

    @property
    def label(self) -> str:
        minutes = self.minutes
        if minutes % 1440 == 0:
            days = minutes // 1440
            return f'{days}d'
        if minutes % 60 == 0:
            hours = minutes // 60
            return f'{hours}h'
        return f'{minutes}m'

    @property
    def source_label(self) -> str:
        return BASE_INTERVAL if self.parent is None else self.parent.label

    @property
    def table_name(self) -> str:
        return f'candles_{self.label}'

    @property
    def table_full(self) -> str:
        return f'{DATABASE_NAME}.{self.table_name}'

    @property
    def view_name(self) -> str:
        return f'mv_{self.source_label}_to_{self.label}'

    @property
    def view_full(self) -> str:
        return f'{DATABASE_NAME}.{self.view_name}'

    @property
    def source_table_full(self) -> str:
        return CANDLES_TABLE_FULL if self.parent is None else self.parent.table_full

    @property
    def source_time_column(self) -> str:
        return 'start' if self.parent is None else 'candle_start'

    @property
    def uses_merge_states(self) -> bool:
        return self.parent is not None

    def interval_expr(self) -> str:
        minutes = self.minutes
        column = self.source_time_column
        if minutes % 1440 == 0:
            days = minutes // 1440
            return f'toStartOfInterval({column}, INTERVAL {days} DAY)'
        if minutes % 60 == 0:
            hours = minutes // 60
            return f'toStartOfInterval({column}, INTERVAL {hours} HOUR)'
        return f'toStartOfInterval({column}, INTERVAL {minutes} MINUTE)'


def build_rollup_specs() -> tuple[RollupSpec, ...]:
    specs: list[RollupSpec] = []
    parent: RollupSpec | None = None
    for minutes in ROLLUP_MINUTES:
        if parent and minutes % parent.minutes != 0:
            raise ValueError(f'Невозможно построить каскад: {minutes} не кратно {parent.minutes}')
        spec = RollupSpec(minutes=minutes, parent=parent)
        specs.append(spec)
        parent = spec
    return tuple(specs)


ROLLUP_SPECS = build_rollup_specs()

CREATE_DATABASE_QUERY = f'CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}'

def ensure_database_exists(config: dict) -> None:
    """
    Ensure ClickHouse database exists. Connect without selecting a database.
    Emits a minimal startup log to stdout.
    """
    host = config.get('CLICKHOUSE_HOST', 'clickhouse')
    port = int(config.get('CLICKHOUSE_PORT', 9000))
    # Database name now derives from EXCHANGE
    db = str(config.get('EXCHANGE', DATABASE_NAME))
    with clickhouse_driver.Client(host=host, port=port) as client:
        client.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
    print(f"ClickHouse database '{db}' is ready")

CREATE_CANDLES_TABLE_QUERY = f'''
    CREATE TABLE IF NOT EXISTS {CANDLES_TABLE_FULL} (
        exchange          LowCardinality(String),
        symbol            LowCardinality(String),
        interval          LowCardinality(String),
        start             DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
        stop              DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
        close_unixtime    UInt64 CODEC(T64, ZSTD(1)),
        trades            UInt32 CODEC(T64, ZSTD(1)),
        open              Float64 CODEC(Gorilla, ZSTD),
        high              Float64 CODEC(Gorilla, ZSTD),
        low               Float64 CODEC(Gorilla, ZSTD),
        close             Float64 CODEC(Gorilla, ZSTD),
        volume            Float64 CODEC(DoubleDelta, ZSTD),
        timestamp         DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
        receipt_timestamp DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(DoubleDelta, ZSTD(1)),
        CONSTRAINT ohlc_finite CHECK isFinite(open) AND isFinite(high) AND isFinite(low) AND isFinite(close) AND isFinite(volume),
        CONSTRAINT ohlc_order CHECK (low <= least(open, close) AND high >= greatest(open, close) AND low <= high),
        CONSTRAINT close_time_positive CHECK close_unixtime > 0,
        CONSTRAINT keys_not_empty CHECK (exchange != '' AND symbol != '' AND interval != '')
    ) ENGINE = ReplacingMergeTree(receipt_timestamp)
    PARTITION BY toYYYYMM(start)
    ORDER BY (exchange, symbol, start, interval)
    SETTINGS index_granularity = 8192
'''

def build_rollup_table_query(spec: RollupSpec) -> str:
    return f'''
        CREATE TABLE IF NOT EXISTS {spec.table_full} (
            exchange       LowCardinality(String),
            symbol         LowCardinality(String),
            candle_start   DateTime64(3, 'UTC'),
            open           AggregateFunction(argMin, Float64, DateTime64(3, 'UTC')),
            high           AggregateFunction(max, Float64),
            low            AggregateFunction(min, Float64),
            close          AggregateFunction(argMax, Float64, DateTime64(3, 'UTC')),
            volume         AggregateFunction(sum, Float64),
            trades         AggregateFunction(sum, UInt64)
        ) ENGINE = AggregatingMergeTree()
        PARTITION BY toYYYYMM(candle_start)
        ORDER BY (exchange, symbol, candle_start)
    '''


def build_rollup_view_query(spec: RollupSpec) -> str:
    interval_expr = spec.interval_expr()
    if spec.uses_merge_states:
        open_expr = 'argMinMergeState(open)'
        high_expr = 'maxMergeState(high)'
        low_expr = 'minMergeState(low)'
        close_expr = 'argMaxMergeState(close)'
        volume_expr = 'sumMergeState(volume)'
        trades_expr = 'sumMergeState(trades)'
        where_clause = ''
        final_clause = ''
    else:
        open_expr = f'argMinState(open, {spec.source_time_column})'
        high_expr = 'maxState(high)'
        low_expr = 'minState(low)'
        close_expr = f'argMaxState(close, {spec.source_time_column})'
        volume_expr = 'sumState(volume)'
        trades_expr = 'sumState(toUInt64(trades))'
        where_clause = f"WHERE interval = '{BASE_INTERVAL}'"
        # FINAL запрещён в MATERIALIZED VIEW — убираем
        final_clause = ''

    return f'''
        CREATE MATERIALIZED VIEW IF NOT EXISTS {spec.view_full}
        TO {spec.table_full}
        AS
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
        FROM {spec.source_table_full} {final_clause}
        {where_clause}
        GROUP BY exchange, symbol, candle_start
    '''


ROLLUP_TABLE_QUERIES = tuple(build_rollup_table_query(spec) for spec in ROLLUP_SPECS)
ROLLUP_VIEW_QUERIES = tuple(build_rollup_view_query(spec) for spec in ROLLUP_SPECS)

def drop_rollup_structures(client: clickhouse_driver.Client) -> None:
    """
    Drop rollup MATERIALIZED VIEWs and tables if they exist.
    Important: drop views first (they depend on tables), then tables.
    This is destructive and will remove aggregated data; base candles table is preserved.
    """
    # Drop all views (no specific order required)
    for spec in ROLLUP_SPECS:
        client.execute(f"DROP VIEW IF EXISTS {spec.view_full}")
    # Drop all tables
    for spec in ROLLUP_SPECS:
        client.execute(f"DROP TABLE IF EXISTS {spec.table_full}")

INSERT_CANDLES_QUERY = f'''
    INSERT INTO {CANDLES_TABLE_FULL}
    (exchange, symbol, interval, start, stop, close_unixtime, trades, open, high, low, close, volume, timestamp, receipt_timestamp)
    VALUES
'''


def ensure_schema(host: str, port: int) -> None:
    """Ensure the ClickHouse database and tables exist."""
    with clickhouse_driver.Client(host=host, port=port) as client:
        client.execute(CREATE_DATABASE_QUERY)
        client.execute(CREATE_CANDLES_TABLE_QUERY)
        # Destructive refresh of rollup structures to align data types (approved by user)
        drop_rollup_structures(client)
        # Re-create rollup tables and views with correct DateTime64(3, 'UTC') aggregate state types
        for query in ROLLUP_TABLE_QUERIES:
            client.execute(query)
        for query in ROLLUP_VIEW_QUERIES:
            client.execute(query)
