# Data Collection Service

Collector that ingests candles from configurable cryptofeed exchanges and stores them in ClickHouse. The stack ships with:

- realtime streamer (`data_collector.py`) with shard rebalancing and ClickHouse retry logic;
- historical backfill (`load_history.py`) that loads in configurable chunks only until the first existing candle;
- quality watchdog (`data_quality_check.py`) that monitors freshness and refills gaps.

## Features
- supports spot/futures Binance feeds and any exchange exposed via cryptofeed (set in `config.yaml`);
- Telegram notifications on startup, failures and stale data;
- automatic gap detection and refill with configurable timeframe;
- optional Docker Compose bundle for quick local spin-up.

## Requirements
If you run locally:

- Python 3.11+ (tested with 3.12)
- ClickHouse 23+
- Docker & Docker Compose (optional but recommended)

## Configuration

Copy `app/config_sample.yaml` to `app/config.yaml` and adjust:

- `EXCHANGE` / `COLLECTOR_EXCHANGE` / `HISTORY_EXCHANGE` — cryptofeed exchange code (`binance`, `binance_futures`, …);
- `TIMEFRAME`, symbol filters and black/whitelists;
- `HISTORY_CHUNK_SIZE` — max candles per request (clamped to 1500);
- ClickHouse host/port overrides and Telegram credentials.

For quality checker you can also override `DATA_QUALITY_EXCHANGE` if нужно другое подключение.

## Running with Docker Compose

```bash
docker compose up --build
```


Ensure ClickHouse is reachable at the host/port defined in `config.yaml`.

## Exploring Data in a Notebook

For ad-hoc analysis you can spin up JupyterLab alongside the services:

```bash
pip install jupyterlab clickhouse-driver
jupyter lab --notebook-dir=notebooks
```

Example notebook snippet (`notebooks/ohlc_overview.ipynb`) to plot latest candles:

```python
import pandas as pd
from clickhouse_driver import Client

client = Client(host='localhost', port=9000)
rows = client.execute(
    """
    SELECT symbol, start, open, high, low, close
    FROM binance_data.candles FINAL
    WHERE symbol IN ('BTC-USDT-PERP', 'ETH-USDT-PERP')
      AND start >= subtractHours(now(), 6)
    ORDER BY symbol, start
    """
)
df = pd.DataFrame(rows, columns=['symbol', 'start', 'open', 'high', 'low', 'close'])
df.set_index('start').groupby('symbol').plot()
```

---


