# Data Collection Service

The Data Collection Service is a Python application that collects market data from the Binance API and stores it in ClickHouse. The service runs in a Docker container, making it easy to set up and use.

## Features:
- receiving real-time data on all coins from Binance-Futures (candles tf:"1m")
- automatic validation and filling missed candles in data
- automatic loading of history
- automatic removal of duplicates


## Requirements
- Python 3.8 or higher
- Cryptofeed
- ClickHouse
- Docker (if running in a Docker container)

# Installation


# Getting Started

## Configuration
Before using the Data Collection Service, you'll need to set up a configuration file named config.yaml. This file should be placed in the [./app](./app) directory of the project. You can use the provided [config_sample.yaml](./app/config_sample.yaml) as a starting point.

### Using Docker
To start the application services, navigate to the root directory of the project in your terminal and run the following command:
```
docker-compose up
```
This will start the data collector and data quality check services, as well as the ClickHouse database service. You should see the logs of the services in the terminal.

### Accessing the Application
Once the services are up and running, you can access the application at http://localhost:8123. This will open the ClickHouse web interface, where you can query the data that was collected and perform other database-related tasks.

### Stopping the Services
To stop the application services, press Ctrl+C in the terminal where you started the services. This will stop and remove the containers, but the data in the data directory will persist.

### Customizing the Configuration
You can customize the services by modifying the docker-compose.yaml file. For example, you can change the image names or build contexts, adjust the container volumes, or set environment variables. Please refer to the [Docker Compose documentation](https://docs.docker.com/compose/) for more information on the available configuration options.

___

# Sample Code Use
## Selecting Last 5000 Candles:
python 
```python
import clickhouse_driver

with clickhouse_driver.Client(host='localhost', port=9000) as ch:
    # select last 5000 candles
    query = f'SELECT * FROM binance_data.candles FINAL ORDER BY timestamp DESC LIMIT 5000'
    result = ch.execute(query)

print(result[:2])
```
output:
```
[('BINANCE_FUTURES',
  'BTC-USDT-PERP',
  datetime.datetime(2023, 2, 21, 20, 39),
  datetime.datetime(2023, 2, 21, 20, 39, 59),
  1677011968.0,
  '1m',
  1880,
  24490.400390625,
  24509.099609375,
  24509.30078125,
  24482.599609375,
  214.322,
  datetime.datetime(2023, 2, 21, 20, 40),
  datetime.datetime(2023, 2, 21, 20, 40)),
 ('BINANCE_FUTURES',
  'BTC-USDT-PERP',
  datetime.datetime(2023, 2, 21, 20, 40),
  datetime.datetime(2023, 2, 21, 20, 40, 59),
  1677012096.0,
  '1m',
  2062,
  24509.099609375,
  24511.19921875,
  24523.69921875,
  24507.5,
  183.713,
  datetime.datetime(2023, 2, 21, 20, 41),
  datetime.datetime(2023, 2, 21, 20, 41))]
```

## Selecting All Candles of a Certain Coin:
```python
import clickhouse_driver

with clickhouse_driver.Client(host='localhost', port=9000) as ch:
    # select all candles of a certain coin
    symbol = 'BTC-USDT-PERP'
    query = '''SELECT * FROM binance_data.candles FINAL WHERE symbol = %(symbol)s'''
    result = ch.execute(query, {'symbol': symbol,})
```


# Project Structure

The project has the following structure:

```
data-collection-service/
├── app/
│   ├── config_sample.yaml
│   ├── data_collector.py
│   ├── data_quality_check.py
|   └── load_history.py
├── docker-compose.yaml
└── README.md
```

- The app directory contains the configuration file and the Python scripts for the data collector and data quality check services.
- The docker-compose.yaml file defines the services and their dependencies.


### data_collector.py
The data_collector.py script collects candle data from Binance futures and stores it in ClickHouse.

### data_quality_check.py
The data_quality_check.py script checks the data quality of the candle data in ClickHouse. 

### load_history.py
This is a script that collects market data from the Binance exchange and stores it in a database called ClickHouse.
The script is also configured using a file named "config.yaml", which contains settings such as the start date for collecting data and whether or not to load historical data. 