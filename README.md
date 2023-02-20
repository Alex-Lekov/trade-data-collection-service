# Data Collection Service

The Data Collection Service is a Python application that collects market data from the Binance API and stores it in ClickHouse. The service runs in a Docker container, making it easy to set up and use.


## Requirements
- Python 3.8 or higher
- Cryptofeed
- ClickHouse
- Docker (if running in a Docker container)


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


# Project Structure

The project has the following structure:

```
data-collection-service/
├── app/
│   ├── config_sample.yaml
│   ├── data_collector.py
│   └── data_quality_check.py
├── docker-compose.yaml
└── README.md
```

- The app directory contains the configuration file and the Python scripts for the data collector and data quality check services.
- The docker-compose.yaml file defines the services and their dependencies.


### data_collector.py
The data_collector.py script collects candle data from Binance futures and stores it in ClickHouse.

### data_quality_check.py
The data_quality_check.py script checks the data quality of the candle data in ClickHouse. 