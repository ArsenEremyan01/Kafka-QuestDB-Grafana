# Using Kafka to Track BTCUSDT Agg Price Trends

This repo polls Coinbase API for various cryptocurrency prices and uses Kafka to calculate moving average prices and stores the data in a timeseries database, [QuestDB](https://questdb.io/), for further analysis.

## Prerequisites

- Docker (min of 4GB memory)
- Python 3.7+

## Structure

- docker-compose: holds docker-compose file to start Kafka (zookeeper, broker, kafka connect), QuestDB, and JSON file to initialize Kafka Connect
- docker: Dockerfile to build Kafka Connect image (if you wish to build locally or extend)
- Python files:
  - getData.py: polls Coinbase API and publishes records to Kafka
  - kafkaHelper.py: wrapper around kafka-python lib

## Quickstart

### Kafka Setup

Start up the Kafka/QuestDB stack:

```
cd docker-compose
docker compose up --build -d 
```

Wait until all the components are healthy (look at Kafka Connect container logs).

Post kafka-postgres-btc sink schema to Kafka Connect:

```
curl -X POST -H "Accept:application/json" -H "Content-Type:application/json" --data @connector.json http://localhost:8083/connectors
```

### Python Setup

Install the necessary packages:

```
pip install -r requirements.txt
```

Run the script to poll Coinbase API:

```
python getData.py
```
