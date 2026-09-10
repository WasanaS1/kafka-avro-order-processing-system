# Kafka Order Processing Assignment

This project demonstrates a Kafka-based order pipeline with:

- Avro serialization for every order message
- A producer that generates order events
- A consumer that computes a real-time running average of prices
- Retry logic for temporary failures
- A dead letter queue (DLQ) for permanently failed messages
- A live demo flow suitable for assignment submission

## Files

- `order.avsc` - Avro schema for the order message
- `producer.py` - Sends Avro-encoded orders to Kafka
- `consumer.py` - Consumes orders, retries temporary failures, updates the running average, and sends permanent failures to the DLQ
- `dlq_consumer.py` - Reads messages from the DLQ topic for inspection
- `docker-compose.yml` - Local Kafka + ZooKeeper stack
- `requirements.txt` - Python dependencies

## Message Schema

Each order message contains:

- `orderId` - unique order identifier
- `product` - product name
- `price` - product price as a float

## Prerequisites

- Python 3.10+
- Docker Desktop or Docker Engine with Docker Compose

## Setup

1. Start Kafka locally:

   ```bash
   docker compose up -d
   ```

2. Install Python dependencies:

   ```bash
   pip install -r requirements.txt
   ```

## Run the Demo

1. Start the consumer:

   ```bash
   python consumer.py
   ```

2. In another terminal, produce sample orders:

   ```bash
   python producer.py --count 10
   ```

3. Optional: inspect the DLQ in a third terminal:

   ```bash
   python dlq_consumer.py
   ```

## How the Failure Demo Works

The consumer uses deterministic rules so the assignment is easy to present live:

- Order IDs ending in `3` simulate a temporary failure on the first two attempts, then succeed.
- Order IDs ending in `9` simulate a permanent failure and are sent to the DLQ.
- Successful orders update the running average immediately.

## Live Demonstration Script

A simple presentation flow is:

1. Show the schema in `order.avsc`.
2. Start Kafka with Docker Compose.
3. Start `consumer.py` and explain the retry/DLQ behavior.
4. Run `producer.py` to send orders.
5. Point out the running average updates in the consumer logs.
6. Show the DLQ output with `dlq_consumer.py`.

## Notes

- The project stores Avro data directly in Kafka message values, so no schema registry is required.
- Topic names can be changed with `ORDERS_TOPIC` and `DLQ_TOPIC` environment variables.
- The Kafka bootstrap server defaults to `localhost:9092`.
