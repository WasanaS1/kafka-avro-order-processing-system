from __future__ import annotations

import io
import json
import os
from pathlib import Path
from typing import Any, Dict

from fastavro import parse_schema, schemaless_reader, schemaless_writer
from kafka import KafkaConsumer, KafkaProducer

BASE_DIR = Path(__file__).resolve().parent
SCHEMA_PATH = BASE_DIR / "order.avsc"
ORDERS_TOPIC = os.getenv("ORDERS_TOPIC", "orders")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "orders.dlq")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CLIENT_ID = os.getenv("KAFKA_CLIENT_ID", "order-assignment")

with SCHEMA_PATH.open("r", encoding="utf-8") as schema_file:
    ORDER_SCHEMA = parse_schema(json.load(schema_file))


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=serialize_order,
        key_serializer=lambda value: value.encode("utf-8") if value is not None else None,
        acks="all",
        retries=5,
        linger_ms=10,
    )


def create_consumer(topic: str, group_id: str) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        key_deserializer=lambda value: value.decode("utf-8") if value is not None else None,
        value_deserializer=deserialize_order,
        client_id=CLIENT_ID,
    )


def serialize_order(order: Dict[str, Any]) -> bytes:
    buffer = io.BytesIO()
    schemaless_writer(buffer, ORDER_SCHEMA, order)
    return buffer.getvalue()


def deserialize_order(payload: bytes) -> Dict[str, Any]:
    buffer = io.BytesIO(payload)
    return schemaless_reader(buffer, ORDER_SCHEMA)
