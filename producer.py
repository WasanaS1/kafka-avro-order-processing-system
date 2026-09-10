from __future__ import annotations

import argparse
import random
from typing import Dict, List

from common import ORDERS_TOPIC, create_producer

PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]


def build_orders(count: int, seed: int | None = None) -> List[Dict[str, object]]:
    rng = random.Random(seed)
    orders: List[Dict[str, object]] = []

    for index in range(count):
        order_id = str(1001 + index)
        product = PRODUCTS[index % len(PRODUCTS)]
        price = round(rng.uniform(10.0, 100.0), 2)
        orders.append({"orderId": order_id, "product": product, "price": price})

    return orders


def main() -> None:
    parser = argparse.ArgumentParser(description="Produce Avro-encoded order messages to Kafka.")
    parser.add_argument("--count", type=int, default=10, help="Number of orders to send.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for repeatable prices.")
    args = parser.parse_args()

    producer = create_producer()
    orders = build_orders(args.count, args.seed)

    for order in orders:
        future = producer.send(ORDERS_TOPIC, key=order["orderId"], value=order)
        metadata = future.get(timeout=10)
        print(
            f"sent orderId={order['orderId']} product={order['product']} price={order['price']:.2f} "
            f"to {metadata.topic}@{metadata.partition}:{metadata.offset}"
        )

    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
