from __future__ import annotations

import argparse

from common import DLQ_TOPIC, create_consumer


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect dead-lettered order messages.")
    parser.add_argument("--group-id", default="order-dlq-inspector", help="Kafka consumer group id.")
    args = parser.parse_args()

    consumer = create_consumer(DLQ_TOPIC, args.group_id)

    print(f"listening on {DLQ_TOPIC}")

    try:
        for message in consumer:
            headers = {key: value.decode("utf-8") for key, value in message.headers or []}
            order = message.value
            print(
                f"dlq received orderId={order['orderId']} product={order['product']} price={float(order['price']):.2f} "
                f"headers={headers} partition={message.partition} offset={message.offset}"
            )
    except KeyboardInterrupt:
        print("stopping DLQ consumer")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
