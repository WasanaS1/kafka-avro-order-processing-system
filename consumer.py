from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from typing import Dict

from kafka.errors import KafkaError

from common import DLQ_TOPIC, ORDERS_TOPIC, create_consumer, create_producer


class TemporaryProcessingError(RuntimeError):
    pass


class PermanentProcessingError(RuntimeError):
    pass


@dataclass
class ProcessingStats:
    count: int = 0
    total_price: float = 0.0

    def add(self, price: float) -> float:
        self.count += 1
        self.total_price += price
        return self.total_price / self.count


def simulate_processing(order: Dict[str, object], attempt: int) -> None:
    order_id = str(order["orderId"])
    last_digit = int(order_id[-1])

    if last_digit == 9:
        raise PermanentProcessingError("simulated permanent validation failure")

    if last_digit == 3 and attempt < 2:
        raise TemporaryProcessingError("simulated transient timeout")


def send_to_dlq(producer, order: Dict[str, object], reason: str, attempts: int) -> None:
    headers = [
        ("error_type", b"processing_failure"),
        ("error_message", reason.encode("utf-8")),
        ("attempts", str(attempts).encode("utf-8")),
        ("source_topic", ORDERS_TOPIC.encode("utf-8")),
    ]
    future = producer.send(DLQ_TOPIC, key=str(order["orderId"]), value=order, headers=headers)
    metadata = future.get(timeout=10)
    print(
        f"dlq orderId={order['orderId']} reason={reason} sent to {metadata.topic}@{metadata.partition}:{metadata.offset}"
    )


def process_order(order: Dict[str, object], dlq_producer, stats: ProcessingStats, max_retries: int, retry_delay: float) -> None:
    last_error: Exception | None = None

    for attempt in range(max_retries + 1):
        try:
            simulate_processing(order, attempt)
            running_average = stats.add(float(order["price"]))
            print(
                f"processed orderId={order['orderId']} price={float(order['price']):.2f} "
                f"running_average={running_average:.2f} count={stats.count}"
            )
            return
        except TemporaryProcessingError as exc:
            last_error = exc
            if attempt < max_retries:
                wait_time = retry_delay * (2**attempt)
                print(
                    f"temporary failure for orderId={order['orderId']} attempt={attempt + 1}/{max_retries + 1}: {exc}; "
                    f"retrying in {wait_time:.1f}s"
                )
                time.sleep(wait_time)
                continue
            send_to_dlq(dlq_producer, order, str(exc), attempt + 1)
            return
        except PermanentProcessingError as exc:
            last_error = exc
            send_to_dlq(dlq_producer, order, str(exc), attempt + 1)
            return

    if last_error is not None:
        send_to_dlq(dlq_producer, order, str(last_error), max_retries + 1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume Avro-encoded order messages from Kafka.")
    parser.add_argument("--group-id", default="order-processor", help="Kafka consumer group id.")
    parser.add_argument("--max-retries", type=int, default=2, help="Maximum retry attempts for temporary failures.")
    parser.add_argument("--retry-delay", type=float, default=1.0, help="Initial delay in seconds between retries.")
    args = parser.parse_args()

    consumer = create_consumer(ORDERS_TOPIC, args.group_id)
    dlq_producer = create_producer()
    stats = ProcessingStats()

    print(f"listening on {ORDERS_TOPIC}; publishing failures to {DLQ_TOPIC}")

    try:
        for message in consumer:
            order = message.value
            print(f"received orderId={order['orderId']} from partition={message.partition} offset={message.offset}")
            process_order(order, dlq_producer, stats, args.max_retries, args.retry_delay)
            consumer.commit()
    except KeyboardInterrupt:
        print("stopping consumer")
    except KafkaError as exc:
        print(f"kafka error: {exc}")
    finally:
        consumer.close()
        dlq_producer.flush()
        dlq_producer.close()


if __name__ == "__main__":
    main()
