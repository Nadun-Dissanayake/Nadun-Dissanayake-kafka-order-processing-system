import json
import os
import time
from dataclasses import dataclass
from io import BytesIO

from confluent_kafka import Consumer, Producer
from fastavro import schemaless_reader, parse_schema


MAX_RETRIES = 3


# Load schema
def load_schema():
    schema_path = os.path.join(
        os.path.dirname(__file__), "..", "schemas", "order.avsc"
    )
    with open(schema_path, "r") as f:
        schema_json = json.load(f)
    return parse_schema(schema_json)

SCHEMA = load_schema()


@dataclass
class RunningAvg:
    total: float = 0
    count: int = 0

    def add(self, value: float) -> float:
        self.total += value
        self.count += 1
        return self.total / self.count


stats = RunningAvg()


def avro_decode(raw: bytes) -> dict:
    return schemaless_reader(BytesIO(raw), SCHEMA)


def create_consumer():
    c = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "order-group",
        "auto.offset.reset": "earliest",
    })
    c.subscribe(["orders"])
    return c


def create_producer():
    return Producer({"bootstrap.servers": "localhost:9092"})


def fail_condition(order):
    return order["price"] > 100


def send_to_dlq(order, reason, producer):
    payload = json.dumps({
        "failed_order": order,
        "reason": reason
    }).encode()

    producer.produce(
        "orders_dlq",
        key=order["orderId"],
        value=payload
    )
    producer.flush()

    print(f"DLQ: {order['orderId']} -> {reason}")


def process(order):
    avg = stats.add(order["price"])
    print(f"Processed {order['orderId']} | price={order['price']} | avg={avg:.2f}")


def retry_process(order, producer):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if fail_condition(order):
                raise Exception("Simulated error: high price")

            process(order)
            return

        except Exception as e:
            print(f"Attempt {attempt}/{MAX_RETRIES} failed: {e}")

            if attempt == MAX_RETRIES:
                send_to_dlq(order, str(e), producer)
            else:
                time.sleep(1)


def main():
    consumer = create_consumer()
    producer = create_producer()

    print("Consumer started…")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            order = avro_decode(msg.value())
            retry_process(order, producer)

    except KeyboardInterrupt:
        print("Stopping consumer…")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
