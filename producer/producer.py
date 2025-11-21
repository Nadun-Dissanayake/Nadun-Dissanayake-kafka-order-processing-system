import json
import os
import random
import time
import uuid
from io import BytesIO

from confluent_kafka import Producer
from fastavro import schemaless_writer, parse_schema


# Load Avro schema
def load_schema():
    schema_path = os.path.join(
        os.path.dirname(__file__), "..", "schemas", "order.avsc"
    )
    with open(schema_path, "r") as f:
        schema_json = json.load(f)
    return parse_schema(schema_json)

SCHEMA = load_schema()

PRODUCTS = ["item1", "item2", "item3", "item4", "item5"]


def avro_encode(record: dict) -> bytes:
    buf = BytesIO()
    schemaless_writer(buf, SCHEMA, record)
    return buf.getvalue()


def delivery_report(err, msg):
    if err:
        print("Delivery failed:", err)
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")


def create_producer():
    return Producer({"bootstrap.servers": "localhost:9092"})


def build_order():
    return {
        "orderId": str(uuid.uuid4()),
        "product": random.choice(PRODUCTS),
        "price": float(round(random.uniform(10, 150), 2)),
    }


def main():
    producer = create_producer()

    print("Producer started (Ctrl+C to stop)…")

    try:
        while True:
            order = build_order()
            encoded = avro_encode(order)

            producer.produce(
                "orders",
                key=order["orderId"],
                value=encoded,
                callback=delivery_report
            )
            producer.flush()

            print("Produced:", order)
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping producer…")


if __name__ == "__main__":
    main()
