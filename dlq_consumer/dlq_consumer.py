from confluent_kafka import Consumer
import json


def create_consumer():
    c = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "dlq-group",
        "auto.offset.reset": "earliest",
    })
    c.subscribe(["orders_dlq"])
    return c


def main():
    consumer = create_consumer()
    print("DLQ consumer started…")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            data = json.loads(msg.value().decode())
            print("\n--- DLQ EVENT ---")
            print(json.dumps(data, indent=2))
    except KeyboardInterrupt:
        print("Stopping DLQ consumer…")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
