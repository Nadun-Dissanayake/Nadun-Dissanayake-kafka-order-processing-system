# Kafka Avro Orders – Producer, Consumer & DLQ (Python)

This project demonstrates a simple Kafka-based order processing pipeline using **Python**, **Avro**, and **Docker**:

- A **producer** sends random `Order` events (Avro-encoded) to the `orders` topic.
- A **consumer** reads from `orders`, computes a running average of prices, and **retries** failed messages up to `MAX_RETRIES`.
- If processing still fails (e.g., price too high), the message is sent to a **Dead Letter Queue (DLQ)** topic `orders_dlq`.
- A **DLQ consumer** reads from `orders_dlq` and prints failed events for inspection.
- Kafka and Zookeeper run via **Docker Compose**.

---

## 1. Project Structure

Suggested structure (matches the files in this repo):

```text
.
├── docker-compose.yml
├── schemas
│   └── order.avsc
├── producer
│   └── producer.py
└── consumer
    ├── consumer.py        # main consumer with retries + DLQ producer
    └── dlq_consumer.py    # DLQ consumer (second script from code)
