# Kafka Order Processing System

A Python and Apache Kafka take-home project that produces and consumes purchase orders using **Avro serialization**, calculates a **running average of successfully processed order prices**, retries simulated temporary failures, and routes permanently failed orders to a **Dead Letter Queue (DLQ)**.

## Features

- Generates orders with `orderId`, `product`, and a randomized `price`.
- Serializes producer messages and deserializes consumer messages with Avro and Confluent Schema Registry.
- Processes orders from Kafka's `orders` topic.
- Calculates the running average of prices for successfully processed orders only.
- Retries temporary processing failures for up to three total attempts.
- Publishes permanently failed orders to `orders-dlq`, which a separate consumer reads.
- Runs Kafka, ZooKeeper, and Schema Registry in Docker; Python scripts run on the host machine.

## Architecture

```text
Python producer.py
    |  Avro serialization
    v
Kafka: orders
    |  Avro deserialization
    v
Python consumer.py
    |-- Success ------------------> Update running average
    |-- Temporary failure -------> Retry locally (up to 3 attempts)
    |                                 |-- Success --> Update average
    |                                 `-- Exhausted --> orders-dlq
    `-- Permanent failure -------------------------> orders-dlq
                                                        |
                                                        v
                                              Python dlq_consumer.py
```

**Note:** The `orders-retry` topic is created in this setup but **is not used by the current retry implementation**. Retries occur within `consumer.py` rather than through a Kafka retry-topic pipeline.

## Technology stack

| Component | Technology |
| --- | --- |
| Message broker | Apache Kafka |
| Coordination for this Kafka configuration | ZooKeeper |
| Schema management | Confluent Schema Registry |
| Message format | Apache Avro |
| Producer and consumers | Python, `confluent-kafka` |
| Infrastructure | Docker Compose |

## Project files

```text
kafka-order-system/
├── docker-compose.yml
├── order.avsc
├── producer.py
├── consumer.py
├── dlq_consumer.py
├── requirements.txt
├── README.md
└── .gitignore
```

The layout above describes the expected project files; ensure they are all present before submission.

## Order schema

`order.avsc` defines the three required fields:

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.assignment.orders",
  "fields": [
    { "name": "orderId", "type": "string" },
    { "name": "product", "type": "string" },
    { "name": "price", "type": "float" }
  ]
}
```

An example order is:

```json
{"orderId": "1001", "product": "Item2", "price": 450.25}
```

## Prerequisites

- Docker Desktop with Docker Compose enabled.
- Python installed and available in the terminal.
- A terminal opened in the project directory.

The example commands below are for **Windows PowerShell**. Kafka is accessible to host-side Python at `localhost:9092`, and Schema Registry at `http://localhost:8081`. Inside Docker, Schema Registry connects to Kafka using `kafka:29092`.

## Setup and run

### 1. Start the infrastructure

```powershell
docker compose up -d
docker compose ps
```

Wait until Kafka and Schema Registry have started. The compose setup runs ZooKeeper, Kafka, and Schema Registry.

### 2. Install Python dependencies

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

If PowerShell blocks virtual-environment activation, use Command Prompt with `venv\Scripts\activate.bat`, or invoke `venv\Scripts\python.exe` directly.

### 3. Create the Kafka topics

Run these commands if the topics do not already exist:

```powershell
docker exec kafka kafka-topics --create --if-not-exists --topic orders --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --create --if-not-exists --topic orders-retry --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --create --if-not-exists --topic orders-dlq --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
```

Verify:

```powershell
docker exec kafka kafka-topics --list --bootstrap-server kafka:29092
```

Expected topic names (ordering may vary):

```text
orders
orders-dlq
orders-retry
```

### 4. Start the main consumer

In terminal 1:

```powershell
python consumer.py
```

### 5. Start the DLQ consumer

In terminal 2:

```powershell
python dlq_consumer.py
```

### 6. Start the producer

In terminal 3:

```powershell
python producer.py
```

The producer continuously sends randomly generated orders. Keep all three terminals visible during the live demonstration.

## Processing and failure behavior

The project uses **simulated** failures to demonstrate the assignment requirements; `Item2` and `Item5` are not inherently invalid real-world products.

| Product | Demonstration behavior | Outcome |
| --- | --- | --- |
| `Item1`, `Item3`, `Item4` | Normal processing | Included in running average |
| `Item2` | Temporary failures on attempts 1 and 2 | Succeeds on attempt 3; included in average |
| `Item5` | Simulated permanent failure | Sent directly to `orders-dlq`; excluded from average |

The retry function allows **three total processing attempts**. The running average is calculated as:

```text
running_average = total_price_of_successful_orders / successful_order_count
```

Both the total and count are held in memory, so the displayed average resets when the main consumer process restarts. The current implementation does not persist aggregation state across restarts.

## Verify Avro registration

After producing at least one message, run:

```powershell
curl.exe http://localhost:8081/subjects
```

The output should include a subject such as `orders-value`; publishing to the DLQ may also register `orders-dlq-value`. The precise list depends on which topics have received messages.

## Stop the system

Stop each Python script with **Ctrl+C**, then stop the infrastructure:

```powershell
docker compose down
```

This removes the project's containers but normally keeps downloaded images. **Warning:** `docker compose down -v` additionally removes associated volumes and may delete stored Kafka data; use it only when you intentionally want to reset the environment.
