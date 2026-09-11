import json
import random
import time
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


# Read Avro schema
with open("order.avsc", "r") as file:
    schema_str = file.read()


# Schema Registry
schema_registry_conf = {
    "url": "http://localhost:8081"
}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)


# Avro serializer
avro_serializer = AvroSerializer(
    schema_registry_client,
    schema_str
)


# Kafka producer
producer_conf = {
    "bootstrap.servers": "localhost:9092",
    "key.serializer": StringSerializer("utf_8"),
    "value.serializer": avro_serializer
}

producer = SerializingProducer(producer_conf)


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Order sent to {msg.topic()} "
            f"[partition {msg.partition()}]"
        )


order_number = 1001

while True:

    order = {
        "orderId": str(order_number),
        "product": f"Item{random.randint(1, 5)}",
        "price": round(random.uniform(100, 1000), 2)
    }

    producer.produce(
        topic="orders",
        key=order["orderId"],
        value=order,
        on_delivery=delivery_report
    )

    producer.flush()

    print("Produced:", order)

    order_number += 1

    time.sleep(2)