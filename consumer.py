from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer


# -----------------------------
# Schema Registry configuration
# -----------------------------
schema_registry_conf = {
    "url": "http://localhost:8081"
}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)


# -----------------------------
# Avro deserializer
# -----------------------------
avro_deserializer = AvroDeserializer(
    schema_registry_client
)


# -----------------------------
# Kafka consumer configuration
# -----------------------------
consumer_conf = {
    "bootstrap.servers": "localhost:9092",

    "key.deserializer": StringDeserializer("utf_8"),

    "value.deserializer": avro_deserializer,

    "group.id": "order-consumer-group",

    "auto.offset.reset": "earliest"
}


consumer = DeserializingConsumer(consumer_conf)


# Subscribe to orders topic
consumer.subscribe(["orders"])


print("Consumer started...")
print("Waiting for orders...\n")

total_price = 0.0
order_count = 0

try:

    while True:

        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("Consumer error:", msg.error())
            continue

        order = msg.value()

        price = order["price"]

        total_price += price
        order_count += 1

        running_average = total_price / order_count

        print("Received Order:")
        print(f"Order ID        : {order['orderId']}")
        print(f"Product         : {order['product']}")
        print(f"Price           : {order['price']:.2f}")
        print(f"Orders Processed: {order_count}")
        print(f"Running Average : {running_average:.2f}")
        print("-" * 40)


except KeyboardInterrupt:

    print("\nConsumer stopped.")


finally:

    consumer.close()