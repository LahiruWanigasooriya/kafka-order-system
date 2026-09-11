from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import time

MAX_RETRIES = 3

def process_order(order):

    product = order["product"]

    # Simulated permanent failure
    if product == "Item5":
        raise ValueError("Permanent failure: invalid/unprocessable product")

    print(
        f"Order {order['orderId']} processed successfully."
    )

def process_with_retry(order):

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            # Simulate a temporary error for Item2
            if order["product"] == "Item2" and attempt < 3:

                raise ConnectionError(
                    "Temporary service failure"
                )

            process_order(order)

            return True


        except ConnectionError as error:

            print(
                f"Temporary failure for Order "
                f"{order['orderId']}"
            )

            print(
                f"Retry attempt {attempt}/{MAX_RETRIES}"
            )

            print("Reason:", error)

            time.sleep(1)


        except ValueError:

            # Permanent errors should not be retried
            raise


    return False
    
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