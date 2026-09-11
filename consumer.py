import time

from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.serialization import StringDeserializer, StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import (
    AvroDeserializer,
    AvroSerializer
)


# =============================
# Configuration
# =============================

MAX_RETRIES = 3

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

ORDERS_TOPIC = "orders"
DLQ_TOPIC = "orders-dlq"


# =============================
# Schema Registry
# =============================

schema_registry_conf = {
    "url": SCHEMA_REGISTRY_URL
}

schema_registry_client = SchemaRegistryClient(
    schema_registry_conf
)


# =============================
# Read Avro schema
# =============================

with open("order.avsc", "r") as file:
    schema_str = file.read()


# =============================
# Avro Serializer
# Used for sending messages to DLQ
# =============================

avro_serializer = AvroSerializer(
    schema_registry_client,
    schema_str
)


# =============================
# Avro Deserializer
# Used for consuming order messages
# =============================

avro_deserializer = AvroDeserializer(
    schema_registry_client
)


# =============================
# DLQ Producer
# =============================

dlq_producer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "key.serializer": StringSerializer("utf_8"),
    "value.serializer": avro_serializer
}

dlq_producer = SerializingProducer(
    dlq_producer_conf
)


# =============================
# Kafka Consumer
# =============================

consumer_conf = {

    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,

    "key.deserializer": StringDeserializer("utf_8"),

    "value.deserializer": avro_deserializer,

    "group.id": "order-consumer-group",

    "auto.offset.reset": "earliest"
}

consumer = DeserializingConsumer(
    consumer_conf
)

consumer.subscribe([ORDERS_TOPIC])


# =============================
# Process Order
# =============================

def process_order(order):

    product = order["product"]

    # Simulated permanent failure
    if product == "Item5":

        raise ValueError(
            "Permanent failure: invalid/unprocessable product"
        )

    print(
        f"Order {order['orderId']} processed successfully."
    )


# =============================
# Retry Logic
# =============================

def process_with_retry(order):

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            # ----------------------------------
            # Simulate temporary failure
            # ----------------------------------
            # Item2 fails first two attempts
            # and succeeds on attempt 3
            # ----------------------------------

            if (
                order["product"] == "Item2"
                and attempt < 3
            ):

                raise ConnectionError(
                    "Temporary service failure"
                )

            process_order(order)

            return True


        except ConnectionError as error:

            print(
                f"\nTemporary failure for "
                f"Order {order['orderId']}"
            )

            print(
                f"Retry attempt "
                f"{attempt}/{MAX_RETRIES}"
            )

            print(
                f"Reason: {error}"
            )

            if attempt < MAX_RETRIES:
                print("Retrying in 1 second...\n")
                time.sleep(1)


        except ValueError:

            # Permanent failures
            # should NOT be retried
            raise


    return False


# =============================
# Send Order to DLQ
# =============================

def send_to_dlq(order):

    dlq_producer.produce(
        topic=DLQ_TOPIC,
        key=order["orderId"],
        value=order
    )

    dlq_producer.flush()

    print(
        f"Order {order['orderId']} "
        f"sent to DLQ."
    )


# =============================
# Running Average
# =============================

total_price = 0.0
order_count = 0


# =============================
# Start Consumer
# =============================

print("Consumer started...")
print("Waiting for orders...\n")


try:

    while True:

        # Wait for Kafka message
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():

            print(
                f"Consumer error: {msg.error()}"
            )

            continue


        # =============================
        # Get deserialized Avro order
        # =============================

        order = msg.value()

        print("\n" + "=" * 50)
        print("Received Order")
        print("=" * 50)

        print(
            f"Order ID : {order['orderId']}"
        )

        print(
            f"Product  : {order['product']}"
        )

        print(
            f"Price    : {order['price']:.2f}"
        )

        print("-" * 50)


        # =============================
        # Process with retry
        # =============================

        try:

            success = process_with_retry(order)


            # =========================
            # Successful Order
            # =========================

            if success:

                price = order["price"]

                total_price += price
                order_count += 1

                running_average = (
                    total_price / order_count
                )

                print("\nProcessing Result")
                print("-" * 50)

                print(
                    f"Order ID         : "
                    f"{order['orderId']}"
                )

                print(
                    f"Product          : "
                    f"{order['product']}"
                )

                print(
                    f"Price            : "
                    f"{price:.2f}"
                )

                print(
                    f"Orders Processed : "
                    f"{order_count}"
                )

                print(
                    f"Running Average  : "
                    f"{running_average:.2f}"
                )

                print("=" * 50)


            # =========================
            # Retries Exhausted
            # =========================

            else:

                print(
                    f"Retries exhausted for "
                    f"Order {order['orderId']}"
                )

                send_to_dlq(order)


        # =============================
        # Permanent Failure
        # =============================

        except ValueError as error:

            print(
                f"\nPermanent failure for "
                f"Order {order['orderId']}"
            )

            print(
                f"Reason: {error}"
            )

            send_to_dlq(order)


except KeyboardInterrupt:

    print("\nConsumer stopped by user.")


finally:

    consumer.close()

    print("Kafka consumer closed.")