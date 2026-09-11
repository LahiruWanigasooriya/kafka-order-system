from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer


schema_registry_client = SchemaRegistryClient({
    "url": "http://localhost:8081"
})


avro_deserializer = AvroDeserializer(
    schema_registry_client
)


consumer_conf = {

    "bootstrap.servers": "localhost:9092",

    "key.deserializer": StringDeserializer("utf_8"),

    "value.deserializer": avro_deserializer,

    "group.id": "dlq-consumer-group",

    "auto.offset.reset": "earliest"
}


consumer = DeserializingConsumer(consumer_conf)

consumer.subscribe(["orders-dlq"])


print("DLQ Consumer started...")
print("Waiting for failed orders...\n")


try:

    while True:

        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("Error:", msg.error())
            continue

        order = msg.value()

        print("DEAD LETTER MESSAGE")
        print(f"Order ID : {order['orderId']}")
        print(f"Product  : {order['product']}")
        print(f"Price    : {order['price']:.2f}")
        print("-" * 40)


except KeyboardInterrupt:

    print("\nDLQ consumer stopped.")


finally:

    consumer.close()