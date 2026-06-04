from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from app.config import SCHEMA_REGISTRY_URL, KAFKA_BOOTSTRAP_SERVERS
from app.config import TOPIC
from app.mongo_reader import MongoEventReader

class EventProducer:
    def __init__(self):
        with open("schemas/event.avsc", "r") as f:
            schema_str = f.read()

        schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        avro_serializer = AvroSerializer(schema_registry_client, schema_str)
        string_serializer = StringSerializer('utf_8')

        self.producer = SerializingProducer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "key.serializer": string_serializer,
                "value.serializer": avro_serializer,
                "acks": "all",
            }
        )
        self.reader = MongoEventReader()

    def delivery_report(self, err, msg):
        if err:
            print(f"Failed: {err}")
        else:
            print(f"Message sent: partition={msg.partition()} offset={msg.offset()} ")

    def run(self):
        for event in self.reader.fetch_events():
            print(
                f"user={event['user_id']}: "
                f"type={event['event_type']}"
            )
            
            self.producer.produce(
                topic=TOPIC,
                key=str(event['user_id']),
                value=event,
                on_delivery=self.delivery_report,
            )
            
            self.producer.poll(0)
            
        self.producer.flush()
        print("Producer finished.\n")

if __name__ == "__main__":
    EventProducer().run()
