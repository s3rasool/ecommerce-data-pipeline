import os

MONGO_DB = "behavioral"
MONGO_COLLECTION = "events"
TOPIC = "ecommerce.events.ingest"
SCHEMA_DIR = "/opt/airflow/schema/avro/behavioral"

SCHEMA_FILE = "event.avsc"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092,kafka3:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")