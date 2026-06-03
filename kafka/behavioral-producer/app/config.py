import os


MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = "behavioral"
MONGO_COLLECTION = "events"

TOPIC = "behavioral.events"

SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
