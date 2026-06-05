import os


MONGO_DB = "behavioral"
MONGO_COLLECTION = "events"
TOPIC = "ecommerce.events.ingest"
SCHEMA_DIR = "/opt/airflow/schema/avro/behavioral"

SCHEMA_FILES = {
    "add_to_cart": "add_to_cart.avsc",
    "cart_view": "cart_view.avsc",
    "checkout_start": "checkout_start.avsc",
    "order_complete": "order_complete.avsc",
    "page_view": "page_view.avsc",
    "payment_attempt": "payment_attempt.avsc",
    "product_search": "product_search.avsc",
    "remove_from_cart": "remove_from_cart.avsc",
    "review_submit": "review_submit.avsc",
    "wishlist_add": "wishlist_add.avsc",
}

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092,kafka3:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

