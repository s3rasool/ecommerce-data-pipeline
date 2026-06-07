import os
import logging

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

from app.mongo_reader import MongoEventReader
from app.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    SCHEMA_REGISTRY_URL,
    TOPIC,
    SCHEMA_DIR,
    SCHEMA_FILE,
)


class EventProducer:
    def __init__(self):
        self.reader = MongoEventReader()

        schema_path = os.path.join(SCHEMA_DIR, SCHEMA_FILE)
        if not os.path.exists(schema_path):
            raise FileNotFoundError(f"Schema file not found: {schema_path}")

        with open(schema_path, "r", encoding="utf-8") as f:
            schema_str = f.read()

        schema_registry_client = SchemaRegistryClient({
            "url": SCHEMA_REGISTRY_URL
        })

        avro_serializer = AvroSerializer(
            schema_registry_client=schema_registry_client,
            schema_str=schema_str,
        )

        self.producer = SerializingProducer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": avro_serializer,
            "acks": "all",
            "enable.idempotence": True,
        })

        self.failed_count = 0
        self.success_count = 0
        self.skipped_count = 0

    def _delivery_report(self, mongo_id):
        def callback(err, msg):
            if err is not None:
                self.failed_count += 1
                logging.error(f"Kafka delivery failed for mongo_id={mongo_id}: {err}")
                self.reader.mark_failed(mongo_id, err)
            else:
                self.success_count += 1
                logging.info(
                    f"Delivered mongo_id={mongo_id} "
                    f"to topic={msg.topic()} "
                    f"partition={msg.partition()} "
                    f"offset={msg.offset()}"
                )
                self.reader.mark_produced(mongo_id)
        return callback

    def run(self):
        for event in self.reader.fetch_events():
            mongo_id = event.get("_mongo_id")
            event_type = event.get("event_type")

            if not event_type:
                self.skipped_count += 1
                logging.warning(f"Skipping document without event_type: {event}")
                continue

            avro_event = self._prepare_event_for_avro(event)

            self.producer.produce(
                topic=TOPIC,
                key=str(avro_event["user_id"]),
                value=avro_event,
                on_delivery=self._delivery_report(mongo_id),
            )

            self.producer.poll(0)

        self.producer.flush()

        logging.info(
            f"Kafka production finished. "
            f"success={self.success_count}, "
            f"failed={self.failed_count}, "
            f"skipped={self.skipped_count}"
        )

        if self.failed_count > 0:
            raise RuntimeError(f"{self.failed_count} Kafka messages failed")

        return {
            "success": self.success_count,
            "failed": self.failed_count,
            "skipped": self.skipped_count,
        }

    def _prepare_event_for_avro(self, event):
        """
        Return event with all schema fields. Missing fields are null.
        """
        return {
            # Common fields
            "timestamp": event.get("timestamp"),
            "user_id": event.get("user_id"),
            "event_type": event.get("event_type"),
            "device": event.get("device"),
            "session_id": event.get("session_id"),
            # page_view
            "url_path": event.get("url_path"),
            "duration_sec": event.get("duration_sec"),
            "http_status": event.get("http_status"),
            # product_search
            "query": event.get("query"),
            "results_count": event.get("results_count"),
            "clicked_position": event.get("clicked_position"),
            # add_to_cart / remove_from_cart
            "product_id": event.get("product_id"),
            "quantity": event.get("quantity"),
            "cart_total_items": event.get("cart_total_items"),
            # cart_view
            "cart_items": event.get("cart_items"),
            "cart_value": event.get("cart_value"),
            # wishlist_add
            "wishlist_name": event.get("wishlist_name"),
            # checkout_start
            "shipping_method": event.get("shipping_method"),
            # payment_attempt
            "payment_type": event.get("payment_type"),
            "success": event.get("success"),
            "error_code": event.get("error_code"),
            # order_complete
            "order_id": event.get("order_id"),
            "fulfillment_speed": event.get("fulfillment_speed"),
            # review_submit
            "rating": event.get("rating"),
            "text_length": event.get("text_length"),
        }