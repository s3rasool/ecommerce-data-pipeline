import os
import logging

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import topic_record_subject_name_strategy

from app.mongo_reader import MongoEventReader
from app.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    SCHEMA_REGISTRY_URL,
    TOPIC,
    SCHEMA_DIR,
    SCHEMA_FILES,
)


class EventProducer:
    def __init__(self):
        self.reader = MongoEventReader()

        self.schema_registry_client = SchemaRegistryClient({
            "url": SCHEMA_REGISTRY_URL
        })

        self.producers = {}
        self.failed_count = 0
        self.success_count = 0
        self.skipped_count = 0

        for event_type, schema_file in SCHEMA_FILES.items():
            schema_path = os.path.join(SCHEMA_DIR, schema_file)

            if not os.path.exists(schema_path):
                raise FileNotFoundError(f"Schema file not found: {schema_path}")

            with open(schema_path, "r", encoding="utf-8") as f:
                schema_str = f.read()

            avro_serializer = AvroSerializer(
                schema_registry_client=self.schema_registry_client,
                schema_str=schema_str,
                conf={
                    "auto.register.schemas": True,
                    "subject.name.strategy": topic_record_subject_name_strategy,
                }
            )

            producer = SerializingProducer({
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "key.serializer": StringSerializer("utf_8"),
                "value.serializer": avro_serializer,
                "acks": "all",
                "enable.idempotence": True,
            })

            self.producers[event_type] = producer

    def _delivery_report(self, mongo_id):
        """
        Create callback with mongo_id bound to this message.
        """

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

            if event_type not in self.producers:
                self.skipped_count += 1
                logging.warning(f"Skipping unknown event_type={event_type}, event={event}")
                continue

            producer = self.producers[event_type]

            avro_event = self._prepare_event_for_avro(event)

            producer.produce(
                topic=TOPIC,
                key=str(avro_event["user_id"]),
                value=avro_event,
                on_delivery=self._delivery_report(mongo_id),
            )

            producer.poll(0)

        for producer in self.producers.values():
            producer.flush()

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
        event_type = event["event_type"]

        allowed_fields_by_event_type = {
            "add_to_cart": [
                "timestamp",
                "user_id",
                "event_type",
                "device",
                "session_id",
                "product_id",
                "quantity",
                "cart_total_items",
            ],
            "cart_view": [
                "timestamp",
                "user_id",
                "event_type",
                "device",
                "session_id",
                "cart_items",
                "cart_value",
            ],
            "checkout_start": [
                "timestamp",
                "user_id",
                "event_type",
                "device",
                "session_id",
                "shipping_method",
                "cart_value",
            ],
            "order_complete": [
                "timestamp",
                "user_id",
                "event_type",
                "device",
                "session_id",
                "order_id",
                "fulfillment_speed",
            ],
            "page_view": [
                "timestamp",
                "user_id",
                "event_type",
                "device",
                "session_id",
                "url_path",
                "duration_sec",
                "http_status",
            ],
            "payment_attempt": [
                "timestamp",
                "user_id",
                "event_type",
                "device",
                "session_id",
                "payment_type",
                "success",
                "error_code",
            ],
            "product_search": [
                "timestamp",
                "user_id",
                "event_type",
                "device",
                "session_id",
                "query",
                "results_count",
                "clicked_position",
            ],
            "remove_from_cart": [
                "timestamp",
                "user_id",
                "event_type",
                "device",
                "session_id",
                "product_id",
                "quantity",
                "cart_total_items",
            ],
            "review_submit": [
                "timestamp",
                "user_id",
                "event_type",
                "device",
                "session_id",
                "product_id",
                "rating",
                "text_length",
            ],
            "wishlist_add": [
                "timestamp",
                "user_id",
                "event_type",
                "device",
                "session_id",
                "product_id",
                "wishlist_name",
            ],
        }

        allowed_fields = allowed_fields_by_event_type[event_type]

        avro_event = {
            field: event.get(field)
            for field in allowed_fields
        }

        return avro_event