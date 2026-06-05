from datetime import datetime, timezone

from airflow.providers.mongo.hooks.mongo import MongoHook

from .config import MONGO_DB, MONGO_COLLECTION


class MongoEventReader:
    def __init__(self):
        mongo_hook = MongoHook(mongo_conn_id="mongo_default")
        self.client = mongo_hook.get_conn()
        self.collection = self.client[MONGO_DB][MONGO_COLLECTION]

    def fetch_events(self, batch_size=100):
        cursor = self.collection.find(
            {"kafka_produced": {"$ne": True}}
        ).batch_size(batch_size)

        for doc in cursor:
            yield self._transform(doc)

    def mark_produced(self, mongo_id):
        self.collection.update_one(
            {"_id": mongo_id},
            {
                "$set": {
                    "kafka_produced": True,
                    "kafka_produced_at": datetime.now(timezone.utc),
                }
            }
        )

    def mark_failed(self, mongo_id, error_message):
        self.collection.update_one(
            {"_id": mongo_id},
            {
                "$set": {
                    "kafka_error": str(error_message),
                    "kafka_failed_at": datetime.now(timezone.utc),
                }
            }
        )

    def _transform(self, doc):
        original_id = doc["_id"]

        event = dict(doc)
        event["_mongo_id"] = original_id
        event["_id"] = str(original_id)

        if "timestamp" in event and event["timestamp"] is not None:
            event["timestamp"] = self._to_timestamp_micros(event["timestamp"])

        event.pop("transformed_at", None)
        event.pop("kafka_produced", None)
        event.pop("kafka_produced_at", None)
        event.pop("kafka_error", None)
        event.pop("kafka_failed_at", None)

        return event

    def _to_timestamp_micros(self, value):
        if isinstance(value, datetime):
            return int(value.timestamp() * 1_000_000)

        if isinstance(value, str):
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1_000_000)

        if isinstance(value, int):
            if value < 10_000_000_000:
                return value * 1_000_000
            if value < 10_000_000_000_000:
                return value * 1_000
            return value

        raise ValueError(f"Unsupported timestamp value: {value}")
