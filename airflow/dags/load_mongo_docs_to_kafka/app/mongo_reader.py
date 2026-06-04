from pymongo import MongoClient
from app.config import MONGO_URI, MONGO_DB, MONGO_COLLECTION


class MongoEventReader:
    def __init__(self):
        print(f"{MONGO_URI}: abbas")
        self.client = MongoClient(MONGO_URI)
        self.col = self.client[MONGO_DB][MONGO_COLLECTION]

    def fetch_events(self, batch_size=100):
        cursor = self.col.find().batch_size(batch_size)
        for doc in cursor:
            yield self._transform(doc)

    def _transform(self, doc):
        doc["_id"] = str(doc["_id"])

        if "timestamp" in doc:
            doc["timestamp"] = int(doc["timestamp"].timestamp())

        return doc
