#!/bin/bash

echo "Waiting for Kafka to be ready..."
until kafka-topics --bootstrap-server kafka1:9092 --list 2>/dev/null; do
  echo "Kafka not ready yet, retrying..."
  sleep 3
done

TOPICS=(
  "ecommerce.users.ingest"
  "ecommerce.products.ingest"
  "ecommerce.orders.ingest"
  "ecommerce.events.ingest"
  "ecommerce.users.migrate"
  "ecommerce.products.migrate"
  "ecommerce.orders.migrate"
  "ecommerce.events.migrate"
)

for topic in "${TOPICS[@]}"; do
  echo "Creating topic: $topic"
  kafka-topics --create \
    --bootstrap-server kafka1:9092,kafka2:9092,kafka3:9092 \
    --topic "$topic" \
    --partitions 1 \
    --replication-factor 2 \
    --if-not-exists
done

echo "All topics created. Current topics:"
kafka-topics --list --bootstrap-server kafka1:9092
