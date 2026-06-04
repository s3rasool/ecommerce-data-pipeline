#!/bin/bash

SCHEMA_DIR="$(dirname "$0")/schemas/avro"
SCHEMA_REGISTRY_URL="http://localhost:8081"

echo "Waiting for Schema Registry to be ready..."
while ! curl -s "$SCHEMA_REGISTRY_URL" > /dev/null; do
  echo "Schema Registry is not ready yet. Waiting..."
  sleep 5
done

echo "Registering schemas..."

###############################################
# 1) TRANSACTIONAL SCHEMAS
###############################################

declare -A TRANSACTIONAL_TOPICS=(
    ["user"]="ecommerce.users.ingest"
    ["product"]="ecommerce.products.ingest"
    ["order"]="ecommerce.orders.ingest"
)

for schema_name in "${!TRANSACTIONAL_TOPICS[@]}"; do
    topic="${TRANSACTIONAL_TOPICS[$schema_name]}"
    file="$SCHEMA_DIR/transactional/$schema_name.avsc"

    echo "Registering transactional schema: $file → subject: $topic-value"

    schema=$(cat "$file" | python3 -c "import json,sys; print(json.dumps(json.dumps(json.load(sys.stdin))))")

    response=$(curl -s -w "\nHTTP_CODE:%{http_code}" -X POST \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d "{\"schema\": $schema}" \
        "$SCHEMA_REGISTRY_URL/subjects/$topic-value/versions")

    echo "$response"
done


###############################################
# 2) BEHAVIORAL SCHEMAS — FIXED
###############################################

for file in "$SCHEMA_DIR/behavioral"/*.avsc; do
    event_name=$(basename "$file" .avsc)
    subject="ecommerce.events.$event_name-value"

    echo "Registering behavioral schema: $file → subject: $subject"

    schema=$(cat "$file" | python3 -c "import json,sys; print(json.dumps(json.dumps(json.load(sys.stdin))))")

    response=$(curl -s -w "\nHTTP_CODE:%{http_code}" -X POST \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d "{\"schema\": $schema}" \
        "$SCHEMA_REGISTRY_URL/subjects/$subject/versions")

    echo "$response"
done

echo "Schema registration complete."
