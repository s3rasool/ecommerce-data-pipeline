#!/bin/bash

SCHEMA_DIR="$(dirname "$0")/schemas/avro"

echo "Registering schemas..."

# transactional
declare -A TRANSACTIONAL_TOPICS=(
    ["user"]="ecommerce.users.ingest"
    ["product"]="ecommerce.products.ingest"
    ["order"]="ecommerce.orders.ingest"
)

for schema_name in "${!TRANSACTIONAL_TOPICS[@]}"; do
    topic="${TRANSACTIONAL_TOPICS[$schema_name]}"
    file="$SCHEMA_DIR/transactional/$schema_name.avsc"
    schema=$(cat "$file" | python3 -c "import json,sys; print(json.dumps(json.load(sys.stdin)))")
    docker exec schema-registry curl -s -X POST \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d "{\"schema\": $schema}" \
        "http://localhost:8081/subjects/$topic-value/versions"
    echo " -> registered: $topic"
done

# behavioral
for file in "$SCHEMA_DIR"/behavioral/*.avsc; do
    topic="ecommerce.events.ingest"
    schema=$(cat "$file" | python3 -c "import json,sys; print(json.dumps(json.load(sys.stdin)))")
    docker exec schema-registry curl -s -X POST \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d "{\"schema\": $schema}" \
        "http://localhost:8081/subjects/$topic-value/versions"
    echo " -> registered: $(basename $file .avsc) under $topic"
done

echo "Done!"