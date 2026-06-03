#!/bin/bash

SCHEMA_DIR="$(dirname "$0")/schemas/avro"
SCHEMA_REGISTRY_URL="http://schema-registry:8081" # نام سرویس Schema Registry در داکر کامپوز

echo "Waiting for Schema Registry to be ready..."
# حلقه انتظار برای بالا آمدن کامل Schema Registry
while ! curl -s "$SCHEMA_REGISTRY_URL" > /dev/null; do
  echo "Schema Registry is not ready yet. Waiting..."
  sleep 5
done

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
    # تبدیل فایل به استرینگ JSON با پایتون
    schema=$(cat "$file" | python3 -c "import json,sys; print(json.dumps(json.dumps(json.load(sys.stdin))))")
    
    curl -s -X POST \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d "{\"schema\": $schema}" \
        "$SCHEMA_REGISTRY_URL/subjects/$topic-value/versions"
    echo " -> registered: $topic"
done

# behavioral
for file in "$SCHEMA_DIR"/behavioral/*.avsc; do
    topic="ecommerce.events.ingest"
    schema=$(cat "$file" | python3 -c "import json,sys; print(json.dumps(json.dumps(json.load(sys.stdin))))")
    
    curl -s -X POST \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d "{\"schema\": $schema}" \
        "$SCHEMA_REGISTRY_URL/subjects/$topic-value/versions"
    echo " -> registered: $(basename $file .avsc) under $topic"
done

echo "Done!"
