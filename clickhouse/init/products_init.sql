CREATE DATABASE IF NOT EXISTS transformed;


CREATE TABLE IF NOT EXISTS transformed.products
(
    product_id String,
    name String,
    price Decimal(10, 2),
    category String,
    inventory Int32,
    popularity_score Decimal(4, 2),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY product_id;

CREATE TABLE IF NOT EXISTS transformed.kafka_products
(
    product_id String,
    name String,
    price Decimal(10, 2),
    category Nullable(String),
    inventory Nullable(Int32),
    popularity_score Nullable(Decimal(4, 2))
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka1:9092,kafka2:9092,kafka3:9092',
    kafka_topic_list = 'ecommerce.products.ingest',
    kafka_group_name = 'clickhouse_products_consumer_group',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_format = 'AvroConfluent',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 1,
    kafka_handle_error_mode = 'stream';

CREATE MATERIALIZED VIEW transformed.products_mv
TO transformed.products
AS
SELECT
    product_id,
    name,
    price,
    ifNull(category, '') AS category,
    ifNull(inventory, 0) AS inventory,
    ifNull(popularity_score, toDecimal32(0, 2)) AS popularity_score
FROM transformed.kafka_products;