CREATE DATABASE IF NOT EXISTS transformed;

CREATE TABLE IF NOT EXISTS transformed.orders
(
    order_id String,
    user_id String,
    timestamp DateTime64(6, 'UTC'),
    order_date Date MATERIALIZED toDate(timestamp),
    total Decimal(10, 2),
    status String,
    payment_method String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_date, user_id, order_id, timestamp);

CREATE TABLE IF NOT EXISTS transformed.kafka_orders
(
    order_id String,
    user_id String,
    timestamp DateTime64(6, 'UTC'),
    total Decimal(10, 2),
    status String,
    payment_method Nullable(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka1:9092,kafka2:9092,kafka3:9092',
    kafka_topic_list = 'ecommerce.orders.ingest',
    kafka_group_name = 'clickhouse_orders_consumer_group',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_format = 'AvroConfluent',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 1,
    kafka_handle_error_mode = 'stream';


CREATE MATERIALIZED VIEW transformed.orders_mv
TO transformed.orders
AS
SELECT
    order_id,
    user_id,
    timestamp,
    total,
    status,
    ifNull(payment_method, '') AS payment_method
FROM transformed.kafka_orders;