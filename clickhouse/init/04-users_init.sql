CREATE DATABASE IF NOT EXISTS transformed;

CREATE TABLE IF NOT EXISTS transformed.users
(
    user_id String,
    name String,
    email String,
    signup_date Date,
    device String,
    loyalty_tier String,
    location String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY user_id;

CREATE TABLE IF NOT EXISTS transformed.kafka_users
(
    user_id String,
    name String,
    email String,
    signup_date Date,
    device Nullable(String),
    loyalty_tier Nullable(String),
    location Nullable(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka1:9092,kafka2:9092,kafka3:9092',
    kafka_topic_list = 'ecommerce.users.ingest',
    kafka_group_name = 'clickhouse_users_consumer_group',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_format = 'AvroConfluent',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 1,
    kafka_handle_error_mode = 'stream';

CREATE MATERIALIZED VIEW transformed.users_mv
TO transformed.users
AS
SELECT
    user_id,
    name,
    email,
    signup_date,
    ifNull(device, '') AS device,
    ifNull(loyalty_tier, '') AS loyalty_tier,
    ifNull(location, '') AS location
FROM transformed.kafka_users;
