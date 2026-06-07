CREATE DATABASE IF NOT EXISTS transformed;

CREATE TABLE
    transformed.kafka_users (
        user_id UInt64,
        name String,
        email String,
        signup_date Date,
        device Nullable (String),
        loyalty_tier Nullable (String),
        location Nullable (String)
    ) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka1:9092,kafka2:9092,kafka3:9092',
    kafka_topic_list = 'ecommerce.users.ingest',
    kafka_group_name = 'clickhouse_users_consumer_group',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_format = 'AvroConfluent',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 1,
    kafka_handle_error_mode = 'stream';

CREATE TABLE
    IF NOT EXISTS transformed.kafka_products (
        product_id UInt64,
        name String,
        price Float64,
        category String,
        inventory UInt32,
        popularity_score Float64
    ) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka1:9092,kafka2:9092,kafka3:9092',
    kafka_topic_list = 'ecommerce.products.ingest',
    kafka_group_name = 'clickhouse_products_consumer_group',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_format = 'AvroConfluent',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 1;

CREATE TABLE
    transformed.kafka_orders (
        order_id UInt64,
        user_id UInt64,
        timestamp DateTime64 (6, 'UTC'),
        total Decimal(10, 2),
        status String,
        payment_method Nullable (String)
    ) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka1:9092,kafka2:9092,kafka3:9092',
    kafka_topic_list = 'ecommerce.orders.ingest',
    kafka_group_name = 'clickhouse_orders_consumer_group',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_format = 'AvroConfluent',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 1,
    kafka_handle_error_mode = 'stream';

CREATE TABLE
    transformed.behavioral_events_raw (
        `timestamp` Nullable (DateTime64 (6, 'UTC')),
        user_id Nullable (String),
        event_type Nullable (String),
        device Nullable (String),
        session_id Nullable (String),
        product_id Nullable (String),
        quantity Nullable (Int32),
        cart_total_items Nullable (Int32),
        cart_items Array (
            Tuple (product_id String, price Float64, quantity Int32)
        ),
        cart_value Nullable (Float64),
        shipping_method Nullable (String),
        order_id Nullable (String),
        fulfillment_speed Nullable (String),
        url_path Nullable (String),
        duration_sec Nullable (Int32),
        http_status Nullable (Int32),
        payment_type Nullable (String),
        success Nullable (Bool),
        error_code Nullable (String),
        `query` Nullable (String),
        results_count Nullable (Int32),
        clicked_position Nullable (Int32),
        rating Nullable (Int32),
        text_length Nullable (Int32),
        wishlist_name Nullable (String)
    ) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka1:9092,kafka2:9092,kafka3:9092',
    kafka_topic_list = 'ecommerce.events.ingest',
    kafka_group_name = 'clickhouse_behavioral_events_consumer_group_v2',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_format = 'AvroConfluent',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 1,
    kafka_handle_error_mode = 'stream',
    input_format_null_as_default = 1;