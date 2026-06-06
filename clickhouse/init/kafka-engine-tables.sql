CREATE DATABASE IF NOT EXISTS transformed;

CREATE TABLE
    IF NOT EXISTS transformed.kafka_users (
        user_id UInt64,
        name String,
        email String,
        signup_date Date,
        device String,
        loyalty_tier String,
        location String
    ) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka1:9092,kafka2:9092,kafka3:9092',
    kafka_topic_list = 'ecommerce.users.ingest',
    kafka_group_name = 'clickhouse_users_consumer_group',
    kafka_format = 'AvroConfluent',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 1;

kafka_handle_error_mode = 'dead_letter_queue';

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
    kafka_format = 'AvroConfluent',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 1;

kafka_handle_error_mode = 'dead_letter_queue';

CREATE TABLE
    IF NOT EXISTS transformed.kafka_orders (
        order_id UInt64,
        user_id UInt64,
        timestamp DateTime,
        total Float64,
        status String,
        payment_method String
    ) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka1:9092,kafka2:9092,kafka3:9092',
    kafka_topic_list = 'ecommerce.orders.ingest',
    kafka_group_name = 'clickhouse_orders_consumer_group',
    kafka_format = 'AvroConfluent',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 1;

kafka_handle_error_mode = 'dead_letter_queue';

CREATE TABLE
    IF NOT EXISTS transformed.behavioral_events_raw (raw String) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka1:9092,kafka2:9092,kafka3:9092',
    kafka_topic_list = 'ecommerce.events.ingest',
    kafka_group_name = 'clickhouse_behavioral_events_consumer_group',
    kafka_format = 'AvroConfluent',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 1;

kafka_thread_per_consumer = 1;

kafka_handle_error_mode = 'dead_letter_queue';