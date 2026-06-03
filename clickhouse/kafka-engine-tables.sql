-- Kafka tables
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
    -- change topic name to what it is in kafka later added
    kafka_topic_list = 'users_topic',
    kafka_group_name = 'clickhouse_users_consumer_group',
    kafka_format = 'Avro',
    -- change the consumers count later in kafka added
    kafka_num_consumers = 2,
    kafka_thread_per_consumer = 1,
    kafka_auto_offset_reset = 'earliest';

CREATE TABLE
    IF NOT EXISTS transformed.kafka_products (
        product_id UInt64,
        name String,
        price Float64,
        category String,
        inventory UInt32,
        popularity_score Float64
    ) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka1:9092,kafka2:9092,kafka3:9092',
    -- change topic name to what it is in kafka later added
    kafka_topic_list = 'products_topic',
    kafka_group_name = 'clickhouse_products_consumer_group',
    kafka_format = 'Avro',
    -- change the consumers count later in kafka added
    kafka_num_consumers = 2,
    kafka_thread_per_consumer = 1,
    kafka_auto_offset_reset = 'earliest';

CREATE TABLE
    IF NOT EXISTS transformed.kafka_orders (
        order_id UInt64,
        user_id UInt64,
        timestamp DateTime,
        total Float64,
        status String,
        payment_method String
    ) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka1:9092,kafka2:9092,kafka3:9092',
    -- change topic name to what it is in kafka later added
    kafka_topic_list = 'orders_topic',
    kafka_group_name = 'clickhouse_orders_consumer_group',
    kafka_format = 'Avro',
    -- change the consumers count later in kafka added
    kafka_num_consumers = 2,
    kafka_thread_per_consumer = 1,
    kafka_auto_offset_reset = 'earliest';

-- change the fields based on the schema registry
CREATE TABLE
    IF NOT EXISTS transformed.kafka_behavioral_events (
        event_id UInt64,
        user_id UInt64,
        event_type String,
        event_timestamp DateTime,
        session_id String,
        product_id Nullable (UInt64),
        page_url String,
        device String
    ) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka1:9092,kafka2:9092,kafka3:9092',
    kafka_topic_list = 'behavioral_events_topic',
    kafka_group_name = 'clickhouse_behavioral_events_consumer_group',
    kafka_format = 'Avro',
    kafka_num_consumers = 2,
    kafka_thread_per_consumer = 1,
    kafka_auto_offset_reset = 'earliest';

