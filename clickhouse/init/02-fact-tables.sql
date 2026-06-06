SET enable_json_type = 1;

CREATE TABLE IF NOT EXISTS transformed.users (
    user_id UInt64,
    name String,
    email String,
    signup_date Date,
    device String,
    loyalty_tier String,
    location String
) ENGINE = MergeTree()
ORDER BY (user_id);

CREATE TABLE IF NOT EXISTS transformed.products (
    product_id UInt64,
    name String,
    price Float64,
    category String,
    inventory UInt32,
    popularity_score Float64
) ENGINE = MergeTree()
ORDER BY (category, product_id);

CREATE TABLE IF NOT EXISTS transformed.orders (
    order_id UInt64,
    user_id UInt64,
    timestamp DateTime,
    total Float64,
    status String,
    payment_method String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, order_id);


CREATE TABLE IF NOT EXISTS transformed.behavioral_events
(
    `timestamp` DateTime64(3, 'UTC'),
    event_date Date MATERIALIZED toDate(`timestamp`),

    user_id String,
    event_type LowCardinality(String),
    device LowCardinality(String),
    session_id String,

    event_properties JSON,

    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_type, event_date, user_id, session_id, `timestamp`);
