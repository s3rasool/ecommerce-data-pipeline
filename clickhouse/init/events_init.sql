CREATE DATABASE IF NOT EXISTS transformed;

SET
    enable_json_type = 1;


CREATE TABLE IF NOT EXISTS transformed.behavioral_events
(
    `timestamp` DateTime64(3, 'UTC'),
    event_date Date MATERIALIZED toDate(`timestamp`),
    user_id String,
    event_type LowCardinality(String),
    device LowCardinality(String),
    session_id String,
    event_properties JSON,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_type, event_date, user_id, session_id, `timestamp`);

CREATE TABLE IF NOT EXISTS transformed.behavioral_events_raw
(
    `timestamp` Nullable(DateTime64(6, 'UTC')),
    user_id Nullable(String),
    event_type Nullable(String),
    device Nullable(String),
    session_id Nullable(String),

    product_id Nullable(String),
    quantity Nullable(Int32),
    cart_total_items Nullable(Int32),

    cart_items Array(
        Tuple(
            product_id String,
            price Float64,
            quantity Int32
        )
    ),

    cart_value Nullable(Float64),
    shipping_method Nullable(String),
    order_id Nullable(String),
    fulfillment_speed Nullable(String),
    url_path Nullable(String),
    duration_sec Nullable(Int32),
    http_status Nullable(Int32),
    payment_type Nullable(String),
    success Nullable(Bool),
    error_code Nullable(String),
    `query` Nullable(String),
    results_count Nullable(Int32),
    clicked_position Nullable(Int32),
    rating Nullable(Int32),
    text_length Nullable(Int32),
    wishlist_name Nullable(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka1:9092,kafka2:9092,kafka3:9092',
    kafka_topic_list = 'ecommerce.events.ingest',
    kafka_group_name = 'clickhouse_behavioral_events_consumer_group',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 1,
    kafka_handle_error_mode = 'stream',
    input_format_null_as_default = 1;


CREATE MATERIALIZED VIEW IF NOT EXISTS transformed.behavioral_events_mv
TO transformed.behavioral_events
AS
SELECT
    toDateTime64(assumeNotNull(`timestamp`), 3, 'UTC') AS `timestamp`,
    assumeNotNull(user_id) AS user_id,
    assumeNotNull(event_type) AS event_type,
    assumeNotNull(device) AS device,
    assumeNotNull(session_id) AS session_id,

    CAST(
        concat(
            '{',
            arrayStringConcat(
                arrayFilter(
                    x -> x != '',
                    [
                        if(product_id IS NOT NULL AND product_id != '',
                            concat('"product_id":', toJSONString(product_id)), ''),

                        if(quantity IS NOT NULL,
                            concat('"quantity":', toJSONString(quantity)), ''),

                        if(cart_total_items IS NOT NULL,
                            concat('"cart_total_items":', toJSONString(cart_total_items)), ''),

                        if(length(cart_items) > 0,
                            concat('"cart_items":', toJSONString(cart_items)), ''),

                        if(cart_value IS NOT NULL,
                            concat('"cart_value":', toJSONString(cart_value)), ''),

                        if(shipping_method IS NOT NULL AND shipping_method != '',
                            concat('"shipping_method":', toJSONString(shipping_method)), ''),

                        if(order_id IS NOT NULL AND order_id != '',
                            concat('"order_id":', toJSONString(order_id)), ''),

                        if(fulfillment_speed IS NOT NULL AND fulfillment_speed != '',
                            concat('"fulfillment_speed":', toJSONString(fulfillment_speed)), ''),

                        if(url_path IS NOT NULL AND url_path != '',
                            concat('"url_path":', toJSONString(url_path)), ''),

                        if(duration_sec IS NOT NULL,
                            concat('"duration_sec":', toJSONString(duration_sec)), ''),

                        if(http_status IS NOT NULL,
                            concat('"http_status":', toJSONString(http_status)), ''),

                        if(payment_type IS NOT NULL AND payment_type != '',
                            concat('"payment_type":', toJSONString(payment_type)), ''),

                        if(success IS NOT NULL,
                            concat('"success":', toJSONString(success)), ''),

                        if(error_code IS NOT NULL AND error_code != '',
                            concat('"error_code":', toJSONString(error_code)), ''),

                        if(`query` IS NOT NULL AND `query` != '',
                            concat('"query":', toJSONString(`query`)), ''),

                        if(results_count IS NOT NULL,
                            concat('"results_count":', toJSONString(results_count)), ''),

                        if(clicked_position IS NOT NULL,
                            concat('"clicked_position":', toJSONString(clicked_position)), ''),

                        if(rating IS NOT NULL,
                            concat('"rating":', toJSONString(rating)), ''),

                        if(text_length IS NOT NULL,
                            concat('"text_length":', toJSONString(text_length)), ''),

                        if(wishlist_name IS NOT NULL AND wishlist_name != '',
                            concat('"wishlist_name":', toJSONString(wishlist_name)), '')
                    ]
                ),
                ','
            ),
            '}'
        ),
        'JSON'
    ) AS event_properties,

    now64(3, 'UTC') AS ingested_at
FROM transformed.behavioral_events_raw
WHERE
    `timestamp` IS NOT NULL
    AND `timestamp` > toDateTime64('2000-01-01 00:00:00', 6, 'UTC')
    AND user_id IS NOT NULL AND user_id != ''
    AND event_type IS NOT NULL AND event_type != ''
    AND device IS NOT NULL AND device != ''
    AND session_id IS NOT NULL AND session_id != '';

