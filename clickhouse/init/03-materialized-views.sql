CREATE DATABASE IF NOT EXISTS transformed;

CREATE MATERIALIZED VIEW IF NOT EXISTS transformed.users_mv TO transformed.users AS
SELECT
    *
FROM
    transformed.kafka_users;

CREATE MATERIALIZED VIEW IF NOT EXISTS transformed.products_mv TO transformed.products AS
SELECT
    *
FROM
    transformed.kafka_products;

CREATE MATERIALIZED VIEW IF NOT EXISTS transformed.orders_mv TO transformed.orders AS
SELECT
    *
FROM
    transformed.kafka_orders;

CREATE DATABASE IF NOT EXISTS transformed;

CREATE MATERIALIZED VIEW IF NOT EXISTS transformed.users_mv TO transformed.users AS
SELECT
    *
FROM
    transformed.kafka_users;

CREATE MATERIALIZED VIEW IF NOT EXISTS transformed.products_mv TO transformed.products AS
SELECT
    *
FROM
    transformed.kafka_products;





CREATE MATERIALIZED VIEW transformed.behavioral_events_mv
TO transformed.behavioral_events
AS
SELECT
    `timestamp`,
    ifNull(user_id, '') AS user_id,
    ifNull(event_type, '') AS event_type,
    ifNull(device, '') AS device,
    ifNull(session_id, '') AS session_id,
    CAST(
        concat(
            '{',
            arrayStringConcat(
                arrayFilter(x -> x != '',
                    [
                        if(product_id IS NOT NULL,
                            concat('"product_id":', toJSONString(product_id)), ''),

                        if(quantity IS NOT NULL,
                            concat('"quantity":', toJSONString(quantity)), ''),

                        if(cart_total_items IS NOT NULL,
                            concat('"cart_total_items":', toJSONString(cart_total_items)), ''),

                        if(cart_items IS NOT NULL,
                            concat('"cart_items":', toJSONString(cart_items)), ''),

                        if(cart_value IS NOT NULL,
                            concat('"cart_value":', toJSONString(cart_value)), ''),

                        if(shipping_method IS NOT NULL,
                            concat('"shipping_method":', toJSONString(shipping_method)), ''),

                        if(order_id IS NOT NULL,
                            concat('"order_id":', toJSONString(order_id)), ''),

                        if(fulfillment_speed IS NOT NULL,
                            concat('"fulfillment_speed":', toJSONString(fulfillment_speed)), ''),

                        if(url_path IS NOT NULL,
                            concat('"url_path":', toJSONString(url_path)), ''),

                        if(duration_sec IS NOT NULL,
                            concat('"duration_sec":', toJSONString(duration_sec)), ''),

                        if(http_status IS NOT NULL,
                            concat('"http_status":', toJSONString(http_status)), ''),

                        if(payment_type IS NOT NULL,
                            concat('"payment_type":', toJSONString(payment_type)), ''),

                        if(success IS NOT NULL,
                            concat('"success":', toJSONString(success)), ''),

                        if(error_code IS NOT NULL,
                            concat('"error_code":', toJSONString(error_code)), ''),

                        if(`query` IS NOT NULL,
                            concat('"query":', toJSONString(`query`)), ''),

                        if(results_count IS NOT NULL,
                            concat('"results_count":', toJSONString(results_count)), ''),

                        if(clicked_position IS NOT NULL,
                            concat('"clicked_position":', toJSONString(clicked_position)), ''),

                        if(rating IS NOT NULL,
                            concat('"rating":', toJSONString(rating)), ''),

                        if(text_length IS NOT NULL,
                            concat('"text_length":', toJSONString(text_length)), ''),

                        if(wishlist_name IS NOT NULL,
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
WHERE `timestamp` IS NOT NULL
  AND user_id IS NOT NULL
  AND event_type IS NOT NULL
  AND device IS NOT NULL
  AND session_id IS NOT NULL;
