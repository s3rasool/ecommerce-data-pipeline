-- User Funnel Table
CREATE TABLE IF NOT EXISTS transformed.user_funnel_daily (
    date Date,
    funnel_step String,
    event_count UInt64
) ENGINE = SummingMergeTree()
ORDER BY (date, funnel_step);

-- User Funnel Materialized View
CREATE MATERIALIZED VIEW IF NOT EXISTS transformed.user_funnel_mv
TO transformed.user_funnel_daily
AS
SELECT
    event_date AS date,
    event_type AS funnel_step,
    COUNT(*) AS event_count
FROM transformed.behavioral_events
WHERE event_type IN (
    'page_view',
    'product_search',
    'add_to_cart',
    'cart_view',
    'checkout_start',
    'payment_attempt',
    'order_complete'
)
GROUP BY date, funnel_step;