## ClickHouse Analytics Queries

### User Purchase Funnel

Shows user drop-off at each step of the purchase journey.

```sql
SELECT 
    funnel_step,
    event_count,
    ROUND(event_count * 100.0 / MAX(event_count) OVER (), 2) AS conversion_pct
FROM transformed.user_funnel_daily
WHERE date = today()
ORDER BY 
    CASE funnel_step
        WHEN 'page_view' THEN 1
        WHEN 'product_search' THEN 2
        WHEN 'add_to_cart' THEN 3
        WHEN 'cart_view' THEN 4
        WHEN 'checkout_start' THEN 5
        WHEN 'payment_attempt' THEN 6
        WHEN 'order_complete' THEN 7
    END;