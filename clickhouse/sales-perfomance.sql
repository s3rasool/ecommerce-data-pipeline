CREATE VIEW
    transformed.overall_sales_performance AS
SELECT
    count() AS total_orders,
    sum(total) AS total_revenue,
    round(avg(total), 2) AS aov,
    uniqExact (user_id) AS unique_buyers
FROM
    transformed.orders
WHERE
    status = 'completed';