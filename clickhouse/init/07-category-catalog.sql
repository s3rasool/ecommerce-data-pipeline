CREATE TABLE
    IF NOT EXISTS transformed.category_catalog_analytics (
        snapshot_date Date,
        category String,
        product_count UInt64,
        total_inventory Int64,
        avg_price Decimal(18, 2),
        avg_popularity Float64,
        max_popularity Float64
    ) ENGINE = SummingMergeTree
ORDER BY
    (snapshot_date, category);

CREATE MATERIALIZED VIEW IF NOT EXISTS transformed.category_catalog_analytics_mv TO transformed.category_catalog_analytics AS
SELECT
    toDate (ingested_at) AS snapshot_date,
    category,
    count() AS product_count,
    sum(toInt64 (inventory)) AS total_inventory,
    toDecimal64 (avg(toFloat64 (price)), 2) AS avg_price,
    avg(toFloat64 (popularity_score)) AS avg_popularity,
    max(toFloat64 (popularity_score)) AS max_popularity
FROM
    transformed.products
GROUP BY
    snapshot_date,
    category;

INSERT INTO
    transformed.category_catalog_analytics
SELECT
    toDate (ingested_at) AS snapshot_date,
    category,
    count() AS product_count,
    sum(toInt64 (inventory)) AS total_inventory,
    toDecimal64 (avg(toFloat64 (price)), 2) AS avg_price,
    avg(toFloat64 (popularity_score)) AS avg_popularity,
    max(toFloat64 (popularity_score)) AS max_popularity
FROM
    transformed.products
GROUP BY
    snapshot_date,
    category;