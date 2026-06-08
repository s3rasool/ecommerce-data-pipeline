CREATE TABLE
    IF NOT EXISTS transformed.product_catalog_analytics (
        snapshot_date Date,
        product_id String,
        name String,
        category String,
        price Decimal(10, 2),
        inventory Int32,
        popularity_score Decimal(4, 2)
    ) ENGINE = ReplacingMergeTree
ORDER BY
    (snapshot_date, category, product_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS transformed.product_catalog_analytics_mv TO transformed.product_catalog_analytics AS
SELECT
    toDate (ingested_at) AS snapshot_date,
    product_id,
    name,
    category,
    price,
    inventory,
    popularity_score
FROM
    transformed.products;

INSERT INTO
    transformed.product_catalog_analytics
SELECT
    toDate (ingested_at) AS snapshot_date,
    product_id,
    name,
    category,
    price,
    inventory,
    popularity_score
FROM
    transformed.products;