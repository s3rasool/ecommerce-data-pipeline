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
```
## Product Catalog Analytics Queries

---

### 1. Top Products by Popularity

This query returns the top 20 products ranked by `popularity_score`. If products have the same popularity score, products with higher inventory are shown first.
  
```sql
SELECT
  snapshot_date,
  product_id,
  name,
  category,
  price,
  inventory,
  popularity_score
FROM 
  transformed.product_catalog_analytics
ORDER BY 
  popularity_score DESC,
  inventory DESC
LIMIT 20;
```
---

### 2. Products in Each Category

This query lists products grouped by category and ordered by popularity within each category.
```sql
SELECT
  category,
  product_id,
  name,
  price,
  inventory,
  popularity_score
FROM 
  transformed.product_catalog_analytics
ORDER BY 
  category,
  popularity_score DESC;
```
---

### 3. Category Performance

This query summarizes catalog-level performance by category and date.

It shows:

- number of products in each category
- total inventory available
- average product price
- average popularity score
- highest popularity score in the category

```sql
SELECT
  snapshot_date,
  category,
  sum(product_count) AS product_count,
  sum(total_inventory) AS total_inventory,
  round(avg(toFloat64(avg_price)), 2) AS avg_price,
  round(avg(avg_popularity), 2) AS avg_popularity,
  max(max_popularity) AS max_popularity
FROM 
  transformed.category_catalog_analytics
GROUP BY
  snapshot_date,
  category
ORDER BY 
  snapshot_date DESC,
  avg_popularity DESC;
```
---

### 4. Low-Inventory, High-Popularity Products

This query identifies popular products with low stock.

It is useful for detecting products that may need restocking soon.

```sql
SELECT
  product_id,
  name,
  category,
  inventory,
  popularity_score
FROM 
  transformed.product_catalog_analytics
WHERE 
  inventory < 10
ORDER BY 
  popularity_score DESC,
  inventory ASC;
```

