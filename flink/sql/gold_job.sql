-- bronze -> gold: three continuously maintained aggregates.
--
-- Reading a Fluss primary-key table gives Flink a full changelog (snapshot,
-- then every insert, update-before/after and delete), so every aggregate
-- and join below retracts and re-emits when its inputs change: a cancelled
-- order leaves its day, a deleted line item leaves its category, a renamed
-- category or a customer's new country updates rows already emitted. A
-- ClickHouse materialized view can't do this: it only ever sees inserts.
--
-- One STATEMENT SET, so the three queries run as one job and share one
-- read of each bronze table.
USE CATALOG fluss_catalog;
SET 'pipeline.name' = 'fluss-gold';

EXECUTE STATEMENT SET
BEGIN

-- Revenue per order day (UTC), cancelled orders excluded.
INSERT INTO gold.daily_revenue
SELECT
    CAST(created_at AS DATE)               AS order_date,
    COUNT(*)                               AS orders_count,
    SUM(CAST(order_total_cents AS BIGINT)) AS revenue_cents
FROM bronze.orders
WHERE status <> 'cancelled'
GROUP BY CAST(created_at AS DATE);

-- Units and revenue per category from line items: items -> orders for
-- status, items -> products for category, then the category name.
INSERT INTO gold.category_revenue
SELECT
    c.category_id,
    c.name AS category_name,
    s.units_sold,
    s.revenue_cents
FROM (
    SELECT
        p.category_id,
        SUM(CAST(oi.quantity AS BIGINT))                       AS units_sold,
        SUM(CAST(oi.quantity AS BIGINT) * oi.unit_price_cents) AS revenue_cents
    FROM bronze.order_items AS oi
    JOIN bronze.orders   AS o ON o.order_id = oi.order_id
    JOIN bronze.products AS p ON p.product_id = oi.product_id
    WHERE o.status <> 'cancelled'
    GROUP BY p.category_id
) AS s
JOIN bronze.categories AS c ON c.category_id = s.category_id;

-- One row per customer, including customers with no orders yet (zeros).
INSERT INTO gold.customer_ltv
SELECT
    cu.customer_id,
    cu.email,
    cu.country,
    COALESCE(a.orders_count, 0)         AS orders_count,
    COALESCE(a.lifetime_value_cents, 0) AS lifetime_value_cents
FROM bronze.customers AS cu
LEFT JOIN (
    SELECT
        customer_id,
        COUNT(*)                               AS orders_count,
        SUM(CAST(order_total_cents AS BIGINT)) AS lifetime_value_cents
    FROM bronze.orders
    WHERE status <> 'cancelled'
    GROUP BY customer_id
) AS a ON a.customer_id = cu.customer_id;

END;
