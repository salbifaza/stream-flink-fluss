-- Fluss -> ClickHouse: streams every bronze and gold table into ClickHouse.
--
-- ClickHouse has no cheap row-level UPDATE or DELETE, so nothing here
-- updates or deletes. Each sink reads the table's Fluss $changelog, an
-- append-only stream with one row per change plus metadata columns:
--   _change_type       insert | update_before | update_after | delete
--   _log_offset        position in the table bucket's log
--   _commit_timestamp  when Fluss committed the change
-- and appends every change as a new row into a
-- ReplacingMergeTree(_version, is_deleted) table (clickhouse/init/):
--   _version   computed by ClickHouse from _fluss_commit_ts and _log_offset.
--              A key always hashes to the same bucket, where both only grow,
--              so the latest change to a key always has the highest version.
--   is_deleted = 1 for deletes. FINAL then hides the row.
-- update_before rows are dropped: the update_after that follows carries the
-- new state at a higher offset.
--
-- Because a replayed change carries the same version as the original, a
-- restart that re-sends rows (the JDBC sink is at-least-once) produces
-- duplicates ClickHouse collapses anyway: the sink is idempotent by design.
--
-- ${CLICKHOUSE_SINK_*} placeholders are rendered by run-sql-jobs.sh.
SET 'pipeline.name' = 'fluss-to-clickhouse';

-- ── bronze sinks ─────────────────────────────────────────────────────────────

CREATE TEMPORARY TABLE ch_categories (
    category_id INT, name STRING, created_at TIMESTAMP(6),
    _log_offset BIGINT, is_deleted TINYINT, _fluss_commit_ts TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:clickhouse://clickhouse:8123/bronze',
    'table-name' = 'categories',
    'username' = '${CLICKHOUSE_SINK_USER}', 'password' = '${CLICKHOUSE_SINK_PASSWORD}',
    'sink.buffer-flush.interval' = '1s', 'sink.buffer-flush.max-rows' = '5000'
);

CREATE TEMPORARY TABLE ch_customers (
    customer_id INT, email STRING, first_name STRING, last_name STRING, country STRING,
    created_at TIMESTAMP(6), updated_at TIMESTAMP(6),
    _log_offset BIGINT, is_deleted TINYINT, _fluss_commit_ts TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:clickhouse://clickhouse:8123/bronze',
    'table-name' = 'customers',
    'username' = '${CLICKHOUSE_SINK_USER}', 'password' = '${CLICKHOUSE_SINK_PASSWORD}',
    'sink.buffer-flush.interval' = '1s', 'sink.buffer-flush.max-rows' = '5000'
);

CREATE TEMPORARY TABLE ch_products (
    product_id INT, sku STRING, name STRING, category_id INT, price_cents INT,
    description STRING, created_at TIMESTAMP(6), updated_at TIMESTAMP(6),
    _log_offset BIGINT, is_deleted TINYINT, _fluss_commit_ts TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:clickhouse://clickhouse:8123/bronze',
    'table-name' = 'products',
    'username' = '${CLICKHOUSE_SINK_USER}', 'password' = '${CLICKHOUSE_SINK_PASSWORD}',
    'sink.buffer-flush.interval' = '1s', 'sink.buffer-flush.max-rows' = '5000'
);

CREATE TEMPORARY TABLE ch_orders (
    order_id INT, customer_id INT, status STRING, order_total_cents INT,
    created_at TIMESTAMP(6), updated_at TIMESTAMP(6),
    _log_offset BIGINT, is_deleted TINYINT, _fluss_commit_ts TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:clickhouse://clickhouse:8123/bronze',
    'table-name' = 'orders',
    'username' = '${CLICKHOUSE_SINK_USER}', 'password' = '${CLICKHOUSE_SINK_PASSWORD}',
    'sink.buffer-flush.interval' = '1s', 'sink.buffer-flush.max-rows' = '5000'
);

CREATE TEMPORARY TABLE ch_order_items (
    order_item_id INT, order_id INT, product_id INT, quantity INT, unit_price_cents INT,
    created_at TIMESTAMP(6),
    _log_offset BIGINT, is_deleted TINYINT, _fluss_commit_ts TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:clickhouse://clickhouse:8123/bronze',
    'table-name' = 'order_items',
    'username' = '${CLICKHOUSE_SINK_USER}', 'password' = '${CLICKHOUSE_SINK_PASSWORD}',
    'sink.buffer-flush.interval' = '1s', 'sink.buffer-flush.max-rows' = '5000'
);

CREATE TEMPORARY TABLE ch_payments (
    payment_id INT, order_id INT, amount_cents INT, `method` STRING, status STRING,
    processed_at TIMESTAMP(6),
    _log_offset BIGINT, is_deleted TINYINT, _fluss_commit_ts TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:clickhouse://clickhouse:8123/bronze',
    'table-name' = 'payments',
    'username' = '${CLICKHOUSE_SINK_USER}', 'password' = '${CLICKHOUSE_SINK_PASSWORD}',
    'sink.buffer-flush.interval' = '1s', 'sink.buffer-flush.max-rows' = '5000'
);

-- ── gold sinks ───────────────────────────────────────────────────────────────

CREATE TEMPORARY TABLE ch_daily_revenue (
    order_date DATE, orders_count BIGINT, revenue_cents BIGINT,
    _log_offset BIGINT, is_deleted TINYINT, _fluss_commit_ts TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:clickhouse://clickhouse:8123/gold',
    'table-name' = 'daily_revenue',
    'username' = '${CLICKHOUSE_SINK_USER}', 'password' = '${CLICKHOUSE_SINK_PASSWORD}',
    'sink.buffer-flush.interval' = '1s', 'sink.buffer-flush.max-rows' = '5000'
);

CREATE TEMPORARY TABLE ch_category_revenue (
    category_id INT, category_name STRING, units_sold BIGINT, revenue_cents BIGINT,
    _log_offset BIGINT, is_deleted TINYINT, _fluss_commit_ts TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:clickhouse://clickhouse:8123/gold',
    'table-name' = 'category_revenue',
    'username' = '${CLICKHOUSE_SINK_USER}', 'password' = '${CLICKHOUSE_SINK_PASSWORD}',
    'sink.buffer-flush.interval' = '1s', 'sink.buffer-flush.max-rows' = '5000'
);

CREATE TEMPORARY TABLE ch_customer_ltv (
    customer_id INT, email STRING, country STRING, orders_count BIGINT, lifetime_value_cents BIGINT,
    _log_offset BIGINT, is_deleted TINYINT, _fluss_commit_ts TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:clickhouse://clickhouse:8123/gold',
    'table-name' = 'customer_ltv',
    'username' = '${CLICKHOUSE_SINK_USER}', 'password' = '${CLICKHOUSE_SINK_PASSWORD}',
    'sink.buffer-flush.interval' = '1s', 'sink.buffer-flush.max-rows' = '5000'
);

-- ── changelog -> ClickHouse ──────────────────────────────────────────────────

EXECUTE STATEMENT SET
BEGIN

INSERT INTO ch_categories
SELECT category_id, name, created_at,
       _log_offset, CAST(IF(_change_type = 'delete', 1, 0) AS TINYINT), CAST(_commit_timestamp AS TIMESTAMP(3))
FROM fluss_catalog.bronze.`categories$changelog` WHERE _change_type <> 'update_before';

INSERT INTO ch_customers
SELECT customer_id, email, first_name, last_name, country, created_at, updated_at,
       _log_offset, CAST(IF(_change_type = 'delete', 1, 0) AS TINYINT), CAST(_commit_timestamp AS TIMESTAMP(3))
FROM fluss_catalog.bronze.`customers$changelog` WHERE _change_type <> 'update_before';

INSERT INTO ch_products
SELECT product_id, sku, name, category_id, price_cents, description, created_at, updated_at,
       _log_offset, CAST(IF(_change_type = 'delete', 1, 0) AS TINYINT), CAST(_commit_timestamp AS TIMESTAMP(3))
FROM fluss_catalog.bronze.`products$changelog` WHERE _change_type <> 'update_before';

INSERT INTO ch_orders
SELECT order_id, customer_id, status, order_total_cents, created_at, updated_at,
       _log_offset, CAST(IF(_change_type = 'delete', 1, 0) AS TINYINT), CAST(_commit_timestamp AS TIMESTAMP(3))
FROM fluss_catalog.bronze.`orders$changelog` WHERE _change_type <> 'update_before';

INSERT INTO ch_order_items
SELECT order_item_id, order_id, product_id, quantity, unit_price_cents, created_at,
       _log_offset, CAST(IF(_change_type = 'delete', 1, 0) AS TINYINT), CAST(_commit_timestamp AS TIMESTAMP(3))
FROM fluss_catalog.bronze.`order_items$changelog` WHERE _change_type <> 'update_before';

INSERT INTO ch_payments
SELECT payment_id, order_id, amount_cents, `method`, status, processed_at,
       _log_offset, CAST(IF(_change_type = 'delete', 1, 0) AS TINYINT), CAST(_commit_timestamp AS TIMESTAMP(3))
FROM fluss_catalog.bronze.`payments$changelog` WHERE _change_type <> 'update_before';

INSERT INTO ch_daily_revenue
SELECT order_date, orders_count, revenue_cents,
       _log_offset, CAST(IF(_change_type = 'delete', 1, 0) AS TINYINT), CAST(_commit_timestamp AS TIMESTAMP(3))
FROM fluss_catalog.gold.`daily_revenue$changelog` WHERE _change_type <> 'update_before';

INSERT INTO ch_category_revenue
SELECT category_id, category_name, units_sold, revenue_cents,
       _log_offset, CAST(IF(_change_type = 'delete', 1, 0) AS TINYINT), CAST(_commit_timestamp AS TIMESTAMP(3))
FROM fluss_catalog.gold.`category_revenue$changelog` WHERE _change_type <> 'update_before';

INSERT INTO ch_customer_ltv
SELECT customer_id, email, country, orders_count, lifetime_value_cents,
       _log_offset, CAST(IF(_change_type = 'delete', 1, 0) AS TINYINT), CAST(_commit_timestamp AS TIMESTAMP(3))
FROM fluss_catalog.gold.`customer_ltv$changelog` WHERE _change_type <> 'update_before';

END;
