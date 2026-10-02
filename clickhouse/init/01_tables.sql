-- ClickHouse replicas of the Fluss bronze and gold tables.
--
-- Every table is ReplacingMergeTree(_version, is_deleted), fed append-only
-- by flink/sql/clickhouse_sink_job.sql: one row per change, carrying its
-- Fluss _log_offset and _fluss_commit_ts, with is_deleted = 1 for deletes.
-- Query with FINAL to get the current state: it keeps the highest _version
-- per key and drops keys whose latest change is a delete, so no
-- WHERE is_deleted = 0 filter is needed.
--
-- Why _version isn't just the log offset: offsets are only ordered within
-- one incarnation of a Fluss table. If the table is ever recreated (tested:
-- losing ZooKeeper's metadata does exactly this), its offsets restart at 0,
-- every new change gets a lower version than the stale rows already here,
-- and FINAL silently keeps the old values. So the version is the commit time
-- in milliseconds, shifted left 22 bits, plus the offset modulo 2^22:
-- ordered by commit time across table incarnations, and by offset within
-- the same millisecond. The assumption it rests on is that the Fluss tablet
-- server's clock never goes backwards.
--
-- _fluss_commit_ts (when Fluss committed the change) and _synced_at (when
-- ClickHouse received it) make per-row latency directly queryable.

CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS gold;

-- ── bronze: one table per source table ───────────────────────────────────────

CREATE TABLE IF NOT EXISTS bronze.categories (
    category_id      Int32,
    name             String,
    created_at       DateTime64(6),
    _log_offset      UInt64,
    is_deleted       UInt8,
    _fluss_commit_ts DateTime64(3),
    _version         UInt64 DEFAULT toUInt64(toUnixTimestamp64Milli(_fluss_commit_ts)) * 4194304 + _log_offset % 4194304,
    _synced_at       DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(_version, is_deleted)
ORDER BY category_id;

CREATE TABLE IF NOT EXISTS bronze.customers (
    customer_id      Int32,
    email            String,
    first_name       String,
    last_name        String,
    country          String,
    created_at       DateTime64(6),
    updated_at       DateTime64(6),
    _log_offset      UInt64,
    is_deleted       UInt8,
    _fluss_commit_ts DateTime64(3),
    _version         UInt64 DEFAULT toUInt64(toUnixTimestamp64Milli(_fluss_commit_ts)) * 4194304 + _log_offset % 4194304,
    _synced_at       DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(_version, is_deleted)
ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS bronze.products (
    product_id       Int32,
    sku              String,
    name             String,
    category_id      Int32,
    price_cents      Int32,
    description      Nullable(String),
    created_at       DateTime64(6),
    updated_at       DateTime64(6),
    _log_offset      UInt64,
    is_deleted       UInt8,
    _fluss_commit_ts DateTime64(3),
    _version         UInt64 DEFAULT toUInt64(toUnixTimestamp64Milli(_fluss_commit_ts)) * 4194304 + _log_offset % 4194304,
    _synced_at       DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(_version, is_deleted)
ORDER BY product_id;

CREATE TABLE IF NOT EXISTS bronze.orders (
    order_id          Int32,
    customer_id       Int32,
    status            LowCardinality(String),
    order_total_cents Int32,
    created_at        DateTime64(6),
    updated_at        DateTime64(6),
    _log_offset       UInt64,
    is_deleted        UInt8,
    _fluss_commit_ts  DateTime64(3),
    _version          UInt64 DEFAULT toUInt64(toUnixTimestamp64Milli(_fluss_commit_ts)) * 4194304 + _log_offset % 4194304,
    _synced_at        DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(_version, is_deleted)
ORDER BY order_id;

CREATE TABLE IF NOT EXISTS bronze.order_items (
    order_item_id    Int32,
    order_id         Int32,
    product_id       Int32,
    quantity         Int32,
    unit_price_cents Int32,
    created_at       DateTime64(6),
    _log_offset      UInt64,
    is_deleted       UInt8,
    _fluss_commit_ts DateTime64(3),
    _version         UInt64 DEFAULT toUInt64(toUnixTimestamp64Milli(_fluss_commit_ts)) * 4194304 + _log_offset % 4194304,
    _synced_at       DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(_version, is_deleted)
ORDER BY order_item_id;

CREATE TABLE IF NOT EXISTS bronze.payments (
    payment_id       Int32,
    order_id         Int32,
    amount_cents     Int32,
    method           LowCardinality(String),
    status           LowCardinality(String),
    processed_at     Nullable(DateTime64(6)),
    _log_offset      UInt64,
    is_deleted       UInt8,
    _fluss_commit_ts DateTime64(3),
    _version         UInt64 DEFAULT toUInt64(toUnixTimestamp64Milli(_fluss_commit_ts)) * 4194304 + _log_offset % 4194304,
    _synced_at       DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(_version, is_deleted)
ORDER BY payment_id;

-- ── gold: aggregates maintained by Flink (flink/sql/gold_job.sql) ────────────

CREATE TABLE IF NOT EXISTS gold.daily_revenue (
    order_date       Date,
    orders_count     Int64,
    revenue_cents    Int64,
    _log_offset      UInt64,
    is_deleted       UInt8,
    _fluss_commit_ts DateTime64(3),
    _version         UInt64 DEFAULT toUInt64(toUnixTimestamp64Milli(_fluss_commit_ts)) * 4194304 + _log_offset % 4194304,
    _synced_at       DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(_version, is_deleted)
ORDER BY order_date;

CREATE TABLE IF NOT EXISTS gold.category_revenue (
    category_id      Int32,
    category_name    String,
    units_sold       Int64,
    revenue_cents    Int64,
    _log_offset      UInt64,
    is_deleted       UInt8,
    _fluss_commit_ts DateTime64(3),
    _version         UInt64 DEFAULT toUInt64(toUnixTimestamp64Milli(_fluss_commit_ts)) * 4194304 + _log_offset % 4194304,
    _synced_at       DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(_version, is_deleted)
ORDER BY category_id;

CREATE TABLE IF NOT EXISTS gold.customer_ltv (
    customer_id          Int32,
    email                String,
    country              String,
    orders_count         Int64,
    lifetime_value_cents Int64,
    _log_offset          UInt64,
    is_deleted           UInt8,
    _fluss_commit_ts     DateTime64(3),
    _version             UInt64 DEFAULT toUInt64(toUnixTimestamp64Milli(_fluss_commit_ts)) * 4194304 + _log_offset % 4194304,
    _synced_at           DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(_version, is_deleted)
ORDER BY customer_id;
