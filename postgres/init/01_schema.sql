-- E-commerce OLTP schema captured by Flink CDC.
--
-- Two choices here are load-bearing, both found by testing Flink CDC 3.6.0
-- with the Fluss pipeline sink:
--
-- 1. Plain TIMESTAMP, not TIMESTAMPTZ. Postgres TIMESTAMPTZ maps to Flink
--    CDC's ZonedTimestampType, which the Fluss sink rejects ("Unsupported
--    data type in fluss TIMESTAMP(6) WITH TIME ZONE"). CAST(... AS TIMESTAMP)
--    in a pipeline transform doesn't help: it hits a NumberFormatException in
--    BinaryRecordData.getZonedTimestamp before the cast runs. Timestamps are
--    stored as UTC wall-clock time instead.
--
-- 2. REPLICA IDENTITY FULL (bottom of file). With the default identity,
--    UPDATE/DELETE WAL records carry only the primary key in the before-image,
--    and Flink CDC's DebeziumSchemaDataTypeInference.inferStruct throws an NPE
--    on the null non-key fields, crash-looping the pipeline on the first
--    UPDATE or DELETE.
--
-- Every table has a primary key: logical replication needs one to identify
-- the row behind an UPDATE or DELETE, and it becomes the Fluss and ClickHouse
-- primary key downstream.
CREATE TABLE categories (
    category_id     SERIAL PRIMARY KEY,
    name             TEXT NOT NULL UNIQUE,
    created_at       TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'UTC')
);

CREATE TABLE customers (
    customer_id      SERIAL PRIMARY KEY,
    email            TEXT NOT NULL UNIQUE,
    first_name       TEXT NOT NULL,
    last_name        TEXT NOT NULL,
    country          TEXT NOT NULL,
    created_at       TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    updated_at       TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'UTC')
);

CREATE TABLE products (
    product_id       SERIAL PRIMARY KEY,
    sku              TEXT NOT NULL UNIQUE,
    name             TEXT NOT NULL,
    category_id      INTEGER NOT NULL REFERENCES categories(category_id),
    price_cents      INTEGER NOT NULL CHECK (price_cents >= 0),
    description      TEXT,
    created_at       TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    updated_at       TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'UTC')
);

CREATE TABLE orders (
    order_id         SERIAL PRIMARY KEY,
    customer_id      INTEGER NOT NULL REFERENCES customers(customer_id),
    status           TEXT NOT NULL DEFAULT 'pending'
                       CHECK (status IN ('pending', 'paid', 'shipped', 'delivered', 'cancelled')),
    order_total_cents INTEGER NOT NULL DEFAULT 0,
    created_at       TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    updated_at       TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'UTC')
);

CREATE TABLE order_items (
    order_item_id    SERIAL PRIMARY KEY,
    order_id         INTEGER NOT NULL REFERENCES orders(order_id),
    product_id       INTEGER NOT NULL REFERENCES products(product_id),
    quantity         INTEGER NOT NULL CHECK (quantity > 0),
    unit_price_cents INTEGER NOT NULL CHECK (unit_price_cents >= 0),
    created_at       TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'UTC')
);

CREATE TABLE payments (
    payment_id       SERIAL PRIMARY KEY,
    order_id         INTEGER NOT NULL REFERENCES orders(order_id),
    amount_cents     INTEGER NOT NULL CHECK (amount_cents >= 0),
    method           TEXT NOT NULL CHECK (method IN ('card', 'paypal', 'bank_transfer')),
    status           TEXT NOT NULL DEFAULT 'pending'
                       CHECK (status IN ('pending', 'succeeded', 'failed', 'refunded')),
    processed_at     TIMESTAMP
);

CREATE INDEX idx_products_category ON products(category_id);
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_order_items_order ON order_items(order_id);
CREATE INDEX idx_order_items_product ON order_items(product_id);
CREATE INDEX idx_payments_order ON payments(order_id);

-- Flink CDC creates its own publication for the tables listed in
-- flink/postgres-to-fluss.yaml, so none is created here.

ALTER TABLE categories   REPLICA IDENTITY FULL;
ALTER TABLE customers    REPLICA IDENTITY FULL;
ALTER TABLE products     REPLICA IDENTITY FULL;
ALTER TABLE orders       REPLICA IDENTITY FULL;
ALTER TABLE order_items  REPLICA IDENTITY FULL;
ALTER TABLE payments     REPLICA IDENTITY FULL;
