-- Gold aggregates as Fluss primary-key tables. Flink writes retractions into
-- them (an updated group is an upsert, an emptied group is a delete), and
-- Fluss turns that into a clean per-key changelog that the ClickHouse sink
-- job reads from gold.<table>$changelog.
USE CATALOG fluss_catalog;

CREATE DATABASE IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.daily_revenue (
    order_date    DATE,
    orders_count  BIGINT,
    revenue_cents BIGINT,
    PRIMARY KEY (order_date) NOT ENFORCED
);

CREATE TABLE IF NOT EXISTS gold.category_revenue (
    category_id   INT,
    category_name STRING,
    units_sold    BIGINT,
    revenue_cents BIGINT,
    PRIMARY KEY (category_id) NOT ENFORCED
);

CREATE TABLE IF NOT EXISTS gold.customer_ltv (
    customer_id          INT,
    email                STRING,
    country              STRING,
    orders_count         BIGINT,
    lifetime_value_cents BIGINT,
    PRIMARY KEY (customer_id) NOT ENFORCED
);
