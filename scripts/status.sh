#!/usr/bin/env bash
# One-screen health check: Flink jobs, replication slot, per-table freshness.
set -uo pipefail

FLINK_REST="http://localhost:${FLINK_UI_PORT:-8089}"
PG="docker compose exec -T source-postgres psql -U ${SOURCE_PG_USER:-ecommerce} -d ${SOURCE_PG_DB:-ecommerce}"
CH="docker compose exec -T clickhouse clickhouse-client --user ${CLICKHOUSE_USER:-ch_admin} --password ${CLICKHOUSE_PASSWORD:-ch_admin_password}"

echo "== Flink jobs =="
curl -sf "$FLINK_REST/jobs/overview" | python3 -c "
import json, sys
jobs = json.load(sys.stdin)['jobs']
for j in sorted(jobs, key=lambda j: j['start-time'], reverse=True):
    print(f\"  {j['name']:<22} {j['state']:<10} {j['jid']}\")
if not jobs:
    print('  (none)')" || echo "  Flink REST API not reachable at $FLINK_REST"

echo
echo "== Replication slot (WAL Postgres must keep until Flink CDC confirms it) =="
$PG -c "SELECT slot_name, active,
               pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal,
               pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS unconfirmed
        FROM pg_replication_slots;"

echo "== ClickHouse: current rows, change rows received, last arrival =="
$CH --format PrettyCompactMonoBlock -q "
SELECT database, table, current_rows, change_rows, last_synced_at
FROM (
    SELECT 'bronze' AS database, 'categories' AS table, (SELECT count() FROM bronze.categories FINAL) AS current_rows, (SELECT count() FROM bronze.categories) AS change_rows, (SELECT max(_synced_at) FROM bronze.categories) AS last_synced_at
    UNION ALL SELECT 'bronze', 'customers', (SELECT count() FROM bronze.customers FINAL), (SELECT count() FROM bronze.customers), (SELECT max(_synced_at) FROM bronze.customers)
    UNION ALL SELECT 'bronze', 'products', (SELECT count() FROM bronze.products FINAL), (SELECT count() FROM bronze.products), (SELECT max(_synced_at) FROM bronze.products)
    UNION ALL SELECT 'bronze', 'orders', (SELECT count() FROM bronze.orders FINAL), (SELECT count() FROM bronze.orders), (SELECT max(_synced_at) FROM bronze.orders)
    UNION ALL SELECT 'bronze', 'order_items', (SELECT count() FROM bronze.order_items FINAL), (SELECT count() FROM bronze.order_items), (SELECT max(_synced_at) FROM bronze.order_items)
    UNION ALL SELECT 'bronze', 'payments', (SELECT count() FROM bronze.payments FINAL), (SELECT count() FROM bronze.payments), (SELECT max(_synced_at) FROM bronze.payments)
    UNION ALL SELECT 'gold', 'daily_revenue', (SELECT count() FROM gold.daily_revenue FINAL), (SELECT count() FROM gold.daily_revenue), (SELECT max(_synced_at) FROM gold.daily_revenue)
    UNION ALL SELECT 'gold', 'category_revenue', (SELECT count() FROM gold.category_revenue FINAL), (SELECT count() FROM gold.category_revenue), (SELECT max(_synced_at) FROM gold.category_revenue)
    UNION ALL SELECT 'gold', 'customer_ltv', (SELECT count() FROM gold.customer_ltv FINAL), (SELECT count() FROM gold.customer_ltv), (SELECT max(_synced_at) FROM gold.customer_ltv)
)"
