#!/usr/bin/env bash
# End-to-end check of Postgres -> Flink CDC -> Fluss -> ClickHouse:
#   1. every bronze table in ClickHouse matches its Postgres row count
#   2. a live insert, update and delete each reach ClickHouse, with the
#      measured Postgres -> ClickHouse latency
#   3. every gold table equals the same aggregate computed in Postgres,
#      before and after changes that need retractions
# Every change is reversible or self-contained, so the script can be re-run.
set -euo pipefail

TIMEOUT=${VERIFY_TIMEOUT:-120}
PG="docker compose exec -T source-postgres psql -U ${SOURCE_PG_USER:-ecommerce} -d ${SOURCE_PG_DB:-ecommerce} -tA -v ON_ERROR_STOP=1"
CH="docker compose exec -T clickhouse clickhouse-client --user ${CLICKHOUSE_USER:-ch_admin} --password ${CLICKHOUSE_PASSWORD:-ch_admin_password}"

TABLES=(categories customers products orders order_items payments)

counts_match() {
    local out="" ok=0
    for t in "${TABLES[@]}"; do
        local src ch
        src=$($PG -c "SELECT count(*) FROM ${t};")
        ch=$($CH -q "SELECT count() FROM bronze.${t} FINAL;" 2>/dev/null || echo "?")
        if [ "$src" = "$ch" ]; then status=OK; else status=MISMATCH; ok=1; fi
        out+=$(printf "  %-12s postgres=%-5s clickhouse=%-5s %s" "$t" "$src" "$ch" "$status")$'\n'
    done
    LAST_COUNTS=$out
    return $ok
}

echo "== Step 1: bronze row counts, Postgres vs. ClickHouse (FINAL) =="
deadline=$((SECONDS + TIMEOUT))
until counts_match; do
    [ $SECONDS -ge $deadline ] && { printf "%s" "$LAST_COUNTS"; echo "Row counts did not converge within ${TIMEOUT}s. Check: make status" >&2; exit 1; }
    sleep 3
done
printf "%s" "$LAST_COUNTS"

echo
echo "== Step 2: live insert / update / delete =="
marker="verify-$(date +%s)"
gone="${marker}-deleted"
new_status=$($PG -c "SELECT CASE status WHEN 'cancelled' THEN 'delivered' ELSE 'cancelled' END FROM orders WHERE order_id = 1;")
echo "  insert category '${marker}'"
echo "  update order_id=1 status -> '${new_status}'"
echo "  insert + delete category '${gone}'"
$PG -c "INSERT INTO categories (name) VALUES ('${marker}'), ('${gone}');" >/dev/null
$PG -c "UPDATE orders SET status = '${new_status}', updated_at = (now() AT TIME ZONE 'UTC') WHERE order_id = 1;" >/dev/null
$PG -c "DELETE FROM categories WHERE name = '${gone}';" >/dev/null
started=$SECONDS

ins=0; upd=0; del=0
deadline=$((SECONDS + TIMEOUT))
while [ $SECONDS -lt $deadline ]; do
    [ $ins -eq 0 ] && [ "$($CH -q "SELECT count() FROM bronze.categories FINAL WHERE name = '${marker}'")" = 1 ] && ins=1
    [ $upd -eq 0 ] && [ "$($CH -q "SELECT status FROM bronze.orders FINAL WHERE order_id = 1")" = "$new_status" ] && upd=1
    # Deleted: the row arrived (a delete is a newer version of it) and FINAL hides it.
    [ $del -eq 0 ] && [ "$($CH -q "SELECT countIf(is_deleted = 1) > 0 AND (SELECT count() FROM bronze.categories FINAL WHERE name = '${gone}') = 0 FROM bronze.categories WHERE name = '${gone}'")" = 1 ] && del=1
    [ $ins -eq 1 ] && [ $upd -eq 1 ] && [ $del -eq 1 ] && break
    sleep 1
done
printf "  insert propagated: %s\n" "$([ $ins -eq 1 ] && echo yes || echo NO)"
printf "  update propagated: %s\n" "$([ $upd -eq 1 ] && echo yes || echo NO)"
printf "  delete propagated: %s\n" "$([ $del -eq 1 ] && echo yes || echo NO)"
if [ $ins -ne 1 ] || [ $upd -ne 1 ] || [ $del -ne 1 ]; then
    echo "Verification FAILED: a change did not reach ClickHouse within ${TIMEOUT}s." >&2
    exit 1
fi
# updated_at is set by the UPDATE itself (UTC), _synced_at by ClickHouse on
# arrival (UTC), so their difference is the end-to-end latency of that row.
latency=$($CH -q "SELECT toUInt64(dateDiff('millisecond', updated_at, _synced_at)) FROM bronze.orders FINAL WHERE order_id = 1")
fluss_to_ch=$($CH -q "SELECT toUInt64(dateDiff('millisecond', _fluss_commit_ts, _synced_at)) FROM bronze.orders FINAL WHERE order_id = 1")
echo "  latency, Postgres UPDATE -> ClickHouse row: ${latency} ms (Fluss commit -> ClickHouse: ${fluss_to_ch} ms)"

echo
echo "== Step 3: gold tables vs. the same aggregates computed in Postgres =="

# Each pair renders identical 'a|b|c' lines in the same order.
PG_DAILY="SELECT d || '|' || n || '|' || r FROM (
    SELECT created_at::date::text AS d, count(*) AS n, sum(order_total_cents) AS r
    FROM orders WHERE status <> 'cancelled' GROUP BY 1) x ORDER BY d;"
CH_DAILY="SELECT concat(toString(order_date), '|', toString(orders_count), '|', toString(revenue_cents))
    FROM gold.daily_revenue FINAL ORDER BY order_date;"
PG_CATEGORY="SELECT c.category_id || '|' || c.name || '|' || s.units || '|' || s.rev FROM (
    SELECT p.category_id, sum(oi.quantity) AS units, sum(oi.quantity * oi.unit_price_cents) AS rev
    FROM order_items oi JOIN orders o USING (order_id) JOIN products p USING (product_id)
    WHERE o.status <> 'cancelled' GROUP BY p.category_id) s
    JOIN categories c USING (category_id) ORDER BY c.category_id;"
CH_CATEGORY="SELECT concat(toString(category_id), '|', category_name, '|', toString(units_sold), '|', toString(revenue_cents))
    FROM gold.category_revenue FINAL ORDER BY category_id;"
PG_LTV="SELECT cu.customer_id || '|' || cu.email || '|' || cu.country || '|' || coalesce(a.n, 0) || '|' || coalesce(a.v, 0)
    FROM customers cu LEFT JOIN (
        SELECT customer_id, count(*) AS n, sum(order_total_cents) AS v
        FROM orders WHERE status <> 'cancelled' GROUP BY customer_id) a USING (customer_id)
    ORDER BY cu.customer_id;"
CH_LTV="SELECT concat(toString(customer_id), '|', email, '|', country, '|', toString(orders_count), '|', toString(lifetime_value_cents))
    FROM gold.customer_ltv FINAL ORDER BY customer_id;"

GOLD=(daily_revenue category_revenue customer_ltv)
declare -A PG_Q=([daily_revenue]="$PG_DAILY" [category_revenue]="$PG_CATEGORY" [customer_ltv]="$PG_LTV")
declare -A CH_Q=([daily_revenue]="$CH_DAILY" [category_revenue]="$CH_CATEGORY" [customer_ltv]="$CH_LTV")

# Waits until every gold table matches; prints one line per table.
gold_converges() {
    local deadline=$((SECONDS + TIMEOUT)) start=$SECONDS
    while true; do
        local all=0
        for g in "${GOLD[@]}"; do
            [ "$($PG -c "${PG_Q[$g]}")" = "$($CH -q "${CH_Q[$g]}" 2>/dev/null)" ] || all=1
        done
        if [ $all -eq 0 ]; then
            for g in "${GOLD[@]}"; do
                printf "  gold.%-17s %3s rows, identical to Postgres  OK\n" "$g" "$($PG -c "${PG_Q[$g]}" | wc -l)"
            done
            GOLD_ELAPSED=$((SECONDS - start))
            return 0
        fi
        if [ $SECONDS -ge $deadline ]; then
            for g in "${GOLD[@]}"; do
                if [ "$($PG -c "${PG_Q[$g]}")" != "$($CH -q "${CH_Q[$g]}" 2>/dev/null)" ]; then
                    printf "  gold.%-17s MISMATCH (< postgres, > clickhouse):\n" "$g"
                    diff <($PG -c "${PG_Q[$g]}") <($CH -q "${CH_Q[$g]}" 2>&1) | sed 's/^/      /' || true
                fi
            done
            return 1
        fi
        sleep 1
    done
}

gold_converges || { echo "Verification FAILED: gold did not match Postgres within ${TIMEOUT}s." >&2; exit 1; }

echo
echo "  changes that need retractions:"
echo "    toggle order_id=5 between 'paid' and 'cancelled'   (revenue leaves / rejoins its day, category, customer)"
echo "    toggle customer_id=4 country US <-> CA             (an already-joined gold row changes in place)"
echo "    toggle a ' (renamed)' suffix on category_id=3      (a dimension change on the right side of a join)"
echo "    insert a line item on order_id=2, then delete it   (an aggregate goes up, then back down)"
$PG -c "UPDATE orders SET status = CASE status WHEN 'cancelled' THEN 'paid' ELSE 'cancelled' END, updated_at = (now() AT TIME ZONE 'UTC') WHERE order_id = 5;" >/dev/null
$PG -c "UPDATE customers SET country = CASE country WHEN 'US' THEN 'CA' ELSE 'US' END, updated_at = (now() AT TIME ZONE 'UTC') WHERE customer_id = 4;" >/dev/null
$PG -c "UPDATE categories SET name = CASE WHEN name LIKE '% (renamed)' THEN left(name, -10) ELSE name || ' (renamed)' END WHERE category_id = 3;" >/dev/null
item=$($PG -c "INSERT INTO order_items (order_id, product_id, quantity, unit_price_cents) VALUES (2, 1, 3, 1000) RETURNING order_item_id;" | head -1)
$PG -c "DELETE FROM order_items WHERE order_item_id = ${item};" >/dev/null

gold_converges || { echo "Verification FAILED: gold did not converge back to Postgres within ${TIMEOUT}s." >&2; exit 1; }
echo "  gold converged ${GOLD_ELAPSED}s after the last source change."

echo
echo "Verification PASSED."
