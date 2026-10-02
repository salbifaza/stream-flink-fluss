#!/bin/bash
# Creates the user Flink's JDBC sink connects as. It can only INSERT into the
# two replica databases: no SELECT, no DDL, no access anywhere else. Tables
# are created up front by 01_tables.sql as the bootstrap admin, which is the
# only other identity, and is used for operator queries.
set -e

CH=(clickhouse-client -u "${CLICKHOUSE_USER}" --password "${CLICKHOUSE_PASSWORD}")

"${CH[@]}" -q "CREATE USER IF NOT EXISTS ${CLICKHOUSE_SINK_USER} IDENTIFIED WITH sha256_password BY '${CLICKHOUSE_SINK_PASSWORD}'"
"${CH[@]}" -q "GRANT INSERT ON bronze.* TO ${CLICKHOUSE_SINK_USER}"
"${CH[@]}" -q "GRANT INSERT ON gold.* TO ${CLICKHOUSE_SINK_USER}"

echo "$0: ${CLICKHOUSE_SINK_USER} provisioned (INSERT on bronze.*, gold.*)"
