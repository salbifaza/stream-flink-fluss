#!/bin/sh
# Submits the Flink SQL jobs. Runs inside flink-jobmanager:
#   run-sql-jobs.sh gold   bronze -> gold aggregates (Fluss)
#   run-sql-jobs.sh sink   bronze + gold changelogs -> ClickHouse
# scripts/deploy.sh calls this only for jobs that aren't already RUNNING, and
# sets RESTORE_PATH to the job's last retained checkpoint when there is one.
set -e

DIR=/opt/pipelines/sql
SQL_CLIENT=/opt/flink/bin/sql-client.sh
BRONZE_TABLES="categories customers products orders order_items payments"

# sql-client.sh -f exits 0 even when a statement fails (it prints [ERROR]
# and stops), so failure has to be detected from its output.
run_sql() {
  log=/tmp/sql-$(basename "$1" .sql).log
  "$SQL_CLIENT" -i "$DIR/init.sql" -f "$1" > "$log" 2>&1 || true
  if grep -q "\[ERROR\]" "$log"; then
    grep -A5 "\[ERROR\]" "$log" >&2
    echo "Failed: $1 (full log: $log in flink-jobmanager)" >&2
    exit 1
  fi
  # A statement set that submitted nothing (e.g. an empty file) also exits 0
  # without an [ERROR], so a job file must produce a Job ID.
  case "$1" in
    *_job*) grep "Job ID" "$log" || { echo "No job submitted from $1 (log: $log)" >&2; exit 1; } ;;
  esac
}

# Prints the bronze tables the CDC pipeline hasn't created in Fluss yet.
missing_bronze() {
  probe=/tmp/probe.sql
  echo "SHOW TABLES FROM fluss_catalog.bronze;" > "$probe"
  out=$("$SQL_CLIENT" -i "$DIR/init.sql" -f "$probe" 2>/dev/null || true)
  for t in $BRONZE_TABLES; do
    echo "$out" | grep -qw "$t" || printf '%s ' "$t"
  done
}

# Copies a job file to /tmp, resuming from RESTORE_PATH if set. Prints the path.
with_restore() {
  out=/tmp/submit-$(basename "$1")
  {
    [ -n "$RESTORE_PATH" ] && echo "SET 'execution.state-recovery.path' = '$RESTORE_PATH';"
    cat "$1"
  } > "$out"
  echo "$out"
}

wait_for_bronze() {
  i=0
  while [ -n "$(missing_bronze)" ]; do
    i=$((i + 1))
    if [ "$i" -ge 30 ]; then
      echo "Bronze tables still missing after ~5 min: $(missing_bronze)" >&2
      echo "Is the CDC pipeline running? (make status)" >&2
      exit 1
    fi
    sleep 10
  done
}

case "$1" in
  gold)
    wait_for_bronze
    run_sql "$DIR/gold_tables.sql"
    echo "Submitting bronze -> gold..."
    run_sql "$(with_restore "$DIR/gold_job.sql")"
    ;;
  sink)
    wait_for_bronze
    # The sink reads gold changelogs too, so the gold tables must exist.
    run_sql "$DIR/gold_tables.sql"
    rendered=/tmp/clickhouse_sink_job.rendered.sql
    sed \
      -e "s/\${CLICKHOUSE_SINK_USER}/${CLICKHOUSE_SINK_USER:-flink_sink}/g" \
      -e "s/\${CLICKHOUSE_SINK_PASSWORD}/${CLICKHOUSE_SINK_PASSWORD:-flink_sink_password}/g" \
      "$DIR/clickhouse_sink_job.sql" > "$rendered"
    echo "Submitting Fluss -> ClickHouse..."
    run_sql "$(with_restore "$rendered")"
    ;;
  *)
    echo "usage: $0 gold|sink" >&2
    exit 2
    ;;
esac
