#!/usr/bin/env bash
# Submits the three Flink jobs, skipping any that are already RUNNING, so
# it's safe to re-run. Order matters: the CDC pipeline creates the bronze
# tables that the gold job and the sink job read.
#
# The JobManager has no HA store, so after it restarts it has forgotten every
# job. Checkpoints are retained on a volume, though, and this script records
# each job's id by name (/opt/flink/checkpoints/jobs/<name>). A job that has
# a completed checkpoint is resumed from it: the CDC pipeline keeps its
# replication slot position and the SQL jobs keep their read offsets and
# join/aggregate state, instead of starting over.
set -euo pipefail

FLINK_REST="http://localhost:${FLINK_UI_PORT:-8089}"
JM="docker compose exec -T flink-jobmanager"

# Prints the id of the live job with this name, if any.
job_id() {
    curl -sf "$FLINK_REST/jobs/overview" | python3 -c "
import json, sys
ids = [j['jid'] for j in json.load(sys.stdin)['jobs']
       if j['name'] == sys.argv[1] and j['state'] in ('RUNNING', 'CREATED', 'RESTARTING', 'INITIALIZING')]
print(ids[0] if ids else '')" "$1"
}

# Prints the newest completed checkpoint of the last job with this name, if any.
latest_checkpoint() {
    $JM sh -c "jid=\$(cat /opt/flink/checkpoints/jobs/$1 2>/dev/null) || exit 0
        ls -d /opt/flink/checkpoints/\$jid/chk-*/_metadata 2>/dev/null | sort -t- -k2 -n | tail -1 | xargs -r dirname"
}

echo "Waiting for the Flink REST API..."
for _ in $(seq 1 60); do
    curl -sf "$FLINK_REST/overview" >/dev/null && break
    sleep 2
done
curl -sf "$FLINK_REST/overview" >/dev/null || { echo "Flink REST API not reachable at $FLINK_REST" >&2; exit 1; }

# A TaskManager registers a few seconds after the JobManager is up.
for _ in $(seq 1 30); do
    [ "$(curl -sf "$FLINK_REST/overview" | python3 -c 'import json,sys; print(json.load(sys.stdin)["taskmanagers"])')" -ge 1 ] && break
    sleep 2
done

submit() {
    local name=$1; shift
    if [ -n "$(job_id "$name")" ]; then
        echo "  $name: already running"
        return
    fi
    local restore
    restore=$(latest_checkpoint "$name")
    if [ -n "$restore" ]; then
        echo "  $name: resuming from $restore"
    else
        echo "  $name: submitting fresh"
    fi
    $JM env RESTORE_PATH="$restore" "$@"
    local jid=""
    for _ in $(seq 1 15); do
        jid=$(job_id "$name")
        [ -n "$jid" ] && break
        sleep 1
    done
    [ -n "$jid" ] || { echo "  $name: submitted, but no live job with that name appeared" >&2; exit 1; }
    $JM sh -c "mkdir -p /opt/flink/checkpoints/jobs && echo $jid > /opt/flink/checkpoints/jobs/$name"
}

echo "Deploying Flink jobs:"
submit postgres-to-fluss   /opt/pipelines/run-cdc-pipeline.sh
submit fluss-gold          /opt/pipelines/run-sql-jobs.sh gold
submit fluss-to-clickhouse /opt/pipelines/run-sql-jobs.sh sink
echo "Done. Flink UI: $FLINK_REST"
