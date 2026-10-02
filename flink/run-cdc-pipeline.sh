#!/bin/sh
# Renders postgres-to-fluss.yaml with credentials from the environment and
# submits it with flink-cdc.sh. Runs inside flink-jobmanager (see
# scripts/deploy.sh). With RESTORE_PATH set, resumes from that checkpoint:
# the replication slot position is part of the job state, so the pipeline
# continues streaming instead of snapshotting every table again.
set -e

RENDERED=/tmp/postgres-to-fluss.rendered.yaml
sed \
  -e "s/\${SOURCE_PG_USER}/${SOURCE_PG_USER:-ecommerce}/g" \
  -e "s/\${SOURCE_PG_PASSWORD}/${SOURCE_PG_PASSWORD:-ecommerce}/g" \
  -e "s/\${SOURCE_PG_DB}/${SOURCE_PG_DB:-ecommerce}/g" \
  /opt/pipelines/postgres-to-fluss.yaml > "$RENDERED"

# The checkpoint interval is required, not a tuning knob: Flink CDC's
# incremental snapshot only switches the source from snapshot to streaming
# after a checkpoint completes. With no interval set, the job snapshots the
# existing rows and then silently never reads the replication slot.
set -- "$RENDERED" --flink-home /opt/flink -D execution.checkpointing.interval=10s
[ -n "$RESTORE_PATH" ] && set -- "$@" -s "$RESTORE_PATH"
"${FLINK_CDC_HOME}/bin/flink-cdc.sh" "$@"
