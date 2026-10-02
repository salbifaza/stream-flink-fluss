-- Session setup shared by every SQL job (passed to sql-client.sh with -i).
CREATE CATALOG fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'fluss-coordinator:9123'
);

SET 'execution.runtime-mode' = 'streaming';
-- Sources and sinks commit on checkpoints; without an interval, nothing
-- makes progress and nothing fails loudly either.
SET 'execution.checkpointing.interval' = '5s';
SET 'parallelism.default' = '1';
-- Fluss's _commit_timestamp is TIMESTAMP_LTZ; render it as UTC wall-clock
-- time, the same convention as the source's TIMESTAMP columns.
SET 'table.local-time-zone' = 'UTC';
-- Submit the statement set and return, instead of blocking on an unbounded job.
SET 'table.dml-sync' = 'false';
