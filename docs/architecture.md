# Architecture

## Data flow

```
Postgres 16 (wal_level=logical, one slot: flink_cdc_slot)
   │  Flink CDC 3.6 pipeline (postgres -> fluss), job "postgres-to-fluss"
   ▼
Fluss bronze.*  (primary-key tables: current state + changelog)
   │                                  │
   │ Flink SQL, job "fluss-gold"      │
   ▼                                  │
Fluss gold.*  (primary-key tables)    │
   │                                  │
   └──────── $changelog ──────────────┘
                 │  Flink SQL, job "fluss-to-clickhouse"
                 │  JDBC sink + ClickHouse dialect (flink/clickhouse-dialect)
                 ▼
ClickHouse bronze.* and gold.*  (ReplacingMergeTree(_version, is_deleted))
```

Three Flink jobs, each at parallelism 1:

| Job | Reads | Writes | Defined in |
|---|---|---|---|
| `postgres-to-fluss` | Postgres replication slot | Fluss `bronze.*` | `flink/postgres-to-fluss.yaml` |
| `fluss-gold` | Fluss `bronze.*` (as a changelog) | Fluss `gold.*` | `flink/sql/gold_job.sql` |
| `fluss-to-clickhouse` | Fluss `bronze.*$changelog`, `gold.*$changelog` | ClickHouse `bronze.*`, `gold.*` | `flink/sql/clickhouse_sink_job.sql` |

## Why Fluss sits in the middle

The job Fluss does that is hard to replace: Flink emits changelogs with
retractions, ClickHouse wants append-only rows, and a Fluss table's
`$changelog` converts one into the other in plain SQL (next section). Flink
1.20 SQL has no built-in equivalent that I know of. Two side benefits:

- **One replication slot, two consumers.** The gold job and the ClickHouse
  sink both read Fluss, so Postgres serves one reader and there is one slot's
  worth of retained WAL to watch.
- **Replay without touching Postgres.** A consumer that falls behind or is
  rebuilt reads from Fluss, not from the production database.

The alternative without Fluss is a Flink DataStream job that reads Flink
CDC's change events directly, writes each one with its `op` field, and uses
the Postgres LSN as `_version`. The LSN is ordered globally, which would make
[finding 1](#1-log-offsets-restart-when-a-fluss-table-is-recreated)
impossible. The cost is custom Java for the bronze rows and for turning gold
retractions into rows, in exchange for three fewer containers.

## Turning a changelog into ClickHouse rows

ClickHouse has no cheap row-level UPDATE or DELETE, and Flink's JDBC sink
would otherwise want to issue both. The sink never does. Each Fluss table
exposes a `$changelog` virtual table: an append-only stream with one row per
change and three metadata columns.

| Column | Values |
|---|---|
| `_change_type` | `insert`, `update_before`, `update_after`, `delete` |
| `_log_offset` | position in the table bucket's log |
| `_commit_timestamp` | when Fluss committed the change |

The sink job drops `update_before` rows (the `update_after` that follows
carries the new state) and appends everything else to a
`ReplacingMergeTree(_version, is_deleted)` table, with `is_deleted = 1` for
deletes. `SELECT ... FINAL` keeps the highest `_version` per key and drops
keys whose latest change is a delete.

Because every write is an append keyed by a deterministic version, the sink
is idempotent. The JDBC sink is at-least-once, and a replayed change carries
the same version as the original, so ClickHouse collapses it.

### The version column

`_version` is computed by ClickHouse, not sent by Flink:

```sql
_version UInt64 DEFAULT toUInt64(toUnixTimestamp64Milli(_fluss_commit_ts)) * 4194304
                        + _log_offset % 4194304
```

That is the commit time in milliseconds shifted left 22 bits, plus the
offset modulo 2^22. Within a bucket both inputs only grow, and a key always
hashes to the same bucket, so the latest change to a key always has the
highest version.

The first version of this pipeline used the log offset alone, the same idea
as using a Kafka offset as the row version. It failed under test; see
[finding 1](#1-log-offsets-restart-when-a-fluss-table-is-recreated). The
remaining assumption is that the Fluss tablet server's clock never goes
backwards. Two changes to the same key inside one millisecond are still
ordered by offset, unless the offset crosses a multiple of 2^22 between
them.

### The ClickHouse dialect

Flink's JDBC connector discovers database dialects through a `JdbcFactory`
SPI keyed on the URL prefix. It ships dialects for MySQL, Postgres, Oracle,
SQL Server, Db2, Trino, CrateDB and OceanBase, but not ClickHouse.
ClickHouse's own Flink connector (`flink-connector-clickhouse` 0.2.0)
supports only the DataStream API; Table/SQL support is listed as planned.

`flink/clickhouse-dialect` fills that gap in about 100 lines of Java: a
factory for `jdbc:clickhouse:` URLs, a dialect with backtick quoting and the
ClickHouse type ranges, and the stock row converter. It deliberately has no
upsert statement, so declaring a sink table with a primary key fails at
planning time instead of turning into row-by-row updates. With the official
`clickhouse-jdbc` 0.9.9 driver, each Flink flush becomes a single multi-row
`INSERT ... VALUES` (checked in `system.query_log`), not one insert per row.

The sink authenticates as `flink_sink`, which holds `INSERT` on `bronze.*`
and `gold.*` and nothing else. No `SELECT` grant is needed.

## Gold layer

`flink/sql/gold_job.sql` maintains three aggregates in Fluss:

| Table | Grain | Reacts to |
|---|---|---|
| `gold.daily_revenue` | order day (UTC) | new and cancelled orders |
| `gold.category_revenue` | category | line items added or deleted, cancelled orders, products recategorised, categories renamed |
| `gold.customer_ltv` | customer | orders, plus the customer's own email and country |

Reading a Fluss primary-key table gives Flink a full changelog, so the
aggregates and joins retract: a cancelled order leaves its day, an emptied
group is deleted, and a change on the right side of a join updates rows
already emitted. A ClickHouse materialized view would only ever see inserts
and would double-count updates.

Writing gold back into Fluss primary-key tables, rather than straight to
ClickHouse, means the ClickHouse sink handles gold exactly like bronze: one
code path, one versioning rule.

## Findings

Tested with Flink 1.20.5, Flink CDC 3.6.0, Fluss 0.9.1-incubating,
flink-connector-jdbc 3.3.0-1.20, clickhouse-jdbc 0.9.9 and ClickHouse 25.8.

### 1. Log offsets restart when a Fluss table is recreated

**Symptom.** After `docker compose down` and `up`, `make verify` failed: an
order cancelled in Postgres stayed `delivered` in ClickHouse.

**Cause.** ZooKeeper had no volume, so `down` erased Fluss's metadata. The
redeployed CDC pipeline created every bronze table again, and their log
offsets restarted at 0. The new `cancelled` row arrived with offset 31,
while the stale `delivered` row already in ClickHouse had offset 35.
`ReplacingMergeTree` kept the higher version, so old data silently won.

```
 order_id │ status    │ _log_offset │ _synced_at
        1 │ delivered │          35 │ 02:52:31   <- previous Fluss table
        1 │ delivered │          28 │ 02:55:14   <- recreated table, re-snapshot
        1 │ cancelled │          31 │ 02:55:40   <- the real latest change, loses
```

**Fix.** Two layers. ZooKeeper now has `/data` and `/datalog` volumes, so an
ordinary restart never recreates tables. And `_version` is ordered by commit
time first ([above](#the-version-column)), so even a deliberate recreation is
safe. I reproduced the original failure by wiping ZooKeeper, Fluss and the
Flink checkpoints while keeping Postgres and ClickHouse. The offsets
repeated the exact 35 / 28 / 31 pattern, and `make verify` passed.

**Residual limit.** If the Fluss tier is lost, a row deleted in Postgres
during the outage never produces a delete event, so its last version stays
in ClickHouse. A full rebuild should truncate the ClickHouse replicas first.

### 2. The Fluss source runs out of direct memory

**Symptom.** While replaying a few thousand changelog rows, the gold job
restarted three times with `OutOfMemoryError: Direct buffer memory`, thrown
from Fluss's shaded Arrow allocator.

**Cause.** The Fluss client decodes Arrow record batches in direct memory,
and Flink reserves 0 bytes of task off-heap memory by default.

**Fix.** `taskmanager.memory.task.off-heap.size: 256m`.

### 3. A restarted JobManager forgets every job

**Symptom.** Without an HA store, a JobManager restart drops all jobs.
Resubmitting them fresh re-snapshots every Postgres table, replays every
Fluss changelog into ClickHouse, and rebuilds gold state from zero.

**Fix.** Checkpoints are retained on a volume
(`RETAIN_ON_CANCELLATION`), and `scripts/deploy.sh` records each job's id
under its name. On resubmit it finds that job's newest completed checkpoint
and resumes from it: `flink-cdc.sh -s` for the pipeline,
`execution.state-recovery.path` for the SQL jobs. Tested with a full
`docker compose down` and `up`: all three jobs resumed, the number of change
rows in ClickHouse was identical before and after, and `make verify` passed.

### 4. `sql-client.sh -f` reports success when nothing was submitted

It exits 0 when a statement fails, which is a known issue. It also exits 0
for an empty file. A bug in my own submit script truncated the rendered sink
SQL to zero bytes, and the client happily "succeeded" with no job running.
`run-sql-jobs.sh` now fails on `[ERROR]` in the output and also requires a
`Job ID` from every job file.

### Carried over from Flink CDC 3.6

Three issues in Flink CDC 3.6.0's Postgres source with the Fluss sink are
handled in the schema and submit scripts:

- `TIMESTAMPTZ` is rejected by the Fluss sink, and `CAST` in a pipeline
  transform hits a separate `NumberFormatException`. The schema uses plain
  `TIMESTAMP` holding UTC.
- With the default `REPLICA IDENTITY`, the first `UPDATE` or `DELETE`
  crash-loops the pipeline with an NPE in `DebeziumSchemaDataTypeInference`.
  Every table is `REPLICA IDENTITY FULL`.
- Without a checkpoint interval, the source snapshots and then never starts
  streaming. The pipeline is submitted with
  `execution.checkpointing.interval=10s`.

## Failure tests

| Test | How | Result |
|---|---|---|
| TaskManager crash | 2,000-row insert, `docker kill` on the TaskManager 0.3 s in | All 2,000 rows in ClickHouse, 2,000 change rows, 0 duplicates; all jobs restored from checkpoint ~15 s after restart |
| Full stack restart | `docker compose down`, `up`, `make deploy` | All 3 jobs resumed from checkpoints; change-row counts unchanged; verify passed |
| Fluss/Flink tier loss | Wipe ZooKeeper, Fluss and checkpoint volumes; keep Postgres and ClickHouse | Offsets restarted at 0; ClickHouse converged to Postgres; verify passed |
| Bulk delete | Delete the 2,000 rows | Counts matched Postgres |

`restart: unless-stopped` does not restart a container stopped with
`docker kill`: Docker treats it as a deliberate operator action. The tests
restart it with `docker compose start`.
