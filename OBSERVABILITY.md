# aux-storage Observability

`aux-storage` services use structured tracing and Prometheus-style metrics for storage, queue, stream, remote-provider, and maintenance-job behavior.

## Logs

Services initialize `tracing_subscriber` and write logs to stdout by default. Prefer lowercase event names and structured fields for bounded operation names, backend names, and request IDs. Do not log credentials, tokens, raw keys, item data, tenant names, or sensitive payloads.

Use `RUST_LOG` or the service configuration layer to raise verbosity during local debugging.

## Metrics

Metrics are emitted through `metrics-facade` and `metrics-exporter-prometheus`.
Logical metric names use the names below. The Prometheus exporter sanitizes
some names, so dotted metrics such as `storage.operation.total` appear in a
Prometheus scrape as `storage_operation_total`.

`storage-api` serves `/metrics` when metrics are enabled in config. The queue and pubsub
services run inside `storage-api`; there is no separate queue metrics endpoint.

The exact exported set depends on service, backend, enabled features, and
whether a metric has been observed since process start.

Queue and pubsub metrics, the pubsub delivery runner, the queue receive waiters and the
queue and pubsub background work exist only when `queue.enabled` or `pubsub.enabled` is `true`
(both default to `false`). With a switch off, the service exports no series.

| Area                                              | Type                                | Metrics                                                                                                                                                                                                                                                                                                                                                                                                                                             | Labels and cardinality                                                                                                                                                                                            |
| ------------------------------------------------- | ----------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Storage operation manager                         | Counter, histogram                  | `storage.operation.total`, `storage.operation.latency.ms`                                                                                                                                                                                                                                                                                                                                                                                           | `operation` is a bounded operation name. Routed writes also add `target`.                                                                                                                                         |
| DynamoDB HTTP API route                           | Counter, histogram                  | `storage.api.dynamodb.requests.total`, `storage.api.dynamodb.request.latency.micros.total`, `storage.api.dynamodb.request.latency.ms`, `storage.api.dynamodb.stage.total`, `storage.api.dynamodb.stage.latency.ms`                                                                                                                                                                                                                                  | `operation`, `status`; stage metrics also add bounded `stage`.                                                                                                                                                    |
| FoundationDB/KV provider stages                   | Counter, histogram                  | `storage.provider.stage.total`, `storage.provider.stage.latency.ms`                                                                                                                                                                                                                                                                                                                                                                                 | Bounded `operation`, `stage`; emitted by provider implementations with explicit stage instrumentation. Postgres transaction phase probes are not exported as metrics.                                           |
| DynamoDB billing approximation                    | Counter                             | `storage.billed.item.ops.total`, `storage.logical.item.bytes.total`                                                                                                                                                                                                                                                                                                                                                                                 | Bounded `ddb_op`, `item_kind`, `direction`.                                                                                                                                                                       |
| Remote storage provider                           | Counter, histogram                  | `remote.storage.request.latency.ms`, `remote.storage.request.bytes.total`, `remote.storage.response.bytes.total`, `remote.storage.failover.count`                                                                                                                                                                                                                                                                                                   | `operation` is bounded; latency adds `outcome`. Failover adds configured endpoint indices as `from_endpoint` and `to_endpoint`; endpoint URLs are omitted. |
| Immediate job queue                               | Counter, histogram                  | `jobs.immediate.empty.receives.total`, `jobs.immediate.queue.message.delay.ms`                                                                                                                                                                                                                                                                                                                                                                      | `runner` is bounded, currently `local`.                                                                                                                                                                           |
| Regional replication lag | Gauge | `storage.regional.lag_ms` | `connection`: the largest age in ms of the oldest unsent history entry of a pair's last page, over the pairs whose turns ran in this process within the lag window (forgotten 3 retry caps plus one lease after their last turn here, at once when the pair ends); 0 when idle. The table is in the `regional.lag.alert` log line. |
| Native attempt deadlines | Counter | `storage.native.attempt.deadline.total` | `provider` in `sqlite`, `turso`, `postgres`, `foundationdb`, `rocksdb`; `class` (7 deadline classes); `phase` (6 phases). 210 series at most. |
| Serialized native writer (SQLite, RocksDB) | Gauge, histogram, counter | `storage.native.writer.queue.depth`, `storage.native.writer.queue.wait.ms`, `storage.native.writer.busy.micros.total`, `storage.native.writer.native.micros.total`, `storage.native.writer.foreign_wait.total` | `provider` in `sqlite`, `rocksdb`. |
| Postgres stream frontier | Gauge | `storage.postgres.stream.frontier.lag.xids`, `storage.postgres.stream.frontier.oldest_xact.age.ms` | No labels. See "Postgres stream publication". |
| Metrics facade health                             | Gauge                               | `metrics_facade_handle_cache_entries`                                                                                                                                                                                                                                                                                                                                                                                                               | `kind` is one of `counter`, `gauge`, or `histogram`.                                                                                                                                                              |

Consumed capacity is intentionally unsupported. A request that asks for it gets zero values, and
billing mode and provisioned throughput are accepted and ignored. `storage.billed.item.ops.total`
and `storage.logical.item.bytes.total` count logical items and bytes for operators. They are not
capacity units.

When adding a metric, document the name, type, labels, expected cardinality, and
service or backend that emits it. Prefer lowercase dot-separated domains and
explicit unit suffixes such as `.ms`, `.micros.total`, or `.bytes.total`. Avoid
labels that can contain unbounded user data, tenant data, credentials, tokens,
raw key material, or payload contents.

## Custom Stream Duration

Custom stream duration cleanup is deliberately allowed to lag. Under load, TTL
cleanup and stream horizon trim should do bounded work and yield rather than
trying to synchronously drain every due marker. A healthy overloaded service can
therefore have old due markers for hours as long as the backlog is moving when
background capacity is available and protected boundaries explain blocked
markers.

Operator interpretation:

- `marker_page_full` or a consistently full due-marker page means the trim job is
  behind; increase background capacity only after storage write pressure is
  healthy.
- stale marker skips are expected after table duration or item policy changes,
  but a growing stale-only backlog indicates marker cleanup is not completing.
- protected markers mean replication, backfill, or another retained-history
  workflow still needs stream rows. Do not force-delete those rows; fix or
  expire the owning protected boundary.
- rows deleted with low runtime and future-dated next markers means the job is
  making bounded progress.
- range-delete counts should appear on KV backends that support safe range
  deletion; point-delete counts appear for pointer indexes and bounded fallback
  paths.

Runbook checks for a stuck custom-duration backlog:

1. Confirm the background job manager is running and not repeatedly reporting
   lock skips or run errors for the stream trim job.
2. Check whether the due marker page is full and whether rows deleted increases
   across runs.
3. Check protected-boundary metrics and replication/backfill sessions before
   changing retention or deleting stream data.
4. If stale markers dominate, verify that marker completion is succeeding on the
   backend and that the provider supports custom stream duration.
5. If range deletes are unavailable or slow, expect bounded point-delete cleanup
   to lag longer and size background capacity from measured rows-deleted per
   pass.

## Table stream capacity

Each stream lineage appends to one ordered tail, and FoundationDB also appends its feed references to a lineage tail. More API servers do not distribute one partition key across lineages: all writes to that key keep the same route. A busy lineage can therefore limit write rate while others remain idle. There is no fixed operations-per-second quota for a lineage; capacity depends on the provider and record sizes. No measured rate is claimed here.

New PostgreSQL and FoundationDB scopes start with four roots by default; other native providers start with one. The configured initial count applies only when a scope is created. Existing scopes need an explicit operator split; there is no automatic splitting. A split distributes the parent's keys across its children and preserves other routes. A single partition key still has one route.

## Alerting Guidance

Production deployments should alert on:

- elevated storage operation latency by operation;
- elevated error rate by operation and backend;
- `regional.lag.alert` log events or `storage.regional.lag_ms` above the configured `lag_alert_ms`: the peer stays attached and history is retained until delivery, so watch stream storage;
- remote storage failover events;
- backfill or TTL jobs repeatedly failing or making no progress;
- queue receive/delete/send latency and error rates;
- stream append/read latency and error rates;
- any increase of `pubsub.delivery.poison` or `pubsub.publish.poison` within
  15 minutes (page): a stored SNS delivery or publish intent does not decode
  on this node; the row is moved one hour ahead with its bytes unchanged and
  is processed when a node decodes it. A delivery row that still cannot be
  delivered after its retry deadline (or whose payload is missing) is deleted
  and counted in the provider drop count with reason `undeliverable_row`;
- `pubsub.delivery.oldest_due_age_ms` (the maximum over nodes) for
  destinations with a closed breaker, and `pubsub.delivery.breaker_open`;

## Dashboard And Alert Contract

Shared platform dashboards should put these signals at the top of the
`aux-storage` page:

- HTTP/storage API latency p50, p90, and p99 by `operation`.
- 5xx/error responses per second by `operation` and backend.
- 4xx responses as a percentage of total requests by `operation`.
- CPU and resident memory pressure from process or Kubernetes container metrics.
- Pod restart count and process health status.
- Native writer queue depth and wait (`storage.native.writer.*`).
- Remote storage failover count, remote request latency, and backend errors.
- Provider stage latency (`storage.provider.stage.*`) by operation and stage.

Log dashboards should expose quick filters for `level`, `operation`, `backend`,
and `request_id`. Keep customer identifiers and payload values out of logs, spans,
and metric labels. Use request IDs to correlate failures across bounded operation events.

Remote storage metrics omit endpoint URLs. Failover labels use configured endpoint indices,
so credentials, query parameters, and customer hostnames are not exported.

## Postgres stream publication

Postgres emits two gauges with no labels (one series each per process):

| Name | Observation |
| --- | --- |
| `storage.postgres.stream.frontier.lag.xids` | Transaction-ID distance between snapshot xmax and xmin, from the existing valid system history read. This is neither a row count nor elapsed time. |
| `storage.postgres.stream.frontier.oldest_xact.age.ms` | Maximum age of visible assigned transactions, excluding the observing connection, across databases. Negative clock ages become zero. |

The age query runs once per due-marker discovery attempt, including empty pages,
before the trim worker's marker and table loops. Retries can resample. It adds one
statement and one aggregate row to the existing read batch, with no extra
transaction, pool checkout, or query wave. A zero marker limit skips discovery.
Query or decode errors follow the existing discovery and job error path.

Normal database roles can have other roles' activity fields hidden. The age gauge
therefore measures the oldest **visible** assigned transaction; zero does not prove
there is no cluster-wide blocker. The alert below is cluster-wide only when the
connection role can see the required `pg_stat_activity` fields. The history gauge
updates when history is read; it is
not a periodic cluster monitor.

Alert rule for a held frontier (any assigned transaction, a prepared transaction, or a
transaction in another database holds the cluster-wide snapshot `xmin`):

```
max(storage_postgres_stream_frontier_oldest_xact_age_ms) > 60000   for 2m   # warning
max(storage_postgres_stream_frontier_oldest_xact_age_ms) > 300000            # page
```

60 s is twice the longest own budget (`schema_change`, 30 s), so the service alone cannot cause it.
Prepared transactions are not in `pg_stat_activity`, so the age gauge does not see them. If the
alert fires with age 0 and lag keeps growing, query `pg_prepared_xacts`.
To find the holder, run as a role with `pg_read_all_stats`:
`SELECT pid, datname, xact_start, state FROM pg_stat_activity WHERE backend_xid IS NOT NULL ORDER BY xact_start LIMIT 5`.

## Native attempt deadlines

Every native attempt has one deadline. The domain picks a deadline class. The backend owns the budgets.
A deadline that fires before commit is a known abort. The client gets HTTP 503 with `Retry-After: 1`
and the shared retry helper does not retry it. A deadline that fires after commit started waits up to
`commit_grace_ms` more, then reports an unknown outcome.

`storage.native.attempt.deadline.total` counts attempts that settled with a deadline abort.
It has labels `provider`, `class` and `phase`. The phase is where the attempt stood:
`acquire`, `queued`, `begin`, `statement`, `rollback` or `settle`. No table, key or payload is a label.
A rising `acquire` or `queued` count means capacity is short. A rising `statement` count means the store is slow.
A rising `settle` count means commits do not finish in `commit_grace_ms`.

| Class | Used by | Default | Config key (`features.storage_deadlines.`) | Environment |
| --- | --- | ---: | --- | --- |
| point read | GetItem, BatchGetItem, DescribeTable, metadata and attribute reads | 5000 ms | `point_read_ms` | `AUX_STORAGE_DEADLINES_POINT_READ_MS` |
| range read | Query or Scan page, GetRecords page, list operations | 10000 ms | `range_read_ms` | `AUX_STORAGE_DEADLINES_RANGE_READ_MS` |
| write | PutItem, UpdateItem, DeleteItem, SendMessage, receive claim, DeleteMessage, Publish | 10000 ms | `write_ms` | `AUX_STORAGE_DEADLINES_WRITE_MS` |
| transaction | TransactWriteItems, TransactGetItems, BatchWriteItem, batch queue actions | 15000 ms | `transaction_ms` | `AUX_STORAGE_DEADLINES_TRANSACTION_MS` |
| schema change | create, update, delete of tables, indexes, queues, topics | 30000 ms | `schema_change_ms` | `AUX_STORAGE_DEADLINES_SCHEMA_CHANGE_MS` |
| background page | backfill, TTL sweep, stream trim, reclaim, job store | 10000 ms | `background_page_ms` | `AUX_STORAGE_DEADLINES_BACKGROUND_PAGE_MS` |
| long poll probe | read-only receive probe | 1000 ms | `long_poll_probe_ms` | `AUX_STORAGE_DEADLINES_LONG_POLL_PROBE_MS` |
| commit grace | extra wait once a commit started | 2000 ms | `commit_grace_ms` | `AUX_STORAGE_DEADLINES_COMMIT_GRACE_MS` |

Every value must be above 0, and `long_poll_probe_ms` must not exceed `point_read_ms`.
Raise a budget only after you know why attempts are slow. The bound of one request on a stopped
database is the admission wait, plus the class budget, plus `commit_grace_ms`.
On FoundationDB a transaction cannot read after about 5 s, whatever the budget.

Other client bounds. The Postgres client derives its keepalive idle time and interval from `commit_grace_ms` (3 probes), and its connect and unacknowledged-data bound from the connection creation budget:

| Key | Default | Meaning |
| --- | ---: | --- |
| `backends.rocksdb.writer_queue_capacity` | 256 | queued write attempts before `WriterQueueFull` |

### Serialized writer metrics

SQLite and RocksDB run all writes on one writer. `rate(storage_native_writer_busy_micros_total[1m]) / 1e6`
is occupancy from 0 to 1. `busy` minus `native` is time the writer spent outside native calls.
`storage.native.writer.queue.depth` and `storage.native.writer.queue.wait.ms` show how long writes wait for the writer.
`storage.native.writer.foreign_wait.total` counts a write operation that awaited something other than its own native
request while it held the writer (a timer, a lock, a channel). It should stay at 0. Any increase is a bug.

### Postgres role settings (runbook)

The client timer is the authority. Server settings are backstops, longer than every client wait,
so the server never decides before the client. The service sends `statement_timeout`,
`idle_in_transaction_session_timeout`, `transaction_timeout` and `client_min_messages` in the `options` startup parameter.
After each connect it reads the four values back. It refuses the connection if a value differs from what it sent,
if the server is older than PostgreSQL 17, or if the server sent a warning during the check.
The role settings below are a backstop for other clients of the role, not for the service.
A transaction-mode pooler is not supported: it rejects `options`, and queue wake needs `LISTEN`.

```
ALTER ROLE aux_storage SET idle_in_transaction_session_timeout = '40s';
ALTER ROLE aux_storage SET statement_timeout = '40s';
ALTER ROLE aux_storage SET transaction_timeout = '60s';          -- PostgreSQL 17 and later
ALTER ROLE aux_storage SET client_connection_check_interval = '2s';
ALTER ROLE aux_storage SET tcp_keepalives_idle = 5;
ALTER ROLE aux_storage SET tcp_keepalives_interval = 2;
ALTER ROLE aux_storage SET tcp_keepalives_count = 3;
ALTER ROLE aux_storage SET tcp_user_timeout = '10s';
```

Do not set `lock_timeout` on the role. The client deadline ends a lock wait with a cancel request.
Schema changes set their own `lock_timeout` per statement.

| Setting | Why |
| --- | --- |
| `idle_in_transaction_session_timeout` | Frees locks and the stream frontier of a stopped or partitioned node. It is always a known abort. Set it for every role and database on the cluster, because a transaction in any database holds the frontier. It must be longer than the largest attempt deadline plus `commit_grace_ms`. With default budgets that is 30 s + 2 s, so 40 s is safe. Raise it if you raise a budget. |
| `statement_timeout` | Stops a runaway statement after the longest client wait (30 s schema change + 2 s grace). It stays above that, because a cancel during the synchronous-replication wait of `COMMIT` returns success to the client. |
| `transaction_timeout` | Backstop for any runaway transaction. A firing in reply to `COMMIT` is an unknown outcome. |
| `client_connection_check_interval` | The server notices a dropped client during a long statement. |
| `tcp_keepalives_*` and `tcp_user_timeout` | The server notices a dead client and frees its locks. The client derives its own bounds from `commit_grace_ms`. |
| `max_prepared_transactions = 0` (`postgresql.conf`, needs restart) | A prepared transaction holds the stream frontier, and no session timeout ends it. The service never prepares transactions. |

Also keep migrations and manual sessions short on a cluster that serves streams.
Avoid a long `UPDATE` or a `CREATE INDEX` without `CONCURRENTLY` inside one transaction.

Each attempt sets `statement_timeout` and `transaction_timeout` with `SET LOCAL` in its BEGIN batch.
The values are the time left in the attempt deadline, and the transaction value adds `commit_grace_ms`.
The service also sets session values at connect: the largest class budget plus `commit_grace_ms` plus 1 ms (32001 ms by default).
The service refuses a connection that does not have these session values.

### Postgres lock capacity and lock waits

The service takes transaction-level advisory locks. Postgres keeps them in the shared lock table until the transaction ends.
A transaction takes one lock for each distinct item key it writes or checks.
It takes one shared lock for each distinct table it touches, and one lock for the request token.
`TransactWriteItems` allows 100 members. The worst case is 100 item locks, 100 table locks and 1 token lock, which is 201 locks.
Schema changes take the table lock exclusive.
Postgres does not limit one transaction to `max_locks_per_transaction`. It sizes the shared lock table as `max_locks_per_transaction` times (`max_connections` plus `max_prepared_transactions`).
When many large transactions run together the table can fill, and Postgres fails a statement with "out of shared memory".
Check that `max_locks_per_transaction` times `max_connections` is larger than the number of locks that all concurrent large transactions hold.
The default value of 64 is enough for ordinary traffic. Raise it (restart needed) if you run many concurrent 100-member transactions.

When writers queue on a lock, `pg_stat_activity` shows `wait_event_type = 'Lock'`.
A wait for an advisory lock (item, table or token lock) has `wait_event = 'advisory'`.
A wait for a row lock has `wait_event = 'transactionid'`, or `'tuple'` when several sessions wait for the same row.
A session that is open but idle between statements has `state = 'idle in transaction'` and `wait_event = 'ClientRead'`.
The client deadline ends a lock wait with a cancel request.
It counts in `storage.native.attempt.deadline.total` with phase `statement`, so a rising `statement` count can mean lock queues and not a slow store.
No separate lock-wait metric exists.

### Queue wake

A waiting receive learns of a new message from a wake hint. A hint only starts a new check. It carries no message data.

On Postgres, each node uses `LISTEN` and `NOTIFY` on the channel `aux_queue_wake`.
Each node holds two extra connections outside the pool: one listener and one notifier.
The connection string must reach Postgres directly, or through a session-mode pooler.
A transaction-mode pooler such as PgBouncer drops `LISTEN`, and the service does not detect this.
In that case no hint arrives, and every waiting queue checks once every 10 s. Receives still work, but they wake late.
The log event `queue.wake.listen.failed` or `queue.wake.notify.failed` means the connection failed. The service reconnects with a backoff of 100 ms to 5 s.

On FoundationDB, each node watches up to 1,024 slot keys, one for each wake slot.
This is far below the FoundationDB default of 10,000 watches for each database handle.
Every second the node reads all slot keys in one read-only transaction. This bounds the delay of a lost watch to about 1 s.
The log event `queue.wake.arm.failed` means arming failed. Slots then fall back to the normal check interval until the next pass.

### Single-table restore

For restore of one table on RocksDB or FoundationDB, see `docs/runbooks/single-table-restore.md`.
For export of a stabilized FoundationDB table into a dedicated empty namespace for inspection, see `docs/table-recovery.md`.
That flow uses `just storage-recovery inspect-source`, `export` and `restore`. `restore` validates the archive and the empty namespace and writes nothing unless `--execute` and `--result` are given.

### FoundationDB cached read version

The cached read version for writes is off by default (`backends.foundationdb.cache_read_version_ms = 0`).
Enable it by setting that key to a value above 0 after a canary on the target cluster.
Until then, leave it at 0.
