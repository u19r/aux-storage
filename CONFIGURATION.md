# aux-storage Configuration

This repository contains the runtime configuration needed to run storage, queue, and pubsub services.

## Inbound access

The service endpoints, including `/metrics`, do not enforce application-layer authentication.
The default listener binds to `0.0.0.0:8080`, and metrics are enabled by default.
Use private ingress, security groups, or an authenticating proxy to restrict inbound access.
The Prometheus `bearer_token` option has been removed. Disable metrics with
`features.metrics.enabled=false` when an ingress policy cannot protect that endpoint.

## Compile-Time Features

Backend features are selected per crate:

| Feature             | Purpose                                                |
| ------------------- | ------------------------------------------------------ |
| `sqlite`            | Embedded SQLite backend.                               |
| `turso`             | Embedded Turso backend where supported.       |
| `postgres`          | Postgres-backed service deployments.                   |
| `rocksdb`           | Embedded RocksDB backend.                              |
| `foundationdb`      | FoundationDB-backed distributed deployments.           |
| `remote`            | Provider/client mode for a separately running service. |

Check the target crate's `Cargo.toml` for the exact feature names and transitive enables.

## Runtime Service

The standalone storage API can be configured through a JSON file, environment variables, and command-line arguments. When configuration sources overlap, command-line arguments take precedence over environment variables, and environment variables take precedence over values from `config.json`.

The standalone storage API can be started directly:

```bash
cargo run -p storage-api -- --config config.json --storage sqlite --db-path ./data/storage --port 3000
```

Common options and flags:

| Option / Flag                     | Purpose                                                                                           |
| --------------------------------- | ------------------------------------------------------------------------------------------------- |
| `--config <path>`                 | Path to a JSON configuration file.                                                                |
| `--port <port>`                   | HTTP listen port.                                                                                 |
| `--storage sqlite`                | Use SQLite.                                                                                       |
| `--storage turso`                 | Use Turso.                                                                                        |
| `--storage rocksdb`               | Use RocksDB.                                                                                      |
| `--storage postgres`              | Use Postgres.                                                                                     |
| `--storage foundationdb`          | Use FoundationDB when compiled with the feature.                                                  |
| `--db-path <path>`                | Local data path for embedded backends.                                                            |
| `--postgres-dsn <dsn>`            | Postgres connection string.                                                                       |
| `--postgres-max-pool-size <n>`    | Postgres pool size.                                                                               |
| `--storage-admission-enabled <bool>` | Enable or disable foreground storage admission.                                                |
| `--storage-admission-<parameter>` | Override a `features.storage_admission` sizing or queue parameter.                              |
| `--enable-internal-helper-routes` | Enable test/helper routes                                                                         |

### Foreground storage admission

`features.storage_admission` controls the optional per-connection adaptive
admission controller. It is enabled by default and starts at the configured
Little's Law estimate (`throughput_rps * latency_ms / 1000`), bounded by the
foreground minimum and maximum. A small control reserve remains available for
health and recovery work. Set `enabled` to `false` to retain only the bounded
maximum-minus-reserve gate; the queue and wait deadline remain bounded.

```json
{
  "features": {
    "storage_admission": {
      "enabled": true,
      "initial_sustainable_throughput_rps": 20000,
      "initial_latency_estimate_ms": 5,
      "minimum_concurrency": 4,
      "maximum_concurrency": 1024,
      "control_reserve_concurrency": 4,
      "queue_capacity": 256,
      "max_queue_wait_ms": 25
    }
  }
}
```

The standalone binary also accepts `--storage-admission-*` flags. Environment
overrides use `AUX_STORAGE_ADMISSION_*` names (for example,
`AUX_STORAGE_ADMISSION_ENABLED=false`); the original throughput shorthand
`AUX_STORAGE_INITIAL_SUSTAINABLE_THROUGHPUT_RPS` is supported as well. If both
throughput names are set, the canonical `AUX_STORAGE_ADMISSION_*` name wins. The
precedence order is file, environment, flags, then explicit `--overrides`.

Each connection gets an independent controller. Queue saturation or an
expired queue wait is returned as HTTP 503 with a bounded `Retry-After` hint;
control permits are reserved so recovery and health work can still proceed.

Service route defaults are configured at `http.routes.storage`,
`http.routes.queue`, and `http.routes.pubsub`, with defaults `/storage`,
`/queue`, and `/pubsub`. `features.backends` is the single backend selector for
all mounted `storage-api` surfaces; queue and pubsub no longer have separate
backend selector objects.

The SQS and SNS code is always compiled into `storage-api`. Two switches turn the
services on; both default to `false`:

| Key              | Effect when `true`                                                                                                             |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `queue.enabled`  | Serves SQS on `http.routes.queue` and builds the queue manager (receive waiters and wake listener).                           |
| `pubsub.enabled` | Serves SNS on `http.routes.pubsub` and starts the delivery runner (`pubsub.delivery.*`). Local SQS deliveries need `queue.enabled`. |

With a switch off, a request to that route gets the "route is not enabled" response (404,
`InvalidAction`) and none of the service runs. A switch that is on, with a backend that has no
provider for the service (the remote backend has none), stops startup with an error that names the
switch. With both switches off the remote backend starts. Set them in the config file or with
`--overrides queue.enabled=true --overrides pubsub.enabled=true`.

### FoundationDB backend

| Key (`features.backends.foundationdb.`) | Default | Effect |
| --------------------------------------- | ------- | ------ |
| `cache_read_version_ms`                 | `0`     | Age of a cached read version for ordinary writes; `0` reads a fresh version. |
| `cluster_file`                          | none    | Cluster file path; absent uses the client default. |
| `tenant_name`                           | none    | Tenant to open inside the database. |
| `subspace_prefix`                       | none    | Key prefix that identifies the storage namespace. |
| `transaction_byte_limit`                | none    | Enforced on every native attempt (at least 1); absent keeps the database default. |
| `reply_byte_limit`                      | `1048576` | Most bytes of one native range reply (client knob `reply_byte_limit`, `80000` to `4194304`). Set once per process before the network starts; a second backend in the process with another value keeps the first and logs a warning. |

### Other backend keys

| Key | Default | Effect |
| --- | ------- | ------ |
| `features.backends.sqlite.db_path` | `/var/lib/aux-storage/db/sqlite/main.db` | SQLite database file. SQLite is the default backend. |
| `features.backends.turso.db_path` | `main.turso.db` | Turso database file. |
| `features.backends.postgres.{dsn,max_pool_size,tls}` | `max_pool_size` 20, `tls` false | Postgres connection string, pool size and TLS switch. |
| `features.backends.rocksdb.db_path` | `/var/lib/aux-storage/db/rocksdb` | RocksDB data directory. |
| `features.backends.rocksdb.writer_queue_capacity` | `256` | Capacity of the single writer queue. |
| `features.backends.initial_stream_lineages` | none | Number of initial stream roots (1 to 128) for a provider scope that does not exist yet. Existing scopes keep their topology. |
| `features.storage_deadlines.*_ms` | `point_read_ms` 5000, `range_read_ms` 10000, `write_ms` 10000, `transaction_ms` 15000, `schema_change_ms` 30000, `background_page_ms` 10000, `long_poll_probe_ms` 1000, `commit_grace_ms` 2000 | Deadline budget of a native attempt for each deadline class, and the wait after a deadline once a commit has begun (`commit_grace_ms`). The defaults are `DEFAULT_ATTEMPT_BUDGETS_MS` and `DEFAULT_COMMIT_GRACE_MS` in `crates/storage-types/src/attempt_deadline.rs`. Environment names are `AUX_STORAGE_DEADLINES_<KEY>_MS`, for example `AUX_STORAGE_DEADLINES_WRITE_MS`. See [OBSERVABILITY.md](OBSERVABILITY.md) for the classes. |
| `jobs.gsi_backfill_interval_ms` | `30000` | Interval of the GSI backfill job. |
| `features.runtime.enable_background_workers` | `true` | Starts background workers. |
| `features.runtime.shutdown_grace_seconds` | `5` | Seconds the HTTP server drains after a shutdown signal. |
| `features.runtime.worker_threads` | available parallelism | Tokio worker threads. |
| `features.runtime.tokio_console` | `false` | Starts the tokio console layer; needs the `tokio-console` build feature. |
| `queue.region`, `queue.partition`, `queue.account_id` | `us-east-1`, `aws`, `000000000000` | Region, partition and account in SQS and SNS ARNs. Set the region before the first topic is created. |
| `queue.public_base_url`, `pubsub.public_base_url` | none | Public base URL of the route; `pubsub.public_base_url` fills `SubscribeURL` and `UnsubscribeURL`. |
| `pubsub.delivery.*` | `idle_probe_millis` 1000, `lease_seconds` 30, `max_in_flight` 64, `per_destination` 8, `request_timeout_seconds` 10 | Delivery runner of each node. `lease_seconds` is at least `2 * request_timeout_seconds + 5` and at most 900. |
| `features.backends.remote.endpoint_urls` | `[]` | Endpoints of the remote service. |
| `features.backends.remote.region` | `us-east-1` | Region of the remote service. |
| `features.backends.remote.tls` | `true` | Use TLS. |
| `features.backends.remote.credentials.static.{access_key,secret_key,session_token}` | none | Static credentials. Without `static`, the remote client uses the default AWS credential chain. |
| `features.backends.remote.timeout_overrides.{connect_timeout_ms,request_timeout_ms}` | connect: a quarter of `request_timeout_ms`, at most 2000 (2000 without it); request: none | Override the remote client timeouts. When both are set, `connect_timeout_ms` must be less than `request_timeout_ms` or the config is rejected at load: only a connect that fails first is known not sent; a write cut off later answers an unknown outcome. Idle pooled connections close after 20 s. |
| `features.storage_connections.{connections,default_connection}` | none | Named storage connections; `default_connection` must name an entry in `connections`, and both must be non-empty. |
| `features.tracing.log_level` | `warn` | Log level. |
| `http.cors.allow_origins` | `[]` | Origins allowed by CORS. |

`crates/config/config.schema.json` lists every key. Regenerate it with `just config-schema`.

## Capacity and billing

Consumed capacity is intentionally unsupported. A request that asks for it (`ReturnConsumedCapacity`)
gets a response with zero values. `BillingMode` and `ProvisionedThroughput` are accepted and ignored:
nothing is stored, echoed or enforced. Foreground load is limited by storage admission (above), not by
table throughput settings.

## Custom Stream Duration Extension

Custom stream duration is an aux-storage DynamoDB API extension. It is request
metadata, not stored item data, and standard DynamoDB-compatible clients are
unchanged when the extension fields are omitted.

Duration values are hours. Omitted values use the service default of 72 hours,
finite values must be in `24..=61320` (`24 * 365 * 7`), and `-1` means forever.
Table stream retention also controls item history. Items have no independent history duration.
Table cleanup preserves the snapshots and predecessors needed by retained DynamoDB stream records.
DynamoDB live-item TTL can delete a current item while its historical snapshots remain readable.

Create or update table-level retention with aux fields:

```json
{
  "TableName": "Orders",
  "AttributeDefinitions": [
    { "AttributeName": "pk", "AttributeType": "S" },
    { "AttributeName": "sk", "AttributeType": "S" }
  ],
  "KeySchema": [
    { "AttributeName": "pk", "KeyType": "HASH" },
    { "AttributeName": "sk", "KeyType": "RANGE" }
  ],
  "BillingMode": "PAY_PER_REQUEST",
  "StreamSpecification": {
    "StreamEnabled": true,
    "StreamViewType": "NEW_AND_OLD_IMAGES"
  },
  "AuxStreamDurationHours": 168
}
```

Backend support:

| Backend | Custom stream duration support |
| ------- | ------------------------------ |
| SQLite  | Supported with focused trim and write-path tests. |
| RocksDB/KV | Supported with focused trim, write-path, and performance tests. |
| FoundationDB | Gated behind ignored live coverage before production use. |
| Postgres | Supported for SQL deployments; validate trim-job scheduling and backlog monitoring in your runtime. |
| Turso | Supported for SQL deployments; validate trim-job scheduling and backlog monitoring in your runtime. |
| Remote | Mirrors the capability of the remote service it calls. |

Enabling a stream on an existing table captures its current items in bounded background
pages. `DescribeTable` reports `UPDATING` and `StreamEnabled: false` until capture completes.
Concurrent writes are captured during initialization. Adding a GSI scans existing items in
bounded transactions while writes maintain the new index. The GSI becomes queryable when
its build completes. Public stream settings remain independent of GSI builds.
Explicit public streams remain enabled. Active indexes receive atomic foreground updates
without requiring history, and TTL alone does not enable history.

Historical and background-work interactions:

- `VersionAt` reads and `ExportTableToPointInTime` are planned historical APIs.
  They must fail closed when required stream history has been trimmed. Until
  those APIs exist, custom stream duration exposes the trim-state fields they
  need but does not add a user-facing historical read/export surface.
- Consumers must catch up within the configured history window. Consumer checkpoints
  do not extend the fixed 24-hour system stream. Table history remains available for
  the table's configured duration.
- TTL cleanup and custom stream trim are cooperative background jobs. They may
  lag for hours under high load; operators should prefer bounded progress over
  aggressive synchronous drain behavior.
- Before broad production enablement on very large tables, run a million-scope
  soak profile for the chosen backend and size background capacity from measured
  rows-deleted per pass.

## Multi-region replication

Regional replication pairs one table between exactly two regions. `features.regional_connections`
maps a native connection id to a JSON file with `local_region`, `listen`, `regions` (two entries
`{name, rank, address}`; rank is the tie-break and race rank, unique, above zero), and optional
`request_timeout_ms` (30000), `frame_limit_bytes` (16 MiB), `page_byte_limit` (1 MiB),
`page_entry_limit` (1000), `max_concurrent_requests` (16), `send_interval_ms` (250), and
`lag_alert_ms` (300000). See [Multi-region replication](writing/multi-region-replication.md).

Ending a replica: `UpdateTable` with `ReplicaUpdates [{Delete: {RegionName: <peer>}}]` is graceful.
The peer stops accepting writes and drains to this region, then both tables are independent. Delete
of the local region's name forces the end: the pair ends here at once, the peer is told best
effort, and changes not yet delivered are dropped.

## Downstream Repositories

During local development, downstream repositories should depend on aux-storage with path dependencies. Keep feature selection explicit so the downstream runtime does not accidentally pull in unused backends.
