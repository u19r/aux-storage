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
Concurrent writes are captured during initialization. Regional replication requires the
completed public stream. Adding a GSI while public streams are off starts temporary internal
capture for its base scan and catch-up. The last completed or cancelled internal build stops
that capture automatically. This does not enable a public stream or add another full-table scan.
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

Multi-region replication uses independent regional backends and explicit peer configuration.
The former `features.storage_sync_replication` configuration and SQLite Raft mode have been
removed. See [Multi-region replication](writing/multi-region-replication.md) for the maintained
configuration, acknowledgement semantics, and recovery behavior.

## Downstream Repositories

During local development, downstream repositories should depend on aux-storage with path dependencies. Keep feature selection explicit so the downstream runtime does not accidentally pull in unused backends.
