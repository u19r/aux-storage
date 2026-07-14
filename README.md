# aux-storage

`aux-storage` is a Rust workspace for running DynamoDB-compatible, SQS-compatible, and SNS-compatible APIs and libraries on pluggable storage backends.

It can run as standalone services or be embedded by another Rust workspace. The supported local and service backends are SQLite, Turso, Postgres, RocksDB, and FoundationDB.

## What Lives Here

- `crates/storage*`: DynamoDB-compatible storage types, providers, API service, cache, and backfill tooling.
- `crates/queue*`: SQS-compatible queue provider and service code.
- `crates/stream*`: SNS-compatible pub/sub and stream primitives used by queue and storage workflows.
- `crates/sql` and `crates/kv`: shared SQL and key-value backend adapters.
- `quint/`: protocol and model checks for storage behavior.

## Quick Start

Build the workspace:

```bash
cargo build --workspace
```

Run the storage API locally with SQLite:

```bash
cargo run -p storage-api --bin storage-api -- --storage sqlite --db-path ./data/storage --port 3000
```

Run the same service from JSON config:

```bash
cargo run -p storage-api --bin storage-api -- --config ./config/storage-api.json
```

The DynamoDB-compatible endpoint is:

```text
POST http://127.0.0.1:3000/storage
```

Queue and stream crates can be used directly as libraries, or through their service binaries where enabled.

## Storage API Configuration

`storage-api` delegates launch configuration to `crates/config`. Precedence is:
defaults, JSON config file, top-level flags, then `--overrides`.

Common top-level flags are:

```bash
--config ./config/storage-api.json
--storage sqlite --db-path ./data/storage --port 3000
--storage postgres --postgres-dsn postgres://localhost/aux_storage --postgres-max-pool-size 16 --postgres-tls true
--enable-internal-helper-routes
```

`--overrides` accepts comma-separated JSON-path assignments and can be repeated.
Values are parsed as JSON when possible and otherwise treated as strings.
Escape commas and equals inside string values with a backslash.

```bash
cargo run -p storage-api --bin storage-api -- \
  --config ./config/storage-api.json \
  --port 3000 \
  --overrides 'http.bind_addr="127.0.0.1:9000",features.runtime.enable_background_workers=false'
```

Example SQLite config using environment variable interpolation:

```json
{
  "http": {
    "bind_addr": "0.0.0.0:3000",
    "routes": {
      "storage": "/storage",
      "queue": "/queue",
      "pubsub": "/pubsub"
    }
  },
  "features": {
    "metrics": {
      "enabled": true,
      "prometheus": {
        "bearer_token": "${STORAGE_API_METRICS_TOKEN}"
      }
    },
    "backends": {
      "sqlite": { "db_path": "./data/storage.sqlite" }
    }
  }
}
```

`features.backends` is the single backend selector for all mounted `storage-api`
surfaces. When `storage-api` is built with the `queue` and/or `pubsub` Cargo
features, those services use the same backend object as `/storage`; there are no
separate `queue.backend.*` or `pubsub.backend.*` selectors.

Example Postgres config using file interpolation:

```json
{
  "features": {
    "backends": {
      "sqlite": null,
      "postgres": {
        "dsn": "file::secrets/postgres-dsn.txt::",
        "max_pool_size": 16,
        "tls": true
      }
    }
  }
}
```

Every JSON string value supports `${ENV}` and `file::path::` interpolation,
including mixed and nested expressions such as `prefix-${ENV}-suffix` and
`${file::env-name.txt::}`. Relative file resolver paths are resolved from the
config file directory. Missing environment variables and unreadable files are
startup errors. Object keys are not interpolated.

Generate the schema with:

```bash
cargo run -p config --bin config -- --write-schema crates/config/config.schema.json
```

Sensitive values include Postgres DSNs, remote credentials, and replication
service tokens. Prometheus metrics are served from `/metrics` by default; set
`features.metrics.enabled` to `false` to disable the endpoint, or set
`features.metrics.prometheus.bearer_token` to require
`Authorization: Bearer <token>`. Do not print effective config in production
logs unless the output is redacted.

## Backend Support

The workspace is structured around backend features:

- `sqlite`: embedded local database via the vendored `tokio-rusqlite` adapter.
- `turso`: remote/libSQL-compatible storage where the relevant crate enables it.
- `postgres`: service database backend for SQL-backed deployments.
- `rocksdb`: embedded key-value backend for local and single-node deployments.
- `foundationdb`: distributed ordered key-value backend for partitioned deployments.
- `remote`: client/provider mode for talking to a separately running storage service. Can be used to proxy through to dynamodb, sqs, or sns.

Backend support varies by crate. Check each crate's `Cargo.toml` before enabling a feature in a downstream workspace.

## ReadSequence Extension

`ReadSequence` is an aux-storage DynamoDB JSON protocol extension for bounded N+1 read workflows.
It runs ordered `Get`, `BatchGet`, and `Query` steps, binds selected parent attributes into child
steps, and returns a nested response with deterministic joins. It is available through the
`DynamoDB_20120810.ReadSequence` target.

Supported consistency is capability-gated by backend. Eventual reads are the default. Strong reads
are allowed for base-table operations and reject GSI reads. Transactional snapshots are currently
enabled only where the provider can prove one backend snapshot covers the whole sequence page; other
backends fail closed before partial execution.

Prefer existing DynamoDB-compatible APIs when they already express the workflow: use `BatchGetItem`
for independent point reads and `TransactGetItems` for small transactional point-read sets. Use
`ReadSequence` when later reads depend on attributes returned by earlier reads and the whole workflow
must stay bounded by fanout, total-read, and response-size limits.

Embedded Rust consumers can use `DatabaseManager::read_sequence_executor` for the same dependent
read boundary without an HTTP hop or JSON request/response conversion. Its wire-native `get_item`
and `query_table` methods require mutable access to the executor, reuse one provider context, reject
cross-connection sequences, and enforce hard operation, item, and response-byte caps. The executor's
stats distinguish started operations from completed operations so cancellation and provider failures
remain observable.

Example target and request shape:

```http
x-amz-target: DynamoDB_20120810.ReadSequence
```

```json
{
  "ReadConsistency": "EVENTUAL",
  "Sequence": [
    {
      "Name": "user",
      "Get": {
        "TableName": "Users",
        "Key": { "pk": { "S": "user#1" } }
      },
      "Select": {
        "org_id": "$.org_id"
      }
    },
    {
      "Name": "org",
      "ForEach": {
        "From": "user.Item.org_id",
        "As": "org_id",
        "OnMissing": "ERROR",
        "Get": {
          "TableName": "Organizations",
          "Key": { "pk": { "S": "${org_id}" } }
        },
        "Join": { "To": "user", "As": "org", "Type": "REQUIRED_ONE" }
      }
    }
  ]
}
```

Operators should treat `NextSequenceToken` as an opaque continuation token. Stale or mismatched
tokens fail validation. Remote `BatchGetItem` partial responses with `UnprocessedKeys` are surfaced
as retryable throttling errors instead of incomplete joined data. Transactional requests on backends
without a provider-owned snapshot context fail before any sequence step executes.

## Sync Replication

Sync replication is the low-latency quorum-replicated storage mode for `storage-api`. The first
production milestone is intentionally narrow: SQLite-to-SQLite sync replication only. Mixed-backend
promotion and other homogeneous backend pairs require the evidence listed in the support matrix
before production use.

Public sync replication docs:

- [support matrix and release warnings](docs/sync-replication-support.md)
- [operator runbooks](docs/sync-replication-operations.md)
- [production readiness checklist](docs/production-readiness-checklist.md)
- [performance budgets](docs/sync-replication-performance.md)
- [throughput investigation](docs/sync-replication-throughput.md)
- [contributor guide](docs/sync-replication-contributors.md)
- [testing guide](docs/sync-replication-testing.md)
- [architecture diagrams](docs/sync-replication-architecture.md)
- [observability guide](docs/sync-replication-observability.md)
- [closeout audit](docs/sync-replication-closeout-audit.md)
- [release candidate evidence packet](docs/sync-replication-release-candidate.md)

### Sync Node Join

Existing sync voters do not need a process restart to admit a new node. A new node joins by starting
with `features.storage_sync_replication.join_as_learner=true`, a fresh `node_id`, an internal
`advertise_url`, persistent storage and sync Raft `data_dir`, the sync internal credential, and at
least one bootstrap peer in `features.storage_sync_replication.peers`. On startup, the learner calls
the bootstrap peer's token-protected `POST /storage/_internal/sync/raft/learners` endpoint. The
leader adds it as a non-voting learner, replicates/catches it up, and the node is promoted only
after the promotion safety gates pass.

The current implementation is not gossip-based discovery: existing nodes do not notice arbitrary
processes automatically, and sync runtime config is assembled at startup. To add capacity or replace
a node, start the new process in learner mode and use the
[learner replacement runbook](docs/sync-replication-operations.md#learner-replacement). Direct
`storage::DatabaseManager` library calls bypass Raft; embedded services must host the `storage-api`
routes and internal sync routes for sync replication.

## DynamoDB Stream Retention Extensions

`aux-storage` supports non-DynamoDB extension fields for retained stream history on backends that
implement custom stream duration:

- `AuxStreamDurationHours` on `CreateTable` and `UpdateTable` sets table stream retention.
- `AuxDefaultItemStreamDurationHours` on `CreateTable` and `UpdateTable` sets the table default for
  item stream retention.
- `AuxItemStreamTtlHours` on `PutItem`, `UpdateItem`, `DeleteItem`, `BatchWriteItem` put/delete
  requests, and `TransactWriteItems` put/update/delete members sets item-specific retention
  metadata.

Omitting these fields keeps the default 72-hour retention. Finite values are hours up to 61,320
(`24 * 365 * 7`); `-1` means forever. Item stream rows are physically retained for at least the
table stream retention, so an item TTL shorter than the table duration does not delete item stream
rows while table stream pointers may still reference them.

See [CONFIGURATION.md](CONFIGURATION.md#custom-stream-duration-extension) for request examples and
[OBSERVABILITY.md](OBSERVABILITY.md#custom-stream-duration) for trim backlog guidance.

## Embedding

Downstream repositories should depend on these crates through path dependencies during local development:

```toml
storage = { path = "../aux-storage/crates/storage" }
storage-api = { path = "../aux-storage/crates/storage-api" }
queue = { path = "../aux-storage/crates/queue" }
stream = { path = "../aux-storage/crates/stream" }
```

The same dependencies can later move to git revisions without changing the crate boundaries.
