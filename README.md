# aux-storage

`aux-storage` is a Rust workspace for running DynamoDB-compatible, SQS-compatible, and SNS-compatible APIs and libraries on pluggable storage backends.

It can run as standalone services or be embedded by another Rust workspace. The supported local and service backends are SQLite, Turso, Postgres, RocksDB, and FoundationDB.

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

Foreground provider admission is configured under
`features.storage_admission`. It is enabled by default and can be changed in
JSON, with `AUX_STORAGE_ADMISSION_*` environment variables, or with
`--storage-admission-*` flags. The precedence is file, environment, flags,
then explicit `--overrides`; `AUX_STORAGE_INITIAL_SUSTAINABLE_THROUGHPUT_RPS`
remains a supported throughput shorthand. The controller is per connection,
bounded by the configured queue and concurrency limits, and reports retryable
overload as HTTP 503 with `Retry-After`.

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

## Networking Security Boundary

Aux-storage treats configured replication peers and HTTP/SNS subscription endpoints as trusted
customer-controlled network destinations. It permits HTTP and private addresses; it does not
enforce TLS, public-address filtering, or network segmentation. Deploy these paths in a private
AWS VPC, VPN, or equivalent private network, and use security groups, routing, and customer
network policy to restrict access. Network transport security is outside aux-storage's scope.

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

`ReadSequence` is an aux-storage DynamoDB JSON protocol extension for bounded graph-shaped read
workflows. It validates a directed acyclic graph of `Get`, `BatchGet`, and `Query` nodes, binds
typed `FromInput` values, executes independent nodes in bounded waves, and returns flat node and
invocation results through the `DynamoDB_20120810.ReadSequence` target.

Supported consistency is capability-gated by backend. Eventual reads are the default. Strong reads
are allowed for base-table operations and reject GSI reads. Transactional snapshots are currently
enabled only where the provider can prove one backend snapshot covers the whole sequence page; other
backends fail closed before partial execution.

Prefer existing DynamoDB-compatible APIs when they already express the workflow: use `BatchGetItem`
for independent point reads and `TransactGetItems` for small transactional point-read sets. Use
`ReadSequence` when later reads depend on attributes returned by earlier reads and the whole workflow
must stay bounded by fanout, total-read, and response-size limits.

The backend lowering mode is configured outside the request under
`features.read_sequence`:

- `on` (the default) uses a supported whole-plan lowering and falls back to the ordinary DAG;
- `shadow` samples an optimized result but returns the ordinary DAG response;
- `off` is the rollback setting and runs the ordinary DAG only.

Set `shadow_sample_percent` from 0 through 100 to bound shadow work. Changing this setting never
restores the removed ordered executor or accepts the superseded request/token contract.

Example target and request shape:

```http
x-amz-target: DynamoDB_20120810.ReadSequence
```

```json
{
  "ReadConsistency": "EVENTUAL",
  "Nodes": [
    {
      "Name": "user",
      "Operation": {
        "Get": {
          "TableName": "Users",
          "Key": { "pk": { "S": "user#1" } }
        }
      },
      "Inputs": {},
      "After": []
    },
    {
      "Name": "org",
      "Operation": {
        "Get": {
          "TableName": "Organizations",
          "Key": { "pk": { "FromInput": "org_id" } }
        }
      },
      "Inputs": {
        "org_id": {
          "From": { "Node": "user", "Select": "$.Get.Item.org_id" },
          "Cardinality": "ONE",
          "OnMissing": "ERROR"
        }
      },
      "After": []
    }
  ],
  "Outputs": ["user", "org"]
}
```

Operators should treat `NextSequenceToken` as an opaque continuation token. Stale or mismatched
tokens fail validation. Remote `BatchGetItem` partial responses with `UnprocessedKeys` are surfaced
as retryable throttling errors instead of incomplete joined data. Transactional requests on backends
without a provider-owned snapshot context fail before any sequence step executes.

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
