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

Queue and pubsub crates can be used directly as libraries or through their service binaries where enabled. The stream crate is a library.

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

Example SQLite config:

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
      "enabled": true
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

Sensitive values include Postgres DSNs and remote credentials. Prometheus
metrics are served from `/metrics` by default; set `features.metrics.enabled`
to `false` to disable the endpoint. Do not print effective config in
production logs unless the output is redacted.

## Networking Security Boundary

The service does not authenticate inbound DynamoDB, queue, pubsub, or metrics requests.
The default bind address is `0.0.0.0:8080`, and `/metrics` is enabled by default.
Restrict inbound access with private ingress, security groups, or an authenticating proxy.
The former Prometheus `bearer_token` configuration is no longer supported; configure
metrics access at the ingress boundary or disable it with `features.metrics.enabled=false`.

Aux-storage treats configured replication peers and HTTP/SNS subscription endpoints as trusted
customer-controlled network destinations. It permits HTTP and private addresses; it does not
enforce TLS, public-address filtering, or network segmentation. Deploy these paths in a private
AWS VPC, VPN, or equivalent private network, and use security groups, routing, and customer
network policy to restrict access. Network transport security is outside aux-storage's scope.

## Backend Support

The workspace is structured around backend features:

- `sqlite`: embedded local database via the vendored `tokio-rusqlite` adapter.
- `turso`: embedded Turso databases where the relevant crate enables them.
- `postgres`: service database backend for SQL-backed deployments.
- `rocksdb`: embedded key-value backend for local and single-node deployments.
- `foundationdb`: distributed ordered key-value backend for partitioned deployments.
- `remote`: client/provider mode for talking to a separately running storage service. Can be used to proxy through to dynamodb, sqs, or sns.

Backend support varies by crate. Check each crate's `Cargo.toml` before enabling a feature in a downstream workspace.

## QuerySourcePage Extension

`QuerySourcePage` is an Aux extension, not a standard DynamoDB API. It queries a table
or index and returns each item together with a separate, decoded storage key. Over HTTP,
use `x-amz-target: DynamoDB_20120810.QuerySourcePage` with the snake_case fields of
[`QueryTableRequest`](crates/storage-types/src/request_response/internal.rs).
It requires an Aux service; AWS DynamoDB does not implement this target.

### Difference from query_table

Both Rust methods use the same query execution path, key conditions, ordering, limits,
consistency rules, and pagination. Their return values differ:

| Method | Each entry in `page.items` | Page metadata |
| --- | --- | --- |
| `query_table` | Item attributes (`WireItem`) | Evaluated item count, evaluated bytes, last evaluated key, continuation token |
| `query_source_page` | `{ key: ItemKey, item: WireItem }` | The same metadata |

`projection_expression` applies to the returned item in both methods. `QuerySourcePage`
does **not** guarantee unprojected items. Omit the expression to keep all attributes available
from the selected table or index. An index query returns the index's stored projection;
it does not automatically fetch the complete base-table item. Continue paging by passing
`page.continuation` as the next request's `exclusive_start_key`.

### What the separate key means

`ItemKey` contains the table name and decoded partition/sort key values. For an index,
it also contains the index name, index key values, and base-table key. It does not expose
raw FoundationDB or RocksDB key bytes, SQL row locations, or Postgres tuple identifiers.

- Postgres, SQLite, and Turso decode the key from dedicated SQL key columns alongside
  the item payload.
- RocksDB and FoundationDB decode base-table keys from the native KV key. For index
  reads, the backend reconstructs the logical key using item attributes and checks the
  native ordered index key, including its base-table key component.

For a complete, valid item, extracting its key attributes is normally enough for ordinary
application reads. Native SQL and KV range readers already check that stored key attributes
agree with the storage key. The separate `key` preserves the storage address through
response projection and remote forwarding, instead of requiring callers to reconstruct it
from the returned attributes. A remote client receives this address from the Aux server;
it does not independently inspect the server's database.

For typed entities, there is another comparison: the entity's fields may imply a different
key even when the stored `pk` and `sk` agree with storage. For example, a record stored at
`pk=roles, sk=role-123` could contain `role_id=role-456`. Entity validation checks that the
decoded entity belongs at the returned address. `QuerySourcePage` supplies the address;
it does not itself validate application entity identity.

### When to use it

- Use `query_table` for ordinary queries that need item attributes.
- Use `query_table_stored_entity<T>` for complete typed entities with automatic stored
  and addressed identity checks. It uses `query_source_page` internally; provide the
  attributes required to decode and validate the entity.
- Use `query_source_page` directly when repository or maintenance code needs each storage
  address separately, including when a projection omits key attributes or when the caller
  needs to validate or reject individual entities itself.
- Use standard DynamoDB `Query` when compatibility with AWS DynamoDB clients and servers
  is required.

The extension does not make index reads fresher or strengthen their consistency guarantees.

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

`AuxStreamDurationHours` on `CreateTable` and `UpdateTable` sets retention for the table stream
and its item history. Omission uses the default 72 hours; `-1` keeps history forever.
Items have no independent history duration. Table cleanup preserves the versions required by
retained DynamoDB stream records. DynamoDB live-item TTL remains independent of history retention.

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
