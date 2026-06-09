# aux-storage Configuration

This repository contains the runtime configuration needed to run storage, queue, and stream services.

## Compile-Time Features

Backend features are selected per crate:

| Feature             | Purpose                                                |
| ------------------- | ------------------------------------------------------ |
| `sqlite`            | Embedded SQLite backend.                               |
| `turso`             | Turso/libSQL-compatible backend where supported.       |
| `postgres`          | Postgres-backed service deployments.                   |
| `rocksdb`           | Embedded RocksDB backend.                              |
| `foundationdb`      | FoundationDB-backed distributed deployments.           |
| `remote`            | Provider/client mode for a separately running service. |
| `distributed-cache` | Distributed cache model where supported.               |

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
| `--storage remote`                | Use a remote provider/client backend.                                                             |
| `--db-path <path>`                | Local data path for embedded backends.                                                            |
| `--postgres-dsn <dsn>`            | Postgres connection string.                                                                       |
| `--postgres-max-pool-size <n>`    | Postgres pool size.                                                                               |
| `--enable-internal-helper-routes` | Enable test/helper routes                                                                         |
| `--immediate-consistency-gsis`    | Enable immediate consistency for Global Secondary Indexes (GSIs) instead of eventual consistency. |

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
finite values must be in `1..=61320` (`24 * 365 * 7`), and `-1` means forever.
The physical item stream retention is:

```text
max(table_stream_retention, item_stream_retention)
```

If either side is forever, the effective item retention is forever. This means a
short item TTL can make item cleanup due earlier, but item stream rows still
remain while table stream retention may expose pointers to those rows.

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
  "AuxStreamDurationHours": 168,
  "AuxDefaultItemStreamDurationHours": 72
}
```

Set an item-specific policy on write operations:

```json
{
  "TableName": "Orders",
  "Item": {
    "pk": { "S": "order#123" },
    "sk": { "S": "v0" },
    "status": { "S": "open" }
  },
  "AuxItemStreamTtlHours": 24
}
```

`DeleteItem` accepts the same field alongside `TableName` and `Key`.
`BatchWriteItem` accepts `AuxItemStreamTtlHours` inside `PutRequest` and
`DeleteRequest`. `TransactWriteItems` accepts it inside `Put`, `Update`, and
`Delete` members. Condition-check operations do not accept item stream TTL
because they do not write an item stream version.

Backend support:

| Backend | Custom stream duration support |
| ------- | ------------------------------ |
| SQLite  | Supported with focused trim and write-path tests. |
| RocksDB/KV | Supported with focused trim, write-path, and performance tests. |
| FoundationDB | Gated behind ignored live coverage before production use. |
| Postgres | Supported for SQL deployments; validate trim-job scheduling and backlog monitoring in your runtime. |
| Turso | Supported for SQL deployments; validate trim-job scheduling and backlog monitoring in your runtime. |
| Remote | Mirrors the capability of the remote service it calls. |

Historical and background-work interactions:

- `VersionAt` reads and `ExportTableToPointInTime` are planned historical APIs.
  They must fail closed when required stream history has been trimmed. Until
  those APIs exist, custom stream duration exposes the trim-state fields they
  need but does not add a user-facing historical read/export surface.
- Sync replication, logical backfill, GSI backfill, and future export sessions
  can protect stream boundaries. Custom duration trim must stop before those
  protected boundaries, even when the configured retention horizon is older.
- TTL cleanup and custom stream trim are cooperative background jobs. They may
  lag for hours under high load; operators should prefer bounded progress over
  aggressive synchronous drain behavior.
- Before broad production enablement on very large tables, run a million-scope
  soak profile for the chosen backend and size background capacity from measured
  rows-deleted per pass.

## Sync Replication Configuration

Sync replication is configured under `features.storage_sync_replication`. The
first production-supported release target is SQLite-to-SQLite only; see
`docs/sync-replication-support.md` before enabling it.

Required when enabled:

- `node_id`: stable non-zero node id.
- `advertise_url`: internal peer URL reachable by other sync nodes.
- `sync_internal_token`: internal peer credential supplied from secret
  management.
- `data_dir`: required when the storage backend does not provide the SQLite
  Raft log path.

Learner startup uses `join_as_learner=true`, at least one bootstrap peer in
`peers`, and optionally `learner_join_peer_node_id` when the orchestrator knows
which configured peer should receive the join request.

Internal sync endpoints must not be exposed through public ingress.

## Downstream Repositories

During local development, downstream repositories should depend on aux-storage with path dependencies. Keep feature selection explicit so the downstream runtime does not accidentally pull in unused backends.
