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
