# storage

`storage` assembles native and remote providers behind `DatabaseManager`. It owns typed
operation orchestration, table namespace routing, provider initialization, operation metrics,
and the storage maintenance runtime.

## Ownership

- [Database manager](src/database_manager/mod.rs) exposes consumer operations and routes them
  through the shared Dynamo operation boundary.
- [Provider bundle](src/provider_bundle/mod.rs) assembles the configured provider capabilities.
- [Point reads](src/point_read/mod.rs) coordinate bounded reads and their source context.
- [Maintenance runtime](src/maintenance_runtime/mod.rs) runs storage-owned background work.
- [Storage API](../storage-api/README.md) owns HTTP parsing and protocol responses.

SQL and KV physical access belongs to `sql-backend` and `kv-backend`. Shared domain executors
own validation and public errors. One customer operation owns one native transaction per
attempt; a retry uses fresh observations, and an unknown commit outcome remains uncertain.

## Consumer guarantees

Namespace operations resolve table names through `Tables::namespace`. Table bootstrap waits
for ACTIVE status. Shared-table key rewrites fail closed when their required mapping is
unavailable. Manager calls record operation metrics and preserve typed storage errors.

Backend features and provider capabilities determine which operations are available. Keep
feature selection explicit when embedding the crate. See the workspace [README](../../README.md)
for supported backends and protocol extensions.
