# Storage

## Purpose

`storage` provides the typed persistence orchestration layer used by aux-storage services and library consumers. It wraps backend providers behind `DatabaseManager`, exposes strongly typed operation inputs, and centralizes table naming and bootstrap behavior so callers can perform data operations without backend-specific code.

## Audience

Primary readers are engineers changing storage operation behavior, table bootstrap logic, and provider initialization paths. Secondary readers are operators debugging storage operation failures and latency behavior.

## What Is Non-Standard Here

The crate blends local provider abstractions, optional remote backends, table-namespace-aware naming, and explicit table storage routing (`st`/`loc`) under one API. It also includes optional background maintenance hooks, online migration orchestration (the caller-supplied migration index backfill + dual-write cutover), and operation-level metrics recording around every persistence call.

## Architecture and Data Flow

`DatabaseManager` in `crates/storage/src/database_manager/` receives typed operation inputs and delegates to a `StorageProvider` implementation created by `create_storage_provider`. Table namespace route resolution and shared-table request rewriting are in `crates/storage/src/migration/namespace_routing/`. Online migration orchestration is in `crates/storage/src/migration/namespace_migration.rs`. Table naming and bootstrap logic are in `crates/storage/src/tables.rs`, including system tables and namespace table creation helpers.

## Critical Invariants

- Table namespace data operations must resolve table names through `Tables::namespace(&table_id)`.
- System tables (`m`, `a`, `j`) use fixed names and explicit create helpers.
- Shared-table (`st=1`) routes must fail closed when partition-key rewrite cannot be proven.
- Namespace location resolution (`loc`) must fail closed for unknown dictionary codes.
- Dual-write cutovers must preserve old/new location metadata and deterministic cutover timestamp behavior.
- Operation metrics are emitted for every storage call.
- Table bootstrap must wait for ACTIVE status before use.

## Non-CRUD Workflows

### Table bootstrap and readiness

Table creation helpers create required system or namespace tables and block until ACTIVE status is observed.

### Global Secondary Index Consistency Modes

Local storage backends support two global secondary index write modes:

- The default mode preserves DynamoDB-like eventual consistency by committing the base-table write first and letting the background `gsi-update` worker publish Global Secondary Index changes afterward.
- `immediate_gsi_consistency=true` disables that worker for the backend and commits the base-table write, Global Secondary Index row mutations, and stream entries in the same transaction.

This setting exists for SQLite, Postgres, RocksDB, and FoundationDB. Use the default mode when you need parity with AWS DynamoDB timing. Use immediate mode only when the deployment is intentionally opting out of that lag model.

### Stream and maintenance integration

`DatabaseManager` exposes stream provider access and optional Global Secondary Index maintenance execution to support asynchronous storage workflows.

### ST routing, dictionary lookup, and cutover watchers

Routing resolver caches table namespace placement and location descriptors, rewrites partition-key fields for shared-table namespaces, and applies scheduled cutover events discovered in polling windows.

### Caller-supplied migration index backfill and dual-write migration coordination

Migration coordinator enumerates typed entity partitions through the caller-supplied migration index, backfills destination location, enables dual-write mode, and records cutover event metadata for later completion.

## Error Semantics and Failure Modes

Storage provider errors map to typed `StorageError` variants and are surfaced through manager methods. Table-not-found, conditional-check failures, and validation errors are preserved for caller-level mapping. Unknown provider or backend initialization errors are surfaced during startup.

## Observability and Debugging

Start with `crates/storage/src/database_manager.rs` for operation behavior and metrics wiring. Use `crates/storage/src/tables.rs` to debug naming and bootstrap issues, and inspect provider initialization in `crates/storage/src/builder.rs` when startup fails.

## Security and Threat Notes

Storage operations are infrastructure-critical and can impact multiple table namespaces. Keep namespace scoping explicit, avoid unsafe table-name construction, and validate all operation payloads at route or manager boundaries.

## Specs and RFC Context

Public DynamoDB-compatible wire behavior is exposed by `storage-api` at `POST /storage`, with operation selection based on `x-amz-target` headers.

## Test Strategy (High Signal)

High-value coverage includes manager behavior, table lifecycle behavior, and wire-level operation handling.

## Known Limits and Technical Debt

Behavior compatibility is shaped around currently supported DynamoDB-compatible operations and provider capabilities. Any extension should preserve typed inputs, deterministic error mapping, and table-namespace-safe semantics.

## Related Files and Symbols

- `crates/storage/src/database_manager.rs`
- `crates/storage/src/namespace_routing.rs`
- `crates/storage/src/namespace_migration.rs`
- `crates/storage/src/migration_index_keys.rs`
- `crates/storage/src/tables.rs`
- `crates/storage/src/builder.rs`
- `crates/storage/src/startup.rs`
- `crates/storage-api/src/routes/dynamodb.rs`
- `crates/storage-api/src/manager/storage_api_manager.rs`
