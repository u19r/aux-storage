# bg-job-store

`bg-job-store` contains internal storage-backed stores for `bg-jobs`.

It provides:

- `SysJobLockStore`, a lease-based job lock store backed by the shared system jobs table.
- Internal table creation and verification during async construction.
- Storage error mapping into `bg-jobs` lock errors.

## Scope

This crate should only own persistent background-job coordination state. Immediate job delivery
belongs in the `queue` crate and should use queue messages directly instead of a storage outbox.

## Use

Create the store through its async constructor so the system table is ready before it is handed to a
`JobManager`:

```rust,ignore
let store = SysJobLockStore::new(storage_provider, worker_id).await?;
let manager = bg_jobs::JobManager::new(Arc::new(store));
```
