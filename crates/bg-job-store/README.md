# bg-job-store

`bg-job-store` contains the aux-storage adapter for the shared scheduled-job store contract.

It provides:

- `ScheduledJobStore`, a bounded ordered store backed by the provider's scheduled-jobs table.
- Explicit bootstrap and capability reporting for the common scheduler runtime.
- Storage error mapping into the `scheduled-bg-jobs` store contract.

## Scope

This crate should only own persistent background-job coordination state. Immediate job delivery
belongs in the `queue` crate and should use queue messages directly instead of a storage outbox.

## Use

Create and bootstrap the store before handing it to `scheduled-bg-jobs`:

```rust,ignore
let store = manager.scheduled_job_store().ok_or("scheduled jobs disabled")?;
scheduled_bg_jobs::JobStore::get(&store, key).await?;
```
