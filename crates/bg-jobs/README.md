# bg-jobs

The crate supports three job modes:

- Timer jobs: use `JobManager` with a `BackgroundJob` implementation for periodic in-process work such as TTL sweeps, GSI maintenance, and repair loops. Timer jobs use `JobLockStore` leases so multiple nodes can register the same job without all running it at once.
- Distributed workers: use `DistributedWorker`, `WorkItemStore`, and `WorkItemProcessor` when work is represented by durable items that need per-item leases, completion, and failure handling.
- Immediate jobs: use `ImmediateJobMessage` plus the queue crate's immediate job runner/client when work should be delivered as queue messages instead of periodic polling.

`AUX_JOBS_MODE=all` enables all registered jobs. `AUX_JOBS_MODE=metrics_only` only allows jobs that do not require full job mode. When the environment variable is unset, the mode defaults to `all`.

## How It Works

Timer jobs run inside a `JobManager`. Before each run, the manager asks its lock store for a lease. Slow jobs acquire a lease once per interval. Fast jobs renew their lease when the interval is shorter than the lock window. Contended locks skip the current run and sleep with jitter.

Distributed workers query due work, try to acquire a per-item lease, process acquired items, and mark each item completed or failed. Work item stores own the backend-specific read and write details; processors only contain business logic.

Immediate jobs should use queues directly. Storage-backed outbox relay patterns do not belong in this crate.
