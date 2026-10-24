# aux-storage Observability

`aux-storage` services use structured tracing and Prometheus-style metrics for storage, queue, stream, remote-provider, and maintenance-job behavior.

## Logs

Services initialize `tracing_subscriber` and write logs to stdout by default. Prefer lowercase event names and structured fields for bounded operation names, backend names, and request IDs. Do not log credentials, tokens, raw keys, item data, tenant names, or sensitive payloads.

Use `RUST_LOG` or the service configuration layer to raise verbosity during local debugging.

## Metrics

Metrics are emitted through `metrics-facade` and `metrics-exporter-prometheus`.
Logical metric names use the names below. The Prometheus exporter sanitizes
some names, so dotted metrics such as `storage.operation.total` appear in a
Prometheus scrape as `storage_operation_total`. Manually appended backend
snapshots already use Prometheus-style underscore names.

Metrics are exposed by:

- `storage-api`: `/metrics` when metrics are enabled in config.
- `queue`: `/internal/metrics`.
- FoundationDB and queue backend snapshots are appended to the service metrics
  response when the relevant backend or service is enabled.

The exact exported set depends on service, backend, enabled features, and
whether a metric has been observed since process start.

| Area                                              | Type                                | Metrics                                                                                                                                                                                                                                                                                                                                                                                                                                             | Labels and cardinality                                                                                                                                                                                            |
| ------------------------------------------------- | ----------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Storage operation manager                         | Counter, histogram                  | `storage.operation.total`, `storage.operation.latency.ms`                                                                                                                                                                                                                                                                                                                                                                                           | `operation` is a bounded operation name. Routed writes also add `target`.                                                                                                                                         |
| DynamoDB HTTP API route                           | Counter, histogram                  | `storage.api.dynamodb.requests.total`, `storage.api.dynamodb.request.latency.micros.total`, `storage.api.dynamodb.request.latency.ms`, `storage.api.dynamodb.stage.total`, `storage.api.dynamodb.stage.latency.ms`                                                                                                                                                                                                                                  | `operation`, `status`; stage metrics also add bounded `stage`.                                                                                                                                                    |
| FoundationDB/KV provider stages                   | Counter, histogram                  | `storage.provider.stage.total`, `storage.provider.stage.latency.ms`                                                                                                                                                                                                                                                                                                                                                                                 | Bounded `operation`, `stage`; emitted by provider implementations with explicit stage instrumentation. Postgres transaction phase probes are not exported as metrics.                                           |
| DynamoDB billing approximation                    | Counter                             | `storage.billed.item.ops.total`, `storage.logical.item.bytes.total`                                                                                                                                                                                                                                                                                                                                                                                 | Bounded `ddb_op`, `item_kind`, `direction`.                                                                                                                                                                       |
| Authoritative preimage and guarded-write fallback | Counter                             | `ddb.authoritative.preimage.hit`, `ddb.authoritative.preimage.miss`, `ddb.guard.conflict.fallback`, `ddb.guard.unsupported.fallback`                                                                                                                                                                                                                                                                                                                | Preimage metrics use bounded `purpose`; guard metrics use bounded `operation`.                                                                                                                                    |
| Remote storage provider                           | Counter, histogram                  | `remote.storage.request.latency.ms`, `remote.storage.request.bytes.total`, `remote.storage.response.bytes.total`, `remote.storage.failover.count`                                                                                                                                                                                                                                                                                                   | `operation` is bounded; latency adds `outcome`. Failover adds configured endpoint indices as `from_endpoint` and `to_endpoint`; endpoint URLs are omitted. |
| GSI maintenance                                   | Counter, gauge, histogram           | `gsi.update.empty.batches`, `gsi.update.ops`, `gsi.update.pointer.batches`, `gsi.update.stream.items`, `gsi.update.runtime.ms`, `gsi.update.oldest.pending.age.ms`, `gsi.update.pending`, `gsi.write.delay.ms`, `gsi.write.throttled.total`                                                                                                                                                                                                         | No labels.                                                                                                                                                                                                        |
| Storage backfill jobs                             | Counter, gauge, histogram           | `backfill.enumerate.latency.ms`, `backfill.job.runtime.ms`, `backfill.lock.acquire.latency.ms`, `backfill.jobs.concurrent.count`, `backfill.jobs.throttled.count`, `backfill.job.idle.count`, `backfill.job.expired.lock.found.count`                                                                                                                                                                                                               | Backfill lock/worker labels are bounded by job identity and lock outcome.                                                                                                                                         |
| TTL sweep                                         | Counter, histogram                  | `ttl.sweep.items.deleted`, `ttl.sweep.runtime.ms`, `ttl.sweep.tables.checked`, `ttl.sweep.shards.checked`, `ttl.sweep.retry.batches`, `ttl.sweep.retry.attempts`, `ttl.sweep.retry.failures`, `ttl.sweep.throttled.count`, `ttl.sweep.expired.lock.found.count`                                                                                                                                                                                     | Bounded job/table scope labels are used by the sweep implementations.                                                                                                                                             |
| Stream TTL cleanup                                | Counter, histogram                  | `stream.ttl.cleanup.items.deleted.total`, `stream.ttl.cleanup.runtime.ms`, `stream.ttl.cleanup.runs.total`, `stream.ttl.cleanup.streams.scanned.total`                                                                                                                                                                                                                                                                                              | No labels.                                                                                                                                                                                                        |
| Stream trim                                       | Counter, histogram                  | `stream.trim.runtime.ms`, `stream.trim.pages.scanned`, `stream.trim.items.scanned`, `stream.trim.groups.deleted`, `stream.trim.delete.batches`, `stream.trim.groups.protected.by.replication`, `stream.trim.decode.failures`                                                                                                                                                                                                                        | No labels.                                                                                                                                                                                                        |
| Custom stream duration trim                       | Counter, histogram                  | Shared stream-trim metrics plus worker stats for due markers scanned, stale markers skipped, forever scopes skipped, protected markers, rows deleted, state writes, runtime, and marker page fullness. Manual provider profile counters include `custom_stream_duration_rows_deleted`, `custom_stream_duration_range_deletes`, and `custom_stream_duration_point_deletes`.                                                                                  | Runtime metrics avoid table, item key, tenant, or stream-name labels. Manual profile counters are local test/process counters and are not exported as tenant-specific metrics.                                   |
| Background job manager | Counter, gauge, histogram | `bg.jobs.runs.total`, `bg.jobs.run.errors.total`, `bg.jobs.running.count`, `bg.jobs.due.delay.ms` | Scheduled jobs use bounded `queue`, `kind`, and `outcome` labels; the running-count gauge uses `queue` and `kind` labels. |
| Background queue worker | Counter, histogram | `bg.worker.items.processed.total`, `bg.worker.lease.conflicts.total`, `bg.worker.process.errors.total`, `bg.worker.run.once.ms` | Bounded registered job `queue` and `kind`; counters add bounded outcome labels. |
| Immediate job queue                               | Counter, histogram                  | `jobs.immediate.empty.receives.total`, `jobs.immediate.queue.message.delay.ms`                                                                                                                                                                                                                                                                                                                                                                      | `runner` is bounded, currently `local`.                                                                                                                                                                           |
| Queue storage snapshot                            | Counter, gauge                      | `queue_storage_operations_total`, `queue_storage_gauge`                                                                                                                                                                                                                                                                                                                                                                                             | `path` is a bounded queue maintenance path; either `operation` or `gauge` gives the bounded measurement name.                                                                                                     |
| Outbound HTTP client                              | Counter, histogram                  | `http.request.attempts`, `http.request.errors`, `http.request.retries`, `http.request.latency.ms`                                                                                                                                                                                                                                                                                                                                                   | `method`; attempts and latency also include bounded `outcome` and `status_class`.                                                                                                                                 |
| FoundationDB operations                           | Counter, histogram/counter snapshot | `foundationdb.get.read.version.latency.ms`, `foundationdb_operations_total`, `foundationdb_operation_bytes_total`, `foundationdb_operation_latency_micros_total`, `foundationdb_operation_latency_count_total`                                                                                                                                                                                                                                      | Snapshot labels use bounded `path` plus `operation`, `direction`, or `stage`.                                                                                                                                     |
| RocksDB conditional writes                        | Counter                             | `kv.rocksdb.conditional.put.retry.total`, `kv.rocksdb.conditional.put.failure.total`                                                                                                                                                                                                                                                                                                                                                                | No labels.                                                                                                                                                                                                        |
| Partition-family reconcile                        | Counter, gauge, histogram           | `partition.load.samples.flushed.total`, `partition.reconcile.actions.total`, `partition.reconcile.runs.total`, `partition.reconcile.runtime.ms`, `partition.routing.retries.total`, `partition.family.hot.families`, `partition.family.managed.families`, `partition.family.open.partitions`, `partition.family.pressure`, `partition.family.transition.partitions`                                                                                 | Bounded `family_kind`, `action`, `result`, `operation`, `reason`, and `state`.                                                                                                                                    |
| Multi-region replication                          | Counter, gauge, histogram           | `storage.multi.region.apply.total`, `storage.multi.region.conflict.total`, `storage.multi.region.replication.lag.ms`, `storage.multi.region.heartbeat.rtt.ms`, `storage.multi.region.heartbeat.staleness.ms`, `storage.multi.region.clock.offset.estimate.ms`, `storage.multi.region.clock.offset.abs.ms`, `storage.multi.region.clock.offset.uncertainty.ms`, `storage.multi.region.sender.queue.depth` | `peer_region` comes from config and should be low-cardinality; conflict/apply labels use bounded `outcome` and `reason`. Clock offset gauges are heartbeat-derived estimates, not host time-sync truth.           |
| Metrics facade health                             | Gauge                               | `metrics_facade_handle_cache_entries`                                                                                                                                                                                                                                                                                                                                                                                                               | `kind` is one of `counter`, `gauge`, or `histogram`.                                                                                                                                                              |

When adding a metric, document the name, type, labels, expected cardinality, and
service or backend that emits it. Prefer lowercase dot-separated domains and
explicit unit suffixes such as `.ms`, `.micros.total`, or `.bytes.total`. Avoid
labels that can contain unbounded user data, tenant data, credentials, tokens,
raw key material, or payload contents.

## Custom Stream Duration

Custom stream duration cleanup is deliberately allowed to lag. Under load, TTL
cleanup and stream horizon trim should do bounded work and yield rather than
trying to synchronously drain every due marker. A healthy overloaded service can
therefore have old due markers for hours as long as the backlog is moving when
background capacity is available and protected boundaries explain blocked
markers.

Operator interpretation:

- `marker_page_full` or a consistently full due-marker page means the trim job is
  behind; increase background capacity only after storage write pressure is
  healthy.
- stale marker skips are expected after table duration or item policy changes,
  but a growing stale-only backlog indicates marker cleanup is not completing.
- protected markers mean replication, backfill, or another retained-history
  workflow still needs stream rows. Do not force-delete those rows; fix or
  expire the owning protected boundary.
- rows deleted with low runtime and future-dated next markers means the job is
  making bounded progress.
- range-delete counts should appear on KV backends that support safe range
  deletion; point-delete counts appear for pointer indexes and bounded fallback
  paths.

Runbook checks for a stuck custom-duration backlog:

1. Confirm the background job manager is running and not repeatedly reporting
   lock skips or run errors for the stream trim job.
2. Check whether the due marker page is full and whether rows deleted increases
   across runs.
3. Check protected-boundary metrics and replication/backfill sessions before
   changing retention or deleting stream data.
4. If stale markers dominate, verify that marker completion is succeeding on the
   backend and that the provider supports custom stream duration.
5. If range deletes are unavailable or slow, expect bounded point-delete cleanup
   to lag longer and size background capacity from measured rows-deleted per
   pass.

## Alerting Guidance

Production deployments should alert on:

- elevated storage operation latency by operation;
- elevated error rate by operation and backend;
- remote storage failover events;
- backfill or TTL jobs repeatedly failing or making no progress;
- queue receive/delete/send latency and error rates;
- stream append/read latency and error rates;

For active-active multi-region tables, alert on clock offset only with the
uncertainty metric. The runtime estimates peer clock offset from heartbeat
timestamps using an NTP-style four-timestamp calculation:
`((peer_received_at - local_sent_at) + (peer_sent_at - local_received_at)) / 2`.
This assumes roughly symmetric network latency; asymmetric routing, queueing,
load balancers, and process scheduling can bias the estimate. A useful alarm is
therefore closer to `storage.multi.region.clock.offset.abs.ms > threshold` and
`storage.multi.region.clock.offset.uncertainty.ms < uncertainty_threshold` for
several consecutive samples. Treat this as an application-level early warning
that complements NTP/chrony host monitoring; it is not a replacement for node
time-sync alerts or a proof that last-writer-wins conflict order is causally
correct.

## Dashboard And Alert Contract

Shared platform dashboards should put these signals at the top of the
`aux-storage` page:

- HTTP/storage API latency p50, p90, and p99 by `operation`.
- 5xx/error responses per second by `operation` and backend.
- 4xx responses as a percentage of total requests by `operation`.
- CPU and resident memory pressure from process or Kubernetes container metrics.
- Pod restart count and process health status.
- Queue depth, worker errors, lease conflicts, and background job duration.
- Remote storage failover count, remote request latency, and backend errors.
- FoundationDB operation latency, operation rate, and bytes by operation/stage
  when the FDB backend is active.

Log dashboards should expose quick filters for `level`, `operation`, `backend`,
and `request_id`. Keep customer identifiers and payload values out of logs, spans,
and metric labels. Use request IDs to correlate failures across bounded operation events.

Remote storage metrics omit endpoint URLs. Failover labels use configured endpoint indices,
so credentials, query parameters, and customer hostnames are not exported.
