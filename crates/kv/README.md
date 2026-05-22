# KV

## Purpose

`kv` provides the sorted key-value storage implementations used by higher-level queue, stream, and storage crates. It owns the FoundationDB and RocksDB backends, the `SortedKvDbStorageProvider`, and the partition-family control plane used to spread hot ordered-log and standard-queue workloads across multiple early key prefixes.

## What Is Non-Standard Here

The FoundationDB path is more than a thin key-value adapter. It carries queue and stream semantics, background reconcile logic, runtime load sampling, exact ordered-log split markers, queue drain-and-retire transitions, and cache invalidation around partition-family metadata. The data model has to preserve queue and stream contracts while still making hot prefixes move.

## Critical Invariants

- Ordered-log families preserve one logical `StreamItemId` contract even when the physical writes are partitioned.
- Pointer streams and generic `key_ordered` streams route by deterministic hash key, not random write fan-out.
- Queue families preserve at-least-once delivery, receipt-handle validation, and drain-before-retire semantics.
- Partition transitions are explicit. A partition cannot be both open and draining because lifecycle is modeled as one `PartitionState`.
- Split metadata for ordered logs is written in the same FoundationDB transaction as the parent close and child creation.
- Family epoch changes invalidate provider-local caches and stale queue senders retry against fresh topology.
- Runtime load sampling is flush-on-reconcile, not an extra durable counter write on every hot-path request.

## Module Ownership

- `crates/kv/src/partition_family/`
  - typed partition-family config, lifecycle state, key encoding helpers, split-boundary helpers
- `crates/kv/src/partition_reconcile.rs`
  - PI controller, sample flush, scale-out / split / drain / retire planning, operator metrics
- `crates/kv/src/stream/`
  - ordered-log family bootstrap, routing, pagination, read-merge logic
- `crates/kv/src/queue_provider.rs` and `crates/kv/src/queue/`
  - standard-queue partition routing, claim / delete / visibility workflows, stale-topology retries
- `crates/kv/src/pubsub/`
  - topic, subscription, delivery, and claimable-delivery storage operations
- `crates/kv/src/ttl/`
  - table TTL config, sweep job wiring, and TTL delete scan logic
- `crates/kv/src/storage_ops/`
  - DynamoDB-compatible table, item, query, GSI, stream side-effect, and write helper operations
- `crates/kv/src/backends/fdb/store.rs`
  - exact split transaction, in-transaction routing, FoundationDB-specific runtime load reporting

## Supported Partition-Family Workflows

- Ordered logs:
  - table pointer streams
  - system pointer streams
  - generic streams created with `key_ordered` partitioning
- Standard queues:
  - queue API families stored under `pqueue/`

Non-FoundationDB backends keep the existing single-partition behavior.

## Partition Families

A partition family is an internal control-plane record that maps one logical hot
resource, such as an ordered log or standard queue, onto multiple physical key
prefixes. Callers keep using the stream or queue API; they should not construct
or reason about partition-family keys directly. The `kv` crate resolves the
family, chooses a writable or readable partition, and invalidates local cache
state when topology changes.

The benefit is write and receive-path fan-out. Ordered logs avoid concentrating
all appends under one prefix, and queues can route sends and receives across many
ready ranges while preserving at-least-once delivery and receipt validation. The
cost is extra metadata, local cache invalidation, split/drain reconcile work, and
more complex read merging for partitioned ordered logs. Keep those costs behind
provider and partition-family helpers; API handlers and higher-level crates should
only see logical stream and queue behavior.

## Testing Strategy

The high-signal coverage lives in four layers:

- Business-rule coverage for partition reconcile logic and partition-family models.
- Live FoundationDB provider coverage for backend behavior.
- Expensive ignored churn tests that force repeated split, drain, and retire cycles without loss.
