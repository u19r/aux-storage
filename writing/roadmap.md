# Aux-Storage Roadmap

Last updated: 2026-06-24

Status: developer-preview

## Stable

- DynamoDB data plane: tables, items, conditions, updates, query, scan, batch, transactions, TTL,
  streams, pagination, projection, filters, and GSIs.
- Provider architecture: thin API handlers, typed storage managers, backend-owned schemas and
  transactions.
- Backends: SQLite, Postgres, RocksDB, FoundationDB, and remote provider mode.
- Performance tuning: p50 and p90 latency similar to AWS DynamoDB. Throughput optimisations.
- Immediate GSI consistency mode.
- ReadSequence: bounded dependent reads to reduce N+1 request patterns.

## Experimental

- Async replication.
- Multi-region active-active replication.
- Multi-region bootstrap and repair.
- Snapshot export.
- Turso backend support.
- SQS-compatible queues.
- SNS-compatible pub/sub.
- Low-latency sync replication for same-region quorum writes.

## Current Development

- Bounded inverted indexes: opt-in set-membership indexes for DynamoDB set attributes.
- VersionAt reads: historical reads at a requested timestamp.
- Tunable limits: knobs to relax selected AWS DynamoDB constraints while preserving bounded work.

## Benchmarks

Rounded benchmark-equivalent capacity, not AWS billing capacity.

| backend      | environment    | rounded result                                                              |
| ------------ | -------------- | --------------------------------------------------------------------------- |
| SQLite       | 16-core M4 Max | ~30k RCU/sec, ~3k WCU/sec; p90 read 2-6 ms, p90 write 5-18 ms.              |
| RocksDB      | 16-core M4 Max | ~30k RCU/sec, ~2k WCU/sec; p90 read 2-6 ms, p90 write usually under 5 ms.   |
| Postgres     | 16-core M4 Max | ~60k RCU/sec, ~30k WCU/sec; p90 read 4-11 ms, p90 write 3-17 ms.            |
| Turso        | 16-core M4 Max | ~6k RCU/sec, ~2k WCU/sec; p90 varies from single-digit ms to larger spikes. |
| FoundationDB | 16-way Linux   | ~44k RCU/sec, ~37k WCU/sec; stable p90 read/write commonly 4-8 ms.          |

## Planned

- Accurate capacity consumption: consumed capacity remains best effort, not AWS billing-equivalent.
- Multi-region writes: use async replication for distant regions; sync
  replication is for low-latency quorum deployments.
- Deprecated request parameters: legacy DynamoDB fields may remain for compatibility, but new work
  uses expressions and typed aux-storage fields.
