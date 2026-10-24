# Aux-Storage Roadmap

Last updated: 2026-10-24

Status: developer-preview

## Available capabilities

- DynamoDB data plane: tables, items, conditions, updates, query, scan, batch, transactions, TTL,
  streams, pagination, projection, filters, and GSIs.
- Provider architecture: thin API handlers, typed storage managers, backend-owned schemas and
  transactions.
- Backends: SQLite, Postgres, RocksDB, FoundationDB, and remote provider mode.
- Immediate GSI consistency mode.
- ReadSequence: bounded dependent reads to reduce N+1 request patterns.

## Experimental

- Async replication.
- [Multi-region active-active replication](multi-region-replication.md).
- Multi-region bootstrap and repair.
- Snapshot export.
- Turso backend support.
- SQS-compatible queues.
- SNS-compatible pub/sub.

## Current Development

- Bounded inverted indexes: opt-in set-membership indexes for DynamoDB set attributes.
- VersionAt reads: historical reads at a requested timestamp.
- Tunable limits: knobs to relax selected AWS DynamoDB constraints while preserving bounded work.

## Performance

Throughput and latency depend on the backend, schema, request mix, and deployment.
Compare providers using the same workload and record concurrency, item sizes, durability,
and latency percentiles with each result.

## Planned

- Accurate capacity consumption: consumed capacity remains best effort, not AWS billing-equivalent.
- Multi-region writes: continue qualifying asynchronous replication and recovery on each backend.
- Deprecated request parameters: legacy DynamoDB fields may remain for compatibility, but new work
  uses expressions and typed aux-storage fields.
