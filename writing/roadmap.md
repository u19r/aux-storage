# Aux-Storage Roadmap

Last updated: 2026-10-24

## Available capabilities

- DynamoDB data plane: tables, items, conditions, updates, query, scan, batch, transactions, TTL,
  streams, pagination, projection, filters, and GSIs.
- Global Tables.
- Backends: SQLite, Postgres, Turso, RocksDB, FoundationDB, and remote provider mode.
- Immediate GSI consistency mode.
- ReadSequence: bounded dependent reads to reduce N+1 request patterns.

## Experimental

- Snapshot export.

## Current Development

- Bounded inverted indexes: opt-in set-membership indexes for DynamoDB set attributes.
- Tunable limits: knobs to relax selected AWS DynamoDB constraints while preserving bounded work.

## Planned

- Accurate capacity consumption: consumed capacity remains best effort, not AWS billing-equivalent.
