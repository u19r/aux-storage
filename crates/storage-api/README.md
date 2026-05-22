# storage-api

## Purpose

Expose DynamoDB-compatible storage operations over HTTP through a single routed endpoint.

## Audience

Primary readers are engineers changing route contracts and request or response behavior. Secondary readers are operators diagnosing API-specific failures and regressions.

## What Is Non-Standard Here

A single route dispatches many operations based on `x-amz-target` headers and operation-specific payload conversion.

## Architecture and Data Flow

Route handler in `crates/storage-api/src/routes/dynamodb.rs` parses headers and payloads, dispatches to `StorageApiManager`, and returns operation-specific response envelopes.

## Critical Invariants

- Operation dispatch remains deterministic by `x-amz-target`.
- Request parsing and conversion failures return typed error responses.
- OpenAPI path remains stable at base storage route.

## Non-CRUD Workflows

### Operation dispatch

Header target values route requests to create, read, update, delete, query, scan, and table workflows.

### Manager conversion

Manager modules convert operation payloads into storage manager calls and response models.

## Error Semantics and Failure Modes

Errors are normalized in `crates/storage-api/src/errors.rs` and route-level validation helpers.

## Observability and Debugging

Start in `crates/storage-api/src/routes/dynamodb.rs` and operation manager modules in `crates/storage-api/src/manager`.

## Security and Threat Notes

Storage routes are high impact and require strict operation validation and caller context protections.

## Specs and RFC Context

Primary contract is the standalone `POST /storage` DynamoDB-compatible route.
When built with `queue` and/or `pubsub` Cargo features, the same binary also
mounts SQS-compatible `POST /queue` and SNS-compatible `POST /pubsub` routes
against the same configured backend.

## Test Strategy (High Signal)

High-signal coverage includes route operation behavior and manager behavior.

## Known Limits and Technical Debt

Single-route multiplexing requires careful additions when introducing new operations.

## Related Files and Symbols

- `crates/storage-api/src/routes/dynamodb.rs`
- `crates/storage-api/src/manager/storage_api_manager.rs`
- `crates/storage-api/src/manager/mod.rs`
- `crates/storage-api/src/types.rs`
- `crates/storage-api/src/errors.rs`
- `crates/storage-api/src/constants.rs`
