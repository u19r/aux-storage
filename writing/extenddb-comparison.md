# ExtendDB comparison with Aux-Storage

Comparison date: 2026-05-22

_First, the dDB at the end of the name is excellent. I still think AWS should have capitalised the first 'd' though._

This is a point-in-time comparison between two young DynamoDB-compatible projects. It is not a claim that one project has "won". ExtendDB is doing useful work, especially around compatibility, auth, and community extensibility. Aux-Storage is taking a different path, with more focus on embedded use, multiple storage engines, local developer workflows, and going beyond DynamoDB APIs.

Both projects are early. Neither should be treated as production-proven.

## ExtendDB

Ready for developer preview and testing. Not ready for production use.

## Goals

- Attempts to provide a close reproduction of DynamoDB, including lesser-used features, SigV4 auth, a mini IAM system, capacity reservation, and throttling.
- Exceptions to compatibility:
  - TLS. DynamoDB is available over regular HTTP in local and test contexts, but ExtendDB explicitly forbids insecure connections.
  - Alternative and lesser-used API options only recently introduced.
  - Certain control-plane operations that should be done at the database backend level.
  - Observability via Prometheus `/metrics` endpoints.
- Architecture is designed to support additional community-developed backends, with a hinted desire for SQLite and Cassandra in addition to first-party Postgres.
- Architecture is designed to support additional authentication structures, with stated goals including Azure support and custom providers.
- Horizontally scalable, but single backend.

## Development status

- Single storage backend: Postgres.
- Single auth backend: embedded IAM.
- Working through API inconsistencies with DynamoDB. The most obvious data-plane differences are quickly being fixed and are minor.
- Throughput performance is not yet optimised. There may be meaningful performance wins available.
- Latency is not yet optimised, especially under load.
- P99 performance needs tuning.
- Good chance of becoming stable and production ready over the next couple months if the current pace continues.

## Interesting notes

- The default GSI consistency delay is only 10ms, which is much shorter than many DynamoDB users will expect.
- Setting GSI consistency to 0 does not turn on immediate consistency. It only minimises the background job delay.
- On-demand mode does not scale from the initial 12,000 RCU / 3,000 WCU limits.
- It is not using existing DynamoDB code or test suites. Conformance testing appears ad hoc at the moment.

## Aux-storage

Ready for developer preview and testing. Not ready for production use.

## Goals

- Attempts to provide a DynamoDB-compatible API, including some legacy features.
- Exceptions to compatibility:
  - PartiQL.
  - LSIs.
  - Capacity reservations.
  - Auth and user-level multi-tenancy. There can be many tables, but no built-in user system.
  - Certain reporting attributes are best effort where exact reporting would hurt performance.
  - Observability via Prometheus `/metrics` endpoint and stdout logging.
- Architecture designed to support SQL and KV-based storage engines.
- Embeddable in Rust applications. Runs as library or binary.
- Horizontally scalable, with multiple backends in HA and multi-region support that mimics global tables, excluding recent strong consistency behavior.
- API extension points for more advanced indexes, simplified streams support, and helpers for easy ETL.
- Additional support for queue and pubsub via SQS and SNS APIs.

## Development status

- Supported backends: SQLite, Turso, Postgres, RocksDB, FoundationDB, and remote pass-through to DynamoDB.
- Experimental support for queues and pubsub.
- Current benchmarks show lower read (50%) and write (50%) latency than ExtendDB in the tested scenarios, with larger differences under load (90-99%). These numbers should be treated as workload-specific, not as a general claim about every deployment. Throughput differences show higher read operations (1.3x) and higher write operations (8x) than ExtendDB. Test for reads are a 90/10 read/write workload with multiple GSIs, 66% of RCU is in GSIs. Test for write are 50/50 and 10/90 read/write workload with multiple GSIs.
- P50 and P90 reads and writes are comparable to DynamoDB in some local benchmark runs. P99 is still unstable and can spike depending on load and backend.
- Extensive conformance test suite.
- Not a community project today. There are no promises or goals for broad DynamoDB API expansion beyond the current scope.
- Experimental support for cached reads.
- No Windows support.

## Interesting notes

- Ability to turn on immediate consistency for GSIs.
- Can run heterogeneous backends in HA with catchup mode. This can be used for zero-downtime backend datastore migrations and server migrations.
- Includes libraries with additional types and tooling to write Rust apps with single-table designs.
- Includes libraries to enable tenancy patterns that isolate customer data to specific tables.
