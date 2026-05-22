# storage-cache

## Business Outcomes

Sub-millisecond cache reads survive single-node failures with automatic failover. Shard ownership
is tracked via Raft consensus so no stale reads are served after reconfiguration. Consistent
hashing minimises key redistribution when nodes join or leave.

| Outcome                          | Mechanism                                                                                  | Measured by                               |
| -------------------------------- | ------------------------------------------------------------------------------------------ | ----------------------------------------- |
| Zero-downtime node replacement   | Raft leader election + hash-ring rebalance                                                 | `cache_cluster_reconfigure_total` counter |
| Read availability during failure | Follower takes departed node's hash range                                                  | `cache_cluster_leave_total` counter       |
| Predictable scale-out            | Add node → automatic membership change → partial ring takeover                             | `cache_cluster_join_total` counter        |
| Observability                    | Counters for join/leave/reconfigure/election/migration; gauges for active nodes and shards | Prometheus scrape                         |

## Purpose

`storage-cache` is not the production cache yet. It is the executable Rust oracle that mirrors the
Quint cache rules in `quint/` and gives us fast model-based tests in the normal Rust toolchain.

## Scope

This crate currently covers:

- eventual and strong `GetItem`
- eventual and strong `BatchGetItem`
- base-table and GSI `Query`
- multiple GSI query spaces
- schema-range invalidation and partial revalidation
- promoted-follower read fencing
- write-through manifest preservation rules
- cross-shard transaction prepare, commit, abort, replay, and promoted-follower recovery

For the currently shipped runtime surface, the Quint and Rust oracle coverage is complete for:

- point-read cache serving and fencing
- base-table and GSI forward or reverse query proof, full-page serve, and cached-prefix plus DB-suffix assembly
- GSI proof preservation rules for single, batch, and transactional writes
- cache routing outcomes, schema/version fencing, stale-epoch handling, and wrong-leader refusal
- opaque response-boundary replay via `LastEvaluatedKey` witnesses when the runtime cannot observe an exact 1 MiB byte cutoff directly

Known deliberate gaps remain outside the shipped runtime surface:

- exact 1 MiB serialized page boundaries in the abstract proof, the runtime currently proxies this with response-boundary witnesses
- multi-partition query routing in the production path

## Non-goals

- runtime networking
- storage backend integration
- production cache persistence
- exact 1 MiB serialized page boundaries

## How It Relates To Quint

The Quint specs remain the source of truth for the human proof story. This crate translates the
same state machine into typed Rust so we can:

- run fast property tests with `proptest`
- build a future differential harness against the production cache
- keep implementation work anchored to a small reference model instead of English prose

## Main Modules

- `query.rs`: request shapes and query-space selection
- `plan.rs`: route and cache-read plans
- `model.rs`: pure state, proof logic, and read decisions
- `differential.rs`: comparison helpers for checking runtime behavior against the oracle
- `transaction.rs`: transaction fencing, outcome application, and replay rules
- `transition.rs`: state transitions used by deterministic and generated tests

## Test Strategy

- scenario tests include a named parity lane, so every `run ... =` scenario in the Quint model
  suite has a same-named Rust test
- property tests generate operation sequences and assert invariants after every transition
- property tests also generate valid states directly to stress the read predicates
- exhaustive proof tests now come in two lanes:
  - a default bounded state exploration for the read model
  - a full reachable-state fixpoint exploration for the transaction model
  - an ignored deep read-model sweep for long-running proof runs

## Boundary Audit

Keep these concerns in `storage-cache`:

- pure read-decision predicates
- query proof and pagination rules
- transition systems used for deterministic, property, and exhaustive tests
- differential comparisons between runtime observations and the oracle

Keep these concerns in `storage`:

- storage routing and shared-table rewrites
- DynamoDB request and response shaping
- cache mutation ordering around real backend writes
- payload fetches, suffix query execution, and stream or job side effects

The current runtime still owns more code than ideal in `query_proof_cache.rs`, but the remaining
move candidates are the pure ordered-page planning pieces, not the backend- or routing-aware glue.

Concrete future move candidates from `storage/query_proof_cache.rs` are:

- contiguous-prefix coverage composition from `ParsedQueryRequest::covering_ranges(...)`
- full-page versus mixed-page planning from `plan_query_read(...)`
- ordered manifest materialization from `materialize_query_read(...)`

Pieces already moved into `storage-cache::runtime_query_proof` are:

- pure range-chaining and request-exhaustion checks
- current-schema range filtering before contiguous-prefix composition
- page-boundary witness and materialized page-shape planning
- ordered manifest-key selection from bounds + coverage + direction

The runtime storage cache adapts its `String`-backed metadata through those pure helpers.

Those pieces are still in `storage` because they currently depend on `QueryTableRequest`,
`StoredTableInfo`, and `AttributeValue` shaping. The backend fetch path, storage routing, shared
table token rewriting, and post-commit mutation ordering should stay in `storage`.

## Proof Commands

Run the normal crate suite:

```bash
cargo test -p storage-cache
```

Run the deep read-model proof sweep explicitly:

```bash
cargo test -p storage-cache deeper_read_model_state_exploration_preserves_invariants -- --ignored --nocapture
```

## Distributed Cache Cluster

When the `distributed_cache.enabled` config flag is `true`, the node participates in a Raft
cluster with consistent-hash-based shard routing.

### Key modules

| Module                  | Purpose                                                              |
| ----------------------- | -------------------------------------------------------------------- |
| `cluster_model.rs`      | Node/shard state machine — roles, epochs, ring assignment            |
| `cluster_transition.rs` | 14-variant transition enum applied via `try_apply`                   |
| `cluster_metrics.rs`    | Metric emission for join/leave/reconfigure/election/migration events |
| `distributed_node.rs`   | `DistributedCacheNode` — ties Raft consensus + hash ring             |
| `raft_types.rs`         | openraft type-config, `CacheRequest`/`CacheResponse`, state machine  |
| `raft_network.rs`       | In-process channel network for test clusters                         |

### Bootstrap

```rust
use storage_cache::distributed_node::{ClusterConfig, bootstrap_cluster};
use storage_types::PartitionKey;

let nodes = bootstrap_cluster(ClusterConfig::default()).await?;
let leader = &nodes[&0];
let pk = PartitionKey::string("namespace#42");
let owner = leader.owner_of_partition_key(&pk).await;
```

### Adding a node at runtime

```rust
use storage_cache::distributed_node::add_node_to_cluster;

let new_node = add_node_to_cluster(
    new_id, leader, &router, &ring, raft_config,
).await?;
```

### Configuration

The distributed cache is controlled by `features.distributed_cache` in storage configuration:

```json
{
  "features": {
    "distributed_cache": {
      "enabled": false,
      "node_count": 3,
      "vnodes_per_node": 64
    }
  }
}
```

All fields have serde defaults and the feature is **off** by default.
