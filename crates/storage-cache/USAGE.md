# storage-cache Usage

## Who This Is For

This crate is for engineers implementing or reviewing the distributed storage cache.

It is the place to answer:

- "is this cache-served read actually safe?"
- "what should happen after schema change or failover?"
- "how do we regression-test new cache rules without waiting on the full runtime?"

## What To Use

Use `CacheState::authoritative_leader_base_state()` as the starting point for deterministic tests.

Use:

- `CacheState::cache_plan(...)`
- `CacheState::eventual_get_decision(...)`
- `CacheState::batch_get_decision(...)`
- `CacheState::query_decision(...)`

for read behavior.

Use `Transition` plus `CacheState::try_apply(...)` for state-machine tests.

Use `ReadRequest`, `ObservedRead`, and `compare_observed_read(...)` when you want to compare a
runtime cache path against the model instead of hand-writing expected outcomes.

## Common Workflow

1. Build or mutate a `CacheState`.
2. Assert `state.is_valid()`.
3. Apply transitions that match the scenario you care about.
4. Compare the resulting read plan against the expected cache or fallback outcome.

## Proof Workflow

Use the default crate tests for the fast proof lane:

```bash
cargo test -p storage-cache
```

Use the ignored deep sweep when you want a heavier read-model proof run:

```bash
cargo test -p storage-cache deeper_read_model_state_exploration_preserves_invariants -- --ignored --nocapture
```

## Current Limits

- query slots are still a tiny finite model
- byte budgets are scaled, not real serialized DynamoDB sizes
- GSI rewrites across query spaces are modeled, but sort-key moves within a single query space are not yet

## Distributed Cache Cluster

The distributed module provides a Raft-backed cluster with consistent hashing.

### Bootstrap a test cluster

```rust
use storage_cache::distributed_node::{ClusterConfig, bootstrap_cluster};

let nodes = bootstrap_cluster(ClusterConfig::default()).await?;
```

### Look up partition-key ownership

```rust
use storage_types::PartitionKey;

let pk = PartitionKey::string("namespace#42");
let owner_id = nodes[&0].owner_of_partition_key(&pk).await;
```

### Add or remove a node at runtime

```rust
use storage_cache::distributed_node::add_node_to_cluster;

// Join
let node = add_node_to_cluster(new_id, leader, &router, &ring, cfg).await?;

// Leave
node.shutdown().await?;
```

### Runtime configuration

Set `features.distributed_cache.enabled = true` in storage configuration. The feature is off by
default. See `DistributedCacheConfig` in the `config` crate for all fields.
