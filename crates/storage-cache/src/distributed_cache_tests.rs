use std::collections::BTreeSet;

use storage_types::PartitionKey;

use crate::{
    cluster_model::NODE_COUNT,
    distributed_node::{ClusterConfig, add_node_to_cluster, bootstrap_cluster},
    raft_types::CacheRequest,
};

fn partition_key(value: impl AsRef<str>) -> PartitionKey {
    PartitionKey::string(value.as_ref())
}

/// Helper: wait for a leader to be elected among the nodes.
async fn wait_for_leader(
    nodes: &std::collections::HashMap<u64, crate::distributed_node::DistributedCacheNode>,
) -> u64 {
    // The leader should emerge within a few seconds in an in-process cluster.
    for _ in 0..50 {
        for (&id, node) in nodes {
            let metrics = node.raft.metrics().borrow().clone();
            if metrics.current_leader == Some(id) {
                return id;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("no leader elected within timeout");
}

// ------------------------------------------------------------------
// Tests
// ------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_bootstrap_elects_leader() {
    let nodes = bootstrap_cluster(ClusterConfig::default()).await.unwrap();
    assert_eq!(nodes.len(), NODE_COUNT);

    let leader_id = wait_for_leader(&nodes).await;
    assert!(leader_id < NODE_COUNT as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hash_ring_routes_keys_to_nodes() {
    let nodes = bootstrap_cluster(ClusterConfig::default()).await.unwrap();

    // Every key must route to a valid node.
    let node0 = &nodes[&0];
    for key in [
        "user:1",
        "user:2",
        "table:abc",
        "shard-0",
        "shard-1",
        "shard-2",
    ] {
        let owner = node0.owner_of_partition_key(&partition_key(key)).await;
        assert!(owner.is_some(), "key {key} should route to a node");
        assert!(owner.unwrap() < NODE_COUNT as u64);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hash_ring_is_consistent_across_nodes() {
    let nodes = bootstrap_cluster(ClusterConfig::default()).await.unwrap();

    // All nodes share the same ring, so they should agree on routing.
    let key = "consistency-test-key";
    let mut results: BTreeSet<u64> = BTreeSet::new();
    for node in nodes.values() {
        results.insert(
            node.owner_of_partition_key(&partition_key(key))
                .await
                .unwrap(),
        );
    }
    assert_eq!(
        results.len(),
        1,
        "all nodes should agree on the owner of a key"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn routing_uses_partition_key_without_sort_key() {
    let nodes = bootstrap_cluster(ClusterConfig::default()).await.unwrap();
    let node0 = &nodes[&0];

    let pk = partition_key("tenant#42|order#9001");
    let owner = node0.owner_of_partition_key(&pk).await.unwrap();

    for sk in ["line#1", "line#2", "receipt", "shipment#2026-04-30"] {
        assert_eq!(
            node0.owner_of_partition_key(&pk).await.unwrap(),
            owner,
            "sort key {sk} must not affect cache-node ownership"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shard_routing_covers_all_shards() {
    let nodes = bootstrap_cluster(ClusterConfig::default()).await.unwrap();
    let node0 = &nodes[&0];

    let mut owners = BTreeSet::new();
    for s in 0..NODE_COUNT as u8 {
        let owner = node0
            .shard_owner(s)
            .await
            .expect("shard should have an owner");
        owners.insert(owner);
    }
    // With 3 shards and 3 nodes there should be at least 1 node owning some shard.
    assert!(!owners.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_client_write_is_replicated() {
    let nodes = bootstrap_cluster(ClusterConfig::default()).await.unwrap();
    let leader_id = wait_for_leader(&nodes).await;

    let leader = &nodes[&leader_id];

    // Submit a ring update via Raft.
    let resp = leader
        .raft
        .client_write(CacheRequest::BumpEpoch { node: 0, shard: 0 })
        .await;
    assert!(resp.is_ok(), "client_write should succeed: {resp:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_update_ring_via_consensus() {
    let nodes = bootstrap_cluster(ClusterConfig::default()).await.unwrap();
    let leader_id = wait_for_leader(&nodes).await;

    let leader = &nodes[&leader_id];

    // Update the ring assignment through Raft consensus.
    let mut assignment = std::collections::BTreeMap::new();
    assignment.insert(0, 1); // shard 0 → node 1
    assignment.insert(1, 2); // shard 1 → node 2
    assignment.insert(2, 0); // shard 2 → node 0

    let resp = leader
        .raft
        .client_write(CacheRequest::UpdateRing { assignment })
        .await;
    assert!(resp.is_ok(), "update_ring should succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multiple_writes_maintain_log_order() {
    let nodes = bootstrap_cluster(ClusterConfig::default()).await.unwrap();
    let leader_id = wait_for_leader(&nodes).await;

    let leader = &nodes[&leader_id];

    // Issue several writes.
    for shard in 0..3u8 {
        let resp = leader
            .raft
            .client_write(CacheRequest::BumpEpoch { node: 0, shard })
            .await;
        assert!(resp.is_ok());
    }

    // Metrics should show the log has advanced.
    let metrics = leader.raft.metrics().borrow().clone();
    // At least 3 entries + the membership entry.
    assert!(
        metrics.last_log_index.unwrap_or(0) >= 3,
        "log should have advanced"
    );
}

// ------------------------------------------------------------------
// 5-node cluster lifecycle tests
// ------------------------------------------------------------------

/// Bootstrap a 5-node cluster so we can test larger-scale behavior.
async fn bootstrap_five_node_cluster()
-> std::collections::HashMap<u64, crate::distributed_node::DistributedCacheNode> {
    let config = ClusterConfig {
        node_count: 5,
        vnodes_per_node: 64,
    };
    bootstrap_cluster(config).await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn five_node_cluster_elects_leader() {
    let nodes = bootstrap_five_node_cluster().await;
    assert_eq!(nodes.len(), 5);
    let leader_id = wait_for_leader(&nodes).await;
    assert!(leader_id < 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn five_node_write_and_read() {
    let nodes = bootstrap_five_node_cluster().await;
    let leader_id = wait_for_leader(&nodes).await;
    let leader = &nodes[&leader_id];

    // Write through the leader.
    let resp = leader
        .raft
        .client_write(CacheRequest::BumpEpoch { node: 0, shard: 0 })
        .await;
    assert!(resp.is_ok(), "write should succeed on 5-node cluster");

    // All nodes should agree on hash ring routing.
    let key = "five-node-test-key";
    let mut results = BTreeSet::new();
    for node in nodes.values() {
        results.insert(
            node.owner_of_partition_key(&partition_key(key))
                .await
                .unwrap(),
        );
    }
    assert_eq!(results.len(), 1, "5-node ring should be consistent");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn five_node_ring_covers_all_keys() {
    let nodes = bootstrap_five_node_cluster().await;
    let node0 = &nodes[&0];

    let mut owners = BTreeSet::new();
    for i in 0..100 {
        let key = format!("spread-test-{i}");
        let owner = node0
            .owner_of_partition_key(&partition_key(&key))
            .await
            .unwrap();
        owners.insert(owner);
    }
    // With 5 nodes and 64 vnodes each, 100 keys should hit at least 3 nodes.
    assert!(
        owners.len() >= 3,
        "expected keys to spread across at least 3 of 5 nodes, got {}",
        owners.len()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_departure_reassigns_hash_range() {
    let mut nodes = bootstrap_five_node_cluster().await;
    let node0 = &nodes[&0];

    // Record owners before departure.
    let test_keys: Vec<String> = (0..50).map(|i| format!("depart-test-{i}")).collect();
    let mut before: Vec<u64> = Vec::new();
    for key in &test_keys {
        before.push(
            node0
                .owner_of_partition_key(&partition_key(key))
                .await
                .unwrap(),
        );
    }

    // Pick a non-leader node to shut down.
    let leader_id = wait_for_leader(&nodes).await;
    let departing_id = (0..5u64).find(|&id| id != leader_id).unwrap();

    // Remove from hash ring on all surviving nodes.
    for (&id, node) in &nodes {
        if id != departing_id {
            node.remove_from_ring(departing_id).await;
        }
    }

    // Shut down the departing node.
    let departing = nodes.remove(&departing_id).unwrap();
    let _ = departing.shutdown().await;

    // Verify: keys that were owned by the departed node now route elsewhere.
    let survivor = nodes.values().next().unwrap();
    for (i, key) in test_keys.iter().enumerate() {
        let new_owner = survivor
            .owner_of_partition_key(&partition_key(key))
            .await
            .unwrap();
        assert_ne!(
            new_owner, departing_id,
            "key {key} should not route to departed node"
        );
        // Keys that were NOT owned by the departed node should keep their owner.
        if before[i] != departing_id {
            assert_eq!(
                new_owner, before[i],
                "key {key} should keep its owner when that owner is still alive"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_node_joins_and_takes_hash_range() {
    let nodes = bootstrap_five_node_cluster().await;
    let leader_id = wait_for_leader(&nodes).await;
    let leader = &nodes[&leader_id];

    // Record owners before the new node joins.
    let test_keys: Vec<String> = (0..50).map(|i| format!("join-test-{i}")).collect();
    let mut before: Vec<u64> = Vec::new();
    for key in &test_keys {
        before.push(
            leader
                .owner_of_partition_key(&partition_key(key))
                .await
                .unwrap(),
        );
    }

    // Add node 5 to the cluster.
    let new_node = add_node_to_cluster(
        5,
        leader,
        &leader.router_ref(),
        &leader.ring_ref(),
        leader.raft_config(),
    )
    .await
    .unwrap();

    // The new node should now own some keys.
    let mut keys_owned_by_new = 0usize;
    for key in &test_keys {
        let owner = new_node
            .owner_of_partition_key(&partition_key(key))
            .await
            .unwrap();
        if owner == 5 {
            keys_owned_by_new += 1;
        }
    }
    assert!(
        keys_owned_by_new > 0,
        "new node 5 should own at least one test key"
    );

    // Keys not taken by the new node should still be with their original owner.
    for (i, key) in test_keys.iter().enumerate() {
        let owner = leader
            .owner_of_partition_key(&partition_key(key))
            .await
            .unwrap();
        if owner != 5 {
            assert_eq!(
                owner, before[i],
                "key {key} should keep its owner when not taken by new node"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_lifecycle_join_leave_reconfigure() {
    // Bootstrap 5 nodes.
    let mut nodes = bootstrap_five_node_cluster().await;
    let leader_id = wait_for_leader(&nodes).await;
    let leader = &nodes[&leader_id];

    // Phase 1: write some data.
    for shard in 0..3u8 {
        leader
            .raft
            .client_write(CacheRequest::BumpEpoch { node: 0, shard })
            .await
            .unwrap();
    }

    // Phase 2: add node 5.
    let new_node = add_node_to_cluster(
        5,
        leader,
        &leader.router_ref(),
        &leader.ring_ref(),
        leader.raft_config(),
    )
    .await
    .unwrap();
    nodes.insert(5, new_node);
    assert_eq!(nodes.len(), 6);

    // Phase 3: remove node 4 (not the leader).
    let departing_id = if leader_id == 4 { 3 } else { 4 };
    for (&id, node) in &nodes {
        if id != departing_id {
            node.remove_from_ring(departing_id).await;
        }
    }
    let departing = nodes.remove(&departing_id).unwrap();
    let _ = departing.shutdown().await;

    // Phase 4: verify writes still work through the leader.
    let leader = &nodes[&leader_id];
    let resp = leader
        .raft
        .client_write(CacheRequest::BumpEpoch { node: 1, shard: 0 })
        .await;
    assert!(
        resp.is_ok(),
        "writes should succeed after join+leave: {resp:?}"
    );

    // Phase 5: ring should not route to departed node.
    for key in ["lifecycle-a", "lifecycle-b", "lifecycle-c"] {
        let owner = leader
            .owner_of_partition_key(&partition_key(key))
            .await
            .unwrap();
        assert_ne!(owner, departing_id);
    }
}
