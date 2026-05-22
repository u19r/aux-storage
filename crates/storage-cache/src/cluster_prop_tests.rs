use std::collections::{BTreeSet, HashMap};

use proptest::prelude::*;
use storage_types::PartitionKey;

use crate::{
    distributed_node::{
        ClusterConfig, DistributedCacheNode, add_node_to_cluster, bootstrap_cluster,
    },
    raft_types::CacheRequest,
};

#[derive(Debug, Clone, Copy)]
enum ClusterOp {
    WriteEpoch { shard: u8 },
    AddNode,
    RemoveNode,
    CheckRouting { key_seed: u16 },
}

fn partition_key(value: impl AsRef<str>) -> PartitionKey {
    PartitionKey::string(value.as_ref())
}

fn cluster_op_strategy() -> impl Strategy<Value = ClusterOp> {
    prop_oneof![
        4 => (0_u8..=2).prop_map(|shard| ClusterOp::WriteEpoch { shard }),
        2 => Just(ClusterOp::AddNode),
        2 => Just(ClusterOp::RemoveNode),
        4 => any::<u16>().prop_map(|key_seed| ClusterOp::CheckRouting { key_seed }),
    ]
}

async fn wait_for_leader(nodes: &HashMap<u64, DistributedCacheNode>) -> u64 {
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

async fn assert_cluster_routing_invariants(
    nodes: &HashMap<u64, DistributedCacheNode>,
    keys: &[String],
) {
    assert!(
        !nodes.is_empty(),
        "cluster should retain at least one live node"
    );

    let live_nodes: BTreeSet<u64> = nodes.keys().copied().collect();
    for key in keys {
        let mut owners = BTreeSet::new();
        for node in nodes.values() {
            let owner = node
                .owner_of_partition_key(&partition_key(key))
                .await
                .expect("key should have an owner");
            owners.insert(owner);
        }
        assert_eq!(
            owners.len(),
            1,
            "all nodes should agree on owner for key {key}"
        );
        let owner = owners.into_iter().next().expect("single owner exists");
        assert!(
            live_nodes.contains(&owner),
            "owner {owner} for key {key} must be a live node"
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 6,
        max_shrink_iters: 0,
        .. ProptestConfig::default()
    })]

    #[test]
    #[ignore]
    fn stateful_cluster_membership_sequences_preserve_routing(
        operations in prop::collection::vec(cluster_op_strategy(), 1..10),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");

        runtime.block_on(async move {
            let mut nodes = bootstrap_cluster(ClusterConfig {
                node_count: 3,
                vnodes_per_node: 64,
            }).await.expect("bootstrap cluster");
            let mut next_node_id = 3_u64;
            let sampled_keys = (0..24)
                .map(|i| format!("cluster-prop-key-{i}"))
                .collect::<Vec<_>>();

            assert_cluster_routing_invariants(&nodes, &sampled_keys).await;

            for operation in operations {
                let leader_id = wait_for_leader(&nodes).await;

                match operation {
                    ClusterOp::WriteEpoch { shard } => {
                        let response = nodes[&leader_id]
                            .raft
                            .client_write(CacheRequest::BumpEpoch { node: 0, shard })
                            .await;
                        assert!(response.is_ok(), "cluster write should succeed: {response:?}");
                    }
                    ClusterOp::AddNode if nodes.len() < 6 => {
                        let leader = &nodes[&leader_id];
                        let new_node = add_node_to_cluster(
                            next_node_id,
                            leader,
                            &leader.router_ref(),
                            &leader.ring_ref(),
                            leader.raft_config(),
                        )
                        .await
                        .expect("add node to cluster");
                        nodes.insert(next_node_id, new_node);
                        next_node_id += 1;
                    }
                    ClusterOp::RemoveNode if nodes.len() > 2 => {
                        if let Some(departing_id) = nodes
                            .keys()
                            .copied()
                            .filter(|id| *id != leader_id)
                            .max()
                        {
                            for (&id, node) in &nodes {
                                if id != departing_id {
                                    node.remove_from_ring(departing_id).await;
                                }
                            }
                            if let Some(departing) = nodes.remove(&departing_id) {
                                let _ = departing.shutdown().await;
                            }
                        }
                    }
                    ClusterOp::CheckRouting { key_seed } => {
                        let dynamic_key = format!("dynamic-routing-key-{key_seed}");
                        assert_cluster_routing_invariants(&nodes, &[dynamic_key]).await;
                    }
                    _ => {}
                }

                assert_cluster_routing_invariants(&nodes, &sampled_keys).await;
            }
        });
    }
}
