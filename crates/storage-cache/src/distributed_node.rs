use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    hash::{Hash, Hasher},
    sync::Arc,
};

use hashring::HashRing;
use openraft::{BasicNode, Config, Raft};
use storage_types::PartitionKey;
use tokio::sync::RwLock;
use xxhash_rust::xxh3::Xxh3Builder;

use crate::{
    cluster_metrics,
    cluster_model::{NODE_COUNT, ShardIndex},
    raft_network::{ChannelNetworkFactory, NodeRouter},
    raft_types::{CacheStateMachine, CacheTypeConfig, MemLogStore},
};

type CacheRaft = Raft<CacheTypeConfig>;
pub type CacheHashRing = HashRing<RingToken, Xxh3Builder>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RingToken {
    node_id: u64,
    vnode: usize,
}

impl RingToken {
    const fn new(node_id: u64, vnode: usize) -> Self {
        Self { node_id, vnode }
    }
}

impl Hash for RingToken {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.node_id.hash(state);
        self.vnode.hash(state);
    }
}

/// A distributed cache node that ties together Raft consensus with a
/// consistent hash ring for shard routing.
pub struct DistributedCacheNode {
    /// Raft node identifier.
    pub id: u64,
    /// The underlying Raft handle for this node.
    pub raft: CacheRaft,
    ring: Arc<RwLock<CacheHashRing>>,
    router: NodeRouter,
    vnodes_per_node: usize,
}

impl DistributedCacheNode {
    /// Which node owns an item's partition key according to the hash ring.
    ///
    /// The sort key must not be included here. Every item with the same
    /// DynamoDB partition key is routed to the same cache node, so queries
    /// over a single partition key never span cache nodes.
    pub async fn owner_of_partition_key(&self, pk: &PartitionKey) -> Option<u64> {
        debug_assert!(
            pk.as_str().len() <= 1024,
            "cache partition key routing assumes pk length <= 1024 bytes"
        );

        let ring = self.ring.read().await;
        let pk = pk.as_str();
        ring.get(&pk).map(|token| token.node_id)
    }

    /// Which node owns a given shard index.
    pub async fn shard_owner(&self, shard: ShardIndex) -> Option<u64> {
        let ring = self.ring.read().await;
        let key = format!("shard-{shard}");
        ring.get(&key).map(|token| token.node_id)
    }

    /// Returns the Raft handle for this node.
    pub fn raft_handle(&self) -> &CacheRaft {
        &self.raft
    }

    /// Add a node to the hash ring so it begins receiving key ownership.
    pub async fn add_to_ring(&self, node_id: u64) {
        let mut ring = self.ring.write().await;
        add_node_tokens(&mut ring, node_id, self.vnodes_per_node);
        cluster_metrics::record_cluster_join(node_id);
    }

    /// Remove a node from the hash ring so its key ranges are reassigned.
    pub async fn remove_from_ring(&self, node_id: u64) {
        let mut ring = self.ring.write().await;
        remove_node_tokens(&mut ring, node_id, self.vnodes_per_node);
        cluster_metrics::record_cluster_leave(node_id);
    }

    /// Shut down this node's Raft instance gracefully.
    pub async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.raft.shutdown().await?;
        self.router.write().await.remove(&self.id);
        cluster_metrics::record_cluster_leave(self.id);
        Ok(())
    }

    /// Borrow the shared node router (for passing to `add_node_to_cluster`).
    pub fn router_ref(&self) -> NodeRouter {
        self.router.clone()
    }

    /// Borrow the shared hash ring (for passing to `add_node_to_cluster`).
    pub fn ring_ref(&self) -> Arc<RwLock<CacheHashRing>> {
        self.ring.clone()
    }

    /// Return the Raft config used by this node.
    pub fn raft_config(&self) -> Arc<Config> {
        self.raft.config().clone()
    }
}

/// Configuration for bootstrapping a distributed cache cluster.
pub struct ClusterConfig {
    pub node_count: usize,
    pub vnodes_per_node: usize,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            node_count: NODE_COUNT,
            vnodes_per_node: 64,
        }
    }
}

/// Bootstrap an entire in-process cluster of [`DistributedCacheNode`]s.
///
/// Returns the nodes keyed by their Raft node-id (0..node_count).
pub async fn bootstrap_cluster(
    config: ClusterConfig,
) -> Result<HashMap<u64, DistributedCacheNode>, Box<dyn std::error::Error>> {
    let router: NodeRouter = Arc::new(RwLock::new(HashMap::new()));

    // Build the hash ring with virtual nodes for even distribution.
    let mut ring = HashRing::with_hasher(Xxh3Builder::new());
    for node_id in 0..config.node_count as u64 {
        add_node_tokens(&mut ring, node_id, config.vnodes_per_node);
    }
    let ring = Arc::new(RwLock::new(ring));

    // Openraft configuration tuned for an in-process test cluster.
    let raft_config = Arc::new(
        Config {
            heartbeat_interval: 200,
            election_timeout_min: 500,
            election_timeout_max: 1000,
            ..Config::default()
        }
        .validate()?,
    );

    let mut nodes: HashMap<u64, DistributedCacheNode> = HashMap::new();

    // Create each Raft node.
    for node_id in 0..config.node_count as u64 {
        let log_store = MemLogStore::new();
        let sm = CacheStateMachine::new();
        let network = ChannelNetworkFactory::new(router.clone());

        let raft = Raft::new(node_id, raft_config.clone(), network, log_store, sm).await?;

        // Register in router so other nodes can reach it.
        router.write().await.insert(node_id, raft.clone());

        cluster_metrics::record_cluster_join(node_id);

        nodes.insert(
            node_id,
            DistributedCacheNode {
                id: node_id,
                raft,
                ring: ring.clone(),
                router: router.clone(),
                vnodes_per_node: config.vnodes_per_node,
            },
        );
    }

    // Initialize cluster membership (all nodes as voters).
    let members: BTreeSet<u64> = (0..config.node_count as u64).collect();
    let member_nodes: BTreeMap<u64, BasicNode> = members
        .iter()
        .map(|id| (*id, BasicNode::new(format!("node-{id}"))))
        .collect();

    // Only need to initialize on one node.
    if let Some(node) = nodes.get(&0) {
        node.raft
            .initialize(member_nodes)
            .await
            .map_err(|e| format!("raft init failed: {e}"))?;

        cluster_metrics::set_active_nodes(config.node_count as u64);
        cluster_metrics::set_active_shards(config.node_count as u64);
        cluster_metrics::record_cluster_reconfigure("initial_membership");
    }

    Ok(nodes)
}

/// Add a new Raft node to an existing cluster and update the hash ring on all
/// existing nodes.
///
/// The caller must supply the shared `router`, `ring`, and `raft_config` from
/// the original bootstrap (accessible via the returned node's fields). The new
/// node is registered in the router and added to the ring before
/// [`Raft::change_membership`] is issued on the current leader.
pub async fn add_node_to_cluster(
    node_id: u64,
    leader: &DistributedCacheNode,
    router: &NodeRouter,
    ring: &Arc<RwLock<CacheHashRing>>,
    raft_config: Arc<Config>,
) -> Result<DistributedCacheNode, Box<dyn std::error::Error>> {
    let log_store = MemLogStore::new();
    let sm = CacheStateMachine::new();
    let network = ChannelNetworkFactory::new(router.clone());

    let raft = Raft::new(node_id, raft_config, network, log_store, sm).await?;

    // Register so existing nodes can replicate to it.
    router.write().await.insert(node_id, raft.clone());

    // Add to the shared hash ring.
    let vnodes_per_node = leader.vnodes_per_node;
    {
        let mut ring = ring.write().await;
        add_node_tokens(&mut ring, node_id, vnodes_per_node);
    }

    // Propose membership change through the leader.
    // First add as a learner, then promote to voter.
    leader
        .raft
        .add_learner(node_id, BasicNode::new(format!("node-{node_id}")), true)
        .await?;

    let mut members: BTreeSet<u64> = {
        let metrics = leader.raft.metrics().borrow().clone();
        metrics.membership_config.membership().voter_ids().collect()
    };
    members.insert(node_id);
    leader.raft.change_membership(members, false).await?;

    cluster_metrics::record_cluster_join(node_id);
    cluster_metrics::record_cluster_reconfigure("node_added");

    Ok(DistributedCacheNode {
        id: node_id,
        raft,
        ring: ring.clone(),
        router: router.clone(),
        vnodes_per_node,
    })
}

fn add_node_tokens(ring: &mut CacheHashRing, node_id: u64, vnodes_per_node: usize) {
    for vnode in 0..vnodes_per_node.max(1) {
        ring.add(RingToken::new(node_id, vnode));
    }
}

fn remove_node_tokens(ring: &mut CacheHashRing, node_id: u64, vnodes_per_node: usize) {
    for vnode in 0..vnodes_per_node.max(1) {
        let _ = ring.remove(&RingToken::new(node_id, vnode));
    }
}
