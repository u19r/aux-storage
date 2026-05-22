use std::collections::{BTreeMap, BTreeSet};

/// Node identifier in the cluster (0, 1, 2).
pub type NodeIndex = u8;
/// Shard identifier (0, 1, 2).
pub type ShardIndex = u8;
/// Raft term.
pub type Term = u8;
/// Epoch for cache authority.
pub type ClusterEpoch = u8;

pub const NODE_COUNT: usize = 3;
pub const NODES: [NodeIndex; 3] = [0, 1, 2];
pub const SHARDS: [ShardIndex; 3] = [0, 1, 2];
pub const QUORUM: usize = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum ClusterRole {
    Leader,
    Follower,
    Candidate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum ShardRole {
    Leader,
    Follower,
    None,
}

/// Per-shard state on a single node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardLocal {
    pub role: ShardRole,
    pub epoch: ClusterEpoch,
    pub item_authority: bool,
    pub query_authority: bool,
    pub serving: bool,
    pub migrating: bool,
}

impl ShardLocal {
    pub fn empty() -> Self {
        Self {
            role: ShardRole::None,
            epoch: 0,
            item_authority: false,
            query_authority: false,
            serving: false,
            migrating: false,
        }
    }

    pub fn leader(epoch: ClusterEpoch) -> Self {
        Self {
            role: ShardRole::Leader,
            epoch,
            item_authority: true,
            query_authority: true,
            serving: true,
            migrating: false,
        }
    }

    pub fn follower(epoch: ClusterEpoch) -> Self {
        Self {
            role: ShardRole::Follower,
            epoch,
            item_authority: false,
            query_authority: false,
            serving: false,
            migrating: false,
        }
    }
}

/// Per-node state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeState {
    pub current_term: Term,
    pub voted_for: Option<NodeIndex>,
    pub role: ClusterRole,
    pub votes_received: BTreeSet<NodeIndex>,
    pub shard_state: BTreeMap<ShardIndex, ShardLocal>,
    pub reachable: BTreeSet<NodeIndex>,
    pub append_acks: BTreeMap<ShardIndex, BTreeSet<NodeIndex>>,
}

/// Hash ring assignment state.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RingState {
    pub assignment: BTreeMap<ShardIndex, NodeIndex>,
    pub migration_source: BTreeMap<ShardIndex, Option<NodeIndex>>,
    pub migration_target: BTreeMap<ShardIndex, Option<NodeIndex>>,
}

impl RingState {
    pub fn default_ring() -> Self {
        Self {
            assignment: BTreeMap::from([(0, 0), (1, 1), (2, 2)]),
            migration_source: BTreeMap::from([(0, None), (1, None), (2, None)]),
            migration_target: BTreeMap::from([(0, None), (1, None), (2, None)]),
        }
    }

    pub fn primary_of(&self, shard: ShardIndex) -> NodeIndex {
        self.assignment[&shard]
    }

    pub fn is_migrating(&self, shard: ShardIndex) -> bool {
        self.migration_source[&shard].is_some()
    }
}

/// Message in the network channel.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Message {
    pub kind: MessageKind,
    pub from: NodeIndex,
    pub to: NodeIndex,
    pub term: Term,
    pub shard: ShardIndex,
    pub granted: bool,
    pub epoch: ClusterEpoch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum MessageKind {
    VoteRequest,
    VoteResponse,
    Append,
    AppendAck,
    ShardTransfer,
}

/// The full cluster state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterState {
    pub nodes: BTreeMap<NodeIndex, NodeState>,
    pub ring: RingState,
    pub network: BTreeSet<Message>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadRoute {
    Ok,
    StaleTerm,
    NotLeader,
    NoAuthority,
    Migrating,
    NoQuorum,
}

impl ClusterState {
    pub fn initial() -> Self {
        let nodes = BTreeMap::from([
            (
                0,
                NodeState {
                    current_term: 0,
                    voted_for: Some(0),
                    role: ClusterRole::Leader,
                    votes_received: BTreeSet::from([0, 1, 2]),
                    shard_state: BTreeMap::from([
                        (0, ShardLocal::leader(0)),
                        (1, ShardLocal::follower(0)),
                        (2, ShardLocal::empty()),
                    ]),
                    reachable: BTreeSet::from(NODES),
                    append_acks: BTreeMap::from([
                        (0, BTreeSet::from(NODES)),
                        (1, BTreeSet::new()),
                        (2, BTreeSet::new()),
                    ]),
                },
            ),
            (
                1,
                NodeState {
                    current_term: 0,
                    voted_for: Some(0),
                    role: ClusterRole::Follower,
                    votes_received: BTreeSet::new(),
                    shard_state: BTreeMap::from([
                        (0, ShardLocal::follower(0)),
                        (1, ShardLocal::leader(0)),
                        (2, ShardLocal::follower(0)),
                    ]),
                    reachable: BTreeSet::from(NODES),
                    append_acks: BTreeMap::from([
                        (0, BTreeSet::new()),
                        (1, BTreeSet::from(NODES)),
                        (2, BTreeSet::new()),
                    ]),
                },
            ),
            (
                2,
                NodeState {
                    current_term: 0,
                    voted_for: Some(0),
                    role: ClusterRole::Follower,
                    votes_received: BTreeSet::new(),
                    shard_state: BTreeMap::from([
                        (0, ShardLocal::empty()),
                        (1, ShardLocal::empty()),
                        (2, ShardLocal::leader(0)),
                    ]),
                    reachable: BTreeSet::from(NODES),
                    append_acks: BTreeMap::from([
                        (0, BTreeSet::new()),
                        (1, BTreeSet::new()),
                        (2, BTreeSet::from(NODES)),
                    ]),
                },
            ),
        ]);
        Self {
            nodes,
            ring: RingState::default_ring(),
            network: BTreeSet::new(),
        }
    }

    pub fn has_quorum(node_state: &NodeState) -> bool {
        node_state.reachable.len() >= QUORUM
    }

    pub fn leader_count(&self, shard: ShardIndex) -> usize {
        NODES
            .iter()
            .filter(|&&n| {
                let sl = &self.nodes[&n].shard_state[&shard];
                sl.role == ShardRole::Leader && sl.serving && sl.item_authority
            })
            .count()
    }

    pub fn shard_has_active_server(&self, shard: ShardIndex) -> bool {
        NODES.iter().any(|&n| {
            let sl = &self.nodes[&n].shard_state[&shard];
            sl.serving && sl.item_authority
        })
    }

    pub fn read_route(&self, node: NodeIndex, shard: ShardIndex, request_term: i8) -> ReadRoute {
        let ns = &self.nodes[&node];
        let sl = &ns.shard_state[&shard];
        if request_term < ns.current_term as i8 {
            ReadRoute::StaleTerm
        } else if sl.role != ShardRole::Leader {
            ReadRoute::NotLeader
        } else if !Self::has_quorum(ns) {
            ReadRoute::NoQuorum
        } else if sl.migrating {
            ReadRoute::Migrating
        } else if !sl.item_authority {
            ReadRoute::NoAuthority
        } else {
            ReadRoute::Ok
        }
    }

    pub fn can_write(&self, node: NodeIndex, shard: ShardIndex) -> bool {
        let ns = &self.nodes[&node];
        let sl = &ns.shard_state[&shard];
        sl.role == ShardRole::Leader
            && Self::has_quorum(ns)
            && sl.item_authority
            && !sl.migrating
            && sl.serving
    }
}
