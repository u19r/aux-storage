use std::collections::BTreeSet;

use crate::cluster_model::{
    ClusterRole, ClusterState, Message, MessageKind, NODES, NodeIndex, QUORUM, SHARDS, ShardIndex,
    ShardRole,
};

/// Cluster-level transitions that mirror the Quint distributed_cache_cluster
/// actions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterTransition {
    /// A node starts an election.
    StartElection { node: NodeIndex },
    /// Leader sends an append/heartbeat to a follower for a shard.
    SendAppend {
        leader: NodeIndex,
        follower: NodeIndex,
        shard: ShardIndex,
    },
    /// Deliver a message from the network.
    DeliverMessage { msg: Message },
    /// Drop a message (models loss).
    DropMessage { msg: Message },
    /// Partition a node (loses all connectivity except self).
    PartitionNode { node: NodeIndex },
    /// Restore full connectivity.
    HealPartition,
    /// Partial partition between two nodes.
    PartialPartition {
        node_a: NodeIndex,
        node_b: NodeIndex,
    },
    /// Node steps down from leadership on quorum loss.
    StepDownOnQuorumLoss { node: NodeIndex },
    /// Step down shard leadership on a specific node/shard.
    StepDownShardLeadership { node: NodeIndex, shard: ShardIndex },
    /// Initiate shard migration.
    InitiateMigration {
        shard: ShardIndex,
        target: NodeIndex,
    },
    /// Complete migration drain: source sends transfer.
    CompleteMigrationDrain { shard: ShardIndex },
    /// Abort migration: restore source authority.
    AbortMigration { shard: ShardIndex },
    /// Bump epoch for a shard on a node.
    BumpEpoch { node: NodeIndex, shard: ShardIndex },
}

impl ClusterState {
    /// Apply a transition, returning `None` if preconditions fail.
    #[must_use]
    pub fn try_apply(&self, transition: &ClusterTransition) -> Option<Self> {
        match transition {
            ClusterTransition::StartElection { node } => self.start_election(*node),
            ClusterTransition::SendAppend {
                leader,
                follower,
                shard,
            } => self.send_append(*leader, *follower, *shard),
            ClusterTransition::DeliverMessage { msg } => self.deliver_message(msg),
            ClusterTransition::DropMessage { msg } => self.drop_message(msg),
            ClusterTransition::PartitionNode { node } => self.partition_node(*node),
            ClusterTransition::HealPartition => Some(self.heal_partition()),
            ClusterTransition::PartialPartition { node_a, node_b } => {
                self.partial_partition(*node_a, *node_b)
            }
            ClusterTransition::StepDownOnQuorumLoss { node } => {
                self.step_down_on_quorum_loss(*node)
            }
            ClusterTransition::StepDownShardLeadership { node, shard } => {
                self.step_down_shard_leadership(*node, *shard)
            }
            ClusterTransition::InitiateMigration { shard, target } => {
                self.initiate_migration(*shard, *target)
            }
            ClusterTransition::CompleteMigrationDrain { shard } => {
                self.complete_migration_drain(*shard)
            }
            ClusterTransition::AbortMigration { shard } => self.abort_migration(*shard),
            ClusterTransition::BumpEpoch { node, shard } => self.bump_epoch(*node, *shard),
        }
    }

    fn start_election(&self, node: NodeIndex) -> Option<Self> {
        let ns = &self.nodes[&node];
        if ns.role == ClusterRole::Leader {
            return None;
        }
        let new_term = if ns.current_term < 3 {
            ns.current_term + 1
        } else {
            return None; // The model caps the election term.
        };
        if new_term <= ns.current_term {
            return None;
        }

        let mut state = self.clone();
        let node_state = state.nodes.get_mut(&node)?;
        node_state.current_term = new_term;
        node_state.voted_for = Some(node);
        node_state.role = ClusterRole::Candidate;
        node_state.votes_received = BTreeSet::from([node]);

        // Send vote requests to all reachable nodes
        let reachable: Vec<NodeIndex> = ns
            .reachable
            .iter()
            .copied()
            .filter(|&n| n != node)
            .collect();
        for target in reachable {
            state.network.insert(Message {
                kind: MessageKind::VoteRequest,
                from: node,
                to: target,
                term: new_term,
                shard: 0, // Vote requests are not shard-specific.
                granted: false,
                epoch: 0,
            });
        }
        Some(state)
    }

    fn send_append(
        &self,
        leader: NodeIndex,
        follower: NodeIndex,
        shard: ShardIndex,
    ) -> Option<Self> {
        let ls = &self.nodes[&leader];
        let sl = &ls.shard_state[&shard];
        if ls.role != ClusterRole::Leader || sl.role != ShardRole::Leader {
            return None;
        }
        if !ls.reachable.contains(&follower) || follower == leader {
            return None;
        }
        let mut state = self.clone();
        state.network.insert(Message {
            kind: MessageKind::Append,
            from: leader,
            to: follower,
            term: ls.current_term,
            shard,
            granted: false,
            epoch: sl.epoch,
        });
        Some(state)
    }

    fn deliver_message(&self, msg: &Message) -> Option<Self> {
        if !self.network.contains(msg) {
            return None;
        }
        if !self.nodes[&msg.to].reachable.contains(&msg.from) {
            return None;
        }
        let mut state = self.clone();
        state.network.remove(msg);

        match msg.kind {
            MessageKind::VoteRequest => state.handle_vote_request(msg),
            MessageKind::VoteResponse => state.handle_vote_response(msg),
            MessageKind::Append => state.handle_append(msg),
            MessageKind::AppendAck => state.handle_append_ack(msg),
            MessageKind::ShardTransfer => {
                // Check migration target matches
                if self.ring.migration_target[&msg.shard] != Some(msg.to) {
                    return None;
                }
                state.handle_shard_transfer(msg);
            }
        }
        Some(state)
    }

    fn handle_vote_request(&mut self, msg: &Message) {
        let Some(receiver) = self.nodes.get_mut(&msg.to) else {
            return;
        };
        let grant = msg.term >= receiver.current_term
            && (receiver.voted_for.is_none()
                || receiver.voted_for == Some(msg.from)
                || msg.term > receiver.current_term);

        if grant {
            receiver.current_term = msg.term;
            receiver.voted_for = Some(msg.from);
            if msg.term > receiver.current_term {
                receiver.role = ClusterRole::Follower;
            }
        } else if msg.term > receiver.current_term {
            receiver.current_term = msg.term;
            receiver.voted_for = None;
            receiver.role = ClusterRole::Follower;
        }

        self.network.insert(Message {
            kind: MessageKind::VoteResponse,
            from: msg.to,
            to: msg.from,
            term: msg.term,
            shard: 0,
            granted: grant,
            epoch: 0,
        });
    }

    fn handle_vote_response(&mut self, msg: &Message) {
        let Some(receiver) = self.nodes.get_mut(&msg.to) else {
            return;
        };
        if msg.term != receiver.current_term || receiver.role != ClusterRole::Candidate {
            return;
        }
        if !msg.granted {
            return;
        }
        receiver.votes_received.insert(msg.from);
        if receiver.votes_received.len() >= QUORUM {
            receiver.role = ClusterRole::Leader;
            // Assume shard leadership for shards where this node is primary
            let node = msg.to;
            for &shard in &SHARDS {
                if self.ring.primary_of(shard) == node {
                    let Some(sl) = receiver.shard_state.get_mut(&shard) else {
                        return;
                    };
                    sl.role = ShardRole::Leader;
                    sl.serving = true;
                    sl.item_authority = true;
                    sl.query_authority = true;
                }
            }
        }
    }

    fn handle_append(&mut self, msg: &Message) {
        let Some(receiver) = self.nodes.get_mut(&msg.to) else {
            return;
        };
        if msg.term < receiver.current_term {
            return; // Stale term
        }
        receiver.current_term = msg.term;
        receiver.role = ClusterRole::Follower;
        receiver.voted_for = Some(msg.from);
        let Some(sl) = receiver.shard_state.get_mut(&msg.shard) else {
            return;
        };
        sl.role = ShardRole::Follower;
        sl.epoch = msg.epoch;

        self.network.insert(Message {
            kind: MessageKind::AppendAck,
            from: msg.to,
            to: msg.from,
            term: msg.term,
            shard: msg.shard,
            granted: true,
            epoch: msg.epoch,
        });
    }

    fn handle_append_ack(&mut self, msg: &Message) {
        let Some(receiver) = self.nodes.get_mut(&msg.to) else {
            return;
        };
        if msg.term != receiver.current_term || receiver.role != ClusterRole::Leader {
            return;
        }
        let Some(acks) = receiver.append_acks.get_mut(&msg.shard) else {
            return;
        };
        acks.insert(msg.from);
    }

    fn handle_shard_transfer(&mut self, msg: &Message) {
        let Some(receiver) = self.nodes.get_mut(&msg.to) else {
            return;
        };
        let Some(sl) = receiver.shard_state.get_mut(&msg.shard) else {
            return;
        };
        sl.role = ShardRole::Leader;
        sl.epoch = msg.epoch + 1;
        sl.item_authority = true;
        sl.query_authority = true;
        sl.serving = true;
        sl.migrating = false;

        self.ring.assignment.insert(msg.shard, msg.to);
        self.ring.migration_source.insert(msg.shard, None);
        self.ring.migration_target.insert(msg.shard, None);
    }

    fn drop_message(&self, msg: &Message) -> Option<Self> {
        if !self.network.contains(msg) {
            return None;
        }
        let mut state = self.clone();
        state.network.remove(msg);
        Some(state)
    }

    fn partition_node(&self, node: NodeIndex) -> Option<Self> {
        let mut state = self.clone();
        state.nodes.get_mut(&node)?.reachable = BTreeSet::from([node]);
        for &n in &NODES {
            if n != node {
                state.nodes.get_mut(&n)?.reachable.remove(&node);
            }
        }
        Some(state)
    }

    fn heal_partition(&self) -> Self {
        let mut state = self.clone();
        for &n in &NODES {
            if let Some(node) = state.nodes.get_mut(&n) {
                node.reachable = BTreeSet::from(NODES);
            }
        }
        state
    }

    fn partial_partition(&self, node_a: NodeIndex, node_b: NodeIndex) -> Option<Self> {
        if node_a == node_b {
            return None;
        }
        let mut state = self.clone();
        state.nodes.get_mut(&node_a)?.reachable.remove(&node_b);
        state.nodes.get_mut(&node_b)?.reachable.remove(&node_a);
        Some(state)
    }

    fn step_down_on_quorum_loss(&self, node: NodeIndex) -> Option<Self> {
        let ns = &self.nodes[&node];
        if ns.role != ClusterRole::Leader || Self::has_quorum(ns) {
            return None;
        }
        let mut state = self.clone();
        let ns = state.nodes.get_mut(&node)?;
        ns.role = ClusterRole::Follower;
        for &shard in &SHARDS {
            let sl = ns.shard_state.get_mut(&shard)?;
            sl.role = ShardRole::Follower;
            sl.item_authority = false;
            sl.query_authority = false;
            sl.serving = false;
        }
        Some(state)
    }

    fn step_down_shard_leadership(&self, node: NodeIndex, shard: ShardIndex) -> Option<Self> {
        let sl = &self.nodes[&node].shard_state[&shard];
        if sl.role != ShardRole::Leader {
            return None;
        }
        let mut state = self.clone();
        let sl = state.nodes.get_mut(&node)?.shard_state.get_mut(&shard)?;
        sl.role = ShardRole::Follower;
        sl.item_authority = false;
        sl.query_authority = false;
        sl.serving = false;
        Some(state)
    }

    fn initiate_migration(&self, shard: ShardIndex, target: NodeIndex) -> Option<Self> {
        let source = self.ring.primary_of(shard);
        if source == target {
            return None;
        }
        if self.ring.is_migrating(shard) {
            return None;
        }
        let source_node = &self.nodes[&source];
        let source_sl = &source_node.shard_state[&shard];
        if source_sl.role != ShardRole::Leader || source_node.role != ClusterRole::Leader {
            return None;
        }

        let mut state = self.clone();
        let sl = state.nodes.get_mut(&source)?.shard_state.get_mut(&shard)?;
        sl.migrating = true;
        sl.query_authority = false;

        state.ring.migration_source.insert(shard, Some(source));
        state.ring.migration_target.insert(shard, Some(target));
        Some(state)
    }

    fn complete_migration_drain(&self, shard: ShardIndex) -> Option<Self> {
        let source = self.ring.migration_source[&shard]?;
        let target = self.ring.migration_target[&shard]?;
        let source_node = &self.nodes[&source];
        let source_sl = &source_node.shard_state[&shard];
        if !source_sl.migrating {
            return None;
        }

        let mut state = self.clone();
        let sl = state.nodes.get_mut(&source)?.shard_state.get_mut(&shard)?;
        sl.role = ShardRole::None;
        sl.item_authority = false;
        sl.query_authority = false;
        sl.serving = false;
        sl.migrating = false;

        state.network.insert(Message {
            kind: MessageKind::ShardTransfer,
            from: source,
            to: target,
            term: source_node.current_term,
            shard,
            granted: false,
            epoch: source_sl.epoch,
        });
        Some(state)
    }

    fn abort_migration(&self, shard: ShardIndex) -> Option<Self> {
        let source = self.ring.migration_source[&shard]?;
        let source_sl = &self.nodes[&source].shard_state[&shard];
        if !source_sl.migrating {
            return None;
        }

        let mut state = self.clone();
        let sl = state.nodes.get_mut(&source)?.shard_state.get_mut(&shard)?;
        sl.migrating = false;
        sl.query_authority = true;

        state.ring.migration_source.insert(shard, None);
        state.ring.migration_target.insert(shard, None);
        Some(state)
    }

    fn bump_epoch(&self, node: NodeIndex, shard: ShardIndex) -> Option<Self> {
        let sl = &self.nodes[&node].shard_state[&shard];
        if sl.role != ShardRole::Leader {
            return None;
        }
        let new_epoch = if sl.epoch < 3 {
            sl.epoch + 1
        } else {
            return None;
        };
        if new_epoch <= sl.epoch {
            return None;
        }
        let mut state = self.clone();
        state
            .nodes
            .get_mut(&node)?
            .shard_state
            .get_mut(&shard)?
            .epoch = new_epoch;
        Some(state)
    }
}
