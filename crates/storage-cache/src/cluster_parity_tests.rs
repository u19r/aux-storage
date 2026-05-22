use std::collections::BTreeSet;

use crate::{
    cluster_model::{
        ClusterEpoch, ClusterRole, ClusterState, Message, MessageKind, NODES, NodeIndex, ReadRoute,
        ShardIndex, ShardRole,
    },
    cluster_transition::ClusterTransition,
};

/// Apply a sequence of transitions, panicking if any precondition fails.
fn apply_all(state: ClusterState, transitions: &[ClusterTransition]) -> ClusterState {
    let mut current = state;
    for (i, t) in transitions.iter().enumerate() {
        current = current
            .try_apply(t)
            .unwrap_or_else(|| panic!("transition {i} failed: {t:?}"));
    }
    current
}

fn vote_request(from: NodeIndex, to: NodeIndex, term: u8) -> Message {
    Message {
        kind: MessageKind::VoteRequest,
        from,
        to,
        term,
        shard: 0,
        granted: false,
        epoch: 0,
    }
}

fn vote_response(from: NodeIndex, to: NodeIndex, term: u8, granted: bool) -> Message {
    Message {
        kind: MessageKind::VoteResponse,
        from,
        to,
        term,
        shard: 0,
        granted,
        epoch: 0,
    }
}

fn append_msg(
    from: NodeIndex,
    to: NodeIndex,
    term: u8,
    shard: ShardIndex,
    epoch: ClusterEpoch,
) -> Message {
    Message {
        kind: MessageKind::Append,
        from,
        to,
        term,
        shard,
        granted: false,
        epoch,
    }
}

fn append_ack_msg(
    from: NodeIndex,
    to: NodeIndex,
    term: u8,
    shard: ShardIndex,
    epoch: ClusterEpoch,
) -> Message {
    Message {
        kind: MessageKind::AppendAck,
        from,
        to,
        term,
        shard,
        granted: true,
        epoch,
    }
}

fn shard_transfer_msg(
    from: NodeIndex,
    to: NodeIndex,
    term: u8,
    shard: ShardIndex,
    epoch: ClusterEpoch,
) -> Message {
    Message {
        kind: MessageKind::ShardTransfer,
        from,
        to,
        term,
        shard,
        granted: false,
        epoch,
    }
}

// ---------------------------------------------------------------------------
//  Quint scenario name list (must match distributed_cache_cluster_tests.qnt)
// ---------------------------------------------------------------------------
const MIRRORED_CLUSTER_SCENARIOS: &[&str] = &[
    "initial_state_has_leader",
    "initial_state_each_shard_has_one_leader",
    "initial_state_all_shards_accessible",
    "follower_cannot_serve_writes",
    "partition_triggers_election",
    "election_with_quorum_succeeds",
    "partitioned_node_cannot_win_election",
    "quorum_loss_strips_all_authority",
    "heal_and_reelect_restores_service",
    "partial_partition_keeps_quorum",
    "migration_initiation_strip_query_authority",
    "migration_drain_sends_transfer",
    "migration_complete_transfers_ownership",
    "migration_blocks_writes",
    "abort_migration_restores_authority",
    "epoch_bump_preserves_service",
    "stale_term_reads_rejected",
    "append_replication_round_trip",
    "dropped_append_is_harmless",
    "split_brain_prevented_by_step_down",
    "migration_during_partition_is_safe",
    "two_node_failure_prevents_all_writes",
    "recovery_from_two_node_failure",
];

/// Parse run scenario names from a Quint file.
fn quint_cluster_run_scenarios(source: &str) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for line in source.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("run ")
            && let Some(name) = rest.split_whitespace().next()
        {
            // Convert camelCase to snake_case
            let mut snake = String::new();
            for (i, ch) in name.chars().enumerate() {
                if ch.is_uppercase() && i > 0 {
                    snake.push('_');
                }
                snake.push(ch.to_ascii_lowercase());
            }
            names.insert(snake);
        }
    }
    names
}

#[test]
fn mirrored_rust_cluster_scenarios_cover_every_quint_run_scenario() {
    let expected = quint_cluster_run_scenarios(include_str!(
        "../../../quint/distributed_cache_cluster_tests.qnt"
    ));
    let actual: BTreeSet<String> = MIRRORED_CLUSTER_SCENARIOS
        .iter()
        .map(|s| (*s).to_owned())
        .collect();
    let missing_in_rust: BTreeSet<_> = expected.difference(&actual).collect();
    let extra_in_rust: BTreeSet<_> = actual.difference(&expected).collect();
    assert!(
        missing_in_rust.is_empty() && extra_in_rust.is_empty(),
        "Missing in Rust: {missing_in_rust:?}\nExtra in Rust: {extra_in_rust:?}"
    );
}

// ---------------------------------------------------------------------------
//  Initial state tests
// ---------------------------------------------------------------------------

#[test]
fn initial_state_has_leader() {
    let state = ClusterState::initial();
    assert_eq!(state.nodes[&0].role, ClusterRole::Leader);
    assert_eq!(state.nodes[&1].role, ClusterRole::Follower);
    assert_eq!(state.nodes[&2].role, ClusterRole::Follower);
}

#[test]
fn initial_state_each_shard_has_one_leader() {
    let state = ClusterState::initial();
    assert_eq!(state.nodes[&0].shard_state[&0].role, ShardRole::Leader);
    assert!(state.nodes[&0].shard_state[&0].serving);
    assert_eq!(state.nodes[&1].shard_state[&1].role, ShardRole::Leader);
    assert!(state.nodes[&1].shard_state[&1].serving);
    assert_eq!(state.nodes[&2].shard_state[&2].role, ShardRole::Leader);
    assert!(state.nodes[&2].shard_state[&2].serving);
}

#[test]
fn initial_state_all_shards_accessible() {
    let state = ClusterState::initial();
    assert_eq!(state.read_route(0, 0, 0), ReadRoute::Ok);
    assert_eq!(state.read_route(1, 1, 0), ReadRoute::Ok);
    assert_eq!(state.read_route(2, 2, 0), ReadRoute::Ok);
    assert!(state.can_write(0, 0));
    assert!(state.can_write(1, 1));
    assert!(state.can_write(2, 2));
}

#[test]
fn follower_cannot_serve_writes() {
    let state = ClusterState::initial();
    assert!(!state.can_write(1, 0));
    assert!(!state.can_write(0, 1));
    assert!(!state.can_write(0, 2));
}

// ---------------------------------------------------------------------------
//  Leader election tests
// ---------------------------------------------------------------------------

#[test]
fn partition_triggers_election() {
    let state = apply_all(
        ClusterState::initial(),
        &[
            ClusterTransition::PartitionNode { node: 0 },
            ClusterTransition::StepDownOnQuorumLoss { node: 0 },
        ],
    );
    assert_eq!(state.nodes[&0].role, ClusterRole::Follower);
    assert!(!state.nodes[&1].reachable.contains(&0));
    assert!(!state.nodes[&2].reachable.contains(&0));

    let state = apply_all(state, &[ClusterTransition::StartElection { node: 1 }]);
    assert_eq!(state.nodes[&1].role, ClusterRole::Candidate);
    assert_eq!(state.nodes[&1].current_term, 1);
}

#[test]
fn election_with_quorum_succeeds() {
    let state = apply_all(
        ClusterState::initial(),
        &[
            ClusterTransition::PartitionNode { node: 0 },
            ClusterTransition::StepDownOnQuorumLoss { node: 0 },
            ClusterTransition::StartElection { node: 1 },
            ClusterTransition::DeliverMessage {
                msg: vote_request(1, 2, 1),
            },
            ClusterTransition::DeliverMessage {
                msg: vote_response(2, 1, 1, true),
            },
        ],
    );
    assert_eq!(state.nodes[&1].role, ClusterRole::Leader);
    assert_eq!(state.nodes[&1].current_term, 1);
}

#[test]
fn partitioned_node_cannot_win_election() {
    let state = apply_all(
        ClusterState::initial(),
        &[
            ClusterTransition::PartitionNode { node: 2 },
            ClusterTransition::StartElection { node: 2 },
        ],
    );
    assert_eq!(state.nodes[&2].role, ClusterRole::Candidate);
    assert_eq!(state.nodes[&2].votes_received, BTreeSet::from([2]));
    // Cannot become leader with only 1 vote
    assert_ne!(state.nodes[&2].role, ClusterRole::Leader);
}

// ---------------------------------------------------------------------------
//  Network partition + quorum loss tests
// ---------------------------------------------------------------------------

#[test]
fn quorum_loss_strips_all_authority() {
    let state = apply_all(
        ClusterState::initial(),
        &[
            ClusterTransition::PartitionNode { node: 0 },
            ClusterTransition::StepDownOnQuorumLoss { node: 0 },
        ],
    );
    assert_eq!(state.nodes[&0].role, ClusterRole::Follower);
    assert!(!state.can_write(0, 0));
    assert!(!state.can_write(0, 1));
    assert!(!state.can_write(0, 2));
    assert_ne!(state.read_route(0, 0, 0), ReadRoute::Ok);
}

#[test]
fn heal_and_reelect_restores_service() {
    let state = apply_all(
        ClusterState::initial(),
        &[
            ClusterTransition::PartitionNode { node: 0 },
            ClusterTransition::StepDownOnQuorumLoss { node: 0 },
        ],
    );
    assert!(!state.can_write(0, 0));

    let state = apply_all(
        state,
        &[
            ClusterTransition::HealPartition,
            ClusterTransition::StartElection { node: 0 },
        ],
    );
    assert_eq!(state.nodes[&0].reachable, BTreeSet::from(NODES));
    assert_eq!(state.nodes[&0].role, ClusterRole::Candidate);
    assert_eq!(state.nodes[&0].current_term, 1);
}

#[test]
fn partial_partition_keeps_quorum() {
    let state = apply_all(
        ClusterState::initial(),
        &[ClusterTransition::PartialPartition {
            node_a: 0,
            node_b: 2,
        }],
    );
    assert_eq!(state.nodes[&0].reachable, BTreeSet::from([0, 1]));
    assert!(ClusterState::has_quorum(&state.nodes[&0]));
    assert_eq!(state.read_route(0, 0, 0), ReadRoute::Ok);
    assert!(state.can_write(0, 0));
}

// ---------------------------------------------------------------------------
//  Shard migration tests
// ---------------------------------------------------------------------------

#[test]
fn migration_initiation_strip_query_authority() {
    let state = apply_all(
        ClusterState::initial(),
        &[ClusterTransition::InitiateMigration {
            shard: 0,
            target: 2,
        }],
    );
    assert_eq!(state.ring.migration_source[&0], Some(0));
    assert_eq!(state.ring.migration_target[&0], Some(2));
    assert!(state.nodes[&0].shard_state[&0].migrating);
    assert!(!state.nodes[&0].shard_state[&0].query_authority);
    assert!(state.nodes[&0].shard_state[&0].item_authority);
}

#[test]
fn migration_drain_sends_transfer() {
    let state = apply_all(
        ClusterState::initial(),
        &[
            ClusterTransition::InitiateMigration {
                shard: 0,
                target: 2,
            },
            ClusterTransition::CompleteMigrationDrain { shard: 0 },
        ],
    );
    assert!(!state.nodes[&0].shard_state[&0].serving);
    assert!(!state.nodes[&0].shard_state[&0].item_authority);
    assert!(!state.network.is_empty());
}

#[test]
fn migration_complete_transfers_ownership() {
    let state = apply_all(
        ClusterState::initial(),
        &[
            ClusterTransition::InitiateMigration {
                shard: 0,
                target: 2,
            },
            ClusterTransition::CompleteMigrationDrain { shard: 0 },
            ClusterTransition::DeliverMessage {
                msg: shard_transfer_msg(0, 2, 0, 0, 0),
            },
        ],
    );
    assert_eq!(state.nodes[&2].shard_state[&0].role, ShardRole::Leader);
    assert!(state.nodes[&2].shard_state[&0].serving);
    assert!(state.nodes[&2].shard_state[&0].item_authority);
    assert_eq!(state.ring.assignment[&0], 2);
    assert!(!state.ring.is_migrating(0));
}

#[test]
fn migration_blocks_writes() {
    let state = apply_all(
        ClusterState::initial(),
        &[ClusterTransition::InitiateMigration {
            shard: 0,
            target: 2,
        }],
    );
    assert!(!state.can_write(0, 0));
    assert_eq!(state.read_route(0, 0, 0), ReadRoute::Migrating);
}

#[test]
fn abort_migration_restores_authority() {
    let state = apply_all(
        ClusterState::initial(),
        &[
            ClusterTransition::InitiateMigration {
                shard: 0,
                target: 2,
            },
            ClusterTransition::AbortMigration { shard: 0 },
        ],
    );
    assert!(state.nodes[&0].shard_state[&0].query_authority);
    assert!(!state.nodes[&0].shard_state[&0].migrating);
    assert!(!state.ring.is_migrating(0));
}

// ---------------------------------------------------------------------------
//  Epoch tests
// ---------------------------------------------------------------------------

#[test]
fn epoch_bump_preserves_service() {
    let state = apply_all(
        ClusterState::initial(),
        &[ClusterTransition::BumpEpoch { node: 0, shard: 0 }],
    );
    assert_eq!(state.nodes[&0].shard_state[&0].epoch, 1);
    assert_eq!(state.read_route(0, 0, 0), ReadRoute::Ok);
    assert!(state.can_write(0, 0));
}

#[test]
fn stale_term_reads_rejected() {
    let state = ClusterState::initial();
    assert_eq!(state.read_route(0, 0, -1), ReadRoute::StaleTerm);
}

// ---------------------------------------------------------------------------
//  Replication (append) tests
// ---------------------------------------------------------------------------

#[test]
fn append_replication_round_trip() {
    let state = apply_all(
        ClusterState::initial(),
        &[
            ClusterTransition::SendAppend {
                leader: 0,
                follower: 1,
                shard: 0,
            },
            ClusterTransition::DeliverMessage {
                msg: append_msg(0, 1, 0, 0, 0),
            },
        ],
    );
    assert_eq!(state.nodes[&1].shard_state[&0].role, ShardRole::Follower);
    assert_eq!(state.nodes[&1].shard_state[&0].epoch, 0);

    let state = apply_all(
        state,
        &[ClusterTransition::DeliverMessage {
            msg: append_ack_msg(1, 0, 0, 0, 0),
        }],
    );
    assert!(state.nodes[&0].append_acks[&0].contains(&1));
}

#[test]
fn dropped_append_is_harmless() {
    let state = apply_all(
        ClusterState::initial(),
        &[
            ClusterTransition::SendAppend {
                leader: 0,
                follower: 1,
                shard: 0,
            },
            ClusterTransition::DropMessage {
                msg: append_msg(0, 1, 0, 0, 0),
            },
        ],
    );
    assert!(state.network.is_empty());
    assert_eq!(state.read_route(0, 0, 0), ReadRoute::Ok);
}

// ---------------------------------------------------------------------------
//  Split-brain prevention tests
// ---------------------------------------------------------------------------

#[test]
fn split_brain_prevented_by_step_down() {
    let state = apply_all(
        ClusterState::initial(),
        &[
            ClusterTransition::PartitionNode { node: 0 },
            ClusterTransition::StepDownOnQuorumLoss { node: 0 },
            ClusterTransition::StartElection { node: 1 },
            ClusterTransition::DeliverMessage {
                msg: vote_request(1, 2, 1),
            },
            ClusterTransition::DeliverMessage {
                msg: vote_response(2, 1, 1, true),
            },
        ],
    );
    assert_eq!(state.nodes[&1].role, ClusterRole::Leader);
    assert_eq!(state.nodes[&0].role, ClusterRole::Follower);
    assert!(!state.can_write(0, 0));
    // Node 1 can write shard 1 (its primary)
    assert!(state.can_write(1, 1));
}

#[test]
fn migration_during_partition_is_safe() {
    let state = apply_all(
        ClusterState::initial(),
        &[
            ClusterTransition::InitiateMigration {
                shard: 0,
                target: 2,
            },
            ClusterTransition::CompleteMigrationDrain { shard: 0 },
            ClusterTransition::PartitionNode { node: 2 },
            ClusterTransition::DropMessage {
                msg: shard_transfer_msg(0, 2, 0, 0, 0),
            },
        ],
    );
    // Source gave up authority, target never got it
    assert!(!state.nodes[&0].shard_state[&0].serving);
    assert!(!state.nodes[&2].shard_state[&0].serving);
    assert!(!state.shard_has_active_server(0));
}

// ---------------------------------------------------------------------------
//  3-node quorum edge cases
// ---------------------------------------------------------------------------

#[test]
fn two_node_failure_prevents_all_writes() {
    let state = apply_all(
        ClusterState::initial(),
        &[
            ClusterTransition::PartitionNode { node: 1 },
            ClusterTransition::PartitionNode { node: 2 },
            ClusterTransition::StepDownOnQuorumLoss { node: 0 },
        ],
    );
    assert_eq!(state.nodes[&0].reachable, BTreeSet::from([0]));
    assert!(!ClusterState::has_quorum(&state.nodes[&0]));
    assert!(!state.can_write(0, 0));
    assert!(!state.can_write(0, 1));
    assert!(!state.can_write(0, 2));
}

#[test]
fn recovery_from_two_node_failure() {
    let state = apply_all(
        ClusterState::initial(),
        &[
            ClusterTransition::PartitionNode { node: 1 },
            ClusterTransition::PartitionNode { node: 2 },
            ClusterTransition::StepDownOnQuorumLoss { node: 0 },
            ClusterTransition::HealPartition,
            ClusterTransition::StartElection { node: 0 },
            ClusterTransition::DeliverMessage {
                msg: vote_request(0, 1, 1),
            },
            ClusterTransition::DeliverMessage {
                msg: vote_response(1, 0, 1, true),
            },
        ],
    );
    assert_eq!(state.nodes[&0].role, ClusterRole::Leader);
    assert!(state.can_write(0, 0));
}
