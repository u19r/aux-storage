#![allow(non_snake_case)]

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;

use crate::{
    SyncPromotionBlockReason, SyncPromotionSafetyDecision, SyncPromotionSafetyGate,
    plan_sync_promotion_safety,
};

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
struct MbtGate {
    #[serde(rename = "learnerAlreadyVoter")]
    learner_already_voter: bool,
    #[serde(rename = "importComplete")]
    import_complete: bool,
    #[serde(rename = "checksumValidated")]
    checksum_validated: bool,
    #[serde(rename = "pendingRequiredDomains")]
    pending_required_domains: u32,
    #[serde(rename = "protectedStreamDrained")]
    protected_stream_drained: bool,
    #[serde(rename = "tombstoneCleanupReady")]
    tombstone_cleanup_ready: bool,
    #[serde(rename = "appliedIndex")]
    applied_index: u64,
    #[serde(rename = "promotionDecisionIndex")]
    promotion_decision_index: u64,
    #[serde(rename = "peerConnectivityHealthy")]
    peer_connectivity_healthy: bool,
    #[serde(rename = "membershipContainsLearner")]
    membership_contains_learner: bool,
    #[serde(rename = "conflictingClusterIdentity")]
    conflicting_cluster_identity: bool,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct SyncPromotionSafetyState {
    #[serde(rename = "lastGate")]
    last_gate: MbtGate,
    #[serde(rename = "lastDecision")]
    last_decision: String,
}

impl State<SyncPromotionSafetyDriver> for SyncPromotionSafetyState {
    fn from_driver(driver: &SyncPromotionSafetyDriver) -> Result<Self> {
        Ok(Self {
            last_gate: driver.last_gate,
            last_decision: driver.last_decision.clone(),
        })
    }
}

#[derive(Debug)]
struct SyncPromotionSafetyDriver {
    last_gate: MbtGate,
    last_decision: String,
}

impl Default for SyncPromotionSafetyDriver {
    fn default() -> Self {
        Self {
            last_gate: MbtGate {
                import_complete: false,
                checksum_validated: false,
                pending_required_domains: 1,
                protected_stream_drained: false,
                tombstone_cleanup_ready: false,
                applied_index: 0,
                promotion_decision_index: 1,
                peer_connectivity_healthy: false,
                membership_contains_learner: false,
                conflicting_cluster_identity: true,
                learner_already_voter: false,
            },
            last_decision: "not_checked".to_string(),
        }
    }
}

impl Driver for SyncPromotionSafetyDriver {
    type State = SyncPromotionSafetyState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            Check(
                importComplete: bool,
                checksumValidated: bool,
                pendingRequiredDomains: u32,
                protectedStreamDrained: bool,
                tombstoneCleanupReady: bool,
                appliedIndex: u64,
                promotionDecisionIndex: u64,
                peerConnectivityHealthy: bool,
                membershipContainsLearner: bool,
                conflictingClusterIdentity: bool,
                learnerAlreadyVoter: bool,
            ) => {
                self.check(MbtGate {
                    learner_already_voter: learnerAlreadyVoter,
                    import_complete: importComplete,
                    checksum_validated: checksumValidated,
                    pending_required_domains: pendingRequiredDomains,
                    protected_stream_drained: protectedStreamDrained,
                    tombstone_cleanup_ready: tombstoneCleanupReady,
                    applied_index: appliedIndex,
                    promotion_decision_index: promotionDecisionIndex,
                    peer_connectivity_healthy: peerConnectivityHealthy,
                    membership_contains_learner: membershipContainsLearner,
                    conflicting_cluster_identity: conflictingClusterIdentity,
                });
            },
            step(
                importComplete: bool?,
                checksumValidated: bool?,
                pendingRequiredDomains: u32?,
                protectedStreamDrained: bool?,
                tombstoneCleanupReady: bool?,
                appliedIndex: u64?,
                promotionDecisionIndex: u64?,
                peerConnectivityHealthy: bool?,
                membershipContainsLearner: bool?,
                conflictingClusterIdentity: bool?,
                learnerAlreadyVoter: bool?,
            ) => {
                if let (
                    Some(import_complete),
                    Some(checksum_validated),
                    Some(pending_required_domains),
                    Some(protected_stream_drained),
                    Some(tombstone_cleanup_ready),
                    Some(applied_index),
                    Some(promotion_decision_index),
                    Some(peer_connectivity_healthy),
                    Some(membership_contains_learner),
                    Some(conflicting_cluster_identity),
                    Some(learner_already_voter),
                ) = (
                    importComplete,
                    checksumValidated,
                    pendingRequiredDomains,
                    protectedStreamDrained,
                    tombstoneCleanupReady,
                    appliedIndex,
                    promotionDecisionIndex,
                    peerConnectivityHealthy,
                    membershipContainsLearner,
                    conflictingClusterIdentity,
                    learnerAlreadyVoter,
                ) {
                    self.check(MbtGate {
                        learner_already_voter,
                        import_complete,
                        checksum_validated,
                        pending_required_domains,
                        protected_stream_drained,
                        tombstone_cleanup_ready,
                        applied_index,
                        promotion_decision_index,
                        peer_connectivity_healthy,
                        membership_contains_learner,
                        conflicting_cluster_identity,
                    });
                }
            },
        })
    }
}

impl SyncPromotionSafetyDriver {
    fn check(&mut self, gate: MbtGate) {
        self.last_decision = decision_name(plan_sync_promotion_safety(SyncPromotionSafetyGate {
            import_complete: gate.import_complete,
            checksum_validated: gate.checksum_validated,
            pending_required_domains: gate.pending_required_domains,
            protected_stream_drained: gate.protected_stream_drained,
            tombstone_cleanup_ready: gate.tombstone_cleanup_ready,
            applied_index: gate.applied_index,
            promotion_decision_index: gate.promotion_decision_index,
            peer_connectivity_healthy: gate.peer_connectivity_healthy,
            membership_contains_learner: gate.membership_contains_learner,
            conflicting_cluster_identity: gate.conflicting_cluster_identity,
            learner_already_voter: gate.learner_already_voter,
        }))
        .to_string();
        self.last_gate = gate;
    }
}

fn decision_name(decision: SyncPromotionSafetyDecision) -> &'static str {
    match decision {
        SyncPromotionSafetyDecision::Allow => "allow",
        SyncPromotionSafetyDecision::AlreadyPromoted => "already_promoted",
        SyncPromotionSafetyDecision::Block(SyncPromotionBlockReason::ImportIncomplete) => {
            "import_incomplete"
        }
        SyncPromotionSafetyDecision::Block(SyncPromotionBlockReason::ChecksumNotValidated) => {
            "checksum_not_validated"
        }
        SyncPromotionSafetyDecision::Block(SyncPromotionBlockReason::RequiredDomainsPending) => {
            "required_domains_pending"
        }
        SyncPromotionSafetyDecision::Block(SyncPromotionBlockReason::ProtectedStreamNotDrained) => {
            "protected_stream_not_drained"
        }
        SyncPromotionSafetyDecision::Block(SyncPromotionBlockReason::TombstoneCleanupNotReady) => {
            "tombstone_cleanup_not_ready"
        }
        SyncPromotionSafetyDecision::Block(
            SyncPromotionBlockReason::AppliedIndexBehindPromotionDecision,
        ) => "applied_index_behind_promotion_decision",
        SyncPromotionSafetyDecision::Block(SyncPromotionBlockReason::PeerConnectivityUnhealthy) => {
            "peer_connectivity_unhealthy"
        }
        SyncPromotionSafetyDecision::Block(
            SyncPromotionBlockReason::LearnerMissingFromMembership,
        ) => "learner_missing_from_membership",
        SyncPromotionSafetyDecision::Block(
            SyncPromotionBlockReason::ConflictingClusterIdentity,
        ) => "conflicting_cluster_identity",
    }
}

#[quint_run(
    spec = "../../quint/sync_promotion_safety_mbt.qnt",
    max_samples = 128,
    max_steps = 8,
    seed = "0xadd506"
)]
fn sync_promotion_safety_mbt_matches_rust_boundary() -> impl Driver {
    SyncPromotionSafetyDriver::default()
}

#[test]
fn promotion_safety_blocks_each_late_fail_closed_gate() {
    let cases = [
        (
            SyncPromotionSafetyGate {
                protected_stream_drained: false,
                ..ready_promotion_gate()
            },
            SyncPromotionBlockReason::ProtectedStreamNotDrained,
        ),
        (
            SyncPromotionSafetyGate {
                tombstone_cleanup_ready: false,
                ..ready_promotion_gate()
            },
            SyncPromotionBlockReason::TombstoneCleanupNotReady,
        ),
        (
            SyncPromotionSafetyGate {
                applied_index: 1,
                promotion_decision_index: 2,
                ..ready_promotion_gate()
            },
            SyncPromotionBlockReason::AppliedIndexBehindPromotionDecision,
        ),
        (
            SyncPromotionSafetyGate {
                peer_connectivity_healthy: false,
                ..ready_promotion_gate()
            },
            SyncPromotionBlockReason::PeerConnectivityUnhealthy,
        ),
        (
            SyncPromotionSafetyGate {
                membership_contains_learner: false,
                ..ready_promotion_gate()
            },
            SyncPromotionBlockReason::LearnerMissingFromMembership,
        ),
        (
            SyncPromotionSafetyGate {
                conflicting_cluster_identity: true,
                ..ready_promotion_gate()
            },
            SyncPromotionBlockReason::ConflictingClusterIdentity,
        ),
    ];

    for (gate, expected_reason) in cases {
        assert_eq!(
            plan_sync_promotion_safety(gate),
            SyncPromotionSafetyDecision::Block(expected_reason)
        );
    }
}

fn ready_promotion_gate() -> SyncPromotionSafetyGate {
    SyncPromotionSafetyGate {
        learner_already_voter: false,
        import_complete: true,
        checksum_validated: true,
        pending_required_domains: 0,
        protected_stream_drained: true,
        tombstone_cleanup_ready: true,
        applied_index: 2,
        promotion_decision_index: 1,
        peer_connectivity_healthy: true,
        membership_contains_learner: true,
        conflicting_cluster_identity: false,
    }
}
