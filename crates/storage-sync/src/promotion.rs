use async_trait::async_trait;
use storage_types::StorageResult;

use crate::SyncNodeId;

/// Inputs to the learner promotion gate.
///
/// Promotion is intentionally fail-closed. A learner that caught up logically
/// still cannot vote until the import, checksum, stream-drain, membership, and
/// applied-index facts all describe the same committed cluster state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncPromotionSafetyGate {
    pub learner_already_voter: bool,
    pub import_complete: bool,
    pub checksum_validated: bool,
    pub pending_required_domains: u32,
    pub protected_stream_drained: bool,
    pub tombstone_cleanup_ready: bool,
    pub applied_index: u64,
    pub promotion_decision_index: u64,
    pub peer_connectivity_healthy: bool,
    pub membership_contains_learner: bool,
    pub conflicting_cluster_identity: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPromotionSafetyDecision {
    Allow,
    AlreadyPromoted,
    Block(SyncPromotionBlockReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPromotionBlockReason {
    ImportIncomplete,
    ChecksumNotValidated,
    RequiredDomainsPending,
    ProtectedStreamNotDrained,
    TombstoneCleanupNotReady,
    AppliedIndexBehindPromotionDecision,
    PeerConnectivityUnhealthy,
    LearnerMissingFromMembership,
    ConflictingClusterIdentity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncLearnerPromotionReport {
    pub node_id: SyncNodeId,
    pub log_index: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncAutoPromotionOutcome {
    Promoted(SyncLearnerPromotionReport),
    AlreadyPromoted { node_id: SyncNodeId },
    Blocked(SyncPromotionBlockReason),
}

#[async_trait]
pub trait SyncLearnerPromoter: Send + Sync {
    async fn promote_sync_learner(
        &self,
        node_id: SyncNodeId,
    ) -> StorageResult<SyncLearnerPromotionReport>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SyncAutoPromotionController;

impl SyncAutoPromotionController {
    pub async fn promote_if_safe<P>(
        &self,
        promoter: &P,
        node_id: SyncNodeId,
        gate: SyncPromotionSafetyGate,
    ) -> StorageResult<SyncAutoPromotionOutcome>
    where
        P: SyncLearnerPromoter,
    {
        match plan_sync_promotion_safety(gate) {
            SyncPromotionSafetyDecision::Allow => promoter
                .promote_sync_learner(node_id)
                .await
                .map(SyncAutoPromotionOutcome::Promoted),
            SyncPromotionSafetyDecision::AlreadyPromoted => {
                Ok(SyncAutoPromotionOutcome::AlreadyPromoted { node_id })
            }
            SyncPromotionSafetyDecision::Block(reason) => {
                Ok(SyncAutoPromotionOutcome::Blocked(reason))
            }
        }
    }
}

#[must_use]
pub const fn plan_sync_promotion_safety(
    gate: SyncPromotionSafetyGate,
) -> SyncPromotionSafetyDecision {
    if !gate.import_complete {
        return SyncPromotionSafetyDecision::Block(SyncPromotionBlockReason::ImportIncomplete);
    }
    if !gate.checksum_validated {
        return SyncPromotionSafetyDecision::Block(SyncPromotionBlockReason::ChecksumNotValidated);
    }
    if gate.pending_required_domains != 0 {
        return SyncPromotionSafetyDecision::Block(
            SyncPromotionBlockReason::RequiredDomainsPending,
        );
    }
    if !gate.protected_stream_drained {
        return SyncPromotionSafetyDecision::Block(
            SyncPromotionBlockReason::ProtectedStreamNotDrained,
        );
    }
    if !gate.tombstone_cleanup_ready {
        return SyncPromotionSafetyDecision::Block(
            SyncPromotionBlockReason::TombstoneCleanupNotReady,
        );
    }
    if gate.applied_index < gate.promotion_decision_index {
        return SyncPromotionSafetyDecision::Block(
            SyncPromotionBlockReason::AppliedIndexBehindPromotionDecision,
        );
    }
    if !gate.peer_connectivity_healthy {
        return SyncPromotionSafetyDecision::Block(
            SyncPromotionBlockReason::PeerConnectivityUnhealthy,
        );
    }
    if !gate.membership_contains_learner {
        return SyncPromotionSafetyDecision::Block(
            SyncPromotionBlockReason::LearnerMissingFromMembership,
        );
    }
    if gate.conflicting_cluster_identity {
        return SyncPromotionSafetyDecision::Block(
            SyncPromotionBlockReason::ConflictingClusterIdentity,
        );
    }
    if gate.learner_already_voter {
        return SyncPromotionSafetyDecision::AlreadyPromoted;
    }
    SyncPromotionSafetyDecision::Allow
}
