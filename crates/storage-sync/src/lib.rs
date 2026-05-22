//! Sync-replication boundary types.
//!
//! This crate intentionally contains no Raft runtime wiring. It owns the stable
//! contracts between request resolution, committed mutation apply, and learner
//! catchup.

mod backend_chaos;
#[cfg(test)]
mod backend_chaos_tests;
mod backend_pair;
#[cfg(test)]
mod backend_pair_tests;
mod catchup;
#[cfg(test)]
mod catchup_tests;
mod compaction;
#[cfg(test)]
mod compaction_tests;
mod conformance;
#[cfg(test)]
mod conformance_tests;
mod crash_boundary;
#[cfg(test)]
mod crash_boundary_tests;
mod health;
#[cfg(test)]
mod health_tests;
mod heterogeneous_behavior;
#[cfg(test)]
mod heterogeneous_behavior_tests;
mod leader_forward;
#[cfg(test)]
mod leader_forward_tests;
mod membership;
#[cfg(test)]
mod membership_tests;
mod metadata;
#[cfg(test)]
mod metadata_tests;
mod multi_region_interop;
#[cfg(test)]
mod multi_region_interop_tests;
mod mutation;
#[cfg(test)]
mod mutation_tests;
mod non_sql_resolved_apply;
#[cfg(test)]
mod non_sql_resolved_apply_tests;
mod not_leader;
#[cfg(test)]
mod not_leader_tests;
mod pipeline;
#[cfg(test)]
mod pipeline_tests;
mod promoted_learner_surface;
#[cfg(test)]
mod promoted_learner_surface_tests;
mod promotion;
#[cfg(test)]
mod promotion_tests;
mod proposal_admission;
mod proposal_coalescing;
#[cfg(test)]
mod proposal_coalescing_tests;
#[cfg(test)]
mod quint_multi_region_clock_skew_lww_tests;
#[cfg(test)]
mod quint_sync_backend_chaos_tests;
#[cfg(test)]
mod quint_sync_backend_pair_policy_tests;
#[cfg(test)]
mod quint_sync_crash_recovery_tests;
#[cfg(test)]
mod quint_sync_heterogeneous_behavior_tests;
#[cfg(test)]
mod quint_sync_leader_forward_tests;
#[cfg(test)]
mod quint_sync_membership_tests;
#[cfg(test)]
mod quint_sync_multi_region_interop_tests;
#[cfg(test)]
mod quint_sync_multi_region_sender_ownership_tests;
#[cfg(test)]
mod quint_sync_non_sql_resolved_apply_tests;
#[cfg(test)]
mod quint_sync_promoted_learner_surface_tests;
#[cfg(test)]
mod quint_sync_promotion_safety_tests;
#[cfg(test)]
mod quint_sync_proposal_admission_tests;
#[cfg(test)]
mod quint_sync_proposal_coalescing_tests;
#[cfg(test)]
mod quint_sync_read_index_tests;
#[cfg(test)]
mod quint_sync_transport_fault_tests;
mod raft_network;
mod raft_types;
#[cfg(test)]
mod raft_types_tests;
mod read_index;
#[cfg(test)]
mod read_index_tests;
mod resolver;
#[cfg(test)]
mod resolver_tests;
mod runtime;
#[cfg(test)]
mod runtime_tests;
mod sender_ownership;
#[cfg(test)]
mod sender_ownership_tests;
mod snapshot;
mod state_machine;
#[cfg(test)]
mod state_machine_tests;
#[cfg(test)]
mod sync_support_tests;
mod traits;
#[cfg(test)]
mod traits_tests;
mod transport_fault;
#[cfg(test)]
mod transport_fault_tests;

pub use backend_chaos::{
    SyncBackendChaosBackend, SyncBackendChaosDecision, SyncBackendChaosFault, SyncBackendChaosGate,
    plan_backend_chaos,
};
pub use backend_pair::{
    SyncBackendPairDecision, SyncBackendPairPlan, SyncBackendPairReason, plan_sync_backend_pair,
    plan_sync_backend_pair_detailed,
};
pub use catchup::{
    SyncLearnerCatchupCheckpoint, SyncLearnerCatchupConfig, SyncLearnerCatchupExecutor,
    SyncLearnerCatchupGate, SyncLearnerCatchupReport, SyncLearnerCatchupRequirement,
    SyncLearnerCatchupStep, sync_learner_catchup_domains,
};
pub use compaction::{
    SyncCompactionBlockReason, SyncCompactionRetentionDecision, SyncCompactionRetentionGate,
    plan_sync_compaction_retention,
};
pub use conformance::{SyncConformanceCase, SyncConformanceExpectation};
pub use crash_boundary::{
    SyncCrashBoundary, SyncCrashRecoveryDecision, SyncCrashRecoveryGate, plan_crash_recovery,
};
pub use health::{SyncHealthResponse, SyncPeerHealth, SyncRaftRole};
pub use heterogeneous_behavior::{
    SyncHeterogeneousBehaviorGate, SyncHeterogeneousBehaviorPlan,
    SyncHeterogeneousLogicalSnapshotBlockReason, SyncHeterogeneousLogicalSnapshotDecision,
    plan_sync_heterogeneous_behavior,
};
pub use leader_forward::{SyncLeaderForward, SyncLeaderForwardDecision, plan_leader_forward};
pub use membership::{SyncMembershipDecision, SyncMembershipGate, plan_membership_activation};
pub use metadata::{
    ResolvedSyncLogEntry, SyncCommitMetadata, SyncItemBaseVersion, SyncLogId, SyncReadSet,
};
pub use multi_region_interop::{
    SyncMultiRegionInboundApplyDecision, SyncMultiRegionInteropDecision,
    plan_sync_multi_region_interop,
};
pub use mutation::{
    ResolvedSyncMutation, ResolvedSyncMutationBatch, SyncCreateTableMutation, SyncDeleteMutation,
    SyncDeleteTableMutation, SyncMutationId, SyncMutationResponse, SyncProposalBatch,
    SyncProposalId, SyncProposalResponse, SyncPutMutation, SyncUpdateTableMutation,
    SyncUpdateTimeToLiveMutation,
};
pub use non_sql_resolved_apply::{
    SyncNonSqlBackend, SyncNonSqlResolvedApplyBlockReason, SyncNonSqlResolvedApplyDecision,
    SyncNonSqlResolvedApplyGate, plan_non_sql_resolved_apply,
};
pub use not_leader::{SYNC_LEADER_HINT_HEADER, SYNC_NOT_LEADER_ERROR_TYPE, SyncNotLeader};
pub use pipeline::{SyncProposalPipelineLimits, SyncProposalPipelineQueueFull, SyncProposalShape};
pub use promoted_learner_surface::{
    SyncPromotedLearnerSurfaceBlockReason, SyncPromotedLearnerSurfaceDecision,
    SyncPromotedLearnerSurfaceGate, plan_promoted_learner_storage_surface,
};
pub use promotion::{
    SyncAutoPromotionController, SyncAutoPromotionOutcome, SyncLearnerPromoter,
    SyncLearnerPromotionReport, SyncPromotionBlockReason, SyncPromotionSafetyDecision,
    SyncPromotionSafetyGate, plan_sync_promotion_safety,
};
pub use proposal_admission::{
    SyncProposalAdmissionDecision, SyncProposalAdmissionGate, plan_proposal_admission,
};
pub use proposal_coalescing::{
    SyncProposalCoalescingDecision, SyncProposalCoalescingGate, plan_proposal_coalescing,
};
pub use raft_network::{
    SyncRaftNetwork, SyncRaftNetworkFactory, SyncRaftRpcClient, SyncRaftTransportError,
};
pub use raft_types::{
    SyncNode, SyncNodeId, SyncRaftRequest, SyncRaftResponse, SyncSnapshotData, SyncTypeConfig,
};
pub use read_index::{SyncStrongReadDecision, SyncStrongReadGate, plan_strong_read_gate};
pub use resolver::{SyncWriteProposalRequest, SyncWriteRequest};
pub use runtime::SyncRaftRuntime;
pub use sender_ownership::{
    SyncMultiRegionSenderOwnershipDecision, plan_multi_region_sender_ownership,
};
pub use snapshot::{SyncRaftSnapshotPayload, SyncSnapshotInstallPhase};
pub use state_machine::{SyncRaftSnapshotBuilder, SyncRaftStateMachine};
pub use traits::{SyncApply, SyncCommandDedupeStore, SyncMutationResolver};
pub use transport_fault::{
    SyncTransportFaultDecision, SyncTransportFaultGate, SyncTransportFaultMode,
    plan_transport_fault_delivery,
};
