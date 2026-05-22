use storage_backfill::LogicalBootstrapPreflightDecision;

use crate::{
    SyncBackendPairDecision, SyncHeterogeneousBehaviorGate,
    SyncHeterogeneousLogicalSnapshotDecision, SyncMultiRegionInboundApplyDecision,
    SyncMultiRegionSenderOwnershipDecision, SyncNonSqlResolvedApplyDecision,
    SyncPromotedLearnerSurfaceDecision, SyncRaftRole, plan_sync_heterogeneous_behavior,
};

#[test]
fn given_complete_mixed_pair_when_planning_behavior_then_all_required_surfaces_allow() {
    let plan = plan_sync_heterogeneous_behavior(complete_gate("sqlite", "postgres"));

    assert_eq!(plan.backend_pair, SyncBackendPairDecision::ValidationOnly);
    assert_eq!(
        plan.logical_snapshot,
        SyncHeterogeneousLogicalSnapshotDecision::Allow
    );
    assert_eq!(plan.resolved_apply, SyncNonSqlResolvedApplyDecision::Allow);
    assert_eq!(
        plan.promoted_surface,
        SyncPromotedLearnerSurfaceDecision::Allow
    );
    assert_eq!(
        plan.bootstrap_preflight,
        LogicalBootstrapPreflightDecision::AllowEmptyDestination
    );
    assert_eq!(
        plan.multi_region_interop.outbound,
        SyncMultiRegionSenderOwnershipDecision::OwnsSender
    );
    assert_eq!(
        plan.multi_region_interop.inbound,
        SyncMultiRegionInboundApplyDecision::Apply
    );
}

#[test]
fn given_remote_pair_when_planning_behavior_then_dependent_surfaces_fail_closed() {
    let plan = plan_sync_heterogeneous_behavior(complete_gate("sqlite", "remote"));

    assert_eq!(plan.backend_pair, SyncBackendPairDecision::Rejected);
    assert!(matches!(
        plan.logical_snapshot,
        SyncHeterogeneousLogicalSnapshotDecision::Block(_)
    ));
    assert!(matches!(
        plan.resolved_apply,
        SyncNonSqlResolvedApplyDecision::Block(_)
    ));
    assert!(matches!(
        plan.promoted_surface,
        SyncPromotedLearnerSurfaceDecision::Block(_)
    ));
}

#[test]
fn given_incomplete_domains_when_planning_behavior_then_snapshot_and_promotion_block() {
    let plan = plan_sync_heterogeneous_behavior(SyncHeterogeneousBehaviorGate {
        logical_snapshot_domains_complete: false,
        ..complete_gate("sqlite", "postgres")
    });

    assert!(matches!(
        plan.logical_snapshot,
        SyncHeterogeneousLogicalSnapshotDecision::Block(_)
    ));
    assert!(matches!(
        plan.promoted_surface,
        SyncPromotedLearnerSurfaceDecision::Block(_)
    ));
}

fn complete_gate<'a>(
    source_backend: &'a str,
    destination_backend: &'a str,
) -> SyncHeterogeneousBehaviorGate<'a> {
    SyncHeterogeneousBehaviorGate {
        source_backend,
        destination_backend,
        logical_snapshot_domains_complete: true,
        resolved_apply_complete: true,
        learner_promoted: true,
        bootstrap_destination_empty: true,
        bootstrap_preflight_marker_present: false,
        sync_role: SyncRaftRole::Leader,
        current_version: 1,
        incoming_version: 2,
    }
}
