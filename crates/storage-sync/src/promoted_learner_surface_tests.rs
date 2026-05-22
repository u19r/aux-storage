use crate::{
    SyncBackendPairDecision, SyncPromotedLearnerSurfaceBlockReason,
    SyncPromotedLearnerSurfaceDecision, SyncPromotedLearnerSurfaceGate,
    plan_promoted_learner_storage_surface,
};

#[test]
fn promoted_validation_pair_can_serve_after_full_storage_surface_is_present() {
    assert_eq!(
        plan_promoted_learner_storage_surface(complete_gate()),
        SyncPromotedLearnerSurfaceDecision::Allow
    );
}

#[test]
fn promoted_learner_surface_blocks_rejected_backend_pair() {
    assert_eq!(
        plan_promoted_learner_storage_surface(SyncPromotedLearnerSurfaceGate {
            backend_pair: SyncBackendPairDecision::Rejected,
            ..complete_gate()
        }),
        SyncPromotedLearnerSurfaceDecision::Block(
            SyncPromotedLearnerSurfaceBlockReason::BackendPairRejected
        )
    );
}

#[test]
fn promoted_learner_surface_requires_domains_needed_by_storage_operations() {
    assert_eq!(
        plan_promoted_learner_storage_surface(SyncPromotedLearnerSurfaceGate {
            gsi_records_imported: false,
            ..complete_gate()
        }),
        SyncPromotedLearnerSurfaceDecision::Block(
            SyncPromotedLearnerSurfaceBlockReason::GsiRecordsMissing
        )
    );
    assert_eq!(
        plan_promoted_learner_storage_surface(SyncPromotedLearnerSurfaceGate {
            ttl_records_imported: false,
            ..complete_gate()
        }),
        SyncPromotedLearnerSurfaceDecision::Block(
            SyncPromotedLearnerSurfaceBlockReason::TtlRecordsMissing
        )
    );
    assert_eq!(
        plan_promoted_learner_storage_surface(SyncPromotedLearnerSurfaceGate {
            durable_revisions_imported: false,
            ..complete_gate()
        }),
        SyncPromotedLearnerSurfaceDecision::Block(
            SyncPromotedLearnerSurfaceBlockReason::DurableRevisionsMissing
        )
    );
}

#[test]
fn promoted_learner_surface_requires_resolved_apply_domains_needed_by_storage_operations() {
    assert_eq!(
        plan_promoted_learner_storage_surface(SyncPromotedLearnerSurfaceGate {
            gsi_apply_conformance: false,
            ..complete_gate()
        }),
        SyncPromotedLearnerSurfaceDecision::Block(
            SyncPromotedLearnerSurfaceBlockReason::GsiApplyMissing
        )
    );
    assert_eq!(
        plan_promoted_learner_storage_surface(SyncPromotedLearnerSurfaceGate {
            sync_control_plane_apply_conformance: false,
            ..complete_gate()
        }),
        SyncPromotedLearnerSurfaceDecision::Block(
            SyncPromotedLearnerSurfaceBlockReason::SyncControlPlaneApplyMissing
        )
    );
    assert_eq!(
        plan_promoted_learner_storage_surface(SyncPromotedLearnerSurfaceGate {
            replay_idempotency_conformance: false,
            ..complete_gate()
        }),
        SyncPromotedLearnerSurfaceDecision::Block(
            SyncPromotedLearnerSurfaceBlockReason::ReplayIdempotencyMissing
        )
    );
}

fn complete_gate() -> SyncPromotedLearnerSurfaceGate {
    SyncPromotedLearnerSurfaceGate {
        backend_pair: SyncBackendPairDecision::ValidationOnly,
        learner_promoted: true,
        table_metadata_imported: true,
        item_records_imported: true,
        durable_revisions_imported: true,
        stream_records_imported: true,
        ttl_records_imported: true,
        gsi_records_imported: true,
        storage_control_plane_imported: true,
        sync_control_plane_imported: true,
        table_lifecycle_apply_conformance: true,
        item_put_delete_apply_conformance: true,
        durable_revision_apply_conformance: true,
        stream_apply_conformance: true,
        ttl_apply_conformance: true,
        gsi_apply_conformance: true,
        sync_control_plane_apply_conformance: true,
        replay_idempotency_conformance: true,
    }
}
