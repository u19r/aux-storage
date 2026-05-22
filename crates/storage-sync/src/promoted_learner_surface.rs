use crate::SyncBackendPairDecision;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncPromotedLearnerSurfaceGate {
    pub backend_pair: SyncBackendPairDecision,
    pub learner_promoted: bool,
    pub table_metadata_imported: bool,
    pub item_records_imported: bool,
    pub durable_revisions_imported: bool,
    pub stream_records_imported: bool,
    pub ttl_records_imported: bool,
    pub gsi_records_imported: bool,
    pub storage_control_plane_imported: bool,
    pub sync_control_plane_imported: bool,
    pub table_lifecycle_apply_conformance: bool,
    pub item_put_delete_apply_conformance: bool,
    pub durable_revision_apply_conformance: bool,
    pub stream_apply_conformance: bool,
    pub ttl_apply_conformance: bool,
    pub gsi_apply_conformance: bool,
    pub sync_control_plane_apply_conformance: bool,
    pub replay_idempotency_conformance: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPromotedLearnerSurfaceDecision {
    Allow,
    Block(SyncPromotedLearnerSurfaceBlockReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPromotedLearnerSurfaceBlockReason {
    BackendPairRejected,
    LearnerNotPromoted,
    TableMetadataMissing,
    ItemRecordsMissing,
    DurableRevisionsMissing,
    StreamRecordsMissing,
    TtlRecordsMissing,
    GsiRecordsMissing,
    StorageControlPlaneMissing,
    SyncControlPlaneMissing,
    TableLifecycleApplyMissing,
    ItemPutDeleteApplyMissing,
    DurableRevisionApplyMissing,
    StreamApplyMissing,
    TtlApplyMissing,
    GsiApplyMissing,
    SyncControlPlaneApplyMissing,
    ReplayIdempotencyMissing,
}

#[must_use]
pub const fn plan_promoted_learner_storage_surface(
    gate: SyncPromotedLearnerSurfaceGate,
) -> SyncPromotedLearnerSurfaceDecision {
    if matches!(gate.backend_pair, SyncBackendPairDecision::Rejected) {
        return SyncPromotedLearnerSurfaceDecision::Block(
            SyncPromotedLearnerSurfaceBlockReason::BackendPairRejected,
        );
    }
    if !gate.learner_promoted {
        return SyncPromotedLearnerSurfaceDecision::Block(
            SyncPromotedLearnerSurfaceBlockReason::LearnerNotPromoted,
        );
    }
    if !gate.table_metadata_imported {
        return SyncPromotedLearnerSurfaceDecision::Block(
            SyncPromotedLearnerSurfaceBlockReason::TableMetadataMissing,
        );
    }
    if !gate.item_records_imported {
        return SyncPromotedLearnerSurfaceDecision::Block(
            SyncPromotedLearnerSurfaceBlockReason::ItemRecordsMissing,
        );
    }
    if !gate.durable_revisions_imported {
        return SyncPromotedLearnerSurfaceDecision::Block(
            SyncPromotedLearnerSurfaceBlockReason::DurableRevisionsMissing,
        );
    }
    if !gate.stream_records_imported {
        return SyncPromotedLearnerSurfaceDecision::Block(
            SyncPromotedLearnerSurfaceBlockReason::StreamRecordsMissing,
        );
    }
    if !gate.ttl_records_imported {
        return SyncPromotedLearnerSurfaceDecision::Block(
            SyncPromotedLearnerSurfaceBlockReason::TtlRecordsMissing,
        );
    }
    if !gate.gsi_records_imported {
        return SyncPromotedLearnerSurfaceDecision::Block(
            SyncPromotedLearnerSurfaceBlockReason::GsiRecordsMissing,
        );
    }
    if !gate.storage_control_plane_imported {
        return SyncPromotedLearnerSurfaceDecision::Block(
            SyncPromotedLearnerSurfaceBlockReason::StorageControlPlaneMissing,
        );
    }
    if !gate.sync_control_plane_imported {
        return SyncPromotedLearnerSurfaceDecision::Block(
            SyncPromotedLearnerSurfaceBlockReason::SyncControlPlaneMissing,
        );
    }
    if !gate.table_lifecycle_apply_conformance {
        return SyncPromotedLearnerSurfaceDecision::Block(
            SyncPromotedLearnerSurfaceBlockReason::TableLifecycleApplyMissing,
        );
    }
    if !gate.item_put_delete_apply_conformance {
        return SyncPromotedLearnerSurfaceDecision::Block(
            SyncPromotedLearnerSurfaceBlockReason::ItemPutDeleteApplyMissing,
        );
    }
    if !gate.durable_revision_apply_conformance {
        return SyncPromotedLearnerSurfaceDecision::Block(
            SyncPromotedLearnerSurfaceBlockReason::DurableRevisionApplyMissing,
        );
    }
    if !gate.stream_apply_conformance {
        return SyncPromotedLearnerSurfaceDecision::Block(
            SyncPromotedLearnerSurfaceBlockReason::StreamApplyMissing,
        );
    }
    if !gate.ttl_apply_conformance {
        return SyncPromotedLearnerSurfaceDecision::Block(
            SyncPromotedLearnerSurfaceBlockReason::TtlApplyMissing,
        );
    }
    if !gate.gsi_apply_conformance {
        return SyncPromotedLearnerSurfaceDecision::Block(
            SyncPromotedLearnerSurfaceBlockReason::GsiApplyMissing,
        );
    }
    if !gate.sync_control_plane_apply_conformance {
        return SyncPromotedLearnerSurfaceDecision::Block(
            SyncPromotedLearnerSurfaceBlockReason::SyncControlPlaneApplyMissing,
        );
    }
    if !gate.replay_idempotency_conformance {
        return SyncPromotedLearnerSurfaceDecision::Block(
            SyncPromotedLearnerSurfaceBlockReason::ReplayIdempotencyMissing,
        );
    }
    SyncPromotedLearnerSurfaceDecision::Allow
}
