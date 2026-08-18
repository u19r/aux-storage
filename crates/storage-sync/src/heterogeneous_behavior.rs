use storage_backfill::{
    LogicalBootstrapPreflightCase, LogicalBootstrapPreflightDecision,
    plan_logical_bootstrap_preflight,
};

use crate::{
    SyncBackendPairDecision, SyncMultiRegionInteropDecision, SyncNonSqlBackend,
    SyncNonSqlResolvedApplyDecision, SyncNonSqlResolvedApplyGate, SyncRaftRole,
    plan_non_sql_resolved_apply, plan_promoted_learner_storage_surface,
    plan_sync_backend_pair_detailed, plan_sync_multi_region_interop,
    promoted_learner_surface::{
        SyncPromotedLearnerSurfaceDecision, SyncPromotedLearnerSurfaceGate,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncHeterogeneousBehaviorGate<'a> {
    pub source_backend: &'a str,
    pub destination_backend: &'a str,
    pub logical_snapshot_domains_complete: bool,
    pub resolved_apply_complete: bool,
    pub learner_promoted: bool,
    pub bootstrap_destination_empty: bool,
    pub bootstrap_preflight_marker_present: bool,
    pub sync_role: SyncRaftRole,
    pub current_version: u64,
    pub incoming_version: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncHeterogeneousBehaviorPlan {
    pub backend_pair: SyncBackendPairDecision,
    pub logical_snapshot: SyncHeterogeneousLogicalSnapshotDecision,
    pub resolved_apply: SyncNonSqlResolvedApplyDecision,
    pub promoted_surface: SyncPromotedLearnerSurfaceDecision,
    pub bootstrap_preflight: LogicalBootstrapPreflightDecision,
    pub multi_region_interop: SyncMultiRegionInteropDecision,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncHeterogeneousLogicalSnapshotDecision {
    Allow,
    Block(SyncHeterogeneousLogicalSnapshotBlockReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncHeterogeneousLogicalSnapshotBlockReason {
    BackendPairRejected,
    DomainsIncomplete,
}

#[must_use]
pub fn plan_sync_heterogeneous_behavior(
    gate: SyncHeterogeneousBehaviorGate<'_>,
) -> SyncHeterogeneousBehaviorPlan {
    let backend_pair =
        plan_sync_backend_pair_detailed(gate.source_backend, gate.destination_backend).decision;
    let logical_snapshot =
        plan_logical_snapshot(backend_pair, gate.logical_snapshot_domains_complete);
    let resolved_apply = plan_non_sql_resolved_apply(resolved_apply_gate(
        backend_pair,
        gate.destination_backend,
        gate.resolved_apply_complete,
    ));
    let promoted_surface = plan_promoted_learner_storage_surface(promoted_surface_gate(
        backend_pair,
        gate.logical_snapshot_domains_complete,
        gate.resolved_apply_complete,
        gate.learner_promoted,
    ));
    let bootstrap_preflight = plan_logical_bootstrap_preflight(LogicalBootstrapPreflightCase {
        destination_empty: gate.bootstrap_destination_empty,
        preflight_marker_present: gate.bootstrap_preflight_marker_present,
    });
    let multi_region_interop = plan_sync_multi_region_interop(
        &gate.sync_role,
        gate.current_version,
        gate.incoming_version,
    );

    SyncHeterogeneousBehaviorPlan {
        backend_pair,
        logical_snapshot,
        resolved_apply,
        promoted_surface,
        bootstrap_preflight,
        multi_region_interop,
    }
}

const fn plan_logical_snapshot(
    backend_pair: SyncBackendPairDecision,
    domains_complete: bool,
) -> SyncHeterogeneousLogicalSnapshotDecision {
    if matches!(backend_pair, SyncBackendPairDecision::Rejected) {
        return SyncHeterogeneousLogicalSnapshotDecision::Block(
            SyncHeterogeneousLogicalSnapshotBlockReason::BackendPairRejected,
        );
    }
    if !domains_complete {
        return SyncHeterogeneousLogicalSnapshotDecision::Block(
            SyncHeterogeneousLogicalSnapshotBlockReason::DomainsIncomplete,
        );
    }
    SyncHeterogeneousLogicalSnapshotDecision::Allow
}

fn resolved_apply_gate(
    backend_pair: SyncBackendPairDecision,
    destination_backend: &str,
    complete: bool,
) -> SyncNonSqlResolvedApplyGate {
    SyncNonSqlResolvedApplyGate {
        backend_pair,
        destination_backend: sync_non_sql_backend(destination_backend),
        table_lifecycle_apply: complete,
        item_put_delete_apply: complete,
        durable_revision_apply: complete,
        stream_apply: complete,
        ttl_apply: complete,
        gsi_apply: complete,
        sync_control_plane_apply: complete,
        log_entry_persistence: complete,
        replay_idempotency: complete,
    }
}

fn promoted_surface_gate(
    backend_pair: SyncBackendPairDecision,
    domains_complete: bool,
    resolved_apply_complete: bool,
    learner_promoted: bool,
) -> SyncPromotedLearnerSurfaceGate {
    SyncPromotedLearnerSurfaceGate {
        backend_pair,
        learner_promoted,
        table_metadata_imported: domains_complete,
        item_records_imported: domains_complete,
        indexer_metadata_imported: domains_complete,
        durable_revisions_imported: domains_complete,
        stream_records_imported: domains_complete,
        ttl_records_imported: domains_complete,
        gsi_records_imported: domains_complete,
        storage_control_plane_imported: domains_complete,
        sync_control_plane_imported: domains_complete,
        table_lifecycle_apply_conformance: resolved_apply_complete,
        item_put_delete_apply_conformance: resolved_apply_complete,
        durable_revision_apply_conformance: resolved_apply_complete,
        stream_apply_conformance: resolved_apply_complete,
        ttl_apply_conformance: resolved_apply_complete,
        gsi_apply_conformance: resolved_apply_complete,
        sync_control_plane_apply_conformance: resolved_apply_complete,
        replay_idempotency_conformance: resolved_apply_complete,
    }
}

fn sync_non_sql_backend(backend: &str) -> SyncNonSqlBackend {
    match backend {
        "rocksdb" => SyncNonSqlBackend::RocksDb,
        "foundationdb" => SyncNonSqlBackend::FoundationDb,
        "postgres" => SyncNonSqlBackend::Postgres,
        "turso" => SyncNonSqlBackend::Turso,
        "sqlite" => SyncNonSqlBackend::Sqlite,
        "remote" => SyncNonSqlBackend::Remote,
        _ => SyncNonSqlBackend::Unknown,
    }
}
