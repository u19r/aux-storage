#![allow(non_snake_case)]

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;

use crate::{
    SyncBackendPairDecision, SyncPromotedLearnerSurfaceBlockReason,
    SyncPromotedLearnerSurfaceDecision, SyncPromotedLearnerSurfaceGate,
    plan_promoted_learner_storage_surface,
};

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
struct SurfaceGate {
    #[serde(rename = "backendDecision")]
    backend_decision: BackendDecisionName,
    #[serde(rename = "learnerPromoted")]
    learner_promoted: bool,
    #[serde(rename = "tableMetadataImported")]
    table_metadata_imported: bool,
    #[serde(rename = "itemRecordsImported")]
    item_records_imported: bool,
    #[serde(rename = "indexerMetadataImported")]
    indexer_metadata_imported: bool,
    #[serde(rename = "durableRevisionsImported")]
    durable_revisions_imported: bool,
    #[serde(rename = "streamRecordsImported")]
    stream_records_imported: bool,
    #[serde(rename = "ttlRecordsImported")]
    ttl_records_imported: bool,
    #[serde(rename = "gsiRecordsImported")]
    gsi_records_imported: bool,
    #[serde(rename = "storageControlPlaneImported")]
    storage_control_plane_imported: bool,
    #[serde(rename = "syncControlPlaneImported")]
    sync_control_plane_imported: bool,
    #[serde(rename = "tableLifecycleApplyConformance")]
    table_lifecycle_apply_conformance: bool,
    #[serde(rename = "itemPutDeleteApplyConformance")]
    item_put_delete_apply_conformance: bool,
    #[serde(rename = "durableRevisionApplyConformance")]
    durable_revision_apply_conformance: bool,
    #[serde(rename = "streamApplyConformance")]
    stream_apply_conformance: bool,
    #[serde(rename = "ttlApplyConformance")]
    ttl_apply_conformance: bool,
    #[serde(rename = "gsiApplyConformance")]
    gsi_apply_conformance: bool,
    #[serde(rename = "syncControlPlaneApplyConformance")]
    sync_control_plane_apply_conformance: bool,
    #[serde(rename = "replayIdempotencyConformance")]
    replay_idempotency_conformance: bool,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum BackendDecisionName {
    ProductionSupported,
    ValidationOnly,
    Rejected,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct PromotedLearnerSurfaceState {
    #[serde(rename = "lastGate")]
    last_gate: SurfaceGate,
    #[serde(rename = "lastDecision")]
    last_decision: String,
}

impl State<PromotedLearnerSurfaceDriver> for PromotedLearnerSurfaceState {
    fn from_driver(driver: &PromotedLearnerSurfaceDriver) -> Result<Self> {
        Ok(Self {
            last_gate: driver.last_gate,
            last_decision: driver.last_decision.clone(),
        })
    }
}

#[derive(Debug)]
struct PromotedLearnerSurfaceDriver {
    last_gate: SurfaceGate,
    last_decision: String,
}

impl Default for PromotedLearnerSurfaceDriver {
    fn default() -> Self {
        Self {
            last_gate: SurfaceGate {
                backend_decision: BackendDecisionName::Rejected,
                learner_promoted: false,
                table_metadata_imported: false,
                item_records_imported: false,
                indexer_metadata_imported: false,
                durable_revisions_imported: false,
                stream_records_imported: false,
                ttl_records_imported: false,
                gsi_records_imported: false,
                storage_control_plane_imported: false,
                sync_control_plane_imported: false,
                table_lifecycle_apply_conformance: false,
                item_put_delete_apply_conformance: false,
                durable_revision_apply_conformance: false,
                stream_apply_conformance: false,
                ttl_apply_conformance: false,
                gsi_apply_conformance: false,
                sync_control_plane_apply_conformance: false,
                replay_idempotency_conformance: false,
            },
            last_decision: "not_checked".to_string(),
        }
    }
}

impl Driver for PromotedLearnerSurfaceDriver {
    type State = PromotedLearnerSurfaceState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            Check(
                backendDecision: BackendDecisionName,
                learnerPromoted: bool,
                tableMetadataImported: bool,
                itemRecordsImported: bool,
                indexerMetadataImported: bool,
                durableRevisionsImported: bool,
                streamRecordsImported: bool,
                ttlRecordsImported: bool,
                gsiRecordsImported: bool,
                storageControlPlaneImported: bool,
                syncControlPlaneImported: bool,
                tableLifecycleApplyConformance: bool,
                itemPutDeleteApplyConformance: bool,
                durableRevisionApplyConformance: bool,
                streamApplyConformance: bool,
                ttlApplyConformance: bool,
                gsiApplyConformance: bool,
                syncControlPlaneApplyConformance: bool,
                replayIdempotencyConformance: bool,
            ) => {
                self.check(SurfaceGate {
                    backend_decision: backendDecision,
                    learner_promoted: learnerPromoted,
                    table_metadata_imported: tableMetadataImported,
                    item_records_imported: itemRecordsImported,
                    indexer_metadata_imported: indexerMetadataImported,
                    durable_revisions_imported: durableRevisionsImported,
                    stream_records_imported: streamRecordsImported,
                    ttl_records_imported: ttlRecordsImported,
                    gsi_records_imported: gsiRecordsImported,
                    storage_control_plane_imported: storageControlPlaneImported,
                    sync_control_plane_imported: syncControlPlaneImported,
                    table_lifecycle_apply_conformance: tableLifecycleApplyConformance,
                    item_put_delete_apply_conformance: itemPutDeleteApplyConformance,
                    durable_revision_apply_conformance: durableRevisionApplyConformance,
                    stream_apply_conformance: streamApplyConformance,
                    ttl_apply_conformance: ttlApplyConformance,
                    gsi_apply_conformance: gsiApplyConformance,
                    sync_control_plane_apply_conformance: syncControlPlaneApplyConformance,
                    replay_idempotency_conformance: replayIdempotencyConformance,
                });
            },
            step(
                backendDecision: BackendDecisionName?,
                learnerPromoted: bool?,
                tableMetadataImported: bool?,
                itemRecordsImported: bool?,
                indexerMetadataImported: bool?,
                durableRevisionsImported: bool?,
                streamRecordsImported: bool?,
                ttlRecordsImported: bool?,
                gsiRecordsImported: bool?,
                storageControlPlaneImported: bool?,
                syncControlPlaneImported: bool?,
                tableLifecycleApplyConformance: bool?,
                itemPutDeleteApplyConformance: bool?,
                durableRevisionApplyConformance: bool?,
                streamApplyConformance: bool?,
                ttlApplyConformance: bool?,
                gsiApplyConformance: bool?,
                syncControlPlaneApplyConformance: bool?,
                replayIdempotencyConformance: bool?,
            ) => {
                if let (
                    Some(backend_decision),
                    Some(learner_promoted),
                    Some(table_metadata_imported),
                    Some(item_records_imported),
                    Some(indexer_metadata_imported),
                    Some(durable_revisions_imported),
                    Some(stream_records_imported),
                    Some(ttl_records_imported),
                    Some(gsi_records_imported),
                    Some(storage_control_plane_imported),
                    Some(sync_control_plane_imported),
                    Some(table_lifecycle_apply_conformance),
                    Some(item_put_delete_apply_conformance),
                    Some(durable_revision_apply_conformance),
                    Some(stream_apply_conformance),
                    Some(ttl_apply_conformance),
                    Some(gsi_apply_conformance),
                    Some(sync_control_plane_apply_conformance),
                    Some(replay_idempotency_conformance),
                ) = (
                    backendDecision,
                    learnerPromoted,
                    tableMetadataImported,
                    itemRecordsImported,
                    indexerMetadataImported,
                    durableRevisionsImported,
                    streamRecordsImported,
                    ttlRecordsImported,
                    gsiRecordsImported,
                    storageControlPlaneImported,
                    syncControlPlaneImported,
                    tableLifecycleApplyConformance,
                    itemPutDeleteApplyConformance,
                    durableRevisionApplyConformance,
                    streamApplyConformance,
                    ttlApplyConformance,
                    gsiApplyConformance,
                    syncControlPlaneApplyConformance,
                    replayIdempotencyConformance,
                ) {
                    self.check(SurfaceGate {
                        backend_decision,
                        learner_promoted,
                        table_metadata_imported,
                        item_records_imported,
                        indexer_metadata_imported,
                        durable_revisions_imported,
                        stream_records_imported,
                        ttl_records_imported,
                        gsi_records_imported,
                        storage_control_plane_imported,
                        sync_control_plane_imported,
                        table_lifecycle_apply_conformance,
                        item_put_delete_apply_conformance,
                        durable_revision_apply_conformance,
                        stream_apply_conformance,
                        ttl_apply_conformance,
                        gsi_apply_conformance,
                        sync_control_plane_apply_conformance,
                        replay_idempotency_conformance,
                    });
                }
            },
        })
    }
}

impl PromotedLearnerSurfaceDriver {
    fn check(&mut self, gate: SurfaceGate) {
        self.last_decision = decision_name(plan_promoted_learner_storage_surface(
            SyncPromotedLearnerSurfaceGate {
                backend_pair: backend_decision(gate.backend_decision),
                learner_promoted: gate.learner_promoted,
                table_metadata_imported: gate.table_metadata_imported,
                item_records_imported: gate.item_records_imported,
                indexer_metadata_imported: gate.indexer_metadata_imported,
                durable_revisions_imported: gate.durable_revisions_imported,
                stream_records_imported: gate.stream_records_imported,
                ttl_records_imported: gate.ttl_records_imported,
                gsi_records_imported: gate.gsi_records_imported,
                storage_control_plane_imported: gate.storage_control_plane_imported,
                sync_control_plane_imported: gate.sync_control_plane_imported,
                table_lifecycle_apply_conformance: gate.table_lifecycle_apply_conformance,
                item_put_delete_apply_conformance: gate.item_put_delete_apply_conformance,
                durable_revision_apply_conformance: gate.durable_revision_apply_conformance,
                stream_apply_conformance: gate.stream_apply_conformance,
                ttl_apply_conformance: gate.ttl_apply_conformance,
                gsi_apply_conformance: gate.gsi_apply_conformance,
                sync_control_plane_apply_conformance: gate.sync_control_plane_apply_conformance,
                replay_idempotency_conformance: gate.replay_idempotency_conformance,
            },
        ))
        .to_string();
        self.last_gate = gate;
    }
}

fn backend_decision(decision: BackendDecisionName) -> SyncBackendPairDecision {
    match decision {
        BackendDecisionName::ProductionSupported => SyncBackendPairDecision::ProductionSupported,
        BackendDecisionName::ValidationOnly => SyncBackendPairDecision::ValidationOnly,
        BackendDecisionName::Rejected => SyncBackendPairDecision::Rejected,
    }
}

fn decision_name(decision: SyncPromotedLearnerSurfaceDecision) -> &'static str {
    match decision {
        SyncPromotedLearnerSurfaceDecision::Allow => "allow",
        SyncPromotedLearnerSurfaceDecision::Block(reason) => match reason {
            SyncPromotedLearnerSurfaceBlockReason::BackendPairRejected => "backend_pair_rejected",
            SyncPromotedLearnerSurfaceBlockReason::LearnerNotPromoted => "learner_not_promoted",
            SyncPromotedLearnerSurfaceBlockReason::TableMetadataMissing => "table_metadata_missing",
            SyncPromotedLearnerSurfaceBlockReason::ItemRecordsMissing => "item_records_missing",
            SyncPromotedLearnerSurfaceBlockReason::IndexerMetadataMissing => {
                "indexer_metadata_missing"
            }
            SyncPromotedLearnerSurfaceBlockReason::DurableRevisionsMissing => {
                "durable_revisions_missing"
            }
            SyncPromotedLearnerSurfaceBlockReason::StreamRecordsMissing => "stream_records_missing",
            SyncPromotedLearnerSurfaceBlockReason::TtlRecordsMissing => "ttl_records_missing",
            SyncPromotedLearnerSurfaceBlockReason::GsiRecordsMissing => "gsi_records_missing",
            SyncPromotedLearnerSurfaceBlockReason::StorageControlPlaneMissing => {
                "storage_control_plane_missing"
            }
            SyncPromotedLearnerSurfaceBlockReason::SyncControlPlaneMissing => {
                "sync_control_plane_missing"
            }
            SyncPromotedLearnerSurfaceBlockReason::TableLifecycleApplyMissing => {
                "table_lifecycle_apply_missing"
            }
            SyncPromotedLearnerSurfaceBlockReason::ItemPutDeleteApplyMissing => {
                "item_put_delete_apply_missing"
            }
            SyncPromotedLearnerSurfaceBlockReason::DurableRevisionApplyMissing => {
                "durable_revision_apply_missing"
            }
            SyncPromotedLearnerSurfaceBlockReason::StreamApplyMissing => "stream_apply_missing",
            SyncPromotedLearnerSurfaceBlockReason::TtlApplyMissing => "ttl_apply_missing",
            SyncPromotedLearnerSurfaceBlockReason::GsiApplyMissing => "gsi_apply_missing",
            SyncPromotedLearnerSurfaceBlockReason::SyncControlPlaneApplyMissing => {
                "sync_control_plane_apply_missing"
            }
            SyncPromotedLearnerSurfaceBlockReason::ReplayIdempotencyMissing => {
                "replay_idempotency_missing"
            }
        },
    }
}

#[quint_run(
    spec = "../../quint/sync_promoted_learner_surface_mbt.qnt",
    max_samples = 128,
    max_steps = 8,
    seed = "0x51a7e"
)]
fn sync_promoted_learner_surface_mbt_matches_rust_boundary() -> impl Driver {
    PromotedLearnerSurfaceDriver::default()
}

#[test]
fn promoted_learner_surface_blocks_each_late_missing_domain_or_conformance_gate() {
    let cases = [
        (
            SyncPromotedLearnerSurfaceGate {
                storage_control_plane_imported: false,
                ..ready_promoted_learner_surface_gate()
            },
            SyncPromotedLearnerSurfaceBlockReason::StorageControlPlaneMissing,
        ),
        (
            SyncPromotedLearnerSurfaceGate {
                sync_control_plane_imported: false,
                ..ready_promoted_learner_surface_gate()
            },
            SyncPromotedLearnerSurfaceBlockReason::SyncControlPlaneMissing,
        ),
        (
            SyncPromotedLearnerSurfaceGate {
                table_lifecycle_apply_conformance: false,
                ..ready_promoted_learner_surface_gate()
            },
            SyncPromotedLearnerSurfaceBlockReason::TableLifecycleApplyMissing,
        ),
        (
            SyncPromotedLearnerSurfaceGate {
                item_put_delete_apply_conformance: false,
                ..ready_promoted_learner_surface_gate()
            },
            SyncPromotedLearnerSurfaceBlockReason::ItemPutDeleteApplyMissing,
        ),
        (
            SyncPromotedLearnerSurfaceGate {
                durable_revision_apply_conformance: false,
                ..ready_promoted_learner_surface_gate()
            },
            SyncPromotedLearnerSurfaceBlockReason::DurableRevisionApplyMissing,
        ),
        (
            SyncPromotedLearnerSurfaceGate {
                stream_apply_conformance: false,
                ..ready_promoted_learner_surface_gate()
            },
            SyncPromotedLearnerSurfaceBlockReason::StreamApplyMissing,
        ),
        (
            SyncPromotedLearnerSurfaceGate {
                ttl_apply_conformance: false,
                ..ready_promoted_learner_surface_gate()
            },
            SyncPromotedLearnerSurfaceBlockReason::TtlApplyMissing,
        ),
        (
            SyncPromotedLearnerSurfaceGate {
                gsi_apply_conformance: false,
                ..ready_promoted_learner_surface_gate()
            },
            SyncPromotedLearnerSurfaceBlockReason::GsiApplyMissing,
        ),
        (
            SyncPromotedLearnerSurfaceGate {
                sync_control_plane_apply_conformance: false,
                ..ready_promoted_learner_surface_gate()
            },
            SyncPromotedLearnerSurfaceBlockReason::SyncControlPlaneApplyMissing,
        ),
        (
            SyncPromotedLearnerSurfaceGate {
                replay_idempotency_conformance: false,
                ..ready_promoted_learner_surface_gate()
            },
            SyncPromotedLearnerSurfaceBlockReason::ReplayIdempotencyMissing,
        ),
    ];

    for (gate, expected_reason) in cases {
        assert_eq!(
            plan_promoted_learner_storage_surface(gate),
            SyncPromotedLearnerSurfaceDecision::Block(expected_reason)
        );
    }
}

fn ready_promoted_learner_surface_gate() -> SyncPromotedLearnerSurfaceGate {
    SyncPromotedLearnerSurfaceGate {
        backend_pair: SyncBackendPairDecision::ValidationOnly,
        learner_promoted: true,
        table_metadata_imported: true,
        item_records_imported: true,
        indexer_metadata_imported: true,
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
