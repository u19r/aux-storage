#![allow(non_snake_case)]

use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;

use crate::{
    SyncBackendPairDecision, SyncNonSqlBackend, SyncNonSqlResolvedApplyBlockReason,
    SyncNonSqlResolvedApplyDecision, SyncNonSqlResolvedApplyGate, plan_non_sql_resolved_apply,
};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct NonSqlResolvedApplyGate {
    #[serde(rename = "backendDecision")]
    backend_decision: String,
    #[serde(rename = "destinationBackend")]
    destination_backend: String,
    #[serde(rename = "tableLifecycleApply")]
    table_lifecycle_apply: bool,
    #[serde(rename = "itemPutDeleteApply")]
    item_put_delete_apply: bool,
    #[serde(rename = "durableRevisionApply")]
    durable_revision_apply: bool,
    #[serde(rename = "streamApply")]
    stream_apply: bool,
    #[serde(rename = "ttlApply")]
    ttl_apply: bool,
    #[serde(rename = "gsiApply")]
    gsi_apply: bool,
    #[serde(rename = "syncControlPlaneApply")]
    sync_control_plane_apply: bool,
    #[serde(rename = "logEntryPersistence")]
    log_entry_persistence: bool,
    #[serde(rename = "replayIdempotency")]
    replay_idempotency: bool,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct NonSqlResolvedApplyState {
    #[serde(rename = "lastGate")]
    last_gate: NonSqlResolvedApplyGate,
    #[serde(rename = "lastDecision")]
    last_decision: String,
}

impl State<NonSqlResolvedApplyDriver> for NonSqlResolvedApplyState {
    fn from_driver(driver: &NonSqlResolvedApplyDriver) -> Result<Self> {
        Ok(Self {
            last_gate: driver.last_gate.clone(),
            last_decision: driver.last_decision.clone(),
        })
    }
}

#[derive(Debug)]
struct NonSqlResolvedApplyDriver {
    last_gate: NonSqlResolvedApplyGate,
    last_decision: String,
}

impl Default for NonSqlResolvedApplyDriver {
    fn default() -> Self {
        Self {
            last_gate: NonSqlResolvedApplyGate {
                backend_decision: "rejected".to_string(),
                destination_backend: String::new(),
                table_lifecycle_apply: false,
                item_put_delete_apply: false,
                durable_revision_apply: false,
                stream_apply: false,
                ttl_apply: false,
                gsi_apply: false,
                sync_control_plane_apply: false,
                log_entry_persistence: false,
                replay_idempotency: false,
            },
            last_decision: "not_checked".to_string(),
        }
    }
}

impl Driver for NonSqlResolvedApplyDriver {
    type State = NonSqlResolvedApplyState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            Check(
                backendDecision: String,
                destinationBackend: String,
                tableLifecycleApply: bool,
                itemPutDeleteApply: bool,
                durableRevisionApply: bool,
                streamApply: bool,
                ttlApply: bool,
                gsiApply: bool,
                syncControlPlaneApply: bool,
                logEntryPersistence: bool,
                replayIdempotency: bool,
            ) => {
                self.check(NonSqlResolvedApplyGate {
                    backend_decision: backendDecision,
                    destination_backend: destinationBackend,
                    table_lifecycle_apply: tableLifecycleApply,
                    item_put_delete_apply: itemPutDeleteApply,
                    durable_revision_apply: durableRevisionApply,
                    stream_apply: streamApply,
                    ttl_apply: ttlApply,
                    gsi_apply: gsiApply,
                    sync_control_plane_apply: syncControlPlaneApply,
                    log_entry_persistence: logEntryPersistence,
                    replay_idempotency: replayIdempotency,
                });
            },
            step(
                backendDecision: String?,
                destinationBackend: String?,
                tableLifecycleApply: bool?,
                itemPutDeleteApply: bool?,
                durableRevisionApply: bool?,
                streamApply: bool?,
                ttlApply: bool?,
                gsiApply: bool?,
                syncControlPlaneApply: bool?,
                logEntryPersistence: bool?,
                replayIdempotency: bool?,
            ) => {
                if let (
                    Some(backend_decision),
                    Some(destination_backend),
                    Some(table_lifecycle_apply),
                    Some(item_put_delete_apply),
                    Some(durable_revision_apply),
                    Some(stream_apply),
                    Some(ttl_apply),
                    Some(gsi_apply),
                    Some(sync_control_plane_apply),
                    Some(log_entry_persistence),
                    Some(replay_idempotency),
                ) = (
                    backendDecision,
                    destinationBackend,
                    tableLifecycleApply,
                    itemPutDeleteApply,
                    durableRevisionApply,
                    streamApply,
                    ttlApply,
                    gsiApply,
                    syncControlPlaneApply,
                    logEntryPersistence,
                    replayIdempotency,
                ) {
                    self.check(NonSqlResolvedApplyGate {
                        backend_decision,
                        destination_backend,
                        table_lifecycle_apply,
                        item_put_delete_apply,
                        durable_revision_apply,
                        stream_apply,
                        ttl_apply,
                        gsi_apply,
                        sync_control_plane_apply,
                        log_entry_persistence,
                        replay_idempotency,
                    });
                }
            },
        })
    }
}

impl NonSqlResolvedApplyDriver {
    fn check(&mut self, gate: NonSqlResolvedApplyGate) {
        self.last_decision =
            decision_name(plan_non_sql_resolved_apply(SyncNonSqlResolvedApplyGate {
                backend_pair: backend_decision(&gate.backend_decision),
                destination_backend: destination_backend(&gate.destination_backend),
                table_lifecycle_apply: gate.table_lifecycle_apply,
                item_put_delete_apply: gate.item_put_delete_apply,
                durable_revision_apply: gate.durable_revision_apply,
                stream_apply: gate.stream_apply,
                ttl_apply: gate.ttl_apply,
                gsi_apply: gate.gsi_apply,
                sync_control_plane_apply: gate.sync_control_plane_apply,
                log_entry_persistence: gate.log_entry_persistence,
                replay_idempotency: gate.replay_idempotency,
            }))
            .to_string();
        self.last_gate = gate;
    }
}

fn backend_decision(name: &str) -> SyncBackendPairDecision {
    match name {
        "production_supported" => SyncBackendPairDecision::ProductionSupported,
        "validation_only" => SyncBackendPairDecision::ValidationOnly,
        _ => SyncBackendPairDecision::Rejected,
    }
}

fn destination_backend(name: &str) -> SyncNonSqlBackend {
    match name {
        "rocksdb" => SyncNonSqlBackend::RocksDb,
        "foundationdb" => SyncNonSqlBackend::FoundationDb,
        "postgres" => SyncNonSqlBackend::Postgres,
        "turso" => SyncNonSqlBackend::Turso,
        "sqlite" => SyncNonSqlBackend::Sqlite,
        "remote" => SyncNonSqlBackend::Remote,
        _ => SyncNonSqlBackend::Unknown,
    }
}

fn decision_name(decision: SyncNonSqlResolvedApplyDecision) -> &'static str {
    match decision {
        SyncNonSqlResolvedApplyDecision::Allow => "allow",
        SyncNonSqlResolvedApplyDecision::Block(reason) => match reason {
            SyncNonSqlResolvedApplyBlockReason::BackendPairRejected => "backend_pair_rejected",
            SyncNonSqlResolvedApplyBlockReason::DestinationNotNonSql => "destination_not_non_sql",
            SyncNonSqlResolvedApplyBlockReason::TableLifecycleApplyMissing => {
                "table_lifecycle_apply_missing"
            }
            SyncNonSqlResolvedApplyBlockReason::ItemPutDeleteApplyMissing => {
                "item_put_delete_apply_missing"
            }
            SyncNonSqlResolvedApplyBlockReason::DurableRevisionApplyMissing => {
                "durable_revision_apply_missing"
            }
            SyncNonSqlResolvedApplyBlockReason::StreamApplyMissing => "stream_apply_missing",
            SyncNonSqlResolvedApplyBlockReason::TtlApplyMissing => "ttl_apply_missing",
            SyncNonSqlResolvedApplyBlockReason::GsiApplyMissing => "gsi_apply_missing",
            SyncNonSqlResolvedApplyBlockReason::SyncControlPlaneApplyMissing => {
                "sync_control_plane_apply_missing"
            }
            SyncNonSqlResolvedApplyBlockReason::LogEntryPersistenceMissing => {
                "log_entry_persistence_missing"
            }
            SyncNonSqlResolvedApplyBlockReason::ReplayIdempotencyMissing => {
                "replay_idempotency_missing"
            }
        },
    }
}

#[quint_run(
    spec = "../../quint/sync_non_sql_resolved_apply_mbt.qnt",
    max_samples = 128,
    max_steps = 8,
    seed = "0x5178a8"
)]
fn sync_non_sql_resolved_apply_mbt_matches_rust_boundary() -> impl Driver {
    NonSqlResolvedApplyDriver::default()
}
