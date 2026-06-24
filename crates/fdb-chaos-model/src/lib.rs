//! Deterministic model and artifact types for FoundationDB chaos workloads.

mod anomaly;
mod background_lease;
mod constants;
mod gsi;
mod history;
mod shared_key;
mod simulation_metadata;
mod table;
mod trim;

#[cfg(test)]
mod lib_tests;

pub use anomaly::{Anomaly, AnomalyKind, AnomalyReport};
pub use background_lease::{
    BackgroundLeaseCheckReport, BackgroundLeaseEvent, BackgroundLeaseEventKind,
    check_background_lease_events,
};
pub use constants::ARTIFACT_SCHEMA_VERSION;
pub use gsi::{GsiEntry, GsiIndexModel};
pub use history::{
    HistoryEvent, OperationHistory, OperationKind, OperationOutcome, classify_operation_error,
};
pub use shared_key::{
    SharedKeyAudit, SharedKeyCheckReport, SharedKeyRead, check_shared_key_audits,
};
pub use simulation_metadata::{SimulationRunMetadata, SimulationRunMetadataInput};
pub use table::{PossibleTableModel, TableModel};
pub use trim::{
    AggregateTrimCheckReport, TrimProviderSnapshot, TrimScopeExpectation, TrimScopeKind,
    TrimScopeReport, TrimStateModel, check_aggregate_trim_scopes,
};
