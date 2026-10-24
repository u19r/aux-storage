use serde::Serialize;
use storage_types::{IndexName, IndexStatus, TableName};

/// The externally visible lifecycle of one global secondary index.
///
/// Providers load this from their durable GSI build state.
#[derive(Serialize)]
pub struct GsiIndexReadiness {
    pub index_status: IndexStatus,
    pub backfilling: bool,
}

pub struct GsiReadinessRecord {
    pub table_name: TableName,
    pub index_name: IndexName,
    pub readiness: GsiIndexReadiness,
}

pub struct GsiReadinessSnapshot(pub(super) Vec<GsiReadinessRecord>);
