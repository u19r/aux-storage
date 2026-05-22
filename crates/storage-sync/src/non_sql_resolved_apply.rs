use crate::SyncBackendPairDecision;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncNonSqlResolvedApplyGate {
    pub backend_pair: SyncBackendPairDecision,
    pub destination_backend: SyncNonSqlBackend,
    pub table_lifecycle_apply: bool,
    pub item_put_delete_apply: bool,
    pub durable_revision_apply: bool,
    pub stream_apply: bool,
    pub ttl_apply: bool,
    pub gsi_apply: bool,
    pub sync_control_plane_apply: bool,
    pub log_entry_persistence: bool,
    pub replay_idempotency: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncNonSqlBackend {
    RocksDb,
    FoundationDb,
    Postgres,
    Turso,
    Sqlite,
    Remote,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncNonSqlResolvedApplyDecision {
    Allow,
    Block(SyncNonSqlResolvedApplyBlockReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncNonSqlResolvedApplyBlockReason {
    BackendPairRejected,
    DestinationNotNonSql,
    TableLifecycleApplyMissing,
    ItemPutDeleteApplyMissing,
    DurableRevisionApplyMissing,
    StreamApplyMissing,
    TtlApplyMissing,
    GsiApplyMissing,
    SyncControlPlaneApplyMissing,
    LogEntryPersistenceMissing,
    ReplayIdempotencyMissing,
}

#[must_use]
pub const fn plan_non_sql_resolved_apply(
    gate: SyncNonSqlResolvedApplyGate,
) -> SyncNonSqlResolvedApplyDecision {
    if matches!(gate.backend_pair, SyncBackendPairDecision::Rejected) {
        return SyncNonSqlResolvedApplyDecision::Block(
            SyncNonSqlResolvedApplyBlockReason::BackendPairRejected,
        );
    }
    if !matches!(
        gate.destination_backend,
        SyncNonSqlBackend::RocksDb
            | SyncNonSqlBackend::FoundationDb
            | SyncNonSqlBackend::Postgres
            | SyncNonSqlBackend::Turso
    ) {
        return SyncNonSqlResolvedApplyDecision::Block(
            SyncNonSqlResolvedApplyBlockReason::DestinationNotNonSql,
        );
    }
    if !gate.table_lifecycle_apply {
        return SyncNonSqlResolvedApplyDecision::Block(
            SyncNonSqlResolvedApplyBlockReason::TableLifecycleApplyMissing,
        );
    }
    if !gate.item_put_delete_apply {
        return SyncNonSqlResolvedApplyDecision::Block(
            SyncNonSqlResolvedApplyBlockReason::ItemPutDeleteApplyMissing,
        );
    }
    if !gate.durable_revision_apply {
        return SyncNonSqlResolvedApplyDecision::Block(
            SyncNonSqlResolvedApplyBlockReason::DurableRevisionApplyMissing,
        );
    }
    if !gate.stream_apply {
        return SyncNonSqlResolvedApplyDecision::Block(
            SyncNonSqlResolvedApplyBlockReason::StreamApplyMissing,
        );
    }
    if !gate.ttl_apply {
        return SyncNonSqlResolvedApplyDecision::Block(
            SyncNonSqlResolvedApplyBlockReason::TtlApplyMissing,
        );
    }
    if !gate.gsi_apply {
        return SyncNonSqlResolvedApplyDecision::Block(
            SyncNonSqlResolvedApplyBlockReason::GsiApplyMissing,
        );
    }
    if !gate.sync_control_plane_apply {
        return SyncNonSqlResolvedApplyDecision::Block(
            SyncNonSqlResolvedApplyBlockReason::SyncControlPlaneApplyMissing,
        );
    }
    if !gate.log_entry_persistence {
        return SyncNonSqlResolvedApplyDecision::Block(
            SyncNonSqlResolvedApplyBlockReason::LogEntryPersistenceMissing,
        );
    }
    if !gate.replay_idempotency {
        return SyncNonSqlResolvedApplyDecision::Block(
            SyncNonSqlResolvedApplyBlockReason::ReplayIdempotencyMissing,
        );
    }
    SyncNonSqlResolvedApplyDecision::Allow
}
