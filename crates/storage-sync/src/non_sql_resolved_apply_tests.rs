use crate::{
    SyncBackendPairDecision, SyncNonSqlBackend, SyncNonSqlResolvedApplyBlockReason,
    SyncNonSqlResolvedApplyDecision, SyncNonSqlResolvedApplyGate, plan_non_sql_resolved_apply,
};

#[test]
fn non_sql_resolved_apply_allows_rocksdb_after_full_surface_is_present() {
    assert_eq!(
        plan_non_sql_resolved_apply(complete_gate()),
        SyncNonSqlResolvedApplyDecision::Allow
    );
}

#[test]
fn non_sql_resolved_apply_rejects_sqlite_destination_for_phase8_surface() {
    assert_eq!(
        plan_non_sql_resolved_apply(SyncNonSqlResolvedApplyGate {
            destination_backend: SyncNonSqlBackend::Sqlite,
            ..complete_gate()
        }),
        SyncNonSqlResolvedApplyDecision::Block(
            SyncNonSqlResolvedApplyBlockReason::DestinationNotNonSql
        )
    );
}

#[test]
fn non_sql_resolved_apply_requires_replay_idempotency() {
    assert_eq!(
        plan_non_sql_resolved_apply(SyncNonSqlResolvedApplyGate {
            replay_idempotency: false,
            ..complete_gate()
        }),
        SyncNonSqlResolvedApplyDecision::Block(
            SyncNonSqlResolvedApplyBlockReason::ReplayIdempotencyMissing
        )
    );
}

fn complete_gate() -> SyncNonSqlResolvedApplyGate {
    SyncNonSqlResolvedApplyGate {
        backend_pair: SyncBackendPairDecision::ValidationOnly,
        destination_backend: SyncNonSqlBackend::RocksDb,
        table_lifecycle_apply: true,
        item_put_delete_apply: true,
        durable_revision_apply: true,
        stream_apply: true,
        ttl_apply: true,
        gsi_apply: true,
        sync_control_plane_apply: true,
        log_entry_persistence: true,
        replay_idempotency: true,
    }
}
