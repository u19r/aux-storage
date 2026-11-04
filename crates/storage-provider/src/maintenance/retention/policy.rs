use storage_types::{
    ItemHistoryAddress, StorageError, StorageResult, StreamName, StreamRetentionDuration,
    TableHistoryId, TimestampMillis, UpdateTableRequest,
};
use stream_provider::StreamRowKind;

use super::{
    StreamTrimDueMarker, StreamTrimScope, StreamTrimState, StreamTrimStateWrite,
    TableHistoryTrimFrontier,
};

/// One stable table-incarnation namespace across SQL and KV retention layouts.
pub fn table_retention_scope_id(history_id: TableHistoryId) -> String {
    format!("table:{:032x}", u128::from_be_bytes(*history_id.as_ref()))
}

/// Resolve a persisted scope to the typed physical stream used by both backend
/// families.
pub fn stream_retention_kind(scope: &StreamTrimScope) -> StorageResult<StreamRowKind> {
    let id = decode_history_id(&scope.scope_id, "table:")?;
    let address = ItemHistoryAddress {
        history_id: id,
        stream_name: StreamName::table_stream(&scope.table_name),
    };
    if table_retention_scope_id(id) != scope.scope_id {
        return Err(StorageError::internal(
            "table retention scope is not canonical",
        ));
    }
    Ok(StreamRowKind::Table(address.physical_stream_name()?))
}

fn decode_history_id(scope_id: &str, prefix: &str) -> StorageResult<TableHistoryId> {
    let hex = scope_id
        .strip_prefix(prefix)
        .ok_or_else(|| StorageError::internal("stream retention scope is invalid"))?;
    let bytes: [u8; 16] = hex::decode(hex)
        .map_err(|_| StorageError::internal("stream retention history is invalid"))?
        .try_into()
        .map_err(|_| StorageError::internal("stream retention history is invalid"))?;
    Ok(TableHistoryId::from(bytes))
}

pub fn plan_table_stream_duration(
    scope: StreamTrimScope,
    frontier: TableHistoryTrimFrontier,
    retention: StreamRetentionDuration,
    updated_at: TimestampMillis,
) -> StorageResult<StreamTrimStateWrite> {
    if scope.scope_id != table_retention_scope_id(frontier.table_history_id) {
        return Err(StorageError::internal(
            "Table retention frontier has another identity",
        ));
    }
    let policy_version = frontier.table_policy_version.get();
    let next_due_at = next_due_for_retention(updated_at, retention);
    let trim_state = StreamTrimState {
        scope: scope.clone(),
        policy_version,
        frontier,
        retention,
        next_due_at,
        updated_at,
    };
    trim_state.require_valid_frontier()?;
    let due_marker =
        next_due_at.map(|due_at| StreamTrimDueMarker::new(due_at, scope, policy_version));

    Ok(StreamTrimStateWrite {
        state: trim_state,
        next_marker: due_marker,
        replaced_marker: None,
    })
}

pub fn update_table_request_has_custom_stream_duration(request: &UpdateTableRequest) -> bool {
    request.aux_stream_duration_hours.is_some()
}

fn next_due_for_retention(
    updated_at: TimestampMillis,
    retention: StreamRetentionDuration,
) -> Option<TimestampMillis> {
    match retention {
        StreamRetentionDuration::Forever => None,
        StreamRetentionDuration::FiniteHours(hours) => {
            Some(updated_at + (i64::from(hours) * 60 * 60 * 1000))
        }
    }
}
