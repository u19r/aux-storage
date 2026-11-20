//! Retention keys are independent of live table-name bindings.

use storage_provider::{StreamTrimDueMarker, StreamTrimScope, StreamTrimState};
use storage_types::{StorageError, StorageResult, TableHistoryId, TableName, TimestampMillis};

use crate::{
    constants::TRIM_DUE_FAMILY,
    range::prefix_upper_bound,
    table_key_layout::{TableStreamFamily, table_stream_prefix},
};

/// The KV table that owns a retention scope; `None` for a scope no KV table
/// owns.
fn scope_table_id(scope: &StreamTrimScope) -> Option<u32> {
    let hex = scope.scope_id.strip_prefix("table:")?;
    if hex.len() != 32 {
        return None;
    }
    let history = TableHistoryId::from(u128::from_str_radix(hex, 16).ok()?.to_be_bytes());
    if storage_provider::table_retention_scope_id(history) != scope.scope_id {
        return None;
    }
    crate::catalog::table_id_from_history(history).ok()
}

/// `root | table_id | TrimState`; `None` for a scope that no KV table owns (it
/// has no state).
pub(crate) fn state_key(scope: &StreamTrimScope) -> Option<Vec<u8>> {
    scope_table_id(scope).map(|id| table_stream_prefix(id, TableStreamFamily::TrimState).to_vec())
}

pub(crate) fn require_state_key(scope: &StreamTrimScope) -> StorageResult<Vec<u8>> {
    state_key(scope).ok_or_else(|| StorageError::internal("KV retention scope has no table"))
}

/// `d | ordered(due_at) | table_id u32 BE | policy_version u64 BE`. A derived
/// work index: the table's `TrimState` is the authority for its schedule,
/// and restore re-writes this entry from it (`derived_due_entry`).
const DUE_KEY_BYTES: usize = 1 + 8 + 4 + 8;

pub(crate) fn due_key(marker: &StreamTrimDueMarker) -> StorageResult<Vec<u8>> {
    let table_id = scope_table_id(&marker.scope)
        .ok_or_else(|| StorageError::internal("KV retention scope has no table"))?;
    let mut key = Vec::with_capacity(DUE_KEY_BYTES);
    key.push(TRIM_DUE_FAMILY);
    key.extend_from_slice(&ordered_timestamp(*marker.due_at));
    key.extend_from_slice(&table_id.to_be_bytes());
    key.extend_from_slice(&marker.policy_version.to_be_bytes());
    Ok(key)
}

/// The entry value: the table name, so discovery builds the marker from one
/// range read.
pub(crate) fn due_value(marker: &StreamTrimDueMarker) -> Vec<u8> {
    marker.scope.table_name.as_ref().as_bytes().to_vec()
}

/// The due entry a stored state schedules; `None` when it schedules nothing.
pub(crate) fn derived_due_entry(
    state: &StreamTrimState,
) -> StorageResult<Option<(Vec<u8>, Vec<u8>)>> {
    state
        .scheduled_marker()
        .map(|marker| Ok((due_key(&marker)?, due_value(&marker))))
        .transpose()
}

pub(super) fn due_bounds(due_before: TimestampMillis) -> StorageResult<(Vec<u8>, Vec<u8>)> {
    let begin = vec![TRIM_DUE_FAMILY];
    let end = if *due_before == i64::MAX {
        prefix_upper_bound(&begin)
            .ok_or_else(|| StorageError::internal("KV stream trim family has no upper bound"))?
    } else {
        let mut end = begin.clone();
        end.extend_from_slice(&ordered_timestamp(*due_before + 1));
        end
    };
    Ok((begin, end))
}

pub(super) fn decode_due_marker(key: &[u8], value: &[u8]) -> StorageResult<StreamTrimDueMarker> {
    let invalid = || StorageError::internal("KV stream trim due key is invalid");
    if key.len() != DUE_KEY_BYTES || key[0] != TRIM_DUE_FAMILY {
        return Err(invalid());
    }
    let ordered = u64::from_be_bytes(key[1..9].try_into().map_err(|_| invalid())?);
    let table_id = u32::from_be_bytes(key[9..13].try_into().map_err(|_| invalid())?);
    let policy_version = u64::from_be_bytes(key[13..21].try_into().map_err(|_| invalid())?);
    let table_name = std::str::from_utf8(value)
        .ok()
        .and_then(|name| TableName::try_from(name).ok())
        .ok_or_else(invalid)?;
    Ok(StreamTrimDueMarker {
        due_at: TimestampMillis::from_timestamp((ordered ^ (1u64 << 63)) as i64),
        scope: StreamTrimScope {
            scope_id: storage_provider::table_retention_scope_id(TableHistoryId::from(table_id)),
            table_name,
        },
        policy_version,
    })
}

fn ordered_timestamp(timestamp: i64) -> [u8; 8] {
    (timestamp as u64 ^ (1u64 << 63)).to_be_bytes()
}
