//! Retention keys are independent of live table-name bindings.

use storage_provider::{StreamTrimDueMarker, StreamTrimScope, StreamTrimScopeKind};
use storage_types::{StorageError, StorageResult, TimestampMillis, storage_serde};

use crate::{
    constants::{TRIM_DUE_FAMILY, TRIM_STATE_FAMILY},
    range::prefix_upper_bound,
};

pub(crate) fn state_key(scope: &StreamTrimScope) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + scope.scope_id.len());
    key.push(TRIM_STATE_FAMILY);
    key.extend_from_slice(&scope_key(scope));
    key
}

pub(super) fn due_key(marker: &StreamTrimDueMarker) -> Vec<u8> {
    let scope = scope_key(&marker.scope);
    let mut key = Vec::with_capacity(1 + 8 + scope.len() + 8);
    key.push(TRIM_DUE_FAMILY);
    key.extend_from_slice(&ordered_timestamp(*marker.due_bucket));
    key.extend_from_slice(&scope);
    key.extend_from_slice(&marker.policy_version.to_be_bytes());
    key
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

pub(super) fn decode_due_marker(key: &[u8], bytes: &[u8]) -> StorageResult<StreamTrimDueMarker> {
    let marker: StreamTrimDueMarker = storage_serde::from_bytes(bytes)?;
    if key != due_key(&marker) {
        return Err(StorageError::internal(
            "KV stream trim due key differs from marker",
        ));
    }
    Ok(marker)
}

fn scope_key(scope: &StreamTrimScope) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + scope.scope_id.len());
    key.push(match scope.kind {
        StreamTrimScopeKind::Table => b't',
        StreamTrimScopeKind::Item => b'i',
    });
    key.extend_from_slice(scope.scope_id.as_bytes());
    key
}

fn ordered_timestamp(timestamp: i64) -> [u8; 8] {
    (timestamp as u64 ^ (1u64 << 63)).to_be_bytes()
}
