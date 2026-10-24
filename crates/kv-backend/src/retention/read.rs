//! Bounded retention discovery and protected wave observations.

use std::num::NonZeroU32;

use storage_provider::{
    StreamTrimDueMarker, StreamTrimScope, StreamTrimScopeKind, StreamTrimState,
    maintenance::StreamRetentionObservation, table_retention_scope_id,
};
use storage_types::{StorageError, StorageResult, TimestampMillis, storage_serde};

use super::key_layout;
use crate::{
    KvAttempt, KvRange, ReadProtection, catalog::KvTableIdentity, constants::RETENTION_READ_BYTES,
};

pub(crate) async fn load_table_retention<A: KvAttempt>(
    attempt: &mut A,
    identity: &KvTableIdentity,
    scope: &StreamTrimScope,
) -> StorageResult<Option<StreamTrimState>> {
    if scope.kind != StreamTrimScopeKind::Table
        || scope.table_name != identity.table_name
        || scope.scope_id != table_retention_scope_id(identity.table_id.into())
    {
        return Err(StorageError::internal(
            "KV table retention scope is invalid",
        ));
    }
    attempt
        .get(&key_layout::state_key(scope), ReadProtection::Conflict)
        .await?
        .map(|bytes| decode_trim_state(&bytes, scope))
        .transpose()
}

pub(crate) async fn list_due_stream_trim_markers<A: KvAttempt>(
    attempt: &mut A,
    due_before: TimestampMillis,
    limit: usize,
) -> StorageResult<Vec<StreamTrimDueMarker>> {
    let Some(limit) = NonZeroU32::new(
        u32::try_from(limit)
            .map_err(|_| StorageError::validation("stream trim marker page limit is too large"))?,
    ) else {
        return Ok(Vec::new());
    };
    let (begin, end) = key_layout::due_bounds(due_before)?;
    let page = attempt
        .scan(
            KvRange {
                entry_bounds: None,
                begin,
                end,
                reverse: false,
                limit,
                max_bytes: RETENTION_READ_BYTES,
            },
            ReadProtection::Snapshot,
        )
        .await?;
    page.entries
        .into_iter()
        .map(|entry| key_layout::decode_due_marker(&entry.key, &entry.value))
        .collect()
}

pub(crate) async fn load_stream_trim_states<A: KvAttempt>(
    attempt: &mut A,
    scopes: &[StreamTrimScope],
    protection: ReadProtection,
) -> StorageResult<Vec<Option<StreamTrimState>>> {
    let keys = scopes.iter().map(key_layout::state_key).collect();
    let values = attempt.get_batch(keys, protection).await?;
    if values.len() != scopes.len() {
        return Err(StorageError::internal(
            "KV stream trim state batch count differs",
        ));
    }
    scopes
        .iter()
        .zip(values)
        .map(|(scope, value)| {
            value
                .map(|bytes| decode_trim_state(&bytes, scope))
                .transpose()
        })
        .collect()
}

pub(crate) async fn load_stream_trim_observations<A: KvAttempt>(
    attempt: &mut A,
    markers: &[StreamTrimDueMarker],
) -> StorageResult<Vec<StreamRetentionObservation>> {
    let mut keys = markers.iter().map(key_layout::due_key).collect::<Vec<_>>();
    keys.extend(
        markers
            .iter()
            .map(|marker| key_layout::state_key(&marker.scope)),
    );
    let values = attempt.get_batch(keys, ReadProtection::Conflict).await?;
    if values.len() != markers.len() * 2 {
        return Err(StorageError::internal(
            "KV stream trim observation batch count differs",
        ));
    }
    let (present, states) = values.split_at(markers.len());
    markers
        .iter()
        .zip(present)
        .zip(states)
        .map(|((marker, value), state)| {
            if let Some(bytes) = value
                && bytes.as_slice() != storage_serde::to_bytes(marker)?.as_slice()
            {
                return Err(StorageError::internal(
                    "KV stream trim marker payload differs",
                ));
            }
            let state = state
                .as_ref()
                .map(|bytes| decode_trim_state(bytes, &marker.scope))
                .transpose()?;
            Ok((value.is_some(), state).into())
        })
        .collect()
}

pub(crate) fn decode_trim_state(
    bytes: &[u8],
    scope: &StreamTrimScope,
) -> StorageResult<StreamTrimState> {
    let state: StreamTrimState = storage_serde::from_bytes(bytes)?;
    if state.scope != *scope {
        return Err(StorageError::internal(
            "KV stream trim state has another scope",
        ));
    }
    Ok(state)
}
