//! Bounded retention discovery and protected wave observations.

use std::num::NonZeroU32;

use storage_provider::{
    StreamTrimDueMarker, StreamTrimScope, StreamTrimState, maintenance::StreamRetentionObservation,
    table_retention_scope_id,
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
    if scope.table_name != identity.table_name
        || scope.scope_id != table_retention_scope_id(identity.table_id.into())
    {
        return Err(StorageError::internal(
            "KV table retention scope is invalid",
        ));
    }
    attempt
        .get(
            &key_layout::require_state_key(scope)?,
            ReadProtection::Conflict,
        )
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
    let keys = scopes.iter().map(key_layout::state_key).collect::<Vec<_>>();
    let present = keys.iter().flatten().cloned().collect::<Vec<_>>();
    let values = attempt.get_batch_exact(present, protection).await?;
    let mut values = values.into_iter();
    scopes
        .iter()
        .zip(keys)
        .map(|(scope, key)| match key {
            None => Ok(None),
            Some(_) => values
                .next()
                .flatten()
                .map(|bytes| decode_trim_state(&bytes, scope))
                .transpose(),
        })
        .collect()
}

pub(crate) async fn load_stream_trim_observations<A: KvAttempt>(
    attempt: &mut A,
    markers: &[StreamTrimDueMarker],
) -> StorageResult<storage_types::Exact<StreamRetentionObservation>> {
    let mut keys = markers
        .iter()
        .map(key_layout::due_key)
        .collect::<StorageResult<Vec<_>>>()?;
    keys.extend(
        markers
            .iter()
            .map(|marker| key_layout::require_state_key(&marker.scope))
            .collect::<StorageResult<Vec<_>>>()?,
    );
    let values = attempt
        .get_batch_exact(keys, ReadProtection::Conflict)
        .await?;
    let (present, states) = values.as_slice().split_at(markers.len());
    let observations = markers
        .iter()
        .zip(present)
        .zip(states)
        .map(|((marker, value), state)| {
            if let Some(bytes) = value
                && bytes.as_slice() != key_layout::due_value(marker).as_slice()
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
        .collect::<StorageResult<Vec<_>>>()?;
    storage_types::Exact::try_new(markers.len(), observations)
}

pub(crate) fn decode_trim_state(
    bytes: &[u8],
    scope: &StreamTrimScope,
) -> StorageResult<StreamTrimState> {
    let state: StreamTrimState = storage_serde::from_bytes(bytes)?;
    state.require_valid_frontier()?;
    if state.scope != *scope {
        return Err(StorageError::internal(
            "KV stream trim state has another scope",
        ));
    }
    Ok(state)
}
