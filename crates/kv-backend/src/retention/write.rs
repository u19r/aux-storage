//! Retention state and conditional marker completion on one KV attempt.

use storage_provider::{
    StreamTrimDueMarker, StreamTrimStateRecord, StreamTrimStateWrite,
    maintenance::{PreparedRetentionFinish, RetentionStateWrite},
    table_retention_scope_id,
};
use storage_types::{StorageError, StorageResult, storage_serde};

use super::key_layout;
use crate::{KvAttempt, KvMutation, ReadProtection, catalog::KvTableIdentity};

pub async fn write_stream_retention<A: KvAttempt>(
    attempt: &mut A,
    identity: &KvTableIdentity,
    write: StreamTrimStateWrite,
) -> StorageResult<()> {
    attempt.stage(stream_retention_mutations(identity, write)?)
}

pub(crate) async fn delete_table_retention<A: KvAttempt>(
    attempt: &mut A,
    scope: storage_provider::StreamTrimScope,
) -> StorageResult<()> {
    let states = super::read::load_stream_trim_states(
        attempt,
        std::slice::from_ref(&scope),
        ReadProtection::Conflict,
    )
    .await?;
    // One scope in, one state out.
    let state: Option<storage_provider::StreamTrimState> = states.into_iter().next().flatten();
    let mut changes = vec![KvMutation::Delete {
        key: key_layout::require_state_key(&scope)?,
    }];
    if let Some((key, _)) = state
        .as_ref()
        .map(key_layout::derived_due_entry)
        .transpose()?
        .flatten()
    {
        changes.push(KvMutation::Delete { key });
    }
    attempt.stage(changes)
}

pub(crate) fn stream_retention_mutations(
    identity: &KvTableIdentity,
    write: StreamTrimStateWrite,
) -> StorageResult<Vec<KvMutation>> {
    if write.state.scope.table_name != identity.table_name
        || write.state.scope.scope_id != table_retention_scope_id(identity.table_id.into())
    {
        return Err(StorageError::internal(
            "KV retention scope has another table identity",
        ));
    }
    let state_key = key_layout::require_state_key(&write.state.scope)?;
    let mut changes = vec![KvMutation::Put {
        key: state_key,
        value: storage_serde::to_bytes(&write.state)?,
    }];
    if let Some(marker) = write.replaced_marker {
        changes.push(KvMutation::Delete {
            key: key_layout::due_key(&marker)?,
        });
    }
    if let Some(marker) = write.next_marker {
        if marker.scope != write.state.scope {
            return Err(StorageError::internal(
                "KV retention marker has another scope",
            ));
        }
        changes.push(KvMutation::Put {
            key: key_layout::due_key(&marker)?,
            value: key_layout::due_value(&marker),
        });
    }
    Ok(changes)
}

/// Read each marker and expected state with conflict protection, then stage
/// the delete of each marker that is present with the same payload and whose
/// state matches. One `get_batch`.
pub(crate) async fn delete_stream_trim_markers<A: KvAttempt>(
    attempt: &mut A,
    finishes: &[PreparedRetentionFinish],
) -> StorageResult<storage_types::Exact<bool>> {
    let mut keys = finishes
        .iter()
        .map(|finish| key_layout::due_key(&finish.marker))
        .collect::<StorageResult<Vec<_>>>()?;
    keys.extend(
        finishes
            .iter()
            .filter(|finish| finish.record.expected_state_blob.is_some())
            .map(|finish| key_layout::require_state_key(&finish.marker.scope))
            .collect::<StorageResult<Vec<_>>>()?,
    );
    let values = attempt
        .get_batch_exact(keys, ReadProtection::Conflict)
        .await?;
    let mut values = values.into_iter();
    let due_values = values.by_ref().take(finishes.len()).collect::<Vec<_>>();
    let mut state_values = values;
    let mut completed = Vec::with_capacity(finishes.len());
    let mut deletes = Vec::new();
    for (finish, due_value) in finishes.iter().zip(due_values) {
        let state_matches = match &finish.record.expected_state_blob {
            Some(expected) => {
                state_values
                    .next()
                    .ok_or_else(|| StorageError::internal("KV stream trim finish state is absent"))?
                    .as_deref()
                    == Some(expected.as_slice())
            }
            None => true,
        };
        if let Some(bytes) = due_value.as_ref()
            && bytes != &key_layout::due_value(&finish.marker)
        {
            return Err(StorageError::internal(
                "KV stream trim marker payload differs",
            ));
        }
        let deleted = due_value.is_some() && state_matches;
        if deleted {
            deletes.push(KvMutation::Delete {
                key: key_layout::due_key(&finish.marker)?,
            });
        }
        completed.push(deleted);
    }
    if !deletes.is_empty() {
        attempt.stage(deletes)?;
    }
    storage_types::Exact::try_new(finishes.len(), completed)
}

/// Stage each state put and its next marker put.
pub(crate) fn store_stream_trim_states<A: KvAttempt>(
    attempt: &mut A,
    states: Vec<RetentionStateWrite>,
) -> StorageResult<()> {
    let mut changes = Vec::new();
    for RetentionStateWrite { scope, record } in states {
        let StreamTrimStateRecord {
            state_blob,
            next_marker,
            ..
        } = record;
        changes.push(KvMutation::Put {
            key: key_layout::require_state_key(&scope)?,
            value: state_blob,
        });
        if let Some(next) = next_marker {
            let marker: StreamTrimDueMarker = next.try_into()?;
            changes.push(KvMutation::Put {
                key: key_layout::due_key(&marker)?,
                value: key_layout::due_value(&marker),
            });
        }
    }
    attempt.stage(changes)
}
