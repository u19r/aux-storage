//! Retention state and conditional marker completion on one KV attempt.

use storage_provider::{
    StreamRetentionFinishRecord, StreamTrimDueMarker, StreamTrimScopeKind, StreamTrimStateRecord,
    StreamTrimStateWrite, maintenance::StreamRetentionFinish, table_retention_scope_id,
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

pub(crate) fn stream_retention_mutations(
    identity: &KvTableIdentity,
    write: StreamTrimStateWrite,
) -> StorageResult<Vec<KvMutation>> {
    if write.state.scope.table_name != identity.table_name
        || (write.state.scope.kind == StreamTrimScopeKind::Table
            && write.state.scope.scope_id != table_retention_scope_id(identity.table_id.into()))
    {
        return Err(StorageError::internal(
            "KV retention scope has another table identity",
        ));
    }
    let state_key = key_layout::state_key(&write.state.scope);
    let mut changes = vec![KvMutation::Put {
        key: state_key,
        value: storage_serde::to_bytes(&write.state)?,
    }];
    if let Some(marker) = write.next_marker {
        if marker.scope != write.state.scope {
            return Err(StorageError::internal(
                "KV retention marker has another scope",
            ));
        }
        changes.push(KvMutation::Put {
            key: key_layout::due_key(&marker),
            value: storage_serde::to_bytes(&marker)?,
        });
    }
    Ok(changes)
}

pub(crate) async fn finish_stream_trim_markers<A: KvAttempt>(
    attempt: &mut A,
    finishes: Vec<StreamRetentionFinish>,
) -> StorageResult<()> {
    let prepared = finishes
        .into_iter()
        .map(PreparedFinish::try_from)
        .collect::<StorageResult<Vec<_>>>()?;
    let mut keys = prepared
        .iter()
        .map(|finish| key_layout::due_key(&finish.marker))
        .collect::<Vec<_>>();
    let state_count = prepared
        .iter()
        .filter(|finish| finish.record.expected_state_blob.is_some())
        .count();
    keys.extend(
        prepared
            .iter()
            .filter(|finish| finish.record.expected_state_blob.is_some())
            .map(|finish| key_layout::state_key(&finish.marker.scope)),
    );
    let values = attempt.get_batch(keys, ReadProtection::Conflict).await?;
    if values.len() != prepared.len() + state_count {
        return Err(StorageError::internal(
            "KV stream trim finish read count differs",
        ));
    }
    let mut values = values.into_iter();
    let due_values = values.by_ref().take(prepared.len()).collect::<Vec<_>>();
    let mut state_values = values;
    let mut changes = Vec::new();
    for (finish, due_value) in prepared.into_iter().zip(due_values) {
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
            && bytes != &finish.marker_blob
        {
            return Err(StorageError::internal(
                "KV stream trim marker payload differs",
            ));
        }
        if due_value.is_some() && state_matches {
            append_completion(&mut changes, finish)?;
        }
    }
    if changes.is_empty() {
        Ok(())
    } else {
        attempt.stage(changes)
    }
}

struct PreparedFinish {
    marker: StreamTrimDueMarker,
    marker_blob: Vec<u8>,
    record: StreamRetentionFinishRecord,
}

impl TryFrom<StreamRetentionFinish> for PreparedFinish {
    type Error = StorageError;

    fn try_from(finish: StreamRetentionFinish) -> StorageResult<Self> {
        let marker = finish.marker.clone();
        let marker_blob = storage_serde::to_bytes(&marker)?;
        let record: StreamRetentionFinishRecord = finish.try_into()?;
        if !record.valid_update {
            return Err(StorageError::internal(
                "stream trim finish has an invalid state update",
            ));
        }
        Ok(Self {
            marker,
            marker_blob,
            record,
        })
    }
}

fn append_completion(changes: &mut Vec<KvMutation>, finish: PreparedFinish) -> StorageResult<()> {
    changes.push(KvMutation::Delete {
        key: key_layout::due_key(&finish.marker),
    });
    if let Some(write) = finish.record.state_write {
        let StreamTrimStateRecord {
            state_blob,
            next_marker,
            ..
        } = write;
        changes.push(KvMutation::Put {
            key: key_layout::state_key(&finish.marker.scope),
            value: state_blob,
        });
        if let Some(next) = next_marker {
            let marker: StreamTrimDueMarker = next.try_into()?;
            changes.push(KvMutation::Put {
                key: key_layout::due_key(&marker),
                value: storage_serde::to_bytes(&marker)?,
            });
        }
    }
    Ok(())
}
