//! Physical KV keys for one shared SystemStream split decision.

use futures::future::try_join;
use storage_types::{
    ReadProtection, StorageEnum, StorageError, StorageResult, SystemStreamPosition, storage_serde,
};
use stream_provider::{
    SystemStreamHistory, SystemStreamSplitOperationId, SystemStreamSplitReceipt,
};

use crate::{
    KvAttempt, KvMutation,
    constants::{
        SYSTEM_STREAM_CATALOG_EPOCH_KEY, SYSTEM_STREAM_LINEAGE_PREFIX,
        SYSTEM_STREAM_SEQUENCE_PREFIX, SYSTEM_STREAM_SPLIT_RECEIPT_PREFIX,
    },
    stream_checkpoint::key_layout::frontier,
    stream_history::{LineageRow, SequenceRow, read_history_protected, segmented_key},
};

fn receipt_key(operation_id: &SystemStreamSplitOperationId) -> StorageResult<Vec<u8>> {
    segmented_key(SYSTEM_STREAM_SPLIT_RECEIPT_PREFIX, operation_id.as_str())
}

pub(crate) async fn read_split_state<A: KvAttempt>(
    attempt: &A,
    operation_id: &SystemStreamSplitOperationId,
) -> StorageResult<(SystemStreamHistory, Option<SystemStreamSplitReceipt>)> {
    let key = receipt_key(operation_id)?;
    let ((_, history), receipt) = try_join(
        read_history_protected(attempt),
        attempt.get(&key, ReadProtection::Conflict),
    )
    .await?;
    let receipt = receipt
        .map(|bytes| storage_serde::from_bytes(&bytes))
        .transpose()?;
    Ok((history, receipt))
}

pub(crate) async fn write_split<A: KvAttempt>(
    attempt: &mut A,
    receipt: &SystemStreamSplitReceipt,
) -> StorageResult<()> {
    let native_version = attempt.native_read_version().await?;
    let split = &receipt.split;
    let scope = split.source_scope();
    let parent = split.sealed_parent();
    let parent_position = SystemStreamPosition::try_new(
        parent.shard_id().as_str(),
        parent.lineage_id().as_str(),
        format!("{:020}", receipt.sealed_after_offset - 1),
    )
    .map_err(|_| unavailable())?;
    let parent_key = segmented_key(SYSTEM_STREAM_LINEAGE_PREFIX, parent.lineage_id().as_str())?;
    let sequence_key = segmented_key(SYSTEM_STREAM_SEQUENCE_PREFIX, parent.shard_id().as_str())?;
    let parent_frontier_key = frontier(scope, &parent_position)?;
    let receipt_key = receipt_key(split.operation_id())?;
    let mut keys = vec![
        SYSTEM_STREAM_CATALOG_EPOCH_KEY.to_vec(),
        parent_key.clone(),
        sequence_key,
        parent_frontier_key.clone(),
        receipt_key.clone(),
    ];
    let mut child_keys = Vec::with_capacity(2);
    for child in split.children() {
        let position = child.initial_frontier().map_err(|_| unavailable())?;
        let lineage = segmented_key(
            SYSTEM_STREAM_LINEAGE_PREFIX,
            child.lineage().lineage_id().as_str(),
        )?;
        let sequence = segmented_key(
            SYSTEM_STREAM_SEQUENCE_PREFIX,
            child.lineage().shard_id().as_str(),
        )?;
        let frontier = frontier(scope, &position)?;
        keys.extend([lineage.clone(), sequence.clone(), frontier.clone()]);
        child_keys.push((lineage, sequence, frontier, position));
    }
    let [
        epoch,
        parent_bytes,
        sequence_bytes,
        parent_frontier,
        prior,
        left_lineage,
        left_sequence,
        left_frontier,
        right_lineage,
        right_sequence,
        right_frontier,
    ] = attempt
        .get_batch(keys, ReadProtection::Conflict)
        .await?
        .try_into()
        .map_err(|_| unavailable())?;
    if prior.is_some()
        || [
            left_lineage,
            left_sequence,
            left_frontier,
            right_lineage,
            right_sequence,
            right_frontier,
        ]
        .into_iter()
        .any(|value| value.is_some())
    {
        return Err(unavailable());
    }
    let epoch = epoch.ok_or_else(unavailable)?;
    let epoch = u64::from_be_bytes(epoch.try_into().map_err(|_| unavailable())?);
    let next_epoch = epoch
        .checked_add(1)
        .filter(|value| *value != 0)
        .ok_or_else(unavailable)?;
    let parent_bytes = parent_bytes.ok_or_else(unavailable)?;
    let mut row: LineageRow =
        storage_serde::from_bytes(&parent_bytes).map_err(|_| unavailable())?;
    let sequence = if native_version.is_some() {
        let bytes = sequence_bytes.ok_or_else(unavailable)?;
        let stamp: [u8; 12] = bytes.as_slice().try_into().map_err(|_| unavailable())?;
        let high = u64::from_be_bytes(stamp[..8].try_into().map_err(|_| unavailable())?);
        SequenceRow {
            scope_id: row.scope_id.clone(),
            generation: row.generation,
            shard_id: row.shard_id.clone(),
            next_offset: high
                .checked_add(1)
                .ok_or_else(unavailable)?
                .max(row.initial_sequence.get()),
        }
    } else {
        storage_serde::from_bytes::<SequenceRow>(&sequence_bytes.ok_or_else(unavailable)?)
            .map_err(|_| unavailable())?
    };
    if row.scope_id != scope.scope_id().as_str()
        || row.generation != scope.generation()
        || row.shard_id != parent.shard_id().as_str()
        || row.lineage_id != parent.lineage_id().as_str()
        || row.parent_lineage_id.as_deref() != parent.parent_lineage_id().map(|id| id.as_str())
        || row.sealed
        || row.sealed_after_offset.is_some()
        || row.initial_sequence.get() > receipt.sealed_after_offset
        || sequence.scope_id != row.scope_id
        || sequence.generation != row.generation
        || sequence.shard_id != row.shard_id
        || sequence.next_offset != receipt.sealed_after_offset
    {
        return Err(unavailable());
    }
    let propagate_frontier = if let Some(bytes) = parent_frontier {
        let position: SystemStreamPosition =
            storage_serde::from_bytes(&bytes).map_err(|_| unavailable())?;
        if frontier(scope, &position)? != parent_frontier_key {
            return Err(unavailable());
        }
        true
    } else {
        false
    };
    row.sealed = true;
    row.sealed_after_offset = Some(receipt.sealed_after_offset);
    let mut changes = vec![
        KvMutation::Put {
            key: parent_key,
            value: storage_serde::to_bytes(&row)?,
        },
        KvMutation::Put {
            key: receipt_key,
            value: storage_serde::to_bytes(receipt)?,
        },
        KvMutation::Put {
            key: SYSTEM_STREAM_CATALOG_EPOCH_KEY.to_vec(),
            value: next_epoch.to_be_bytes().to_vec(),
        },
    ];
    for (child, (lineage, sequence, frontier_key, position)) in
        split.children().iter().zip(child_keys)
    {
        changes.push(KvMutation::Put {
            key: lineage,
            value: storage_serde::to_bytes(&LineageRow {
                scope_id: row.scope_id.clone(),
                generation: row.generation,
                shard_id: child.lineage().shard_id().as_str().to_owned(),
                lineage_id: child.lineage().lineage_id().as_str().to_owned(),
                parent_lineage_id: Some(parent.lineage_id().as_str().to_owned()),
                sealed: false,
                initial_sequence: child.initial_sequence(),
                sealed_after_offset: None,
            })?,
        });
        changes.push(KvMutation::Put {
            key: sequence,
            value: if native_version.is_some() {
                vec![0; 12]
            } else {
                storage_serde::to_bytes(&SequenceRow {
                    scope_id: row.scope_id.clone(),
                    generation: row.generation,
                    shard_id: child.lineage().shard_id().as_str().to_owned(),
                    next_offset: child.initial_sequence().get(),
                })?
            },
        });
        if propagate_frontier {
            changes.push(KvMutation::Put {
                key: frontier_key,
                value: storage_serde::to_bytes(&position)?,
            });
        }
    }
    attempt.stage(changes)
}

fn unavailable() -> StorageError {
    StorageEnum::HistoryUnavailable.into()
}
