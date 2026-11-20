//! Physical KV keys for one shared SystemStream split decision.

use futures::future::try_join;
use storage_types::{ReadProtection, StorageEnum, StorageError, StorageResult, storage_serde};
use stream_provider::{
    SystemStreamHistory, SystemStreamSplitOperationId, SystemStreamSplitPlan,
    SystemStreamSplitReceipt,
};

use crate::{
    KvAttempt, KvMutation,
    constants::{
        SYSTEM_STREAM_CATALOG_EPOCH_KEY, SYSTEM_STREAM_LINEAGE_PREFIX,
        SYSTEM_STREAM_SPLIT_RECEIPT_PREFIX,
    },
    stream_history::{
        LineageRow, decode_lineage_row, encode_lineage, encode_native_sealed_lineage,
        read_history_protected, segmented_key,
    },
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
        .map(|bytes| -> StorageResult<SystemStreamSplitReceipt> {
            match storage_serde::from_bytes::<SystemStreamSplitPlan>(&bytes)? {
                SystemStreamSplitPlan::KnownEnd(receipt) => {
                    if history.uses_commit_versionstamps() {
                        return Err(unavailable());
                    }
                    Ok(receipt)
                }
                SystemStreamSplitPlan::EndAtCommit(split) => {
                    if !history.uses_commit_versionstamps() {
                        return Err(unavailable());
                    }
                    let end = history
                        .sealed_end(split.sealed_parent().lineage_id())
                        .map_err(|_| unavailable())?;
                    Ok(SystemStreamSplitReceipt {
                        split,
                        sealed_after_offset: end.get(),
                    })
                }
            }
        })
        .transpose()?;
    Ok((history, receipt))
}

pub(crate) async fn write_split<A: KvAttempt>(
    attempt: &mut A,
    plan: &SystemStreamSplitPlan,
) -> StorageResult<()> {
    let is_native = matches!(plan, SystemStreamSplitPlan::EndAtCommit(_));
    if is_native
        != matches!(
            attempt.backend_engine(),
            storage_common::diagnostics::NativeEngine::FoundationDb
        )
    {
        return Err(unavailable());
    }
    let split = plan.split();
    let scope = split.source_scope();
    let parent = split.sealed_parent();
    let parent_key = segmented_key(SYSTEM_STREAM_LINEAGE_PREFIX, parent.lineage_id().as_str())?;
    let receipt_key = receipt_key(split.operation_id())?;
    let mut keys = vec![
        SYSTEM_STREAM_CATALOG_EPOCH_KEY.to_vec(),
        parent_key.clone(),
        receipt_key.clone(),
    ];
    let mut child_keys = Vec::with_capacity(2);
    for child in split.children() {
        let lineage = segmented_key(
            SYSTEM_STREAM_LINEAGE_PREFIX,
            child.lineage().lineage_id().as_str(),
        )?;
        keys.push(lineage.clone());
        child_keys.push(lineage);
    }
    let count = keys.len();
    let values = attempt.get_batch(keys, ReadProtection::Conflict).await?;
    if values.len() != count {
        return Err(unavailable());
    }
    let mut values = values.into_iter();
    let epoch = values.next().ok_or_else(unavailable)?;
    let parent_bytes = values.next().ok_or_else(unavailable)?;
    if values.next().ok_or_else(unavailable)?.is_some() {
        return Err(unavailable());
    }
    if values.any(|value| value.is_some()) {
        return Err(unavailable());
    }
    let epoch = epoch.ok_or_else(unavailable)?;
    let epoch = u64::from_be_bytes(epoch.try_into().map_err(|_| unavailable())?);
    let next_epoch = epoch
        .checked_add(1)
        .filter(|value| *value != 0)
        .ok_or_else(unavailable)?;
    let parent_bytes = parent_bytes.ok_or_else(unavailable)?;
    let mut row = decode_lineage_row(&parent_bytes)?;
    let known_end = match plan {
        SystemStreamSplitPlan::KnownEnd(receipt) => Some(receipt.sealed_after_offset),
        SystemStreamSplitPlan::EndAtCommit(_) => None,
    };
    if let Some(end) = known_end {
        let snapshot = attempt
            .native_snapshot_sequence()
            .await?
            .ok_or_else(unavailable)?;
        if row.initial_sequence.get() > end || snapshot.checked_add(1) != Some(end) {
            return Err(unavailable());
        }
    }
    if row.scope_id != scope.scope_id().as_str()
        || row.generation != scope.generation()
        || row.shard_id != parent.shard_id().as_str()
        || row.lineage_id != parent.lineage_id().as_str()
        || row.parent_lineage_id.as_deref() != parent.parent_lineage_id().map(|id| id.as_str())
        || row.sealed
        || row.sealed_after_offset.is_some()
    {
        return Err(unavailable());
    }
    row.sealed = true;
    row.sealed_after_offset = known_end;
    attempt
        .root_attempt()
        .native_record_catalog_change()
        .await?;
    if is_native {
        attempt.stage_versionstamped_value(
            parent_key.clone(),
            encode_native_sealed_lineage(&row)?,
            1,
        )?;
    }
    let mut changes = vec![
        KvMutation::Put {
            key: receipt_key,
            value: storage_serde::to_bytes(plan)?,
        },
        KvMutation::Put {
            key: SYSTEM_STREAM_CATALOG_EPOCH_KEY.to_vec(),
            value: next_epoch.to_be_bytes().to_vec(),
        },
    ];
    if !is_native {
        changes.push(KvMutation::Put {
            key: parent_key,
            value: encode_lineage(&row)?,
        });
    }
    for (child, lineage) in split.children().iter().zip(child_keys) {
        changes.push(KvMutation::Put {
            key: lineage,
            value: encode_lineage(&LineageRow {
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
    }
    attempt.stage(changes)
}

fn unavailable() -> StorageError {
    StorageEnum::HistoryUnavailable.into()
}
