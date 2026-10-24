//! Atomic receipt and minimum frontier writes.

use storage_types::{
    ReadProtection, StorageEnum, StorageError, StorageResult, SystemStreamCheckpoint,
    SystemStreamPosition, storage_serde,
};
use stream_provider::SystemStreamCheckpointIssue;

use super::key_layout;
use crate::{KvAttempt, KvMutation};

pub(crate) async fn write_checkpoint<A: KvAttempt>(
    attempt: &mut A,
    issue: &SystemStreamCheckpointIssue,
    checkpoint: &SystemStreamCheckpoint,
) -> StorageResult<()> {
    let token_key = key_layout::token(&issue.issued_token);
    let operation_key = key_layout::operation(&issue.operation_key);
    let frontier = checkpoint
        .frontier()
        .iter()
        .map(|position| {
            Ok((
                key_layout::frontier(checkpoint.scope(), position)?,
                position,
            ))
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let mut keys = vec![token_key.clone(), operation_key.clone()];
    keys.extend(frontier.iter().map(|(key, _)| key.clone()));
    let values = attempt.get_batch(keys, ReadProtection::Conflict).await?;
    if values.len() != frontier.len() + 2 {
        return Err(StorageError::internal(
            "KV checkpoint read batch count differs",
        ));
    }
    let mut values = values.into_iter();
    if values.by_ref().take(2).any(|value| value.is_some()) {
        return Err(StorageEnum::TransactionConflict {
            message: "SystemStream checkpoint was issued concurrently".to_owned(),
        }
        .into());
    }
    let receipt = issue.receipt(checkpoint);
    let value = storage_serde::to_bytes(&receipt)?;
    let mut changes = vec![
        KvMutation::Put {
            key: token_key,
            value: value.clone(),
        },
        KvMutation::Put {
            key: operation_key,
            value,
        },
    ];
    for ((key, position), value) in frontier.into_iter().zip(values) {
        let previous = value
            .map(|bytes| storage_serde::from_bytes::<SystemStreamPosition>(&bytes))
            .transpose()?;
        let order = position
            .offset()
            .order()
            .map_err(|_| StorageEnum::HistoryUnavailable)?;
        let previous_order = previous
            .as_ref()
            .map(|previous| {
                previous
                    .offset()
                    .order()
                    .map_err(|_| StorageEnum::HistoryUnavailable)
            })
            .transpose()?;
        if previous_order.is_none_or(|previous_order| order < previous_order) {
            changes.push(KvMutation::Put {
                key,
                value: storage_serde::to_bytes(position)?,
            });
        }
    }
    attempt.stage(changes)
}
