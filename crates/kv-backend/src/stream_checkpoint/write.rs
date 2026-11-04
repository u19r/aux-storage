//! Atomic checkpoint token and operation receipts.

use storage_types::{
    ReadProtection, StorageEnum, StorageError, StorageResult, SystemStreamCheckpoint, storage_serde,
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
    let keys = vec![token_key.clone(), operation_key.clone()];
    let values = attempt.get_batch(keys, ReadProtection::Conflict).await?;
    if values.len() != 2 {
        return Err(StorageError::internal(
            "KV checkpoint read batch count differs",
        ));
    }
    if values.into_iter().any(|value| value.is_some()) {
        return Err(StorageEnum::TransactionConflict {
            message: "SystemStream checkpoint was issued concurrently".to_owned(),
        }
        .into());
    }
    let receipt = issue.receipt(checkpoint);
    let value = storage_serde::to_bytes(&receipt)?;
    let changes = vec![
        KvMutation::Put {
            key: token_key,
            value: value.clone(),
        },
        KvMutation::Put {
            key: operation_key,
            value,
        },
    ];
    attempt.stage(changes)
}
