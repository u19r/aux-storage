//! Repeated receipt actions observe earlier actions on the same native attempt.

use serde::{Deserialize, Serialize};
use storage_types::{StorageError, StorageResult, TimestampMillis};

use crate::{
    QueueAttempt, QueueError, QueueIncarnation, QueueMessageRecord, QueueOperation, QueueResult,
    QueueValidationKind, ReceiptHandle,
};

#[derive(Clone, Serialize, Deserialize)]
pub enum QueueReceiptAction {
    Delete,
    Visibility(TimestampMillis),
    Checkpoint(String),
}

impl QueueReceiptAction {
    fn updated_record(self, previous: &QueueMessageRecord) -> Option<QueueMessageRecord> {
        match self {
            Self::Delete => None,
            Self::Visibility(visible_at) => {
                let mut updated = previous.clone();
                updated.message.visibility_timestamp = Some(visible_at);
                Some(updated)
            }
            Self::Checkpoint(data) => {
                let mut updated = previous.clone();
                updated.checkpoint_data = Some(data);
                Some(updated)
            }
        }
    }
}

impl<A: QueueAttempt> QueueOperation<'_, A> {
    pub async fn apply_receipts(
        self,
        entries: Vec<(ReceiptHandle, QueueReceiptAction)>,
    ) -> StorageResult<Vec<QueueResult<()>>> {
        let results = apply_receipt_waves(self.attempt, self.record.incarnation, entries).await?;
        Ok(results
            .into_iter()
            .map(|found| {
                if found {
                    Ok(())
                } else {
                    Err(QueueError::validation(
                        QueueValidationKind::MessageNotFoundOrAlreadyProcessed,
                    ))
                }
            })
            .collect())
    }
}

async fn apply_receipt_waves<A: QueueAttempt>(
    attempt: &mut A,
    incarnation: QueueIncarnation,
    entries: Vec<(ReceiptHandle, QueueReceiptAction)>,
) -> StorageResult<Vec<bool>> {
    let mut pending = entries.into_iter().peekable();
    let mut results = Vec::new();
    while pending.peek().is_some() {
        let mut wave = Vec::new();
        while let Some((receipt, _)) = pending.peek() {
            if wave
                .iter()
                .any(|(seen, _): &(ReceiptHandle, QueueReceiptAction)| seen.0 == receipt.0)
            {
                break;
            }
            let Some(action) = pending.next() else {
                return Err(StorageError::internal(
                    "peeked queue receipt action is missing",
                ));
            };
            wave.push(action);
        }
        // Repeated receipts observe effects from earlier waves on this attempt.
        results.extend(apply_receipt_batch(attempt, incarnation, wave).await?);
    }
    Ok(results)
}

async fn apply_receipt_batch<A: QueueAttempt>(
    attempt: &mut A,
    incarnation: QueueIncarnation,
    entries: Vec<(ReceiptHandle, QueueReceiptAction)>,
) -> StorageResult<Vec<bool>> {
    let rows = attempt
        .read_messages_by_receipt(
            incarnation,
            entries.iter().map(|(receipt, _)| receipt.clone()).collect(),
        )
        .await?;
    if rows.len() != entries.len() {
        return Err(StorageError::internal(
            "queue receipt batch read count differs",
        ));
    }
    let mut results = Vec::with_capacity(entries.len());
    let mut deletes = Vec::new();
    let mut writes = Vec::new();
    for ((receipt, action), previous) in entries.into_iter().zip(rows) {
        if !receipt.has_valid_shape() {
            results.push(false);
            continue;
        }
        let Some(previous) = previous else {
            results.push(false);
            continue;
        };
        match action.updated_record(&previous) {
            None => deletes.push(previous),
            Some(updated) => writes.push((updated, Some(previous))),
        }
        results.push(true);
    }
    attempt.delete_message_records(deletes).await?;
    attempt.write_message_records(writes).await?;
    Ok(results)
}
