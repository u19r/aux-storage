//! Repeated receipt actions observe earlier actions on the same native attempt.

use serde::{Deserialize, Serialize};
use storage_types::{StorageError, StorageResult, TimestampMillis};

use crate::{
    QueueAttempt, QueueCatalogRecord, QueueError, QueueMessageState, QueueOperation, QueueResult,
    QueueValidationKind, QueueWakeSlot, ReceiptHandle, newtypes::DecodedReceipt,
};

#[derive(Clone, Serialize, Deserialize)]
pub enum QueueReceiptAction {
    Delete,
    Visibility(TimestampMillis),
    Checkpoint(String),
}

impl QueueReceiptAction {
    fn updated_state(self, previous: &QueueMessageState) -> Option<QueueMessageState> {
        match self {
            Self::Delete => None,
            Self::Visibility(visible_at) => {
                let mut updated = previous.clone();
                updated.visibility_timestamp = Some(visible_at);
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

/// What one receipt entry found (decision D7).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReceiptOutcome {
    Applied,
    /// A valid handle with no current row: an older receipt, a deleted
    /// message, or a retired incarnation.
    Stale,
    Malformed,
    /// A handle issued by another queue.
    OtherQueue,
    /// A handle issued by an earlier queue of the same name.
    DeletedQueue,
}

impl ReceiptOutcome {
    /// Whether the handle names this queue; only then is a row read.
    fn of_handle(receipt: &ReceiptHandle, queue: &QueueCatalogRecord) -> Option<Self> {
        match receipt.decode() {
            DecodedReceipt::Unrecognised | DecodedReceipt::NotBase64 => Some(Self::Malformed),
            DecodedReceipt::Minted { queue_name, .. } if queue_name != queue.queue.queue_name => {
                Some(Self::OtherQueue)
            }
            DecodedReceipt::Minted { queue_id, .. } if queue_id != queue.id => {
                Some(Self::DeletedQueue)
            }
            DecodedReceipt::Minted { .. } => None,
        }
    }

    /// SQS answers: a stale delete succeeds without an effect; a stale
    /// visibility change is `InvalidParameterValue`; a malformed handle and a
    /// stale checkpoint are `ReceiptHandleIsInvalid`; a handle of another
    /// queue is `ReceiptHandleIsInvalid` with its own text, and a handle of a
    /// deleted queue of this name finds no queue.
    fn result(
        self,
        action: &QueueReceiptAction,
        receipt: &ReceiptHandle,
        queue: &QueueCatalogRecord,
    ) -> QueueResult<()> {
        match (self, action) {
            (Self::Applied, _) | (Self::Stale, QueueReceiptAction::Delete) => Ok(()),
            (Self::Stale, QueueReceiptAction::Visibility(_)) => {
                Err(QueueError::validation_with_detail(
                    QueueValidationKind::InvalidParameterValue,
                    crate::constants::SQS_VISIBILITY_MESSAGE_NOT_AVAILABLE_MESSAGE,
                ))
            }
            (Self::Stale, QueueReceiptAction::Checkpoint(_)) | (Self::Malformed, _) => {
                Err(QueueError::validation_with_detail(
                    QueueValidationKind::MessageNotFoundOrAlreadyProcessed,
                    crate::receipt_handle_invalid_message(receipt),
                ))
            }
            (Self::OtherQueue, _) => Err(QueueError::validation_with_detail(
                QueueValidationKind::MessageNotFoundOrAlreadyProcessed,
                format!("The receipt handle \"{receipt}\" is not valid for this queue."),
            )),
            (Self::DeletedQueue, _) => Err(QueueError::ResourceNotFound {
                resource_type: "queue",
                resource_id: queue.queue.queue_url.clone(),
            }),
        }
    }
}

impl<A: QueueAttempt> QueueOperation<'_, A> {
    pub async fn apply_receipts(
        self,
        entries: Vec<(ReceiptHandle, QueueReceiptAction)>,
    ) -> StorageResult<Vec<QueueResult<()>>> {
        let sent = entries.clone();
        let slot = QueueWakeSlot::of_queue(&self.record.queue.queue_name);
        let outcomes = apply_receipt_waves(self.attempt, &self.record, slot, entries).await?;
        Ok(outcomes
            .into_iter()
            .zip(&sent)
            .map(|(outcome, (receipt, action))| outcome.result(action, receipt, &self.record))
            .collect())
    }
}

async fn apply_receipt_waves<A: QueueAttempt>(
    attempt: &mut A,
    queue: &QueueCatalogRecord,
    slot: QueueWakeSlot,
    entries: Vec<(ReceiptHandle, QueueReceiptAction)>,
) -> StorageResult<Vec<ReceiptOutcome>> {
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
        results.extend(apply_receipt_batch(attempt, queue, slot, wave).await?);
    }
    Ok(results)
}

/// Shape is checked before the storage read: a malformed handle reads nothing.
async fn apply_receipt_batch<A: QueueAttempt>(
    attempt: &mut A,
    queue: &QueueCatalogRecord,
    slot: QueueWakeSlot,
    entries: Vec<(ReceiptHandle, QueueReceiptAction)>,
) -> StorageResult<Vec<ReceiptOutcome>> {
    let refused = entries
        .iter()
        .map(|(receipt, _)| ReceiptOutcome::of_handle(receipt, queue))
        .collect::<Vec<_>>();
    let receipts = entries
        .iter()
        .zip(&refused)
        .filter(|(_, refused)| refused.is_none())
        .map(|((receipt, _), _)| receipt.clone())
        .collect::<Vec<_>>();
    let mut rows = if receipts.is_empty() {
        storage_types::Exact::empty()
    } else {
        attempt
            .read_messages_by_receipt(queue.incarnation, receipts)
            .await?
    }
    .into_iter();
    let mut results = Vec::with_capacity(entries.len());
    let mut deletes = Vec::new();
    let mut writes = Vec::new();
    for ((_, action), refused) in entries.into_iter().zip(refused) {
        if let Some(outcome) = refused {
            results.push(outcome);
            continue;
        }
        let Some(previous) = rows.next().flatten() else {
            results.push(ReceiptOutcome::Stale);
            continue;
        };
        match action.updated_state(&previous) {
            None => deletes.push(previous),
            Some(updated) => writes.push((updated, previous)),
        }
        results.push(ReceiptOutcome::Applied);
    }
    if writes.iter().any(
        |(updated, previous): &(QueueMessageState, QueueMessageState)| {
            updated.visible_at() < previous.visible_at()
        },
    ) {
        attempt.stage_queue_wake(slot)?;
    }
    attempt.delete_messages(deletes).await?;
    attempt.write_message_states(writes).await?;
    Ok(results)
}
