//! Queue selection, original responses, and lease effects share one native
//! attempt.

use std::num::NonZeroU32;

use storage_common::retry::retry_native_conflicts;
use storage_types::{AttemptMode, DurationSeconds, StorageError, StorageResult, TimestampMillis};

use super::{
    MessageResponse, QueueAttempt, QueueDueRequest, QueueError, QueueIncarnation,
    QueueMessageRecord, QueueOperation, QueuePort, QueueResult, QueueValidationKind, ReceiptHandle,
    ReceiveMessageResponse, ReceiveMessagesInput,
    constants::{MAX_RECEIVE_MESSAGES, MAX_VISIBILITY_TIMEOUT_SECONDS, QUEUE_WRITE_RETRY},
    queue_catalog::read_queue_by_url,
    queue_message::queue_not_found,
};

/// The selected original response and its pending expiration and lease effects.
///
/// The caller binds the current protected queue incarnation to its source
/// before preparing, then applies these effects in the same native attempt.
/// This value supplies no source authority and does not open or retry an
/// attempt.
pub struct PreparedReceiveMessages<'native, A> {
    attempt: &'native mut A,
    response: ReceiveMessageResponse,
    expired: Vec<QueueMessageRecord>,
    claims: Vec<(QueueMessageRecord, Option<QueueMessageRecord>)>,
}

impl<'native, A: QueueAttempt> QueueOperation<'native, A> {
    pub async fn prepare_receive_messages(
        self,
        input: &ReceiveMessagesInput,
    ) -> StorageResult<PreparedReceiveMessages<'native, A>> {
        PreparedReceiveMessages::prepare(self.attempt, self.record.incarnation, input).await
    }
}

impl<'native, A: QueueAttempt> PreparedReceiveMessages<'native, A> {
    /// Select using validated captured time and exactly one UUID per requested
    /// slot.
    async fn prepare(
        attempt: &'native mut A,
        incarnation: QueueIncarnation,
        input: &ReceiveMessagesInput,
    ) -> StorageResult<Self> {
        let captured_at = input.captured_at();
        let next_visible = captured_at
            + input
                .visibility_timeout()
                .map_err(|_| invalid_receive_input())?;
        let due = attempt
            .read_due_messages(&QueueDueRequest {
                incarnation,
                captured_at,
                limit: NonZeroU32::new(input.max_messages().map_err(|_| invalid_receive_input())?)
                    .ok_or_else(|| {
                        StorageError::internal("validated queue receive limit is zero")
                    })?,
            })
            .await?;
        validate_due_records(&due, incarnation, input)?;
        let receipts = input
            .receipt_handle_entropy()
            .iter()
            .map(|entropy| ReceiptHandle::new(next_visible.timestamp_millis(), *entropy));
        let mut prepared = Self::new(attempt);
        prepared.plan_claims(due, receipts, captured_at, next_visible);
        Ok(prepared)
    }

    /// Borrow the original response for durable encoding before any message
    /// write.
    #[must_use]
    pub const fn response(&self) -> &ReceiveMessageResponse {
        &self.response
    }

    /// Consume the selected effects without recapturing time, IDs, or message
    /// state.
    pub async fn apply(self) -> StorageResult<ReceiveMessageResponse> {
        self.attempt.delete_message_records(self.expired).await?;
        self.attempt.write_message_records(self.claims).await?;
        Ok(self.response)
    }

    fn new(attempt: &'native mut A) -> Self {
        Self {
            attempt,
            response: ReceiveMessageResponse {
                messages: Vec::new(),
            },
            expired: Vec::new(),
            claims: Vec::new(),
        }
    }

    fn plan_claims(
        &mut self,
        due: Vec<QueueMessageRecord>,
        receipts: impl IntoIterator<Item = ReceiptHandle>,
        captured_at: TimestampMillis,
        next_visible: TimestampMillis,
    ) {
        self.response.messages.reserve(due.len());
        for (previous, receipt) in due.into_iter().zip(receipts) {
            if previous.expires_at <= captured_at {
                self.expired.push(previous);
                continue;
            }
            let mut claimed = previous.clone();
            claimed.message.receive_count = claimed.message.receive_count.saturating_add(1);
            claimed.message.receipt_handle = Some(receipt.clone());
            claimed.message.visibility_timestamp = Some(next_visible);
            self.response.messages.push(MessageResponse::from_message(
                claimed.message.clone(),
                receipt,
            ));
            self.claims.push((claimed, Some(previous)));
        }
    }
}

pub(super) async fn receive_messages<P: QueuePort>(
    port: &P,
    queue_url: &str,
    max_messages: u32,
    visibility_timeout: DurationSeconds,
) -> QueueResult<Vec<MessageResponse>> {
    if !(1..=MAX_RECEIVE_MESSAGES).contains(&max_messages)
        || *visibility_timeout > MAX_VISIBILITY_TIMEOUT_SECONDS
    {
        return Err(QueueError::validation(
            QueueValidationKind::InvalidParameterValue,
        ));
    }
    let limit = NonZeroU32::new(max_messages)
        .ok_or_else(|| StorageError::internal("validated queue receive limit is zero"))?;
    let captured_at = port.now();
    let next_visible = captured_at + visibility_timeout;
    let receipts = (0..max_messages)
        .map(|_| ReceiptHandle::new(next_visible.timestamp_millis(), uuid::Uuid::now_v7()))
        .collect::<Vec<_>>();
    let queue_url = queue_url.to_owned();
    let received = retry_native_conflicts(port, QUEUE_WRITE_RETRY, || {
        let queue_url = queue_url.clone();
        let receipts = receipts.clone();
        port.with_queue_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                let Some(queue) = read_queue_by_url(attempt, &queue_url).await? else {
                    return Ok(None);
                };
                let due = attempt
                    .read_due_messages(&QueueDueRequest {
                        incarnation: queue.incarnation,
                        captured_at,
                        limit,
                    })
                    .await?;
                let mut prepared = PreparedReceiveMessages::new(attempt);
                prepared.plan_claims(due, receipts, captured_at, next_visible);
                Ok(Some(prepared.apply().await?.messages))
            })
        })
    })
    .await?;
    received.ok_or_else(|| queue_not_found(queue_url))
}

fn validate_due_records(
    due: &[QueueMessageRecord],
    incarnation: QueueIncarnation,
    input: &ReceiveMessagesInput,
) -> StorageResult<()> {
    if due.len() > input.receipt_handle_entropy().len() {
        return Err(StorageError::internal(
            "queue due result exceeds captured receive limit",
        ));
    }
    let mut previous = None;
    for record in due {
        let cursor = (record.visible_at(), record.message.message_id);
        if record.incarnation != incarnation
            || record.message.queue_url != input.request().queue_url
            || cursor.0 > input.captured_at()
            || previous.is_some_and(|previous| cursor <= previous)
        {
            return Err(StorageError::internal(
                "queue due result differs from captured selection",
            ));
        }
        previous = Some(cursor);
    }
    Ok(())
}

fn invalid_receive_input() -> StorageError {
    StorageError::internal("validated queue receive input lost required values")
}
