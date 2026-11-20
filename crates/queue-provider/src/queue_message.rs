//! Queue delivery decisions shared by the SQL and KV families.

use std::{collections::HashMap, num::NonZeroU32, sync::Arc};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use storage_types::{
    AttemptDurability, DurationSeconds, ReadProtection, StorageError, StorageResult,
    TimestampMillis,
};

use crate::{
    MessageAttributeValue, MessageId, QueueAttempt, QueueError, QueueIncarnation, QueueMessage,
    QueueOperation, QueuePort, QueueReceiptAction, QueueResult, QueueValidationKind, ReceiptHandle,
    constants::{CLEANUP_PAGE, message_request},
    queue_name_of,
};

#[derive(Clone, Serialize, Deserialize)]
pub struct QueueMessageRecord {
    pub incarnation: QueueIncarnation,
    pub message: QueueMessage,
    pub expires_at: TimestampMillis,
    pub checkpoint_data: Option<String>,
    /// The ARN of the queue that a dead-letter move took this message from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dead_letter_source_arn: Option<String>,
}

/// The part of a stored message that claims, receipt actions and cleanup read
/// and change (W7 P6). Backends compare it to detect a concurrent change; it
/// holds no map, so its serialized bytes are deterministic.
#[derive(Clone, Serialize, Deserialize)]
pub struct QueueMessageState {
    pub incarnation: QueueIncarnation,
    pub message_id: MessageId,
    /// The queue URL or name of the send; cleanup reads its queue name.
    pub queue_url: String,
    pub created_at: TimestampMillis,
    pub expires_at: TimestampMillis,
    pub receive_count: u32,
    pub receipt_handle: Option<ReceiptHandle>,
    pub visibility_timestamp: Option<TimestampMillis>,
    pub checkpoint_data: Option<String>,
    pub dead_letter_source_arn: Option<String>,
}

/// The immutable part of a stored message, written once and never compared.
#[derive(Clone, Serialize, Deserialize)]
pub struct QueueMessageBody {
    pub body: String,
    pub message_attributes: Option<HashMap<String, MessageAttributeValue>>,
    pub trace_header: Option<String>,
}

impl QueueMessageState {
    #[must_use]
    pub fn visible_at(&self) -> TimestampMillis {
        self.visibility_timestamp.unwrap_or(self.created_at)
    }

    #[must_use]
    pub fn cleanup_cursor(&self) -> QueueCleanupCursor {
        QueueCleanupCursor {
            incarnation: self.incarnation,
            message_id: self.message_id,
        }
    }
}

/// One claim attempt's selection rule. The provider owns every decision here;
/// a backend only reads the rows or index entries that the rule names.
#[derive(Clone)]
pub struct QueueDueRequest {
    pub incarnation: QueueIncarnation,
    /// Fixed for the operation, also across its retries.
    pub captured_at: TimestampMillis,
    /// Largest clock difference between nodes (decision D14). A row with a
    /// receipt is due for a new claim only when its visibility time is at or
    /// before `captured_at - clock_skew_millis`, so a node whose clock is
    /// ahead cannot claim a message inside the holder's lease.
    pub clock_skew_millis: i64,
    pub limit: NonZeroU32,
    /// Due-index entries that an ordered-key store reads with a snapshot read
    /// before it selects (SQL ignores it and claims the head).
    pub window: NonZeroU32,
    /// Marks after `captured_at` that a claim reads to report the next due
    /// time.
    pub head: NonZeroU32,
    pub first_attempt: bool,
    /// The caller's random value for the slot choice.
    pub rotation: u32,
    /// The name of a catalog row (the dead-letter target, design D10) that
    /// the claim reads with `Snapshot` in the same read wave as its due rows,
    /// so that a redrive policy adds no round trip.
    pub dead_letter_target: Option<String>,
}

/// The visibility time of one stored message and whether it has a receipt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueDueMark {
    pub due_at: TimestampMillis,
    pub leased: bool,
}

/// What one claim attempt read: the selected rows, whether a due message
/// that this attempt did not select remains, and the earliest known due time
/// after the attempt (decisions D15 and D16).
pub struct QueueDueBatch {
    pub records: Vec<QueueMessageRecord>,
    pub drained: bool,
    pub next_due: Option<TimestampMillis>,
    /// The row that `QueueDueRequest::dead_letter_target` names, if it exists.
    pub dead_letter_target: Option<crate::QueueCatalogRecord>,
}

/// The positions in an observed window that one claim reads with conflict
/// protection, and the facts that the claim reports.
#[derive(Debug, PartialEq, Eq)]
pub struct QueueDueSelection {
    pub positions: Vec<usize>,
    pub drained: bool,
    pub next_due: Option<TimestampMillis>,
}

impl QueueDueRequest {
    /// The lease guard: a row with a receipt waits `clock_skew_millis` more.
    #[must_use]
    pub fn is_due(&self, mark: QueueDueMark) -> bool {
        mark.due_at <= self.captured_at
            && (!mark.leased || mark.due_at <= self.captured_at - self.clock_skew_millis)
    }

    /// Visibility time below which no row is due for this claim.
    #[must_use]
    pub fn leased_due_by(&self) -> TimestampMillis {
        self.captured_at - self.clock_skew_millis
    }

    /// Select from a snapshot window of due-index entries at or before
    /// `captured_at`, ordered by `(due_at, id)`, plus the first `head`
    /// entries after `captured_at` (`after`). Returns window positions.
    #[must_use]
    pub fn select(
        &self,
        window: &[QueueDueMark],
        window_full: bool,
        after: &[QueueDueMark],
    ) -> QueueDueSelection {
        let eligible = window
            .iter()
            .enumerate()
            .filter(|(_, mark)| self.is_due(**mark))
            .map(|(position, _)| position)
            .collect::<Vec<_>>();
        let positions = storage_types::ClaimSlot {
            limit: self.limit,
            first_attempt: self.first_attempt,
            rotation: self.rotation,
        }
        .positions(eligible.len(), window_full)
        .into_iter()
        .map(|index| eligible[index])
        .collect::<Vec<_>>();
        let more_due = window_full || eligible.len() > positions.len();
        let later = window
            .iter()
            .filter(|mark| !self.is_due(**mark))
            .chain(after);
        let (drained, next_due) = self.outcome(more_due, later);
        QueueDueSelection {
            positions,
            drained,
            next_due,
        }
    }

    /// A head claim (SQL) took `taken` rows while an unlocked count of at most
    /// `limit + 1` due rows saw `due_count`.
    #[must_use]
    pub fn head_outcome(
        &self,
        taken: usize,
        due_count: u64,
        after: &[QueueDueMark],
    ) -> (bool, Option<TimestampMillis>) {
        self.outcome(due_count > taken as u64, after.iter())
    }

    fn outcome<'a>(
        &self,
        more_due: bool,
        later: impl Iterator<Item = &'a QueueDueMark>,
    ) -> (bool, Option<TimestampMillis>) {
        if more_due {
            return (false, Some(self.captured_at));
        }
        (true, earliest_due(later, self.clock_skew_millis))
    }
}

/// The earliest time at which one of `marks` can be claimed.
pub(crate) fn earliest_due<'a>(
    marks: impl Iterator<Item = &'a QueueDueMark>,
    clock_skew_millis: i64,
) -> Option<TimestampMillis> {
    marks
        .map(|mark| {
            if mark.leased {
                mark.due_at + clock_skew_millis
            } else {
                mark.due_at
            }
        })
        .min()
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct QueueCleanupCursor {
    pub incarnation: QueueIncarnation,
    pub message_id: MessageId,
}

pub struct QueueCleanupRequest {
    pub exclusive_start: Option<QueueCleanupCursor>,
    pub limit: NonZeroU32,
}

pub struct QueueCleanupPage {
    pub rows: Vec<QueueMessageState>,
    pub more: bool,
}

impl QueueCleanupPage {
    pub(super) fn finish(
        mut self,
        request: &QueueCleanupRequest,
    ) -> StorageResult<(Vec<QueueMessageState>, Option<QueueCleanupCursor>)> {
        if self.rows.len() > request.limit.get() as usize + 1 || self.more && self.rows.is_empty() {
            return Err(StorageError::internal(
                "queue cleanup page has invalid bounds",
            ));
        }
        let mut previous = request.exclusive_start;
        for record in &self.rows {
            let cursor = record.cleanup_cursor();
            if previous.is_some_and(|previous| cursor <= previous) {
                return Err(StorageError::internal(
                    "queue cleanup page has invalid order",
                ));
            }
            previous = Some(cursor);
        }
        let has_more = self.more || self.rows.len() > request.limit.get() as usize;
        self.rows.truncate(request.limit.get() as usize);
        let next = if has_more {
            self.rows.last().map(QueueMessageState::cleanup_cursor)
        } else {
            None
        };
        Ok((self.rows, next))
    }
}

impl QueueMessageRecord {
    #[must_use]
    pub fn visible_at(&self) -> TimestampMillis {
        self.message
            .visibility_timestamp
            .unwrap_or(self.message.created_at)
    }

    #[must_use]
    pub fn cleanup_cursor(&self) -> QueueCleanupCursor {
        QueueCleanupCursor {
            incarnation: self.incarnation,
            message_id: self.message.message_id,
        }
    }

    #[must_use]
    pub fn state(&self) -> QueueMessageState {
        QueueMessageState {
            incarnation: self.incarnation,
            message_id: self.message.message_id,
            queue_url: self.message.queue_url.clone(),
            created_at: self.message.created_at,
            expires_at: self.expires_at,
            receive_count: self.message.receive_count,
            receipt_handle: self.message.receipt_handle.clone(),
            visibility_timestamp: self.message.visibility_timestamp,
            checkpoint_data: self.checkpoint_data.clone(),
            dead_letter_source_arn: self.dead_letter_source_arn.clone(),
        }
    }

    /// Moves the body out without copying it.
    #[must_use]
    pub fn into_parts(self) -> (QueueMessageState, QueueMessageBody) {
        let state = self.state();
        let QueueMessage {
            body,
            message_attributes,
            trace_header,
            ..
        } = self.message;
        (
            state,
            QueueMessageBody {
                body,
                message_attributes,
                trace_header,
            },
        )
    }

    #[must_use]
    pub fn from_parts(state: QueueMessageState, body: QueueMessageBody) -> Self {
        Self {
            incarnation: state.incarnation,
            expires_at: state.expires_at,
            checkpoint_data: state.checkpoint_data,
            dead_letter_source_arn: state.dead_letter_source_arn,
            message: QueueMessage {
                receive_count: state.receive_count,
                message_id: state.message_id,
                queue_url: state.queue_url,
                body: body.body,
                message_attributes: body.message_attributes,
                receipt_handle: state.receipt_handle,
                created_at: state.created_at,
                visibility_timestamp: state.visibility_timestamp,
                trace_header: body.trace_header,
            },
        }
    }
}

/// The captured time and page bounds of one cleanup page.
struct CleanupInput {
    cursor: Option<QueueCleanupCursor>,
    limit: NonZeroU32,
    captured_at: TimestampMillis,
}

/// Delete at most one bounded page of expired or retired queue messages.
pub async fn cleanup_queue_messages_page<P: QueuePort>(
    port: &P,
    cursor: Option<QueueCleanupCursor>,
    limit: NonZeroU32,
) -> QueueResult<Option<QueueCleanupCursor>> {
    if limit.get() > 100 {
        return Err(QueueError::validation(
            QueueValidationKind::InvalidParameterValue,
        ));
    }
    let input = Arc::new(CleanupInput {
        cursor,
        limit,
        captured_at: port.now(),
    });
    port.run(CLEANUP_PAGE, input, |attempt, input, _| {
        Box::pin(cleanup_in_attempt(attempt, input))
    })
    .await
    .map_err(QueueError::from)
}

async fn cleanup_in_attempt<A: QueueAttempt>(
    attempt: &mut A,
    input: &CleanupInput,
) -> StorageResult<Option<QueueCleanupCursor>> {
    let request = QueueCleanupRequest {
        exclusive_start: input.cursor,
        limit: input.limit,
    };
    let (rows, next) = attempt
        .read_message_cleanup_page(&request)
        .await?
        .finish(&request)?;
    let mut names = Vec::new();
    for record in &rows {
        let name = queue_name_of(&record.queue_url);
        if record.expires_at > input.captured_at && !names.iter().any(|known| known == name) {
            names.push(name.to_owned());
        }
    }
    let queues = attempt
        .read_queues_by_name(names.clone(), ReadProtection::Conflict)
        .await?;
    let queues = queues.keyed_by(names);
    let mut stale = Vec::new();
    for record in rows {
        if record.expires_at <= input.captured_at {
            stale.push(record);
            continue;
        }
        let name = queue_name_of(&record.queue_url);
        if queues
            .get(name)
            .and_then(Option::as_ref)
            .is_none_or(|queue| queue.incarnation != record.incarnation)
        {
            stale.push(record);
        }
    }
    attempt.delete_messages(stale).await?;
    Ok(next)
}

/// The scheduler calls the same bounded queue cleanup operation as a concrete
/// SQL or KV store, without holding a provider-specific store type.
#[async_trait]
pub trait QueueMessageCleanupRepository: Send + Sync {
    async fn cleanup_queue_messages_page(
        &self,
        cursor: Option<QueueCleanupCursor>,
        limit: NonZeroU32,
    ) -> QueueResult<Option<QueueCleanupCursor>>;
}

#[async_trait]
impl<P: QueuePort> QueueMessageCleanupRepository for P {
    async fn cleanup_queue_messages_page(
        &self,
        cursor: Option<QueueCleanupCursor>,
        limit: NonZeroU32,
    ) -> QueueResult<Option<QueueCleanupCursor>> {
        cleanup_queue_messages_page(self, cursor, limit).await
    }
}

pub(crate) async fn send_message<P: QueuePort>(
    port: &P,
    message: QueueMessage,
) -> QueueResult<MessageId> {
    send_messages_as(port, vec![message], false)
        .await?
        .pop()
        .ok_or_else(|| {
            QueueError::from(StorageError::internal(
                "queue send returned no message result",
            ))
        })?
}

/// The validated messages of one send; ids are set before the first try.
struct SendInput {
    /// A batch entry reports an oversize message with the batch text.
    batch: bool,
    queue_url: String,
    messages: Vec<QueueMessage>,
}

pub(crate) async fn send_messages<P: QueuePort>(
    port: &P,
    messages: Vec<QueueMessage>,
) -> QueueResult<Vec<QueueResult<MessageId>>> {
    send_messages_as(port, messages, true).await
}

async fn send_messages_as<P: QueuePort>(
    port: &P,
    mut messages: Vec<QueueMessage>,
    batch: bool,
) -> QueueResult<Vec<QueueResult<MessageId>>> {
    let Some(first) = messages.first() else {
        return Ok(Vec::new());
    };
    let queue_url = first.queue_url.clone();
    if messages
        .iter()
        .any(|message| queue_name_of(&message.queue_url) != queue_name_of(&queue_url))
    {
        return Err(QueueError::validation_with_detail(
            QueueValidationKind::InvalidParameterValue,
            "All messages of one batch must address the same queue.",
        ));
    }
    for message in &mut messages {
        message.validate_for_send()?;
        if message.message_id == MessageId::default() {
            message.message_id = MessageId::random();
        }
    }
    let request = message_request(messages.len(), AttemptDurability::Durable);
    let input = Arc::new(SendInput {
        batch,
        queue_url,
        messages,
    });
    port.run(request, Arc::clone(&input), |attempt, input, _| {
        Box::pin(send_in_attempt(attempt, input))
    })
    .await
    .map_err(QueueError::from)?
    // Probe 6: a missing queue fails the whole request, not each entry.
    .ok_or_else(|| queue_not_found(input.queue_url.clone()))
}

/// One try: the queue row without a lock, then the send policy and one batched
/// write.
async fn send_in_attempt<A: QueueAttempt>(
    attempt: &mut A,
    input: &SendInput,
) -> StorageResult<Option<Vec<QueueResult<MessageId>>>> {
    let Some(queue) = QueueOperation::load_for_messages(attempt, &input.queue_url).await? else {
        return Ok(None);
    };
    queue
        .send_messages(input.messages.clone(), input.batch)
        .await
        .map(Some)
}

pub(crate) async fn delete_message<P: QueuePort>(
    port: &P,
    queue_url: &str,
    receipt: ReceiptHandle,
) -> QueueResult<()> {
    apply_receipt(port, queue_url, receipt, QueueReceiptAction::Delete).await
}

pub(crate) async fn delete_messages<P: QueuePort>(
    port: &P,
    queue_url: &str,
    receipts: Vec<ReceiptHandle>,
) -> QueueResult<Vec<QueueResult<()>>> {
    apply_receipts(
        port,
        queue_url,
        receipts
            .into_iter()
            .map(|receipt| (receipt, QueueReceiptAction::Delete))
            .collect(),
    )
    .await
}

pub(crate) async fn change_message_visibility<P: QueuePort>(
    port: &P,
    queue_url: &str,
    receipt: ReceiptHandle,
    timeout: DurationSeconds,
) -> QueueResult<()> {
    if *timeout > crate::constants::MAX_VISIBILITY_TIMEOUT_SECONDS {
        return Err(QueueError::validation(
            QueueValidationKind::InvalidParameterValue,
        ));
    }
    apply_receipt(
        port,
        queue_url,
        receipt,
        QueueReceiptAction::Visibility(visibility_end(
            port.now(),
            timeout,
            port.max_clock_skew_millis(),
        )),
    )
    .await
}

pub(crate) async fn change_message_visibilities<P: QueuePort>(
    port: &P,
    queue_url: &str,
    entries: Vec<(ReceiptHandle, DurationSeconds)>,
) -> QueueResult<Vec<QueueResult<()>>> {
    if entries
        .iter()
        .any(|(_, timeout)| **timeout > crate::constants::MAX_VISIBILITY_TIMEOUT_SECONDS)
    {
        return Err(QueueError::validation(
            QueueValidationKind::InvalidParameterValue,
        ));
    }
    let captured_at = port.now();
    apply_receipts(
        port,
        queue_url,
        entries
            .into_iter()
            .map(|(receipt, timeout)| {
                (
                    receipt,
                    QueueReceiptAction::Visibility(visibility_end(
                        captured_at,
                        timeout,
                        port.max_clock_skew_millis(),
                    )),
                )
            })
            .collect(),
    )
    .await
}

pub(crate) async fn update_message_snapshot_checkpoint<P: QueuePort>(
    port: &P,
    queue_url: &str,
    receipt: ReceiptHandle,
    checkpoint_data: String,
) -> QueueResult<()> {
    if checkpoint_data.len() > crate::constants::MAX_CHECKPOINT_DATA_BYTES
        || serde_json::to_string(&checkpoint_data).map_or(usize::MAX, |json| json.len())
            > crate::constants::MAX_CHECKPOINT_JSON_BYTES
    {
        return Err(QueueError::validation(
            QueueValidationKind::InvalidParameterValue,
        ));
    }
    apply_receipt(
        port,
        queue_url,
        receipt,
        QueueReceiptAction::Checkpoint(checkpoint_data),
    )
    .await
}

/// The visibility time of a leased message (a receive or a visibility
/// change). A row with a receipt is due only `clock_skew_millis` after its
/// visibility time (decision D14), which protects a lease that the holder
/// keeps. A timeout of zero promises no lease, so it needs no margin: the time
/// is set back by the margin and the message is due again at once, as in SQS.
pub(crate) fn visibility_end(
    captured_at: TimestampMillis,
    timeout: DurationSeconds,
    clock_skew_millis: i64,
) -> TimestampMillis {
    if *timeout == 0 {
        captured_at - clock_skew_millis
    } else {
        captured_at + timeout
    }
}

async fn apply_receipt<P: QueuePort>(
    port: &P,
    queue_url: &str,
    receipt: ReceiptHandle,
    action: QueueReceiptAction,
) -> QueueResult<()> {
    apply_receipts(port, queue_url, vec![(receipt, action)])
        .await?
        .pop()
        .ok_or_else(|| {
            QueueError::from(StorageError::internal("queue receipt returned no result"))
        })?
}

struct ReceiptsInput {
    queue_url: String,
    entries: Vec<(ReceiptHandle, QueueReceiptAction)>,
}

async fn apply_receipts<P: QueuePort>(
    port: &P,
    queue_url: &str,
    entries: Vec<(ReceiptHandle, QueueReceiptAction)>,
) -> QueueResult<Vec<QueueResult<()>>> {
    if entries.is_empty() {
        return Ok(Vec::new());
    }
    let count = entries.len();
    let input = Arc::new(ReceiptsInput {
        queue_url: queue_url.to_owned(),
        entries,
    });
    let outcome = port
        .run(
            message_request(count, AttemptDurability::Relaxed),
            Arc::clone(&input),
            |attempt, input, _| Box::pin(receipts_in_attempt(attempt, input)),
        )
        .await?;
    // Probe 6: a missing queue fails the whole request, not each entry.
    outcome.ok_or_else(|| queue_not_found(input.queue_url.clone()))
}

async fn receipts_in_attempt<A: QueueAttempt>(
    attempt: &mut A,
    input: &ReceiptsInput,
) -> StorageResult<Option<Vec<QueueResult<()>>>> {
    let Some(queue) = QueueOperation::load_for_messages(attempt, &input.queue_url).await? else {
        return Ok(None);
    };
    Ok(Some(queue.apply_receipts(input.entries.clone()).await?))
}

pub(super) fn queue_not_found(queue_url: String) -> QueueError {
    QueueError::ResourceNotFound {
        resource_type: "queue",
        resource_id: queue_url,
    }
}
