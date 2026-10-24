//! Queue delivery decisions shared by the SQL and KV families.

use std::num::NonZeroU32;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use storage_common::retry::retry_native_conflicts;
use storage_types::{AttemptMode, DurationSeconds, StorageError, StorageResult, TimestampMillis};

use crate::{
    MessageId, QueueAttempt, QueueError, QueueIncarnation, QueueMessage, QueueOperation, QueuePort,
    QueueReceiptAction, QueueResult, QueueValidationKind, ReceiptHandle,
    constants::QUEUE_WRITE_RETRY,
};

#[derive(Clone, Serialize, Deserialize)]
pub struct QueueMessageRecord {
    pub incarnation: QueueIncarnation,
    pub message: QueueMessage,
    pub expires_at: TimestampMillis,
    pub checkpoint_data: Option<String>,
}

#[derive(Clone, Copy)]
pub struct QueueDueRequest {
    pub incarnation: QueueIncarnation,
    pub captured_at: TimestampMillis,
    pub limit: NonZeroU32,
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
    pub rows: Vec<QueueMessageRecord>,
    pub more: bool,
}

impl QueueCleanupPage {
    pub(super) fn finish(
        mut self,
        request: &QueueCleanupRequest,
    ) -> StorageResult<(Vec<QueueMessageRecord>, Option<QueueCleanupCursor>)> {
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
            self.rows.last().map(QueueMessageRecord::cleanup_cursor)
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
    let captured_at = port.now();
    retry_native_conflicts(port, QUEUE_WRITE_RETRY, || {
        port.with_queue_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                let request = QueueCleanupRequest {
                    exclusive_start: cursor,
                    limit,
                };
                let (rows, next) = attempt
                    .read_message_cleanup_page(&request)
                    .await?
                    .finish(&request)?;
                let mut urls = Vec::new();
                for record in &rows {
                    if record.expires_at > captured_at && !urls.contains(&record.message.queue_url)
                    {
                        urls.push(record.message.queue_url.clone());
                    }
                }
                let queues = attempt.read_queues_by_url(urls.clone()).await?;
                if queues.len() != urls.len() {
                    return Err(StorageError::internal(
                        "queue cleanup catalog batch count differs",
                    ));
                }
                let mut stale = Vec::new();
                for record in rows {
                    if record.expires_at <= captured_at {
                        stale.push(record);
                        continue;
                    }
                    let position = urls
                        .iter()
                        .position(|url| url == &record.message.queue_url)
                        .ok_or_else(|| {
                            StorageError::internal(
                                "queue cleanup URL is missing from its catalog batch",
                            )
                        })?;
                    if queues[position]
                        .as_ref()
                        .is_none_or(|queue| queue.incarnation != record.incarnation)
                    {
                        stale.push(record);
                    }
                }
                attempt.delete_message_records(stale).await?;
                Ok(next)
            })
        })
    })
    .await
    .map_err(QueueError::from)
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
    send_messages(port, vec![message])
        .await?
        .pop()
        .ok_or_else(|| {
            QueueError::from(StorageError::internal(
                "queue send returned no message result",
            ))
        })?
}

pub(crate) async fn send_messages<P: QueuePort>(
    port: &P,
    mut messages: Vec<QueueMessage>,
) -> QueueResult<Vec<QueueResult<MessageId>>> {
    let Some(first) = messages.first() else {
        return Ok(Vec::new());
    };
    let queue_url = first.queue_url.clone();
    if messages
        .iter()
        .any(|message| message.queue_url != queue_url)
    {
        return Err(QueueError::validation(
            QueueValidationKind::InvalidParameterValue,
        ));
    }
    for message in &mut messages {
        if message.message_id == MessageId::default() {
            message.message_id = MessageId::random();
        }
    }
    let ids = messages
        .iter()
        .map(|message| message.message_id)
        .collect::<Vec<_>>();
    let found = retry_native_conflicts(port, QUEUE_WRITE_RETRY, || {
        let messages = messages.clone();
        let queue_url = queue_url.clone();
        port.with_queue_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                let Some(queue) = QueueOperation::load(attempt, &queue_url).await? else {
                    return Ok(false);
                };
                queue.send_messages(messages).await?;
                Ok(true)
            })
        })
    })
    .await?;
    Ok(ids
        .into_iter()
        .map(|id| {
            if found {
                Ok(id)
            } else {
                Err(queue_not_found(queue_url.clone()))
            }
        })
        .collect())
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
        QueueReceiptAction::Visibility(port.now() + timeout),
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
                    QueueReceiptAction::Visibility(captured_at + timeout),
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
    apply_receipt(
        port,
        queue_url,
        receipt,
        QueueReceiptAction::Checkpoint(checkpoint_data),
    )
    .await
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

async fn apply_receipts<P: QueuePort>(
    port: &P,
    queue_url: &str,
    entries: Vec<(ReceiptHandle, QueueReceiptAction)>,
) -> QueueResult<Vec<QueueResult<()>>> {
    if entries.is_empty() {
        return Ok(Vec::new());
    }
    let count = entries.len();
    let queue_url = queue_url.to_owned();
    let outcome = retry_native_conflicts(port, QUEUE_WRITE_RETRY, || {
        let queue_url = queue_url.clone();
        let entries = entries.clone();
        port.with_queue_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                let Some(queue) = QueueOperation::load(attempt, &queue_url).await? else {
                    return Ok(None);
                };
                Ok(Some(queue.apply_receipts(entries).await?))
            })
        })
    })
    .await?;
    Ok(outcome.unwrap_or_else(|| {
        (0..count)
            .map(|_| Err(queue_not_found(queue_url.clone())))
            .collect()
    }))
}

pub(super) fn queue_not_found(queue_url: String) -> QueueError {
    QueueError::ResourceNotFound {
        resource_type: "queue",
        resource_id: queue_url,
    }
}
