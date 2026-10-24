//! A bounded cleanup selection carries identities, never mutable message
//! snapshots.

use std::num::NonZeroU32;

use serde::{Deserialize, Deserializer, Serialize};
use storage_types::{StorageError, StorageResult, TimestampMillis};

use crate::{
    MessageId, QueueAttempt, QueueCleanupCursor, QueueCleanupRequest, QueueIncarnation,
    read_queue_by_url,
};

/// One queue incarnation is the whole cleanup destination, including retired
/// queues.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueCleanupInput {
    pub queue_url: String,
    pub incarnation: QueueIncarnation,
    pub captured_at: TimestampMillis,
    pub exclusive_start: Option<MessageId>,
    pub limit: NonZeroU32,
}

#[derive(Clone, Serialize)]
#[serde(transparent)]
pub struct QueueCleanupSelection(QueueCleanupData);

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct QueueCleanupData {
    queue_url: String,
    incarnation: QueueIncarnation,
    captured_at: TimestampMillis,
    ids: Vec<MessageId>,
    next: Option<MessageId>,
}

impl QueueCleanupSelection {
    /// Capture eligibility once inside the caller's protected native attempt.
    pub async fn capture<A: QueueAttempt>(
        attempt: &mut A,
        input: QueueCleanupInput,
    ) -> StorageResult<Self> {
        input.validate()?;
        let request = QueueCleanupRequest {
            exclusive_start: Some(QueueCleanupCursor {
                incarnation: input.incarnation,
                message_id: input.exclusive_start.unwrap_or_default(),
            }),
            limit: input.limit,
        };
        let (rows, next) = attempt
            .read_message_cleanup_page(&request)
            .await?
            .finish(&request)?;
        let queue = read_queue_by_url(attempt, &input.queue_url).await?;
        let is_retired = queue.is_none_or(|queue| queue.incarnation != input.incarnation);
        let mut ids = Vec::with_capacity(rows.len());
        for row in &rows {
            if row.incarnation != input.incarnation {
                break;
            }
            if row.message.queue_url != input.queue_url {
                return Err(StorageError::internal(
                    "queue cleanup destination differs from message",
                ));
            }
            if is_retired || row.expires_at <= input.captured_at {
                ids.push(row.message.message_id);
            }
        }
        // A global physical scan can reach another incarnation. It never
        // becomes this task's continuation or selection, even if its queue was
        // deleted.
        let next = next
            .filter(|cursor| cursor.incarnation == input.incarnation)
            .map(|cursor| cursor.message_id);
        let selection = Self(QueueCleanupData {
            queue_url: input.queue_url,
            incarnation: input.incarnation,
            captured_at: input.captured_at,
            ids,
            next,
        });
        selection.validate()?;
        Ok(selection)
    }

    /// Re-read only the captured IDs. Visibility/receipt changes do not change
    /// expiration eligibility; deletion uses the current protected native rows.
    /// Missing rows are harmless and an old incarnation cannot delete new rows.
    pub async fn apply<A: QueueAttempt>(
        &self,
        attempt: &mut A,
        captured_at: TimestampMillis,
    ) -> StorageResult<Option<MessageId>> {
        if captured_at != self.0.captured_at {
            return Err(StorageError::validation(
                "queue cleanup capture time differs from command",
            ));
        }
        if self.0.ids.is_empty() {
            return Ok(self.0.next);
        }
        let queue = read_queue_by_url(attempt, &self.0.queue_url).await?;
        let is_retired = queue.is_none_or(|queue| queue.incarnation != self.0.incarnation);
        let rows = attempt
            .read_messages_by_id(self.0.incarnation, self.0.ids.clone())
            .await?;
        if rows.len() != self.0.ids.len() {
            return Err(StorageError::internal(
                "queue cleanup selection batch count differs",
            ));
        }
        let mut stale = Vec::with_capacity(rows.len());
        for (id, row) in self.0.ids.iter().zip(rows) {
            let Some(row) = row else {
                continue;
            };
            if row.incarnation != self.0.incarnation
                || row.message.message_id != *id
                || row.message.queue_url != self.0.queue_url
                || (!is_retired && row.expires_at > self.0.captured_at)
            {
                return Err(StorageError::internal(
                    "queue cleanup selection no longer matches its destination",
                ));
            }
            stale.push(row);
        }
        attempt.delete_message_records(stale).await?;
        Ok(self.0.next)
    }

    pub fn destination(&self) -> (&str, QueueIncarnation) {
        (&self.0.queue_url, self.0.incarnation)
    }

    fn validate(&self) -> StorageResult<()> {
        if self.0.queue_url.is_empty()
            || self.0.ids.len() > 100
            || self.0.ids.iter().any(|id| *id == MessageId::default())
            || self.0.ids.windows(2).any(|ids| ids[0] >= ids[1])
            || self.0.next.is_some_and(|next| {
                next == MessageId::default() || self.0.ids.last().is_some_and(|last| *last > next)
            })
        {
            return Err(StorageError::validation("invalid queue cleanup selection"));
        }
        Ok(())
    }
}

impl QueueCleanupInput {
    fn validate(&self) -> StorageResult<()> {
        if self.queue_url.is_empty()
            || self.limit.get() > 100
            || self.exclusive_start == Some(MessageId::default())
        {
            return Err(StorageError::validation("invalid queue cleanup page"));
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for QueueCleanupSelection {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let selection = Self(QueueCleanupData::deserialize(deserializer)?);
        selection.validate().map_err(serde::de::Error::custom)?;
        Ok(selection)
    }
}
