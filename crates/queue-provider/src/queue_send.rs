//! Materialize accepted queue sends using the original catalog settings.

use storage_types::{DurationSeconds, StorageError, StorageResult};

use crate::{QueueAttempt, QueueMessage, QueueMessageRecord, QueueOperation};

impl<A: QueueAttempt> QueueOperation<'_, A> {
    /// The caller captures IDs and validates that messages address this queue
    /// before loading the operation. No IDs or time are generated here.
    pub async fn send_messages(self, messages: Vec<QueueMessage>) -> StorageResult<()> {
        let policy = QueueSendPolicy::try_from(&self.record)?;
        let records = messages
            .into_iter()
            .map(|message| (policy.prepare_message(message), None))
            .collect();
        self.attempt.write_message_records(records).await
    }
}

pub(super) struct QueueSendPolicy {
    incarnation: crate::QueueIncarnation,
    retention: DurationSeconds,
    default_delay: DurationSeconds,
}
impl TryFrom<&crate::QueueCatalogRecord> for QueueSendPolicy {
    type Error = StorageError;
    fn try_from(record: &crate::QueueCatalogRecord) -> StorageResult<Self> {
        Ok(Self {
            incarnation: record.incarnation,
            retention: retention_seconds(&record.queue)?,
            default_delay: default_delay_seconds(&record.queue)?,
        })
    }
}
impl QueueSendPolicy {
    pub(super) fn prepare_message(&self, mut message: QueueMessage) -> QueueMessageRecord {
        if message.visibility_timestamp.is_none() {
            message.visibility_timestamp = Some(message.created_at + self.default_delay);
        }
        QueueMessageRecord {
            incarnation: self.incarnation,
            expires_at: message.created_at + self.retention,
            message,
            checkpoint_data: None,
        }
    }
}

fn retention_seconds(queue: &crate::Queue) -> StorageResult<DurationSeconds> {
    let value = queue
        .attributes
        .get("MessageRetentionPeriod")
        .map_or(Ok(345_600), |raw| {
            raw.parse::<u32>()
                .map_err(|_| StorageError::internal("queue retention attribute is invalid"))
        })?;
    if !(crate::constants::MIN_MESSAGE_RETENTION_SECONDS
        ..=crate::constants::MAX_MESSAGE_RETENTION_SECONDS)
        .contains(&value)
    {
        return Err(StorageError::internal(
            "queue retention attribute is out of range",
        ));
    }
    Ok(value.into())
}

fn default_delay_seconds(queue: &crate::Queue) -> StorageResult<DurationSeconds> {
    let value = queue.attributes.get("DelaySeconds").map_or(Ok(0), |raw| {
        raw.parse::<u32>()
            .map_err(|_| StorageError::internal("queue delay attribute is invalid"))
    })?;
    if value > crate::constants::MAX_DELAY_SECONDS {
        return Err(StorageError::internal(
            "queue delay attribute is out of range",
        ));
    }
    Ok(DurationSeconds::from(value))
}
