//! Materialize accepted queue sends using the original catalog settings.

use base64::{Engine as _, engine::general_purpose::STANDARD};
use storage_types::{DurationSeconds, StorageError, StorageResult};

use crate::{QueueAttempt, QueueMessage, QueueMessageRecord, QueueOperation};

impl<A: QueueAttempt> QueueOperation<'_, A> {
    /// The caller captures IDs and validates that messages address this queue
    /// before loading the operation. No IDs or time are generated here.
    pub async fn send_messages(
        self,
        messages: Vec<QueueMessage>,
    ) -> StorageResult<Vec<crate::QueueResult<crate::MessageId>>> {
        let policy = QueueSendPolicy::try_from(&self.record)?;
        let mut records = Vec::with_capacity(messages.len());
        let results = messages
            .into_iter()
            .map(|message| {
                policy
                    .validate_message(&message)
                    .map_err(crate::QueueError::from)?;
                let id = message.message_id;
                records.push((policy.prepare_message(message), None));
                Ok(id)
            })
            .collect();
        if !records.is_empty() {
            self.attempt.write_message_records(records).await?;
        }
        Ok(results)
    }
}

pub(super) struct QueueSendPolicy {
    incarnation: crate::QueueIncarnation,
    retention: DurationSeconds,
    default_delay: DurationSeconds,
    maximum_message_size: usize,
}
impl TryFrom<&crate::QueueCatalogRecord> for QueueSendPolicy {
    type Error = StorageError;
    fn try_from(record: &crate::QueueCatalogRecord) -> StorageResult<Self> {
        Ok(Self {
            incarnation: record.incarnation,
            retention: retention_seconds(&record.queue)?,
            default_delay: default_delay_seconds(&record.queue)?,
            maximum_message_size: maximum_message_size(&record.queue)?,
        })
    }
}
impl QueueSendPolicy {
    pub(super) fn validate_message(&self, message: &QueueMessage) -> StorageResult<()> {
        let attributes = message.message_attributes.as_ref().map_or(0, |attributes| {
            attributes
                .iter()
                .map(|(name, value)| {
                    name.len()
                        + value.data_type.len()
                        + value.string_value.as_deref().map_or(0, str::len)
                        + value.binary_value.as_deref().map_or(0, |encoded| {
                            STANDARD.decode(encoded).map_or(0, |bytes| bytes.len())
                        })
                })
                .sum()
        });
        let size = message.body.len() + attributes;
        if size > self.maximum_message_size {
            return Err(StorageError::validation(format!(
                "Message exceeds the queue MaximumMessageSize of {} bytes",
                self.maximum_message_size
            )));
        }
        Ok(())
    }

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

fn maximum_message_size(queue: &crate::Queue) -> StorageResult<usize> {
    let value = queue.attributes.get("MaximumMessageSize").map_or(
        Ok(crate::constants::MAX_MAXIMUM_MESSAGE_SIZE_BYTES),
        |raw| {
            raw.parse::<u32>()
                .map_err(|_| StorageError::internal("queue maximum message size is invalid"))
        },
    )?;
    if !(crate::constants::MIN_MAXIMUM_MESSAGE_SIZE_BYTES
        ..=crate::constants::MAX_MAXIMUM_MESSAGE_SIZE_BYTES)
        .contains(&value)
    {
        return Err(StorageError::internal(
            "queue maximum message size is out of range",
        ));
    }
    Ok(value as usize)
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
