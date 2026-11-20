//! Materialize accepted queue sends using the original catalog settings.

use std::collections::HashMap;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use storage_types::{DurationSeconds, StorageError, StorageResult};

use crate::{
    MessageAttributeValue, QueueAttempt, QueueMessage, QueueMessageRecord, QueueOperation,
};

impl<A: QueueAttempt> QueueOperation<'_, A> {
    /// The caller captures IDs and validates that messages address this queue
    /// before loading the operation. No IDs or time are generated here.
    pub async fn send_messages(
        self,
        messages: Vec<QueueMessage>,
        batch: bool,
    ) -> StorageResult<Vec<crate::QueueResult<crate::MessageId>>> {
        let policy = QueueSendPolicy::try_from(&self.record)?;
        let mut records = Vec::with_capacity(messages.len());
        let results = messages
            .into_iter()
            .map(|message| {
                // Suite script: in a batch the oversize entry fails alone, with
                // its own text; a single send keeps the request text.
                policy.validate_message(&message).map_err(|_| {
                    if batch {
                        crate::QueueError::batch_entry(
                            true,
                            crate::constants::SQS_INVALID_PARAMETER_VALUE_ERROR_TYPE.to_owned(),
                            message_too_long_in_batch(policy.maximum_message_size),
                        )
                    } else {
                        crate::QueueError::validation_with_detail(
                            crate::QueueValidationKind::InvalidParameterValue,
                            message_too_long(policy.maximum_message_size),
                        )
                    }
                })?;
                let id = message.message_id;
                records.push(policy.prepare_message(message));
                Ok(id)
            })
            .collect();
        if !records.is_empty() {
            self.attempt.insert_messages(records).await?;
            self.attempt
                .stage_queue_wake(crate::QueueWakeSlot::of_queue(
                    &self.record.queue.queue_name,
                ))?;
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
/// SQS message size: body bytes plus, for each attribute, its name, data
/// type, and value (decoded bytes for a binary value). One owner for the
/// queue `MaximumMessageSize` and the batch total (decision D8).
pub(crate) fn message_payload_bytes(
    body: &str,
    attributes: Option<&HashMap<String, MessageAttributeValue>>,
) -> usize {
    let attributes = attributes.map_or(0, |attributes| {
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
    body.len() + attributes
}

/// The AWS text for a message over a size limit.
pub(crate) fn message_too_long(maximum: usize) -> String {
    format!(
        "One or more parameters are invalid. Reason: Message must be shorter than {maximum} bytes."
    )
}

/// The AWS text of a batch entry over the size limit.
fn message_too_long_in_batch(maximum: usize) -> String {
    format!(
        "One or more parameters cannot be validated. Reason: Message must be shorter than \
         {maximum} bytes."
    )
}

impl QueueSendPolicy {
    pub(super) fn validate_message(&self, message: &QueueMessage) -> StorageResult<()> {
        let size = message_payload_bytes(&message.body, message.message_attributes.as_ref());
        if size > self.maximum_message_size {
            return Err(StorageError::validation(message_too_long(
                self.maximum_message_size,
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
            dead_letter_source_arn: None,
        }
    }

    /// A dead-letter copy of `source` in this queue (design D10 with the AWS
    /// results): the same ID, body, attributes, send time, and receive count;
    /// no receipt; visible at `captured_at`; retention from the first send.
    pub(super) fn prepare_moved(
        &self,
        source: &QueueMessageRecord,
        queue_name: &str,
        source_arn: String,
        captured_at: storage_types::TimestampMillis,
    ) -> QueueMessageRecord {
        let mut message = source.message.clone();
        message.queue_url = queue_name.to_owned();
        message.receipt_handle = None;
        message.visibility_timestamp = Some(captured_at);
        QueueMessageRecord {
            incarnation: self.incarnation,
            expires_at: message.created_at + self.retention,
            message,
            checkpoint_data: None,
            dead_letter_source_arn: Some(source_arn),
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
