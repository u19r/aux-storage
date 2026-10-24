//! A stable publication identity coalesces one pending native queue message.
use http_error::HttpApiError;
use serde::{Deserialize, Serialize};
use storage_common::retry::retry_native_conflicts;
use storage_types::{AttemptMode, StorageError, StorageResult};

use crate::{
    MessageId, QueueAttempt, QueueError, QueueMessage, QueueOperation, QueuePort, QueueResult,
    QueueValidationKind, SendMessageRequest, constants::QUEUE_WRITE_RETRY,
    queue_send::QueueSendPolicy, request_validation::QueueRequestValidation,
};

/// AuxFn-native publication, distinct from ordinary random-identity
/// SendMessage. An existing message retains its visibility, receipt, expiry and
/// checkpoint.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PublishMessageRequest {
    #[serde(rename = "MessageId")]
    #[schema(value_type = String, min_length = 24, max_length = 24, pattern = "^[0-9a-fA-F]{24}$", example = "070707070707070707070707")]
    /// Stable 24-character hexadecimal identity. The all-zero identity is
    /// reserved.
    pub message_id: MessageId,
    #[serde(rename = "Message")]
    pub message: SendMessageRequest,
}

impl QueueRequestValidation for PublishMessageRequest {
    fn validate_request(&self) -> QueueResult<()> {
        if self.message_id == MessageId::default() {
            return Err(QueueError::validation(
                QueueValidationKind::InvalidParameterValue,
            ));
        }
        self.message.validate_request()
    }
}

impl PublishMessageRequest {
    pub fn from_json(value: serde_json::Value) -> Result<Self, HttpApiError> {
        let request: Self = serde_json::from_value(value)
            .map_err(|_| HttpApiError::validation_error("invalid PublishMessage request"))?;
        request.validate_request().map_err(HttpApiError::from)?;
        Ok(request)
    }
}

enum QueuePublication {
    Accepted,
    MissingQueue,
    IdentityConflict,
}

pub(crate) async fn publish_message<P: QueuePort>(
    port: &P,
    message: QueueMessage,
) -> QueueResult<MessageId> {
    if message.message_id == MessageId::default() {
        return Err(QueueError::validation(
            QueueValidationKind::InvalidParameterValue,
        ));
    }
    let id = message.message_id;
    let queue_url = message.queue_url.clone();
    let outcome = retry_native_conflicts(port, QUEUE_WRITE_RETRY, || {
        let message = message.clone();
        port.with_queue_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                let Some(queue) = QueueOperation::load(attempt, &message.queue_url).await? else {
                    return Ok(QueuePublication::MissingQueue);
                };
                queue.publish_message(message).await
            })
        })
    })
    .await?;
    match outcome {
        QueuePublication::Accepted => Ok(id),
        QueuePublication::MissingQueue => Err(crate::queue_message::queue_not_found(queue_url)),
        QueuePublication::IdentityConflict => Err(QueueError::validation_with_detail(
            QueueValidationKind::InvalidParameterValue,
            "publication identity already has another message",
        )),
    }
}

impl<A: QueueAttempt> QueueOperation<'_, A> {
    async fn publish_message(self, message: QueueMessage) -> StorageResult<QueuePublication> {
        let mut records = self
            .attempt
            .read_messages_by_id(self.record.incarnation, vec![message.message_id])
            .await?;
        if records.len() != 1 {
            return Err(StorageError::internal(
                "queue publication point-read count differs",
            ));
        }
        let previous = records.pop().flatten();
        if let Some(record) = previous.as_ref() {
            if record.message.body != message.body
                || record.message.message_attributes != message.message_attributes
            {
                return Ok(QueuePublication::IdentityConflict);
            }
            if record.expires_at > message.created_at {
                return Ok(QueuePublication::Accepted);
            }
        }
        let record = QueueSendPolicy::try_from(&self.record)?.prepare_message(message);
        self.attempt
            .write_message_records(vec![(record, previous)])
            .await?;
        Ok(QueuePublication::Accepted)
    }
}
