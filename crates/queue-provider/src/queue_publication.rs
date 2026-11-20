//! A stable publication identity coalesces one pending native queue message.
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use crate::{
    MessageId, QueueAttempt, QueueError, QueueMessage, QueueOperation, QueuePort, QueueResult,
    QueueValidationKind, SendMessageRequest, constants::MESSAGE_PUBLISH,
    queue_send::QueueSendPolicy, request_validation::QueueRequestValidation,
};

/// AuxFn-native publication, distinct from ordinary random-identity
/// SendMessage. An existing message retains its visibility, receipt, expiry and
/// checkpoint.
#[derive(Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[cfg_attr(any(test, feature = "test-support"), derive(Debug))]
#[serde(deny_unknown_fields)]
pub struct PublishMessageRequest {
    #[serde(rename = "MessageId")]
    #[schema(value_type = String, min_length = 36, max_length = 36, pattern = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$", example = "07070707-0707-0707-0707-070707070707")]
    /// Stable identity: a hyphenated UUID. The nil UUID is reserved.
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

enum QueuePublication {
    Accepted,
    MissingQueue,
    IdentityConflict,
}

pub(crate) async fn publish_message<P: QueuePort>(
    port: &P,
    message: QueueMessage,
) -> QueueResult<MessageId> {
    message.validate_for_send()?;
    if message.message_id == MessageId::default() {
        return Err(QueueError::validation(
            QueueValidationKind::InvalidParameterValue,
        ));
    }
    let id = message.message_id;
    let queue_url = message.queue_url.clone();
    let outcome = port
        .run(MESSAGE_PUBLISH, Arc::new(message), |attempt, message, _| {
            Box::pin(publish_in_attempt(attempt, message))
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

async fn publish_in_attempt<A: QueueAttempt>(
    attempt: &mut A,
    message: &QueueMessage,
) -> StorageResult<QueuePublication> {
    let Some(queue) = QueueOperation::load_for_messages(attempt, &message.queue_url).await? else {
        return Ok(QueuePublication::MissingQueue);
    };
    queue.publish_message(message.clone()).await
}

impl<A: QueueAttempt> QueueOperation<'_, A> {
    async fn publish_message(self, message: QueueMessage) -> StorageResult<QueuePublication> {
        let [previous] = self
            .attempt
            .read_messages_by_id(self.record.incarnation, vec![message.message_id])
            .await?
            .into_array::<1>()?;
        if let Some(record) = previous.as_ref() {
            if record.message.body != message.body
                || record.message.message_attributes != message.message_attributes
                || record.message.trace_header != message.trace_header
            {
                return Ok(QueuePublication::IdentityConflict);
            }
            if record.expires_at > message.created_at {
                return Ok(QueuePublication::Accepted);
            }
        }
        let policy = QueueSendPolicy::try_from(&self.record)?;
        policy.validate_message(&message)?;
        let record = policy.prepare_message(message);
        if let Some(previous) = previous {
            // An expired row with this identity that cleanup has not removed
            // yet.
            self.attempt.delete_messages(vec![previous.state()]).await?;
        }
        self.attempt.insert_messages(vec![record]).await?;
        self.attempt
            .stage_queue_wake(crate::QueueWakeSlot::of_queue(
                &self.record.queue.queue_name,
            ))?;
        Ok(QueuePublication::Accepted)
    }
}
