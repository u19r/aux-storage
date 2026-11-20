//! One entry for decoded SQS wire requests: each action is one manager call.

use queue_provider::{QueueError, QueueInternalKind, QueueRequest, QueueResponse, QueueResult};

use super::{QueueManager, queue_not_found};

impl QueueManager {
    /// Run one decoded wire request. `on_wait` runs once when a receive
    /// starts to wait.
    pub async fn dispatch(
        &self,
        request: QueueRequest,
        on_wait: impl FnOnce() + Send,
    ) -> QueueResult<QueueResponse> {
        if self.endpoint.is_none() {
            return Err(QueueError::internal(
                QueueInternalKind::QueueEndpointNotConfigured,
            ));
        }
        if let Some(queue_url) = request.queue_url() {
            self.check_queue_account(queue_url)?;
        }
        Ok(match request {
            QueueRequest::CreateQueue(request) => {
                QueueResponse::CreateQueue(self.create_queue(request.into_inner()).await?)
            }
            QueueRequest::DeleteQueue(request) => {
                self.delete_queue(request.into_inner()).await?;
                QueueResponse::DeleteQueue
            }
            QueueRequest::ListQueues(request) => {
                QueueResponse::ListQueues(self.list_queues(request.into_inner()).await?)
            }
            QueueRequest::GetQueueUrl(request) => {
                QueueResponse::GetQueueUrl(self.get_queue_url(request.into_inner()).await?)
            }
            QueueRequest::GetQueueAttributes(request) => QueueResponse::GetQueueAttributes(
                self.get_queue_attributes(request.into_inner()).await?,
            ),
            QueueRequest::SetQueueAttributes(request) => {
                self.set_queue_attributes(request.into_inner()).await?;
                QueueResponse::SetQueueAttributes
            }
            QueueRequest::PurgeQueue(request) => {
                self.purge_queue(request.into_inner()).await?;
                QueueResponse::PurgeQueue
            }
            QueueRequest::SendMessage(request) => {
                QueueResponse::SendMessage(self.send_message(request.into_inner()).await?)
            }
            QueueRequest::PublishMessage(request) => {
                QueueResponse::PublishMessage(self.publish_message(request.into_inner()).await?)
            }
            QueueRequest::SendMessageBatch(request) => QueueResponse::SendMessageBatch(
                self.send_message_batch(request.into_inner()).await?,
            ),
            QueueRequest::ReceiveMessage(request) => QueueResponse::ReceiveMessage(
                self.receive_message_with_wait_hook(request.into_inner(), on_wait)
                    .await?,
            ),
            QueueRequest::DeleteMessage(request) => {
                self.delete_message(request.into_inner()).await?;
                QueueResponse::DeleteMessage
            }
            QueueRequest::DeleteMessageBatch(request) => QueueResponse::DeleteMessageBatch(
                self.delete_message_batch(request.into_inner()).await?,
            ),
            QueueRequest::ChangeMessageVisibility(request) => {
                self.change_message_visibility(request.into_inner()).await?;
                QueueResponse::ChangeMessageVisibility
            }
            QueueRequest::ChangeMessageVisibilityBatch(request) => {
                QueueResponse::ChangeMessageVisibilityBatch(
                    self.change_message_visibility_batch(request.into_inner())
                        .await?,
                )
            }
            QueueRequest::TagQueue(request) => {
                self.tag_queue(request.into_inner()).await?;
                QueueResponse::TagQueue
            }
            QueueRequest::UntagQueue(request) => {
                self.untag_queue(request.into_inner()).await?;
                QueueResponse::UntagQueue
            }
            QueueRequest::ListQueueTags(request) => {
                QueueResponse::ListQueueTags(self.list_queue_tags(request.into_inner()).await?)
            }
            QueueRequest::ListDeadLetterSourceQueues(request) => {
                QueueResponse::ListDeadLetterSourceQueues(
                    self.list_dead_letter_source_queues(request.into_inner())
                        .await?,
                )
            }
        })
    }
}

impl QueueManager {
    /// D7 and review finding 2: a queue is found by its name, the last path
    /// segment, so a URL that another node or another base URL produced
    /// names the same queue. A URL whose segment before the name is another
    /// account is a missing queue, answered with no storage read. A bare
    /// name has no account and is this account's queue.
    fn check_queue_account(&self, queue_url: &str) -> QueueResult<()> {
        let mut segments = queue_url.trim_end_matches('/').rsplit('/');
        let name = segments.next().unwrap_or_default();
        let account = segments.next();
        if name.is_empty() || account.is_some_and(|account| account != self.identity.account_id) {
            return Err(queue_not_found(queue_url));
        }
        Ok(())
    }
}
