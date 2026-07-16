use async_trait::async_trait;
use storage_types::DurationSeconds;

use crate::{
    errors::QueueResult,
    newtypes::{MessageId, ReceiptHandle},
    types::{MessageResponse, Queue, QueueMessage, QueueMessageCounts},
};

/// Trait for queue backends that can store and manage queues and messages.
///
/// Implementations must provide at-least-once delivery semantics. Delete,
/// visibility, and checkpoint operations are expected to be idempotent because
/// callers may retry after partial persistence or process restarts.
#[async_trait]
pub trait QueueProvider: Send + Sync {
    /// Initialize the queue storage backend.
    async fn initialize(&self) -> QueueResult<()>;

    /// Create a new queue.
    async fn create_queue(&self, queue: Queue) -> QueueResult<Queue>;

    /// Get a queue by URL.
    async fn get_queue(&self, queue_url: &str) -> QueueResult<Option<Queue>>;

    /// Get a queue by name.
    async fn get_queue_by_name(&self, queue_name: &str) -> QueueResult<Option<Queue>>;

    /// List queues, optionally filtered by prefix.
    async fn list_queues(&self, queue_name_prefix: Option<&str>) -> QueueResult<Vec<Queue>>;

    /// Delete a queue by URL.
    async fn delete_queue(&self, queue_url: &str) -> QueueResult<()>;

    /// Purge all messages from a queue.
    async fn purge_queue(&self, queue_url: &str) -> QueueResult<()>;

    /// Replace the attribute set for a queue.
    async fn set_queue_attributes(
        &self,
        queue_url: &str,
        attributes: std::collections::HashMap<String, String>,
    ) -> QueueResult<()>;

    /// Return approximate queue message counts.
    async fn get_queue_message_counts(&self, _queue_url: &str) -> QueueResult<QueueMessageCounts> {
        Ok(QueueMessageCounts::default())
    }

    /// Return queue metadata and approximate counts from one provider-owned snapshot.
    async fn get_queue_with_message_counts(
        &self,
        queue_url: &str,
    ) -> QueueResult<Option<(Queue, QueueMessageCounts)>> {
        let Some(queue) = self.get_queue(queue_url).await? else {
            return Ok(None);
        };
        let counts = self.get_queue_message_counts(queue_url).await?;
        Ok(Some((queue, counts)))
    }

    /// Send a message to a queue.
    async fn send_message(&self, message: QueueMessage) -> QueueResult<MessageId>;

    /// Send multiple messages to one queue.
    ///
    /// The default implementation preserves existing per-message behavior.
    /// Backends with a native queue layout can override this to reduce commit
    /// count for protocol-level SendMessageBatch calls.
    async fn send_messages(
        &self,
        messages: Vec<QueueMessage>,
    ) -> QueueResult<Vec<QueueResult<MessageId>>> {
        let mut results = Vec::with_capacity(messages.len());
        for message in messages {
            results.push(self.send_message(message).await);
        }
        Ok(results)
    }

    /// Receive messages from a queue with visibility timeout.
    async fn receive_messages(
        &self,
        queue_url: &str,
        max_messages: u32,
        visibility_timeout: DurationSeconds,
        wait_time_seconds: DurationSeconds,
    ) -> QueueResult<Vec<MessageResponse>>;

    /// Delete a message.
    async fn delete_message(
        &self,
        queue_url: &str,
        receipt_handle: ReceiptHandle,
    ) -> QueueResult<()>;

    /// Delete multiple messages from one queue.
    ///
    /// The default implementation preserves existing per-message behavior.
    /// Backends with a native queue layout can override this to group receipt
    /// handles by partition and reduce transaction count.
    async fn delete_messages(
        &self,
        queue_url: &str,
        receipt_handles: Vec<ReceiptHandle>,
    ) -> QueueResult<Vec<QueueResult<()>>> {
        let mut results = Vec::with_capacity(receipt_handles.len());
        for receipt_handle in receipt_handles {
            results.push(self.delete_message(queue_url, receipt_handle).await);
        }
        Ok(results)
    }

    /// Change message visibility timeout.
    async fn change_message_visibility(
        &self,
        queue_url: &str,
        receipt_handle: ReceiptHandle,
        visibility_timeout: DurationSeconds,
    ) -> QueueResult<()>;

    /// Change visibility timeout for multiple messages from one queue.
    ///
    /// The default implementation preserves existing per-message behavior.
    /// Backends with a native queue layout can override this to group receipt
    /// handles by partition and reduce transaction count.
    async fn change_message_visibilities(
        &self,
        queue_url: &str,
        entries: Vec<(ReceiptHandle, DurationSeconds)>,
    ) -> QueueResult<Vec<QueueResult<()>>> {
        let mut results = Vec::with_capacity(entries.len());
        for (receipt_handle, visibility_timeout) in entries {
            results.push(
                self.change_message_visibility(queue_url, receipt_handle, visibility_timeout)
                    .await,
            );
        }
        Ok(results)
    }

    /// Update message checkpoint data.
    async fn update_message_snapshot_checkpoint(
        &self,
        queue_url: &str,
        receipt_handle: ReceiptHandle,
        checkpoint_data: String,
    ) -> QueueResult<()>;
}
