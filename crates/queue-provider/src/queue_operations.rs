//! Queue catalog operations use one shared decision sequence across backends.

use std::collections::HashMap;

use async_trait::async_trait;
use storage_types::DurationSeconds;

use crate::{
    MessageId, MessageResponse, Queue, QueueMessage, QueueMessageCounts, QueuePort, QueueResult,
    ReceiptHandle,
};

#[async_trait]
pub trait QueueOperations: Send + Sync {
    fn take_admission_pressure_signal(&self) -> bool {
        false
    }

    async fn create_queue(&self, queue: Queue) -> QueueResult<Queue>;
    async fn get_queue(&self, queue_url: &str) -> QueueResult<Option<Queue>>;
    async fn get_queue_by_name(&self, queue_name: &str) -> QueueResult<Option<Queue>>;
    async fn get_queue_url_by_name(&self, queue_name: &str) -> QueueResult<Option<String>> {
        self.get_queue_by_name(queue_name)
            .await
            .map(|queue| queue.map(|queue| queue.queue_url))
    }
    async fn list_queues(&self, prefix: Option<&str>) -> QueueResult<Vec<Queue>>;
    async fn delete_queue(&self, queue_url: &str) -> QueueResult<()>;
    async fn purge_queue(&self, queue_url: &str) -> QueueResult<()>;
    async fn get_queue_with_message_counts(
        &self,
        queue_url: &str,
    ) -> QueueResult<Option<(Queue, QueueMessageCounts)>>;
    async fn set_queue_attributes(
        &self,
        queue_url: &str,
        attributes: HashMap<String, String>,
    ) -> QueueResult<()>;
    /// Coalesce an immutable stable identity while its native message is
    /// pending.
    async fn publish_message(&self, message: QueueMessage) -> QueueResult<MessageId>;

    async fn send_message(&self, message: QueueMessage) -> QueueResult<MessageId>;
    async fn send_messages(
        &self,
        messages: Vec<QueueMessage>,
    ) -> QueueResult<Vec<QueueResult<MessageId>>>;
    async fn receive_messages(
        &self,
        queue_url: &str,
        max_messages: u32,
        visibility_timeout: DurationSeconds,
        wait_time_seconds: DurationSeconds,
    ) -> QueueResult<Vec<MessageResponse>>;
    async fn delete_message(&self, queue_url: &str, receipt: ReceiptHandle) -> QueueResult<()>;
    async fn delete_messages(
        &self,
        queue_url: &str,
        receipts: Vec<ReceiptHandle>,
    ) -> QueueResult<Vec<QueueResult<()>>>;
    async fn change_message_visibility(
        &self,
        queue_url: &str,
        receipt: ReceiptHandle,
        visibility_timeout: DurationSeconds,
    ) -> QueueResult<()>;
    async fn change_message_visibilities(
        &self,
        queue_url: &str,
        entries: Vec<(ReceiptHandle, DurationSeconds)>,
    ) -> QueueResult<Vec<QueueResult<()>>>;
    async fn update_message_snapshot_checkpoint(
        &self,
        queue_url: &str,
        receipt: ReceiptHandle,
        checkpoint_data: String,
    ) -> QueueResult<()>;
}

#[async_trait]
impl<P: QueuePort> QueueOperations for P {
    async fn create_queue(&self, queue: Queue) -> QueueResult<Queue> {
        crate::queue_catalog::create_queue(self, queue).await
    }

    async fn get_queue(&self, queue_url: &str) -> QueueResult<Option<Queue>> {
        crate::queue_catalog::get_queue(self, queue_url).await
    }

    async fn get_queue_by_name(&self, queue_name: &str) -> QueueResult<Option<Queue>> {
        crate::queue_catalog::get_queue_by_name(self, queue_name).await
    }

    async fn list_queues(&self, prefix: Option<&str>) -> QueueResult<Vec<Queue>> {
        crate::queue_catalog::list_queues(self, prefix).await
    }

    async fn delete_queue(&self, queue_url: &str) -> QueueResult<()> {
        crate::queue_catalog::delete_queue(self, queue_url).await
    }

    async fn purge_queue(&self, queue_url: &str) -> QueueResult<()> {
        crate::queue_catalog::purge_queue(self, queue_url).await
    }

    async fn get_queue_with_message_counts(
        &self,
        queue_url: &str,
    ) -> QueueResult<Option<(Queue, QueueMessageCounts)>> {
        crate::queue_catalog::get_queue_with_message_counts(self, queue_url).await
    }

    async fn set_queue_attributes(
        &self,
        queue_url: &str,
        attributes: HashMap<String, String>,
    ) -> QueueResult<()> {
        crate::queue_catalog::set_queue_attributes(self, queue_url, attributes).await
    }

    async fn publish_message(&self, message: QueueMessage) -> QueueResult<MessageId> {
        crate::queue_publication::publish_message(self, message).await
    }

    async fn send_message(&self, message: QueueMessage) -> QueueResult<MessageId> {
        crate::queue_message::send_message(self, message).await
    }

    async fn send_messages(
        &self,
        messages: Vec<QueueMessage>,
    ) -> QueueResult<Vec<QueueResult<MessageId>>> {
        crate::queue_message::send_messages(self, messages).await
    }

    async fn receive_messages(
        &self,
        queue_url: &str,
        max_messages: u32,
        visibility_timeout: DurationSeconds,
        _wait_time_seconds: DurationSeconds,
    ) -> QueueResult<Vec<MessageResponse>> {
        super::queue_receive::receive_messages(self, queue_url, max_messages, visibility_timeout)
            .await
    }

    async fn delete_message(&self, queue_url: &str, receipt: ReceiptHandle) -> QueueResult<()> {
        crate::queue_message::delete_message(self, queue_url, receipt).await
    }

    async fn delete_messages(
        &self,
        queue_url: &str,
        receipts: Vec<ReceiptHandle>,
    ) -> QueueResult<Vec<QueueResult<()>>> {
        crate::queue_message::delete_messages(self, queue_url, receipts).await
    }

    async fn change_message_visibility(
        &self,
        queue_url: &str,
        receipt: ReceiptHandle,
        visibility_timeout: DurationSeconds,
    ) -> QueueResult<()> {
        crate::queue_message::change_message_visibility(
            self,
            queue_url,
            receipt,
            visibility_timeout,
        )
        .await
    }

    async fn change_message_visibilities(
        &self,
        queue_url: &str,
        entries: Vec<(ReceiptHandle, DurationSeconds)>,
    ) -> QueueResult<Vec<QueueResult<()>>> {
        crate::queue_message::change_message_visibilities(self, queue_url, entries).await
    }

    async fn update_message_snapshot_checkpoint(
        &self,
        queue_url: &str,
        receipt: ReceiptHandle,
        checkpoint_data: String,
    ) -> QueueResult<()> {
        crate::queue_message::update_message_snapshot_checkpoint(
            self,
            queue_url,
            receipt,
            checkpoint_data,
        )
        .await
    }
}
