//! Adapt the public queue provider contract to shared QueuePort operations.

use std::collections::HashMap;

use async_trait::async_trait;
use storage_types::DurationSeconds;

use crate::{
    MessageId, MessageResponse, Queue, QueueMessage, QueueMessageCounts, QueueOperations,
    QueuePort, QueueProvider, QueueResult, ReceiptHandle,
};

#[async_trait]
impl<P: QueuePort> QueueProvider for P {
    async fn initialize(&self) -> QueueResult<()> {
        // Family stores are initialized before they are published to callers.
        Ok(())
    }

    async fn create_queue(&self, queue: Queue) -> QueueResult<Queue> {
        QueueOperations::create_queue(self, queue).await
    }

    async fn get_queue(&self, queue_url: &str) -> QueueResult<Option<Queue>> {
        QueueOperations::get_queue(self, queue_url).await
    }

    async fn get_queue_by_name(&self, queue_name: &str) -> QueueResult<Option<Queue>> {
        QueueOperations::get_queue_by_name(self, queue_name).await
    }

    async fn list_queues(&self, prefix: Option<&str>) -> QueueResult<Vec<Queue>> {
        QueueOperations::list_queues(self, prefix).await
    }

    async fn delete_queue(&self, queue_url: &str) -> QueueResult<()> {
        QueueOperations::delete_queue(self, queue_url).await
    }

    async fn purge_queue(&self, queue_url: &str) -> QueueResult<()> {
        QueueOperations::purge_queue(self, queue_url).await
    }

    async fn set_queue_attributes(
        &self,
        queue_url: &str,
        attributes: HashMap<String, String>,
    ) -> QueueResult<()> {
        QueueOperations::set_queue_attributes(self, queue_url, attributes).await
    }

    async fn get_queue_message_counts(&self, queue_url: &str) -> QueueResult<QueueMessageCounts> {
        Ok(
            QueueOperations::get_queue_with_message_counts(self, queue_url)
                .await?
                .map_or_else(QueueMessageCounts::default, |(_, counts)| counts),
        )
    }

    async fn get_queue_with_message_counts(
        &self,
        queue_url: &str,
    ) -> QueueResult<Option<(Queue, QueueMessageCounts)>> {
        QueueOperations::get_queue_with_message_counts(self, queue_url).await
    }

    async fn publish_message(&self, message: QueueMessage) -> QueueResult<MessageId> {
        QueueOperations::publish_message(self, message).await
    }

    async fn send_message(&self, message: QueueMessage) -> QueueResult<MessageId> {
        QueueOperations::send_message(self, message).await
    }

    async fn send_messages(
        &self,
        messages: Vec<QueueMessage>,
    ) -> QueueResult<Vec<QueueResult<MessageId>>> {
        QueueOperations::send_messages(self, messages).await
    }

    async fn receive_messages(
        &self,
        queue_url: &str,
        max_messages: u32,
        visibility_timeout: DurationSeconds,
        wait_time_seconds: DurationSeconds,
    ) -> QueueResult<Vec<MessageResponse>> {
        QueueOperations::receive_messages(
            self,
            queue_url,
            max_messages,
            visibility_timeout,
            wait_time_seconds,
        )
        .await
    }

    async fn delete_message(&self, queue_url: &str, receipt: ReceiptHandle) -> QueueResult<()> {
        QueueOperations::delete_message(self, queue_url, receipt).await
    }

    async fn delete_messages(
        &self,
        queue_url: &str,
        receipts: Vec<ReceiptHandle>,
    ) -> QueueResult<Vec<QueueResult<()>>> {
        QueueOperations::delete_messages(self, queue_url, receipts).await
    }

    async fn change_message_visibility(
        &self,
        queue_url: &str,
        receipt: ReceiptHandle,
        timeout: DurationSeconds,
    ) -> QueueResult<()> {
        QueueOperations::change_message_visibility(self, queue_url, receipt, timeout).await
    }

    async fn change_message_visibilities(
        &self,
        queue_url: &str,
        entries: Vec<(ReceiptHandle, DurationSeconds)>,
    ) -> QueueResult<Vec<QueueResult<()>>> {
        QueueOperations::change_message_visibilities(self, queue_url, entries).await
    }

    async fn update_message_snapshot_checkpoint(
        &self,
        queue_url: &str,
        receipt: ReceiptHandle,
        checkpoint_data: String,
    ) -> QueueResult<()> {
        QueueOperations::update_message_snapshot_checkpoint(
            self,
            queue_url,
            receipt,
            checkpoint_data,
        )
        .await
    }
}
