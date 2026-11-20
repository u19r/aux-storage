//! Queue catalog operations use one shared decision sequence across backends.

use std::{
    collections::{BTreeMap, HashMap},
    num::NonZeroU32,
};

use async_trait::async_trait;
use storage_types::DurationSeconds;

use crate::{
    ListDeadLetterSourceQueuesResponse, MessageId, Queue, QueueMessage, QueueMessageCounts,
    QueuePort, QueueResult, ReceiptHandle, ReceiveProbe, ReceivedMessages,
};

/// Every `queue_url` argument is a bare queue name or any URL whose last path
/// segment is the name; a store finds the queue by that name
/// (`queue_name_of`). A remote provider forwards the argument as it is.
///
/// Delivery is at least once. Delete, visibility and checkpoint operations
/// are idempotent: a caller may repeat them after a partial write or a restart.
#[async_trait]
pub trait QueueOperations: Send + Sync {
    /// Whether the caller waits for messages with `probe_receive`. A provider
    /// that forwards ReceiveMessage to another server returns false and
    /// performs the whole long poll in `receive_messages`.
    fn waits_with_local_probe(&self) -> bool {
        true
    }

    /// See `QueuePort::queue_wake_board`.
    fn queue_wake_board(&self) -> Option<std::sync::Arc<crate::QueueWakeBoard>> {
        None
    }

    async fn create_queue(&self, queue: Queue) -> QueueResult<Queue>;
    async fn get_queue(&self, queue_url: &str) -> QueueResult<Option<Queue>>;
    async fn get_queue_by_name(&self, queue_name: &str) -> QueueResult<Option<Queue>>;
    async fn get_queue_url_by_name(&self, queue_name: &str) -> QueueResult<Option<String>> {
        self.get_queue_by_name(queue_name)
            .await
            .map(|queue| queue.map(|queue| queue.queue_url))
    }
    /// Queues in name order after `after`, at most `limit`, and the last name
    /// when more remain.
    async fn list_queue_page(
        &self,
        prefix: Option<&str>,
        after: Option<&str>,
        limit: NonZeroU32,
    ) -> QueueResult<(Vec<Queue>, Option<String>)>;
    async fn delete_queue(&self, queue_url: &str) -> QueueResult<()>;
    /// Add or replace `added`, then remove `removed`, under the tag rule.
    async fn change_queue_tags(
        &self,
        queue_url: &str,
        added: BTreeMap<String, String>,
        removed: Vec<String>,
    ) -> QueueResult<()>;
    /// The stored tags; `None` when the queue does not exist.
    async fn list_queue_tags(
        &self,
        queue_url: &str,
    ) -> QueueResult<Option<BTreeMap<String, String>>>;
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
    /// One page of the queues whose redrive policy names `queue_url` as their
    /// dead-letter queue, in name order, after `exclusive_start` (a previous
    /// `next_token`). `None` means that the queue does not exist.
    async fn list_dead_letter_source_queues(
        &self,
        queue_url: &str,
        exclusive_start: Option<&str>,
        max_results: NonZeroU32,
    ) -> QueueResult<Option<ListDeadLetterSourceQueuesResponse>>;
    /// Coalesce an immutable stable identity while its native message is
    /// pending.
    async fn publish_message(&self, message: QueueMessage) -> QueueResult<MessageId>;

    async fn send_message(&self, message: QueueMessage) -> QueueResult<MessageId>;
    async fn send_messages(
        &self,
        messages: Vec<QueueMessage>,
    ) -> QueueResult<Vec<QueueResult<MessageId>>>;
    /// Claim due messages. An omitted visibility timeout uses the queue
    /// attribute. Local stores ignore the wait time and return at once; a
    /// provider without a local probe performs the long poll itself.
    async fn receive_messages(
        &self,
        queue_url: &str,
        max_messages: u32,
        visibility_timeout: Option<DurationSeconds>,
        wait_time_seconds: Option<DurationSeconds>,
    ) -> QueueResult<ReceivedMessages>;
    /// Read the queue's receive defaults and earliest due time without a
    /// write. `None` means the queue does not exist.
    async fn probe_receive(&self, queue_url: &str) -> QueueResult<Option<ReceiveProbe>>;
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
    fn queue_wake_board(&self) -> Option<std::sync::Arc<crate::QueueWakeBoard>> {
        QueuePort::queue_wake_board(self)
    }

    async fn create_queue(&self, queue: Queue) -> QueueResult<Queue> {
        crate::queue_catalog::create_queue(self, queue).await
    }

    async fn get_queue(&self, queue_url: &str) -> QueueResult<Option<Queue>> {
        crate::queue_catalog::get_queue(self, queue_url).await
    }

    async fn get_queue_by_name(&self, queue_name: &str) -> QueueResult<Option<Queue>> {
        crate::queue_catalog::get_queue_by_name(self, queue_name).await
    }

    async fn list_queue_page(
        &self,
        prefix: Option<&str>,
        after: Option<&str>,
        limit: NonZeroU32,
    ) -> QueueResult<(Vec<Queue>, Option<String>)> {
        crate::queue_catalog::list_queue_page(self, prefix, after, limit).await
    }

    async fn delete_queue(&self, queue_url: &str) -> QueueResult<()> {
        crate::queue_catalog::delete_queue(self, queue_url).await
    }

    async fn change_queue_tags(
        &self,
        queue_url: &str,
        added: BTreeMap<String, String>,
        removed: Vec<String>,
    ) -> QueueResult<()> {
        crate::queue_catalog::change_queue_tags(self, queue_url, added, removed).await
    }

    async fn list_queue_tags(
        &self,
        queue_url: &str,
    ) -> QueueResult<Option<BTreeMap<String, String>>> {
        Ok(self.get_queue(queue_url).await?.map(|queue| queue.tags))
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

    async fn list_dead_letter_source_queues(
        &self,
        queue_url: &str,
        exclusive_start: Option<&str>,
        max_results: NonZeroU32,
    ) -> QueueResult<Option<ListDeadLetterSourceQueuesResponse>> {
        crate::queue_catalog::list_dead_letter_source_queues(
            self,
            queue_url,
            exclusive_start,
            max_results,
        )
        .await
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
        visibility_timeout: Option<DurationSeconds>,
        _wait_time_seconds: Option<DurationSeconds>,
    ) -> QueueResult<ReceivedMessages> {
        super::queue_receive::receive_messages(self, queue_url, max_messages, visibility_timeout)
            .await
    }

    async fn probe_receive(&self, queue_url: &str) -> QueueResult<Option<ReceiveProbe>> {
        super::queue_receive::probe_receive(self, queue_url).await
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
