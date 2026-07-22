use crate::queue_provider::*;

#[async_trait]
impl<S> QueueProvider for SortedKvDbStorageProvider<S>
where S: QueueKvStore + 'static
{
    async fn initialize(&self) -> QueueResult<()> {
        self.initialize_operation().await
    }

    async fn create_queue(&self, queue: Queue) -> QueueResult<Queue> {
        self.create_queue_operation(queue).await
    }

    async fn get_queue(&self, queue_url: &str) -> QueueResult<Option<Queue>> {
        self.get_queue_operation(queue_url).await
    }

    async fn get_queue_with_message_counts(
        &self,
        queue_url: &str,
    ) -> QueueResult<Option<(Queue, QueueMessageCounts)>> {
        self.get_queue_with_message_counts_operation(queue_url)
            .await
    }

    async fn get_queue_by_name(&self, queue_name: &str) -> QueueResult<Option<Queue>> {
        self.get_queue_by_name_operation(queue_name).await
    }

    async fn list_queues(&self, queue_name_prefix: Option<&str>) -> QueueResult<Vec<Queue>> {
        self.list_queues_operation(queue_name_prefix).await
    }

    async fn delete_queue(&self, queue_url: &str) -> QueueResult<()> {
        self.delete_queue_operation(queue_url).await
    }

    async fn purge_queue(&self, queue_url: &str) -> QueueResult<()> {
        self.purge_queue_operation(queue_url).await
    }

    async fn set_queue_attributes(
        &self,
        queue_url: &str,
        attributes: HashMap<String, String>,
    ) -> QueueResult<()> {
        self.set_queue_attributes_operation(queue_url, attributes)
            .await
    }

    async fn send_message(&self, message: QueueMessage) -> QueueResult<MessageId> {
        self.send_message_operation(message).await
    }

    async fn send_messages(
        &self,
        messages: Vec<QueueMessage>,
    ) -> QueueResult<Vec<QueueResult<MessageId>>> {
        self.send_messages_operation(messages).await
    }

    async fn receive_messages(
        &self,
        queue_url: &str,
        max_messages: u32,
        visibility_timeout: DurationSeconds,
        wait_time_seconds: DurationSeconds,
    ) -> QueueResult<Vec<MessageResponse>> {
        self.receive_messages_operation(
            queue_url,
            max_messages,
            visibility_timeout,
            wait_time_seconds,
        )
        .await
    }

    async fn delete_message(
        &self,
        queue_url: &str,
        receipt_handle: ReceiptHandle,
    ) -> QueueResult<()> {
        self.delete_message_operation(queue_url, receipt_handle)
            .await
    }

    async fn delete_messages(
        &self,
        queue_url: &str,
        receipt_handles: Vec<ReceiptHandle>,
    ) -> QueueResult<Vec<QueueResult<()>>> {
        self.delete_messages_operation(queue_url, receipt_handles)
            .await
    }

    async fn change_message_visibility(
        &self,
        queue_url: &str,
        receipt_handle: ReceiptHandle,
        visibility_timeout: DurationSeconds,
    ) -> QueueResult<()> {
        self.change_message_visibility_operation(queue_url, receipt_handle, visibility_timeout)
            .await
    }

    async fn change_message_visibilities(
        &self,
        queue_url: &str,
        entries: Vec<(ReceiptHandle, DurationSeconds)>,
    ) -> QueueResult<Vec<QueueResult<()>>> {
        self.change_message_visibilities_operation(queue_url, entries)
            .await
    }

    async fn update_message_snapshot_checkpoint(
        &self,
        queue_url: &str,
        receipt_handle: ReceiptHandle,
        checkpoint_data: String,
    ) -> QueueResult<()> {
        self.update_message_snapshot_checkpoint_operation(
            queue_url,
            receipt_handle,
            checkpoint_data,
        )
        .await
    }
}
