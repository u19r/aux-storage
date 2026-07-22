use super::*;

#[async_trait::async_trait]
impl QueueKvStore for FoundationDbKvStore {
    async fn write_partitioned_queue_message(
        &self,
        message: PartitionedQueueMessageWrite,
    ) -> StorageResult<()> {
        self.write_partitioned_queue_message_operation(message)
            .await
    }

    async fn write_partitioned_queue_messages(
        &self,
        messages: Vec<PartitionedQueueMessageWrite>,
    ) -> StorageResult<()> {
        self.write_partitioned_queue_messages_operation(messages)
            .await
    }

    async fn prewarm_partitioned_queue(
        &self,
        queue_url: &str,
        partitions: Vec<QueuePrewarmPartition>,
    ) -> StorageResult<()> {
        self.prewarm_partitioned_queue_operation(queue_url, partitions)
            .await
    }

    async fn claim_queue_messages_from_ranges(
        &self,
        ranges: Vec<QueueClaimRange>,
        now: TimestampMillis,
        visibility_timeout: DurationSeconds,
        max_claims: usize,
    ) -> StorageResult<QueueClaimBatch> {
        self.claim_queue_messages_from_ranges_operation(ranges, now, visibility_timeout, max_claims)
            .await
    }
}
