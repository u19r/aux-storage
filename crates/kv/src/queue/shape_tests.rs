use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use queue_provider::{Queue, QueueMessage, QueueProvider, ReceiptHandle};
use storage_condition::Condition;
use storage_types::{
    DurationSeconds, SerializesToKey, StorageResult, StreamItemId, StreamName, TimestampMillis,
};

use crate::{
    RocksDbKvStore, SortedKvDbStorageProvider,
    kv_support_tests::rocksdb_test_path,
    partition_family::{PartitionFamilyKind, PartitionFamilyKvStore, RuntimePartitionLoadSample},
    queue::{
        PartitionedQueueMessageWrite, QueueClaimBatch, QueueClaimRange, QueueKvStore,
        QueuePrewarmPartition,
        storage::{
            claim_queue_messages_from_ranges_generic, prewarm_partitioned_queue_generic,
            write_partitioned_queue_message_generic,
        },
    },
    sorted_kv_store::{
        BatchItem, DirectWriteOperation, OldNewItems, RangeResult, RangeValuesResult,
        SortedKvStore, TransactWriteOperation, TransactWriteOutput, TransactWriteTableOperation,
    },
};

const QUEUE_URL: &str = "https://queue.example.test/000000000000/shape-profile";
const QUEUE_NAME: &str = "shape-profile";

#[derive(Clone)]
struct ObservingQueueKvStore {
    inner: RocksDbKvStore,
    stats: Arc<Mutex<QueueShapeStats>>,
}

#[derive(Clone, Debug, Default)]
struct QueueShapeStats {
    point_gets: u64,
    ordinary_point_reads: u64,
    snapshot_point_reads: u64,
    multi_get_calls: u64,
    multi_get_keys: u64,
    range_reads: u64,
    ordinary_range_reads: u64,
    snapshot_range_reads: u64,
    range_entries: u64,
    transact_writes: u64,
    unchecked_transact_writes: u64,
    batch_writes: u64,
    puts: u64,
    deletes: u64,
    checks: u64,
    check_values: u64,
    updates: u64,
    blind_writes: u64,
    read_modify_writes: u64,
    bytes_read: u64,
    bytes_written: u64,
    read_key_bytes: u64,
    write_key_bytes: u64,
    queue_claim_calls: u64,
    queue_claim_ranges: u64,
    queue_claim_ready_entries_seen: u64,
    queue_claim_messages: u64,
    queue_message_writes: u64,
    await_total: u64,
    await_serial_total: u64,
}

impl ObservingQueueKvStore {
    fn new(inner: RocksDbKvStore) -> Self {
        Self {
            inner,
            stats: Arc::new(Mutex::new(QueueShapeStats::default())),
        }
    }

    fn reset(&self) {
        *self.lock_stats() = QueueShapeStats::default();
    }

    fn snapshot(&self) -> QueueShapeStats {
        self.lock_stats().clone()
    }

    fn lock_stats(&self) -> MutexGuard<'_, QueueShapeStats> {
        match self.stats.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn record_await(&self) {
        let mut stats = self.lock_stats();
        stats.await_total = stats.await_total.saturating_add(1);
    }

    fn record_serial_await(&self) {
        let mut stats = self.lock_stats();
        stats.await_total = stats.await_total.saturating_add(1);
        stats.await_serial_total = stats.await_serial_total.saturating_add(1);
    }
}

#[async_trait::async_trait]
impl PartitionFamilyKvStore for ObservingQueueKvStore {
    fn supports_partition_families(&self) -> bool {
        self.inner.supports_partition_families()
    }

    async fn append_partitioned_ordered_log_item(
        &self,
        stream_name: &StreamName,
        routing_key: &[u8],
        value: &[u8],
        fallback_item_id: StreamItemId,
    ) -> StorageResult<Option<StreamItemId>> {
        self.record_await();
        self.inner
            .append_partitioned_ordered_log_item(stream_name, routing_key, value, fallback_item_id)
            .await
    }

    async fn drain_runtime_partition_load_samples(
        &self,
    ) -> StorageResult<Vec<RuntimePartitionLoadSample>> {
        self.record_await();
        self.inner.drain_runtime_partition_load_samples().await
    }

    fn partition_runtime_load_hint(
        &self,
        family_kind: PartitionFamilyKind,
        family_component: &str,
        partition_id: u16,
    ) -> u64 {
        self.inner
            .partition_runtime_load_hint(family_kind, family_component, partition_id)
    }

    async fn wait_for_change(&self, key: &[u8], timeout: Duration) -> StorageResult<bool> {
        self.record_await();
        self.inner.wait_for_change(key, timeout).await
    }

    async fn split_partitioned_ordered_log_family(
        &self,
        family_component: &str,
        partition_id: u16,
        now_ms: i64,
    ) -> StorageResult<bool> {
        self.record_await();
        self.inner
            .split_partitioned_ordered_log_family(family_component, partition_id, now_ms)
            .await
    }
}

#[async_trait::async_trait]
impl SortedKvStore for ObservingQueueKvStore {
    async fn transact_write(
        &self,
        operations: Vec<TransactWriteOperation>,
    ) -> StorageResult<TransactWriteOutput> {
        self.record_serial_await();
        record_transact_ops(&mut self.lock_stats(), &operations);
        self.inner.transact_write(operations).await
    }

    async fn transact_write_unchecked(
        &self,
        operations: Vec<DirectWriteOperation>,
    ) -> StorageResult<()> {
        self.record_serial_await();
        record_direct_ops(&mut self.lock_stats(), &operations);
        self.inner.transact_write_unchecked(operations).await
    }

    async fn transact_write_table(
        &self,
        operations: Vec<TransactWriteTableOperation>,
        immediate_gsi_consistency: bool,
    ) -> StorageResult<Vec<OldNewItems>> {
        self.record_serial_await();
        {
            let mut stats = self.lock_stats();
            stats.transact_writes = stats.transact_writes.saturating_add(1);
        }
        self.inner
            .transact_write_table(operations, immediate_gsi_consistency)
            .await
    }

    async fn batch_write(&self, items: Vec<BatchItem>) -> StorageResult<()> {
        self.record_serial_await();
        {
            let mut stats = self.lock_stats();
            stats.batch_writes = stats.batch_writes.saturating_add(1);
            for item in &items {
                match &item.value {
                    Some(value) => {
                        stats.puts = stats.puts.saturating_add(1);
                        stats.bytes_written = stats
                            .bytes_written
                            .saturating_add(u64::try_from(value.len()).unwrap_or(u64::MAX));
                    }
                    None => stats.deletes = stats.deletes.saturating_add(1),
                }
            }
        }
        self.inner.batch_write(items).await
    }

    async fn get(&self, key: &[u8], consistent_read: bool) -> StorageResult<Option<Vec<u8>>> {
        self.record_await();
        let value = self.inner.get(key, consistent_read).await?;
        let mut stats = self.lock_stats();
        stats.point_gets = stats.point_gets.saturating_add(1);
        record_read_shape(&mut stats, consistent_read, 1, 0);
        stats.bytes_read = stats
            .bytes_read
            .saturating_add(bytes_for_key_value(key.len(), value.as_ref().map(Vec::len)));
        stats.read_key_bytes = stats
            .read_key_bytes
            .saturating_add(u64::try_from(key.len()).unwrap_or(u64::MAX));
        Ok(value)
    }

    async fn multi_get(
        &self,
        keys: Vec<Vec<u8>>,
        consistent_read: bool,
    ) -> StorageResult<Vec<Option<Vec<u8>>>> {
        self.record_await();
        let key_bytes = keys.iter().map(Vec::len).sum::<usize>();
        let key_count = keys.len();
        let values = self.inner.multi_get(keys, consistent_read).await?;
        let value_bytes = values
            .iter()
            .filter_map(Option::as_ref)
            .map(Vec::len)
            .sum::<usize>();
        let mut stats = self.lock_stats();
        stats.multi_get_calls = stats.multi_get_calls.saturating_add(1);
        stats.multi_get_keys = stats
            .multi_get_keys
            .saturating_add(u64::try_from(key_count).unwrap_or(u64::MAX));
        record_read_shape(
            &mut stats,
            consistent_read,
            u64::try_from(key_count).unwrap_or(u64::MAX),
            0,
        );
        stats.bytes_read = stats
            .bytes_read
            .saturating_add(u64::try_from(key_bytes + value_bytes).unwrap_or(u64::MAX));
        stats.read_key_bytes = stats
            .read_key_bytes
            .saturating_add(u64::try_from(key_bytes).unwrap_or(u64::MAX));
        Ok(values)
    }

    async fn put(
        &self,
        key: &[u8],
        value: &[u8],
        condition: Option<Condition>,
    ) -> StorageResult<()> {
        self.record_serial_await();
        {
            let mut stats = self.lock_stats();
            stats.puts = stats.puts.saturating_add(1);
            if condition.is_some() {
                stats.read_modify_writes = stats.read_modify_writes.saturating_add(1);
            } else {
                stats.blind_writes = stats.blind_writes.saturating_add(1);
            }
            stats.bytes_written = stats
                .bytes_written
                .saturating_add(u64::try_from(value.len()).unwrap_or(u64::MAX));
            stats.write_key_bytes = stats
                .write_key_bytes
                .saturating_add(u64::try_from(key.len()).unwrap_or(u64::MAX));
            if condition.is_some() {
                stats.checks = stats.checks.saturating_add(1);
                stats.ordinary_point_reads = stats.ordinary_point_reads.saturating_add(1);
                stats.read_key_bytes = stats
                    .read_key_bytes
                    .saturating_add(u64::try_from(key.len()).unwrap_or(u64::MAX));
            }
        }
        self.inner.put(key, value, condition).await
    }

    async fn delete(&self, key: &[u8]) -> StorageResult<()> {
        self.record_serial_await();
        {
            let mut stats = self.lock_stats();
            stats.deletes = stats.deletes.saturating_add(1);
            stats.blind_writes = stats.blind_writes.saturating_add(1);
            stats.write_key_bytes = stats
                .write_key_bytes
                .saturating_add(u64::try_from(key.len()).unwrap_or(u64::MAX));
        }
        self.inner.delete(key).await
    }

    async fn delete_prefix(&self, prefix: Vec<u8>) -> StorageResult<()> {
        self.record_serial_await();
        {
            let mut stats = self.lock_stats();
            stats.deletes = stats.deletes.saturating_add(1);
        }
        self.inner.delete_prefix(prefix).await
    }

    async fn get_range(
        &self,
        start: &[u8],
        exclusive_end: &[u8],
        limit: Option<u32>,
        page_token: Option<impl SerializesToKey + Send + Sync>,
        consistent_read: bool,
    ) -> StorageResult<RangeResult> {
        self.record_await();
        let result = self
            .inner
            .get_range(start, exclusive_end, limit, page_token, consistent_read)
            .await?;
        let mut stats = self.lock_stats();
        stats.range_reads = stats.range_reads.saturating_add(1);
        record_read_shape(&mut stats, consistent_read, 0, 1);
        stats.range_entries = stats
            .range_entries
            .saturating_add(u64::try_from(result.items.len()).unwrap_or(u64::MAX));
        stats.bytes_read = stats.bytes_read.saturating_add(
            result
                .items
                .iter()
                .map(|(key, value)| key.len().saturating_add(value.len()))
                .sum::<usize>()
                .try_into()
                .unwrap_or(u64::MAX),
        );
        stats.read_key_bytes = stats.read_key_bytes.saturating_add(
            result
                .items
                .iter()
                .map(|(key, _)| key.len())
                .sum::<usize>()
                .try_into()
                .unwrap_or(u64::MAX),
        );
        Ok(result)
    }

    async fn get_range_values(
        &self,
        start: &[u8],
        exclusive_end: &[u8],
        limit: Option<u32>,
        page_token: Option<impl SerializesToKey + Send + Sync>,
        consistent_read: bool,
    ) -> StorageResult<RangeValuesResult> {
        self.record_await();
        let result = self
            .inner
            .get_range_values(start, exclusive_end, limit, page_token, consistent_read)
            .await?;
        let mut stats = self.lock_stats();
        stats.range_reads = stats.range_reads.saturating_add(1);
        record_read_shape(&mut stats, consistent_read, 0, 1);
        stats.range_entries = stats
            .range_entries
            .saturating_add(u64::try_from(result.values.len()).unwrap_or(u64::MAX));
        stats.bytes_read = stats.bytes_read.saturating_add(
            result
                .values
                .iter()
                .map(Vec::len)
                .sum::<usize>()
                .try_into()
                .unwrap_or(u64::MAX),
        );
        Ok(result)
    }
}

#[async_trait::async_trait]
impl QueueKvStore for ObservingQueueKvStore {
    async fn claim_queue_messages_from_ranges(
        &self,
        ranges: Vec<QueueClaimRange>,
        now: TimestampMillis,
        visibility_timeout: DurationSeconds,
        max_claims: usize,
    ) -> StorageResult<QueueClaimBatch> {
        {
            let mut stats = self.lock_stats();
            stats.queue_claim_calls = stats.queue_claim_calls.saturating_add(1);
            stats.queue_claim_ranges = stats
                .queue_claim_ranges
                .saturating_add(u64::try_from(ranges.len()).unwrap_or(u64::MAX));
        }
        let batch = claim_queue_messages_from_ranges_generic(
            self,
            ranges,
            now,
            visibility_timeout,
            max_claims,
        )
        .await?;
        let mut stats = self.lock_stats();
        stats.queue_claim_ready_entries_seen = stats
            .queue_claim_ready_entries_seen
            .saturating_add(u64::try_from(batch.ready_entries_seen).unwrap_or(u64::MAX));
        stats.queue_claim_messages = stats
            .queue_claim_messages
            .saturating_add(u64::try_from(batch.messages.len()).unwrap_or(u64::MAX));
        Ok(batch)
    }

    async fn write_partitioned_queue_message(
        &self,
        message: PartitionedQueueMessageWrite,
    ) -> StorageResult<()> {
        {
            let mut stats = self.lock_stats();
            stats.queue_message_writes = stats.queue_message_writes.saturating_add(1);
        }
        write_partitioned_queue_message_generic(self, message).await
    }

    async fn prewarm_partitioned_queue(
        &self,
        queue_url: &str,
        partitions: Vec<QueuePrewarmPartition>,
    ) -> StorageResult<()> {
        let _ = queue_url;
        prewarm_partitioned_queue_generic(partitions).await
    }
}

fn record_transact_ops(stats: &mut QueueShapeStats, operations: &[TransactWriteOperation]) {
    stats.transact_writes = stats.transact_writes.saturating_add(1);
    for operation in operations {
        match operation {
            TransactWriteOperation::Put {
                key,
                value,
                condition,
            } => {
                stats.puts = stats.puts.saturating_add(1);
                stats.write_key_bytes = stats
                    .write_key_bytes
                    .saturating_add(u64::try_from(key.len()).unwrap_or(u64::MAX));
                if condition.is_some() {
                    stats.read_modify_writes = stats.read_modify_writes.saturating_add(1);
                    stats.ordinary_point_reads = stats.ordinary_point_reads.saturating_add(1);
                    stats.read_key_bytes = stats
                        .read_key_bytes
                        .saturating_add(u64::try_from(key.len()).unwrap_or(u64::MAX));
                } else {
                    stats.blind_writes = stats.blind_writes.saturating_add(1);
                }
                stats.bytes_written = stats
                    .bytes_written
                    .saturating_add(u64::try_from(value.len()).unwrap_or(u64::MAX));
            }
            TransactWriteOperation::PutTemplate {
                template,
                value,
                condition,
            } => {
                stats.puts = stats.puts.saturating_add(1);
                let key = template.rocks_key();
                stats.write_key_bytes = stats
                    .write_key_bytes
                    .saturating_add(u64::try_from(key.len()).unwrap_or(u64::MAX));
                if condition.is_some() {
                    stats.read_modify_writes = stats.read_modify_writes.saturating_add(1);
                    stats.ordinary_point_reads = stats.ordinary_point_reads.saturating_add(1);
                    stats.read_key_bytes = stats
                        .read_key_bytes
                        .saturating_add(u64::try_from(key.len()).unwrap_or(u64::MAX));
                } else {
                    stats.blind_writes = stats.blind_writes.saturating_add(1);
                }
                stats.bytes_written = stats
                    .bytes_written
                    .saturating_add(u64::try_from(value.len()).unwrap_or(u64::MAX));
            }
            TransactWriteOperation::Delete { key, condition } => {
                stats.deletes = stats.deletes.saturating_add(1);
                stats.write_key_bytes = stats
                    .write_key_bytes
                    .saturating_add(u64::try_from(key.len()).unwrap_or(u64::MAX));
                if condition.is_some() {
                    stats.read_modify_writes = stats.read_modify_writes.saturating_add(1);
                    stats.ordinary_point_reads = stats.ordinary_point_reads.saturating_add(1);
                    stats.read_key_bytes = stats
                        .read_key_bytes
                        .saturating_add(u64::try_from(key.len()).unwrap_or(u64::MAX));
                } else {
                    stats.blind_writes = stats.blind_writes.saturating_add(1);
                }
            }
            TransactWriteOperation::Check { key, .. } => {
                stats.checks = stats.checks.saturating_add(1);
                stats.ordinary_point_reads = stats.ordinary_point_reads.saturating_add(1);
                stats.read_key_bytes = stats
                    .read_key_bytes
                    .saturating_add(u64::try_from(key.len()).unwrap_or(u64::MAX));
            }
            TransactWriteOperation::CheckValue { key, .. } => {
                stats.check_values = stats.check_values.saturating_add(1);
                stats.ordinary_point_reads = stats.ordinary_point_reads.saturating_add(1);
                stats.read_key_bytes = stats
                    .read_key_bytes
                    .saturating_add(u64::try_from(key.len()).unwrap_or(u64::MAX));
            }
            TransactWriteOperation::Update { key, condition, .. } => {
                stats.updates = stats.updates.saturating_add(1);
                stats.read_modify_writes = stats.read_modify_writes.saturating_add(1);
                stats.ordinary_point_reads = stats.ordinary_point_reads.saturating_add(1);
                stats.write_key_bytes = stats
                    .write_key_bytes
                    .saturating_add(u64::try_from(key.len()).unwrap_or(u64::MAX));
                stats.read_key_bytes = stats
                    .read_key_bytes
                    .saturating_add(u64::try_from(key.len()).unwrap_or(u64::MAX));
                if condition.is_some() {
                    stats.ordinary_point_reads = stats.ordinary_point_reads.saturating_add(1);
                    stats.read_key_bytes = stats
                        .read_key_bytes
                        .saturating_add(u64::try_from(key.len()).unwrap_or(u64::MAX));
                }
            }
        }
    }
}

fn record_direct_ops(stats: &mut QueueShapeStats, operations: &[DirectWriteOperation]) {
    stats.unchecked_transact_writes = stats.unchecked_transact_writes.saturating_add(1);
    let mut write_count = 0u64;
    for operation in operations {
        match operation {
            DirectWriteOperation::Put { key, value } => {
                stats.puts = stats.puts.saturating_add(1);
                write_count = write_count.saturating_add(1);
                stats.write_key_bytes = stats
                    .write_key_bytes
                    .saturating_add(u64::try_from(key.len()).unwrap_or(u64::MAX));
                stats.bytes_written = stats
                    .bytes_written
                    .saturating_add(u64::try_from(value.len()).unwrap_or(u64::MAX));
            }
            DirectWriteOperation::PutTemplate { template, value } => {
                stats.puts = stats.puts.saturating_add(1);
                write_count = write_count.saturating_add(1);
                let key = template.rocks_key();
                stats.write_key_bytes = stats
                    .write_key_bytes
                    .saturating_add(u64::try_from(key.len()).unwrap_or(u64::MAX));
                stats.bytes_written = stats
                    .bytes_written
                    .saturating_add(u64::try_from(value.len()).unwrap_or(u64::MAX));
            }
            DirectWriteOperation::Delete { key } => {
                stats.deletes = stats.deletes.saturating_add(1);
                write_count = write_count.saturating_add(1);
                stats.write_key_bytes = stats
                    .write_key_bytes
                    .saturating_add(u64::try_from(key.len()).unwrap_or(u64::MAX));
            }
            DirectWriteOperation::DeleteRange {
                start,
                exclusive_end,
            } => {
                stats.deletes = stats.deletes.saturating_add(1);
                write_count = write_count.saturating_add(1);
                stats.write_key_bytes = stats.write_key_bytes.saturating_add(
                    u64::try_from(start.len().saturating_add(exclusive_end.len()))
                        .unwrap_or(u64::MAX),
                );
            }
            DirectWriteOperation::CheckValue { key, .. } => {
                stats.check_values = stats.check_values.saturating_add(1);
                stats.ordinary_point_reads = stats.ordinary_point_reads.saturating_add(1);
                stats.read_key_bytes = stats
                    .read_key_bytes
                    .saturating_add(u64::try_from(key.len()).unwrap_or(u64::MAX));
            }
        }
    }
    if operations
        .iter()
        .any(|operation| matches!(operation, DirectWriteOperation::CheckValue { .. }))
    {
        stats.read_modify_writes = stats.read_modify_writes.saturating_add(write_count);
    } else {
        stats.blind_writes = stats.blind_writes.saturating_add(write_count);
    }
}

fn bytes_for_key_value(key_len: usize, value_len: Option<usize>) -> u64 {
    u64::try_from(key_len.saturating_add(value_len.unwrap_or_default())).unwrap_or(u64::MAX)
}

fn record_read_shape(
    stats: &mut QueueShapeStats,
    consistent_read: bool,
    point_reads: u64,
    range_reads: u64,
) {
    if consistent_read {
        stats.ordinary_point_reads = stats.ordinary_point_reads.saturating_add(point_reads);
        stats.ordinary_range_reads = stats.ordinary_range_reads.saturating_add(range_reads);
    } else {
        stats.snapshot_point_reads = stats.snapshot_point_reads.saturating_add(point_reads);
        stats.snapshot_range_reads = stats.snapshot_range_reads.saturating_add(range_reads);
    }
}

fn queue() -> Queue {
    Queue {
        queue_name: QUEUE_NAME.to_string(),
        queue_url: QUEUE_URL.to_string(),
        attributes: HashMap::new(),
        created_at: TimestampMillis::from(1_700_000_000_000),
    }
}

fn message(index: usize) -> QueueMessage {
    QueueMessage {
        queue_url: QUEUE_URL.to_string(),
        body: format!("shape-message-{index:04}"),
        created_at: TimestampMillis::from(1_700_000_000_000 + index as i64),
        visibility_timestamp: Some(TimestampMillis::from(1_700_000_000_000)),
        ..Default::default()
    }
}

async fn create_observed_provider() -> (
    SortedKvDbStorageProvider<ObservingQueueKvStore>,
    ObservingQueueKvStore,
) {
    let store =
        ObservingQueueKvStore::new(RocksDbKvStore::new(rocksdb_test_path("queue-shape")).unwrap());
    let provider = SortedKvDbStorageProvider::new(store.clone());
    provider.initialize().await.expect("initialize provider");
    provider.create_queue(queue()).await.expect("create queue");
    (provider, store)
}

#[tokio::test]
async fn send_1_small_message_shape_tests() {
    let (provider, store) = create_observed_provider().await;
    provider
        .send_message(message(0))
        .await
        .expect("warm partition family");
    store.reset();

    provider
        .send_message(message(1))
        .await
        .expect("send small message");

    let stats = store.snapshot();
    assert_eq!(stats.queue_message_writes, 1);
    assert_eq!(stats.unchecked_transact_writes, 1);
    assert_eq!(stats.check_values, 0);
    assert_eq!(stats.puts, 5);
    assert_eq!(stats.deletes, 0);
    assert_eq!(stats.ordinary_point_reads, 0);
    assert_eq!(stats.snapshot_point_reads, 0);
    assert_eq!(stats.blind_writes, 5);
    assert_eq!(stats.read_modify_writes, 0);
    assert!(stats.write_key_bytes > 0);
    assert_eq!(stats.await_serial_total, 1);
}

#[tokio::test]
async fn queue_receive_delete_and_drain_shape_tests() {
    let (provider, store) = create_observed_provider().await;
    for index in 0..40 {
        provider
            .send_message(message(index))
            .await
            .expect("seed message");
    }
    store.reset();

    let mut messages = Vec::new();
    for _ in 0..16 {
        if messages.len() >= 10 {
            break;
        }
        let mut received = provider
            .receive_messages(
                QUEUE_URL,
                10 - u32::try_from(messages.len()).unwrap_or(10),
                DurationSeconds::from(30),
                DurationSeconds::from(0),
            )
            .await
            .expect("receive messages");
        messages.append(&mut received);
    }

    let stats = store.snapshot();
    assert_eq!(messages.len(), 10);
    assert!((1..=32).contains(&stats.queue_claim_calls));
    assert!(stats.queue_claim_ranges <= 1024);
    assert!(stats.range_reads <= 1024);
    assert!(stats.multi_get_calls <= 128);
    assert!(stats.multi_get_keys <= 2048);
    assert!(stats.snapshot_point_reads >= stats.multi_get_keys);
    assert_eq!(stats.snapshot_range_reads, 0);
    assert!(stats.ordinary_point_reads >= 10);
    assert!(stats.ordinary_range_reads >= 1);
    assert_eq!(stats.queue_claim_messages, 10);
    assert!(stats.queue_claim_ready_entries_seen >= 10);
    assert_eq!(stats.unchecked_transact_writes, 10);
    assert_eq!(stats.read_modify_writes, 30);
    assert!(stats.read_key_bytes > 0);
    assert!(stats.write_key_bytes > 0);

    store.reset();

    let visibility_results = provider
        .change_message_visibilities(
            QUEUE_URL,
            messages
                .iter()
                .map(|message| {
                    (
                        ReceiptHandle(message.receipt_handle.clone()),
                        DurationSeconds::from(60),
                    )
                })
                .collect(),
        )
        .await
        .expect("change visibility batch");
    assert!(visibility_results.into_iter().all(|result| result.is_ok()));

    let stats = store.snapshot();
    assert!((10..=128).contains(&stats.point_gets));
    assert_eq!(stats.unchecked_transact_writes, 1);
    assert_eq!(stats.check_values, 10);
    assert_eq!(stats.deletes, 10);
    assert_eq!(stats.puts, 21);
    assert!((20..=256).contains(&stats.ordinary_point_reads));
    assert_eq!(stats.snapshot_point_reads, 0);
    assert_eq!(stats.blind_writes, 0);
    assert_eq!(stats.read_modify_writes, 31);

    for index in 0..10 {
        provider
            .send_message(message(index + 50))
            .await
            .expect("seed delete message");
    }
    let mut delete_messages = Vec::new();
    for _ in 0..16 {
        if delete_messages.len() >= 10 {
            break;
        }
        let mut received = provider
            .receive_messages(
                QUEUE_URL,
                10 - u32::try_from(delete_messages.len()).unwrap_or(10),
                DurationSeconds::from(30),
                DurationSeconds::from(0),
            )
            .await
            .expect("receive delete messages");
        delete_messages.append(&mut received);
    }
    assert_eq!(delete_messages.len(), 10);
    store.reset();

    let delete_results = provider
        .delete_messages(
            QUEUE_URL,
            delete_messages
                .into_iter()
                .map(|message| ReceiptHandle(message.receipt_handle))
                .collect(),
        )
        .await
        .expect("delete batch");
    assert!(delete_results.into_iter().all(|result| result.is_ok()));

    let stats = store.snapshot();
    assert!(stats.point_gets <= 1);
    assert_eq!(stats.unchecked_transact_writes, 1);
    assert_eq!(stats.check_values, 10);
    assert_eq!(stats.puts, 10);
    assert_eq!(stats.deletes, 10);
    assert!((10..=16).contains(&stats.ordinary_point_reads));
    assert_eq!(stats.snapshot_point_reads, 0);
    assert_eq!(stats.blind_writes, 0);
    assert_eq!(stats.read_modify_writes, 20);
    assert!(stats.read_key_bytes > 0);
    assert!(stats.write_key_bytes > 0);

    for index in 0..25 {
        provider
            .send_message(message(index + 100))
            .await
            .expect("seed message");
    }

    let mut received = 0usize;
    for _ in 0..5 {
        let messages = provider
            .receive_messages(
                QUEUE_URL,
                10,
                DurationSeconds::from(30),
                DurationSeconds::from(0),
            )
            .await
            .expect("receive batch");
        received = received.saturating_add(messages.len());
        if received >= 25 {
            break;
        }
    }

    assert!(received >= 25);
}
