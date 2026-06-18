use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
};

use pubsub_provider::{
    ClaimDeliveryRecordsRequest, CreateTopicRequest, DeliveryRecord, DeliveryRecordId,
    DeliveryRecordKind, DeliveryStatus, DeliveryTarget, PubsubMessageId, PubsubProvider,
    SubscribeRequest, SubscriptionProtocol, TopicName,
};
use storage_condition::Condition;
use storage_types::{SerializesToKey, StorageResult, TimestampMillis};
use uuid::Uuid;

use crate::{
    RocksDbKvStore, SortedKvDbStorageProvider,
    kv_support_tests::rocksdb_test_path,
    sorted_kv_store::{
        BatchItem, DirectWriteOperation, OldNewItems, RangeResult, RangeValuesResult,
        SortedKvStore, TransactWriteOperation, TransactWriteOutput, TransactWriteTableOperation,
    },
};

#[derive(Clone)]
struct ObservingPubsubKvStore {
    inner: RocksDbKvStore,
    stats: Arc<Mutex<PubsubShapeStats>>,
}

#[derive(Clone, Debug, Default)]
struct PubsubShapeStats {
    point_gets: u64,
    multi_get_calls: u64,
    multi_get_keys: u64,
    range_reads: u64,
    range_entries: u64,
    unchecked_transact_writes: u64,
    puts: u64,
    deletes: u64,
    check_values: u64,
    ordinary_point_reads: u64,
    ordinary_range_reads: u64,
    blind_writes: u64,
    read_modify_writes: u64,
    read_key_bytes: u64,
    write_key_bytes: u64,
    serial_awaits: u64,
}

impl ObservingPubsubKvStore {
    fn new(inner: RocksDbKvStore) -> Self {
        Self {
            inner,
            stats: Arc::new(Mutex::new(PubsubShapeStats::default())),
        }
    }

    fn reset(&self) {
        *self.lock_stats() = PubsubShapeStats::default();
    }

    fn snapshot(&self) -> PubsubShapeStats {
        self.lock_stats().clone()
    }

    fn lock_stats(&self) -> MutexGuard<'_, PubsubShapeStats> {
        match self.stats.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn record_serial_await(&self) {
        let mut stats = self.lock_stats();
        stats.serial_awaits = stats.serial_awaits.saturating_add(1);
    }
}

#[async_trait::async_trait]
impl SortedKvStore for ObservingPubsubKvStore {
    async fn transact_write(
        &self,
        operations: Vec<TransactWriteOperation>,
    ) -> StorageResult<TransactWriteOutput> {
        self.record_serial_await();
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
        self.inner
            .transact_write_table(operations, immediate_gsi_consistency)
            .await
    }

    async fn batch_write(&self, items: Vec<BatchItem>) -> StorageResult<()> {
        self.record_serial_await();
        self.inner.batch_write(items).await
    }

    async fn get(&self, key: &[u8], consistent_read: bool) -> StorageResult<Option<Vec<u8>>> {
        let value = self.inner.get(key, consistent_read).await?;
        let mut stats = self.lock_stats();
        stats.point_gets = stats.point_gets.saturating_add(1);
        if consistent_read {
            stats.ordinary_point_reads = stats.ordinary_point_reads.saturating_add(1);
        }
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
        let key_count = keys.len();
        let key_bytes = keys.iter().map(Vec::len).sum::<usize>();
        let values = self.inner.multi_get(keys, consistent_read).await?;
        let mut stats = self.lock_stats();
        stats.multi_get_calls = stats.multi_get_calls.saturating_add(1);
        stats.multi_get_keys = stats
            .multi_get_keys
            .saturating_add(u64::try_from(key_count).unwrap_or(u64::MAX));
        if consistent_read {
            stats.ordinary_point_reads = stats
                .ordinary_point_reads
                .saturating_add(u64::try_from(key_count).unwrap_or(u64::MAX));
        }
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
            stats.write_key_bytes = stats
                .write_key_bytes
                .saturating_add(u64::try_from(key.len()).unwrap_or(u64::MAX));
            if condition.is_some() {
                stats.read_modify_writes = stats.read_modify_writes.saturating_add(1);
            } else {
                stats.blind_writes = stats.blind_writes.saturating_add(1);
            }
        }
        self.inner.put(key, value, condition).await
    }

    async fn delete(&self, key: &[u8]) -> StorageResult<()> {
        self.record_serial_await();
        self.inner.delete(key).await
    }

    async fn delete_prefix(&self, prefix: Vec<u8>) -> StorageResult<()> {
        self.record_serial_await();
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
        let result = self
            .inner
            .get_range(start, exclusive_end, limit, page_token, consistent_read)
            .await?;
        let mut stats = self.lock_stats();
        stats.range_reads = stats.range_reads.saturating_add(1);
        stats.range_entries = stats
            .range_entries
            .saturating_add(u64::try_from(result.items.len()).unwrap_or(u64::MAX));
        if consistent_read {
            stats.ordinary_range_reads = stats.ordinary_range_reads.saturating_add(1);
        }
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
        let range = self
            .get_range(start, exclusive_end, limit, page_token, consistent_read)
            .await?;
        Ok(range.into_values_result())
    }
}

fn record_direct_ops(stats: &mut PubsubShapeStats, operations: &[DirectWriteOperation]) {
    stats.unchecked_transact_writes = stats.unchecked_transact_writes.saturating_add(1);
    let mut write_count = 0u64;
    let mut has_check = false;
    for operation in operations {
        match operation {
            DirectWriteOperation::Put { key, .. } => {
                stats.puts = stats.puts.saturating_add(1);
                write_count = write_count.saturating_add(1);
                stats.write_key_bytes = stats
                    .write_key_bytes
                    .saturating_add(u64::try_from(key.len()).unwrap_or(u64::MAX));
            }
            DirectWriteOperation::PutTemplate { template, .. } => {
                let key = template.rocks_key();
                stats.puts = stats.puts.saturating_add(1);
                write_count = write_count.saturating_add(1);
                stats.write_key_bytes = stats
                    .write_key_bytes
                    .saturating_add(u64::try_from(key.len()).unwrap_or(u64::MAX));
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
                has_check = true;
            }
        }
    }
    if has_check {
        stats.read_modify_writes = stats.read_modify_writes.saturating_add(write_count);
    } else {
        stats.blind_writes = stats.blind_writes.saturating_add(write_count);
    }
}

async fn observed_provider() -> (
    SortedKvDbStorageProvider<ObservingPubsubKvStore>,
    ObservingPubsubKvStore,
) {
    let store = ObservingPubsubKvStore::new(
        RocksDbKvStore::new(rocksdb_test_path("pubsub-shape")).unwrap(),
    );
    let provider = SortedKvDbStorageProvider::new(store.clone());
    PubsubProvider::initialize(&provider).await.unwrap();
    (provider, store)
}

fn delivery_record(subscription_arn: pubsub_provider::SubscriptionArn, id: &str) -> DeliveryRecord {
    DeliveryRecord {
        id: DeliveryRecordId(id.to_string()),
        kind: DeliveryRecordKind::Notification,
        message_id: PubsubMessageId::new_from_string(format!("message-{id}")).unwrap(),
        subscription_arn,
        message_body: Some("body".to_string()),
        subject: None,
        message_attributes: HashMap::new(),
        target: DeliveryTarget::BuiltIn,
        status: DeliveryStatus::Pending,
        attempts: 0,
        next_attempt_at: None,
        lease_owner: None,
        lease_expires_at: None,
        last_error: None,
        created_at: TimestampMillis::from(1_000),
        updated_at: TimestampMillis::from(1_000),
    }
}

#[tokio::test]
async fn pubsub_delivery_record_shape_tests() {
    let (provider, store) = observed_provider().await;
    let topic = provider
        .create_topic(CreateTopicRequest {
            name: TopicName::new(format!("pubsub-shape-{}", Uuid::now_v7())).unwrap(),
            attributes: HashMap::new(),
        })
        .await
        .unwrap();
    let subscription = provider
        .create_subscription(SubscribeRequest {
            topic_arn: topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Queue,
            endpoint: "https://queue.example.test/000000000000/pubsub-shape".to_string(),
            attributes: HashMap::new(),
            extra_json: serde_json::Value::Null,
        })
        .await
        .unwrap();

    store.reset();
    provider
        .put_delivery_records(vec![
            delivery_record(subscription.subscription_arn.clone(), "record-1"),
            delivery_record(subscription.subscription_arn.clone(), "record-2"),
        ])
        .await
        .unwrap();
    let stats = store.snapshot();
    assert_eq!(stats.multi_get_calls, 0);
    assert_eq!(stats.multi_get_keys, 0);
    assert!((3..=5).contains(&stats.unchecked_transact_writes));
    assert!(stats.puts >= 10);
    assert!(stats.blind_writes >= 8);
    assert!(stats.read_modify_writes >= 2);

    store.reset();
    let claim = provider
        .claim_delivery_records(ClaimDeliveryRecordsRequest {
            owner: "shape-worker".to_string(),
            now: TimestampMillis::from(2_000),
            lease_expires_at: TimestampMillis::from(3_000),
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(claim.records.len(), 1);
    let stats = store.snapshot();
    assert_eq!(stats.range_reads, 1);
    assert!((2..=4).contains(&stats.point_gets));
    assert_eq!(stats.unchecked_transact_writes, 1);
    assert_eq!(stats.check_values, 1);
    assert_eq!(stats.puts, 1);
    assert_eq!(stats.read_modify_writes, 1);

    store.reset();
    provider.delete_topic(&topic.topic_arn).await.unwrap();
    let stats = store.snapshot();
    assert!(stats.range_reads >= 2);
    assert!(stats.point_gets >= 1);
    assert_eq!(stats.unchecked_transact_writes, 1);
    assert!(stats.deletes >= 6);
    assert!(stats.write_key_bytes > 0);
}
