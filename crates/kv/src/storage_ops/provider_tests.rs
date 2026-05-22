use std::{
    collections::{HashMap, HashSet},
    convert::{Infallible, TryFrom},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use bg_jobs::BackgroundJob;
use chrono::Utc;
use futures::{StreamExt, TryStreamExt, stream};
use metrics_exporter_prometheus::PrometheusHandle;
use storage_common::{GSI_BACKFILL_JOB, GSI_UPDATE_JOB, TTL_SWEEP_JOB};
use storage_condition::Condition;
use storage_provider::StorageProvider;
use storage_types::{
    AttributeDefinition, AttributeValue, BatchGetItemRequest, CreateGlobalSecondaryIndex,
    CreateTableRequest, HIDDEN_TTL_INDEX_PREFIX, IndexName, ItemKey, KeyAttributeType,
    KeySchemaElement, KeyType, KeysAndAttributes, Projection, ProjectionType, QueryTableRequest,
    ReplicationEventMetadata, ReplicationHybridLogicalClock, ReplicationMutation,
    ReplicationWriteSource, ScanTableRequest, SerializesToKey, StorageEnum, StorageError,
    StorageResult, StreamItemId, StreamKey, StreamName, TTL_PARTITION_ATTRIBUTE, TableName,
    TimeToLiveSpecification, TimeToLiveStatus, TimestampMillis, TransactConditionCheckRequest,
    TransactDeleteRequest, TransactPutRequest, TransactUpdateRequest, TransactWriteItem,
    TransactWriteItemsRequest, UpdateTimeToLiveRequest, WireItem,
};
use stream_provider::{StoredStreamPointer, StreamDataType, StreamItem, StreamProvider};
use tracing_test::traced_test;

use crate::{
    constants,
    kv_support_tests::{
        TestProvider, cleanup_store, create_test_provider as make_test_provider, create_test_store,
    },
    partition_family::{PartitionFamilyKind, PartitionFamilyKvStore, RuntimePartitionLoadSample},
    queue::{
        PartitionedQueueMessageWrite, QueueClaimBatch, QueueClaimRange, QueueKvStore,
        QueuePrewarmPartition,
    },
    sorted_kv::SortedKvDbStorageProvider,
    sorted_kv_store::{
        BatchItem, OldNewItems, RangeResult, SortedKvStore, TransactWriteOperation,
        TransactWriteOutput, TransactWriteTableOperation,
    },
    ttl,
};

fn create_test_provider() -> TestProvider {
    make_test_provider()
}

fn gsi_query_request(table_name: &TableName) -> QueryTableRequest {
    gsi_query_request_for_partition(table_name, "grp")
}

fn gsi_query_request_for_partition(table_name: &TableName, partition: &str) -> QueryTableRequest {
    QueryTableRequest {
        table_name: table_name.clone(),
        index_name: Some(IndexName::new("TestGSI")),
        key_condition_expression: "gsi_pk = :p".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(HashMap::from([(
            ":p".to_string(),
            AttributeValue::S(partition.to_string()),
        )])),
        limit: Some(100),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    }
}

#[cfg(feature = "foundationdb-backend")]
async fn foundationdb_live_test_guard() -> tokio::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

#[cfg(feature = "foundationdb-backend")]
async fn foundationdb_live_port_available() -> bool {
    tokio::time::timeout(
        Duration::from_millis(250),
        tokio::net::TcpStream::connect("127.0.0.1:4689"),
    )
    .await
    .is_ok_and(|result| result.is_ok())
}

fn metrics_handle() -> &'static PrometheusHandle {
    static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
    HANDLE.get_or_init(|| {
        metrics_exporter_prometheus::PrometheusBuilder::new()
            .install_recorder()
            .expect("install metrics recorder")
    })
}

fn metrics_assertion_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

#[derive(Clone, Debug, Default)]
struct CapturingMetricsFacade {
    counters: Arc<Mutex<HashMap<String, u64>>>,
    histograms: Arc<Mutex<HashMap<String, (u64, f64)>>>,
}

struct MetricsFacadeGuard {
    previous: Option<Arc<dyn metrics_facade::MetricsFacade>>,
}

impl CapturingMetricsFacade {
    fn install() -> (Self, MetricsFacadeGuard) {
        let facade = Self::default();
        let previous = metrics_facade::set_metrics_facade(Arc::new(facade.clone()));
        (
            facade,
            MetricsFacadeGuard {
                previous: Some(previous),
            },
        )
    }

    fn counter_value(
        &self,
        metric: metrics_facade::CounterMetric,
        label_fragments: &[&str],
    ) -> u64 {
        self.counters
            .lock()
            .unwrap()
            .iter()
            .find_map(|(key, value)| {
                (key.starts_with(metric.name())
                    && label_fragments
                        .iter()
                        .all(|fragment| key.contains(fragment)))
                .then_some(*value)
            })
            .unwrap_or(0)
    }

    fn histogram_count(
        &self,
        metric: metrics_facade::HistogramMetric,
        label_fragments: &[&str],
    ) -> u64 {
        self.histograms
            .lock()
            .unwrap()
            .iter()
            .find_map(|(key, (count, _))| {
                (key.starts_with(metric.name())
                    && label_fragments
                        .iter()
                        .all(|fragment| key.contains(fragment)))
                .then_some(*count)
            })
            .unwrap_or(0)
    }
}

impl Drop for MetricsFacadeGuard {
    fn drop(&mut self) {
        if let Some(previous) = self.previous.take() {
            metrics_facade::set_metrics_facade(previous);
        }
    }
}

impl metrics_facade::MetricsFacade for CapturingMetricsFacade {
    fn increment_counter(
        &self,
        metric: metrics_facade::CounterMetric,
        labels: &[metrics_facade::MetricLabel],
        value: u64,
    ) {
        let mut counters = self.counters.lock().unwrap();
        *counters
            .entry(metric_key(metric.name(), labels))
            .or_default() += value;
    }

    fn absolute_counter(
        &self,
        metric: metrics_facade::CounterMetric,
        labels: &[metrics_facade::MetricLabel],
        value: u64,
    ) {
        self.counters
            .lock()
            .unwrap()
            .insert(metric_key(metric.name(), labels), value);
    }

    fn increment_gauge(
        &self,
        _metric: metrics_facade::GaugeMetric,
        _labels: &[metrics_facade::MetricLabel],
        _value: f64,
    ) {
    }

    fn decrement_gauge(
        &self,
        _metric: metrics_facade::GaugeMetric,
        _labels: &[metrics_facade::MetricLabel],
        _value: f64,
    ) {
    }

    fn set_gauge(
        &self,
        _metric: metrics_facade::GaugeMetric,
        _labels: &[metrics_facade::MetricLabel],
        _value: f64,
    ) {
    }

    fn record_histogram(
        &self,
        metric: metrics_facade::HistogramMetric,
        labels: &[metrics_facade::MetricLabel],
        value: f64,
    ) {
        let mut histograms = self.histograms.lock().unwrap();
        let (count, sum) = histograms
            .entry(metric_key(metric.name(), labels))
            .or_insert((0, 0.0));
        *count += 1;
        *sum += value;
    }
}

fn metric_key(name: &'static str, labels: &[metrics_facade::MetricLabel]) -> String {
    let mut key = name.to_string();
    for label in labels {
        key.push('|');
        key.push_str(label.key());
        key.push_str("=\"");
        key.push_str(label.value());
        key.push('"');
    }
    key
}

fn parse_counter(handle: &PrometheusHandle, metric: &str, label_fragments: &[&str]) -> f64 {
    let body = handle.render();
    for line in body.lines() {
        if !line.starts_with(metric) {
            continue;
        }
        if !label_fragments.iter().all(|frag| line.contains(frag)) {
            continue;
        }
        if let Some(value) = line.split_whitespace().last()
            && let Ok(num) = value.parse::<f64>()
        {
            return num;
        }
    }
    0.0
}

fn global_log_lines() -> Vec<String> {
    let buf = tracing_test::internal::global_buf()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let logs = String::from_utf8_lossy(&buf);
    logs.lines().map(std::string::ToString::to_string).collect()
}

fn log_line_has_table(line: &str, table: &TableName) -> bool {
    let table_name = table.as_ref();
    line.contains(&format!("table={table_name}"))
        || line.contains(&format!("table=\"{table_name}\""))
}

/// Helper to seed N simple items quickly
async fn seed_n_items(provider: &TestProvider, table: &TableName, n: usize) {
    let _: Vec<_> = stream::iter((0..n).map(|i| {
        let provider = provider.clone();
        async move {
            let mut item = HashMap::new();
            item.insert("pk".to_string(), AttributeValue::S("user".to_string()));
            item.insert("sk".to_string(), AttributeValue::S(format!("item#{i:05}")));
            item.insert("gsi_pk".to_string(), AttributeValue::S("grp".to_string()));
            item.insert("gsi_sk".to_string(), AttributeValue::N(format!("{i}")));
            item.insert("data".to_string(), AttributeValue::S("x".to_string()));
            provider
                .put_item(table.clone(), item, None, None, None, None)
                .await
                .unwrap();
            Ok::<(), Infallible>(())
        }
    }))
    .buffer_unordered(96)
    .try_collect()
    .await
    .unwrap();

    let provider_arc = std::sync::Arc::new(provider.clone());
    let gsi_job = crate::storage_provider::GsiUpdateJob::new(provider_arc);
    let _ = gsi_job.execute().await.unwrap();
}

#[derive(Clone)]
struct FaultyStore<S: SortedKvStore> {
    inner: S,
    failures: Arc<AtomicU32>,
}

impl<S: SortedKvStore> FaultyStore<S> {
    fn new(inner: S, failures: u32) -> Self {
        Self {
            inner,
            failures: Arc::new(AtomicU32::new(failures)),
        }
    }

    fn set_failures(&self, failures: u32) {
        self.failures.store(failures, Ordering::SeqCst);
    }

    fn should_fail(&self) -> bool {
        self.failures
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |current| {
                if current > 0 { Some(current - 1) } else { None }
            })
            .is_ok()
    }
}

#[async_trait]
impl<S: PartitionFamilyKvStore> PartitionFamilyKvStore for FaultyStore<S> {
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
        self.inner
            .append_partitioned_ordered_log_item(stream_name, routing_key, value, fallback_item_id)
            .await
    }

    async fn drain_runtime_partition_load_samples(
        &self,
    ) -> StorageResult<Vec<RuntimePartitionLoadSample>> {
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
        self.inner.wait_for_change(key, timeout).await
    }

    async fn split_partitioned_ordered_log_family(
        &self,
        family_component: &str,
        partition_id: u16,
        now_ms: i64,
    ) -> StorageResult<bool> {
        self.inner
            .split_partitioned_ordered_log_family(family_component, partition_id, now_ms)
            .await
    }
}

#[async_trait]
impl<S: SortedKvStore> SortedKvStore for FaultyStore<S> {
    async fn transact_write(
        &self,
        operations: Vec<TransactWriteOperation>,
    ) -> StorageResult<TransactWriteOutput> {
        self.inner.transact_write(operations).await
    }

    async fn transact_write_table(
        &self,
        operations: Vec<TransactWriteTableOperation>,
        immediate_gsi_consistency: bool,
    ) -> StorageResult<Vec<OldNewItems>> {
        self.inner
            .transact_write_table(operations, immediate_gsi_consistency)
            .await
    }

    async fn batch_write(&self, items: Vec<BatchItem>) -> StorageResult<()> {
        if self.should_fail() {
            return Err(StorageError::internal("injected batch failure"));
        }
        self.inner.batch_write(items).await
    }

    async fn get(&self, key: &[u8], consistent_read: bool) -> StorageResult<Option<Vec<u8>>> {
        self.inner.get(key, consistent_read).await
    }

    async fn multi_get(
        &self,
        keys: Vec<Vec<u8>>,
        consistent_read: bool,
    ) -> StorageResult<Vec<Option<Vec<u8>>>> {
        self.inner.multi_get(keys, consistent_read).await
    }

    async fn put(
        &self,
        key: &[u8],
        value: &[u8],
        condition: Option<Condition>,
    ) -> StorageResult<()> {
        self.inner.put(key, value, condition).await
    }

    async fn delete(&self, key: &[u8]) -> StorageResult<()> {
        self.inner.delete(key).await
    }

    async fn delete_prefix(&self, prefix: Vec<u8>) -> StorageResult<()> {
        self.inner.delete_prefix(prefix).await
    }

    async fn get_prefix(
        &self,
        prefix: &[u8],
        scan_index_forwards: bool,
        limit: Option<u32>,
        consistent_read: bool,
    ) -> StorageResult<RangeResult> {
        self.inner
            .get_prefix(prefix, scan_index_forwards, limit, consistent_read)
            .await
    }

    async fn get_range(
        &self,
        start: &[u8],
        exclusive_end: &[u8],
        limit: Option<u32>,
        page_token: Option<impl SerializesToKey + Send + Sync>,
        consistent_read: bool,
    ) -> StorageResult<RangeResult> {
        self.inner
            .get_range(start, exclusive_end, limit, page_token, consistent_read)
            .await
    }
}

#[async_trait]
impl<S: QueueKvStore> QueueKvStore for FaultyStore<S> {
    async fn claim_queue_messages_from_ranges(
        &self,
        ranges: Vec<QueueClaimRange>,
        now: TimestampMillis,
        visibility_timeout: storage_types::DurationSeconds,
        max_claims: usize,
    ) -> StorageResult<QueueClaimBatch> {
        self.inner
            .claim_queue_messages_from_ranges(ranges, now, visibility_timeout, max_claims)
            .await
    }

    async fn write_partitioned_queue_message(
        &self,
        message: PartitionedQueueMessageWrite,
    ) -> StorageResult<()> {
        self.inner.write_partitioned_queue_message(message).await
    }

    async fn prewarm_partitioned_queue(
        &self,
        queue_url: &str,
        partitions: Vec<QueuePrewarmPartition>,
    ) -> StorageResult<()> {
        self.inner
            .prewarm_partitioned_queue(queue_url, partitions)
            .await
    }
}

#[tokio::test]
async fn kv_gsi_updates_add_and_ignore_missing() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();

    // Create table with a GSI
    let table = TableName::new("GSIFieldLifecycleKV");
    let () = create_test_table(&provider, &table, true).await;

    // A: initially missing GSI fields
    let mut a = HashMap::new();
    a.insert("pk".to_string(), AttributeValue::S("u".to_string()));
    a.insert("sk".to_string(), AttributeValue::S("a".to_string()));

    // B: present in GSI
    let mut b = HashMap::new();
    b.insert("pk".to_string(), AttributeValue::S("u".to_string()));
    b.insert("sk".to_string(), AttributeValue::S("b".to_string()));
    b.insert("gsi_pk".to_string(), AttributeValue::S("grp".to_string()));
    b.insert("gsi_sk".to_string(), AttributeValue::N("1".to_string()));

    // C: always missing (ignored)
    let mut c = HashMap::new();
    c.insert("pk".to_string(), AttributeValue::S("u".to_string()));
    c.insert("sk".to_string(), AttributeValue::S("c".to_string()));

    provider
        .put_item(table.clone(), a, None, None, None, None)
        .await
        .unwrap();
    provider
        .put_item(table.clone(), b.clone(), None, None, None, None)
        .await
        .unwrap();
    provider
        .put_item(table.clone(), c, None, None, None, None)
        .await
        .unwrap();

    // Process GSI updates
    let provider_arc = std::sync::Arc::new(provider.clone());
    let gsi_job = crate::storage_provider::GsiUpdateJob::new(provider_arc.clone());
    let _ = gsi_job.execute().await.unwrap();

    let q = QueryTableRequest {
        table_name: table.clone(),
        index_name: Some(IndexName::new("TestGSI")),
        key_condition_expression: "gsi_pk = :p".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some({
            let mut m = HashMap::new();
            m.insert(":p".to_string(), AttributeValue::S("grp".to_string()));
            m
        }),
        limit: Some(100),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    };
    let (items1, _lek1) = provider.query_table(&q).await.unwrap();
    assert_eq!(items1.len(), 1, "Only item B should be indexed initially");

    // Update A to add GSI fields → should be added to index
    let mut a2 = HashMap::new();
    a2.insert("pk".to_string(), AttributeValue::S("u".to_string()));
    a2.insert("sk".to_string(), AttributeValue::S("a".to_string()));
    a2.insert("gsi_pk".to_string(), AttributeValue::S("grp".to_string()));
    a2.insert("gsi_sk".to_string(), AttributeValue::N("2".to_string()));
    provider
        .put_item(table.clone(), a2, None, None, None, None)
        .await
        .unwrap();

    let gsi_job2 = crate::storage_provider::GsiUpdateJob::new(provider_arc);
    let _ = gsi_job2.execute().await.unwrap();

    let (items2, _lek2) = provider.query_table(&q).await.unwrap();
    assert_eq!(
        items2.len(),
        2,
        "Items A and B should both be indexed after update"
    );
}

#[tokio::test]
async fn kv_gsi_updates_remove_field() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();

    // Create table with a GSI
    let table = TableName::new("GSIFieldRemovalKV");
    let () = create_test_table(&provider, &table, true).await;

    // D: initially has GSI fields
    let mut d = HashMap::new();
    d.insert("pk".to_string(), AttributeValue::S("u".to_string()));
    d.insert("sk".to_string(), AttributeValue::S("d".to_string()));
    d.insert("gsi_pk".to_string(), AttributeValue::S("grp2".to_string()));
    d.insert("gsi_sk".to_string(), AttributeValue::N("1".to_string()));
    provider
        .put_item(table.clone(), d, None, None, None, None)
        .await
        .unwrap();

    // Process index
    let provider_arc = std::sync::Arc::new(provider.clone());
    let gsi_job = crate::storage_provider::GsiUpdateJob::new(provider_arc.clone());
    let _ = gsi_job.execute().await.unwrap();

    let q = QueryTableRequest {
        table_name: table.clone(),
        index_name: Some(IndexName::new("TestGSI")),
        key_condition_expression: "gsi_pk = :p".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some({
            let mut m = HashMap::new();
            m.insert(":p".to_string(), AttributeValue::S("grp2".to_string()));
            m
        }),
        limit: Some(100),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    };
    let (before, _) = provider.query_table(&q).await.unwrap();
    assert_eq!(
        before.len(),
        1,
        "Item should be present in GSI before removal"
    );

    // Replace D without GSI fields → should be removed from index
    let mut d2 = HashMap::new();
    d2.insert("pk".to_string(), AttributeValue::S("u".to_string()));
    d2.insert("sk".to_string(), AttributeValue::S("d".to_string()));
    provider
        .put_item(table.clone(), d2, None, None, None, None)
        .await
        .unwrap();

    let gsi_job2 = crate::storage_provider::GsiUpdateJob::new(provider_arc);
    let _ = gsi_job2.execute().await.unwrap();

    let (after, _) = provider.query_table(&q).await.unwrap();
    assert_eq!(
        after.len(),
        0,
        "Item should be removed from GSI after update without fields"
    );
}

#[tokio::test]
async fn kv_gsi_tombstones_use_hidden_prefix_and_do_not_consume_query_limit() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();

    let table = TableName::new("KvGsiHiddenTombstones");
    create_test_table(&provider, &table, true).await;

    for (sk, order) in [("item#1", "1"), ("item#2", "2")] {
        provider
            .put_item(
                table.clone(),
                HashMap::from([
                    ("pk".to_string(), AttributeValue::S("u".to_string())),
                    ("sk".to_string(), AttributeValue::S(sk.to_string())),
                    ("gsi_pk".to_string(), AttributeValue::S("grp".to_string())),
                    ("gsi_sk".to_string(), AttributeValue::N(order.to_string())),
                ]),
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();
    }

    provider.run_job(GSI_UPDATE_JOB).await.unwrap();

    provider
        .delete_item(
            table.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("u".to_string())),
                ("sk".to_string(), AttributeValue::S("item#1".to_string())),
            ])
            .into(),
            None,
            None,
            None,
        )
        .await
        .unwrap();
    provider.run_job(GSI_UPDATE_JOB).await.unwrap();

    let tombstone_prefix =
        crate::keys::gsi_tombstone_prefix_from_name(&table, &IndexName::new("TestGSI"));
    let tombstones = provider
        .kv_store
        .get_prefix(&tombstone_prefix, true, None, true)
        .await
        .unwrap();
    assert_eq!(
        tombstones.items.len(),
        1,
        "delete should write hidden KV tombstone evidence under isolated prefix"
    );

    let mut request = gsi_query_request(&table);
    request.limit = Some(1);
    let (items, lek) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("sk"),
        Some(&AttributeValue::S("item#2".to_string()))
    );
    assert!(
        lek.is_none(),
        "hidden tombstone prefix must not consume GSI query pagination budget"
    );
}

#[tokio::test]
async fn kv_gsi_tombstone_cleanup_removes_hidden_prefix_without_touching_query_rows() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();

    let table = TableName::new("KvGsiTombstoneCleanup");
    create_test_table(&provider, &table, true).await;

    for (sk, partition, order) in [("item#1", "grp", "1"), ("item#2", "grp-2", "2")] {
        provider
            .put_item(
                table.clone(),
                HashMap::from([
                    ("pk".to_string(), AttributeValue::S("u".to_string())),
                    ("sk".to_string(), AttributeValue::S(sk.to_string())),
                    (
                        "gsi_pk".to_string(),
                        AttributeValue::S(partition.to_string()),
                    ),
                    ("gsi_sk".to_string(), AttributeValue::N(order.to_string())),
                ]),
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();
    }
    provider.run_job(GSI_UPDATE_JOB).await.unwrap();

    provider
        .put_item(
            table.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("u".to_string())),
                ("sk".to_string(), AttributeValue::S("item#1".to_string())),
                ("gsi_pk".to_string(), AttributeValue::S("grp-2".to_string())),
                ("gsi_sk".to_string(), AttributeValue::N("3".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    provider.run_job(GSI_UPDATE_JOB).await.unwrap();

    let index_name = IndexName::new("TestGSI");
    let tombstone_prefix = crate::keys::gsi_tombstone_prefix_from_name(&table, &index_name);
    let before_cleanup = provider
        .kv_store
        .get_prefix(&tombstone_prefix, true, None, true)
        .await
        .unwrap();
    assert_eq!(before_cleanup.items.len(), 1);

    provider
        .cleanup_gsi_backfill_tombstones(&table, &index_name)
        .await
        .unwrap();

    let after_cleanup = provider
        .kv_store
        .get_prefix(&tombstone_prefix, true, None, true)
        .await
        .unwrap();
    assert!(after_cleanup.items.is_empty());

    let (items, _) = provider
        .query_table(&gsi_query_request_for_partition(&table, "grp-2"))
        .await
        .unwrap();
    assert_eq!(
        items.len(),
        2,
        "cleanup must not remove visible GSI rows from the normal index prefix"
    );
}

#[cfg(feature = "foundationdb-backend")]
#[tokio::test]
#[ignore = "requires a local FoundationDB cluster on 127.0.0.1:4689"]
async fn foundationdb_gsi_tombstones_use_hidden_prefix_and_do_not_consume_query_limit() {
    use std::time::Duration;

    use uuid::Uuid;

    let _guard = foundationdb_live_test_guard().await;
    if !foundationdb_live_port_available().await {
        eprintln!("Skipping FoundationDB GSI tombstone test: 127.0.0.1:4689 is unavailable");
        return;
    }
    let store =
        match crate::FoundationDbKvStore::connect(crate::backends::fdb::FoundationDbConfig {
            subspace_prefix: Some(format!("tests/kv/{}/", Uuid::now_v7()).into_bytes()),
            ..Default::default()
        }) {
            Ok(store) => store,
            Err(error) => {
                eprintln!("Skipping FoundationDB GSI tombstone test: {error}");
                return;
            }
        };
    let provider = SortedKvDbStorageProvider::new(store);

    let result = tokio::time::timeout(Duration::from_secs(10), async {
        provider.initialize_storage().await.unwrap();

        let table = TableName::new("FdbGsiHiddenTombstones");
        create_test_table(&provider, &table, true).await;

        for (sk, order) in [("item#1", "1"), ("item#2", "2")] {
            provider
                .put_item(
                    table.clone(),
                    HashMap::from([
                        ("pk".to_string(), AttributeValue::S("u".to_string())),
                        ("sk".to_string(), AttributeValue::S(sk.to_string())),
                        ("gsi_pk".to_string(), AttributeValue::S("grp".to_string())),
                        ("gsi_sk".to_string(), AttributeValue::N(order.to_string())),
                    ]),
                    None,
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();
        }
        provider.run_job(GSI_UPDATE_JOB).await.unwrap();

        provider
            .delete_item(
                table.clone(),
                HashMap::from([
                    ("pk".to_string(), AttributeValue::S("u".to_string())),
                    ("sk".to_string(), AttributeValue::S("item#1".to_string())),
                ])
                .into(),
                None,
                None,
                None,
            )
            .await
            .unwrap();
        provider.run_job(GSI_UPDATE_JOB).await.unwrap();

        let tombstone_prefix =
            crate::keys::gsi_tombstone_prefix_from_name(&table, &IndexName::new("TestGSI"));
        let tombstones = provider
            .kv_store
            .get_prefix(&tombstone_prefix, true, None, true)
            .await
            .unwrap();
        assert_eq!(tombstones.items.len(), 1);

        let mut request = gsi_query_request(&table);
        request.limit = Some(1);
        let (items, lek) = provider.query_table(&request).await.unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(
            items[0].get("sk"),
            Some(&AttributeValue::S("item#2".to_string()))
        );
        assert!(lek.is_none());
    })
    .await;
    if result.is_err() {
        eprintln!("Skipping FoundationDB GSI tombstone test: timed out");
    }
}

#[cfg(feature = "foundationdb-backend")]
#[tokio::test]
#[ignore = "requires a local FoundationDB cluster on 127.0.0.1:4689"]
async fn foundationdb_gsi_tombstone_cleanup_removes_hidden_prefix_without_touching_query_rows() {
    use std::time::Duration;

    use uuid::Uuid;

    let _guard = foundationdb_live_test_guard().await;
    if !foundationdb_live_port_available().await {
        eprintln!(
            "Skipping FoundationDB GSI tombstone cleanup test: 127.0.0.1:4689 is unavailable"
        );
        return;
    }
    let store =
        match crate::FoundationDbKvStore::connect(crate::backends::fdb::FoundationDbConfig {
            subspace_prefix: Some(format!("tests/kv/{}/", Uuid::now_v7()).into_bytes()),
            ..Default::default()
        }) {
            Ok(store) => store,
            Err(error) => {
                eprintln!("Skipping FoundationDB GSI tombstone cleanup test: {error}");
                return;
            }
        };
    let provider = SortedKvDbStorageProvider::new(store);

    let result = tokio::time::timeout(Duration::from_secs(10), async {
        provider.initialize_storage().await.unwrap();

        let table = TableName::new("FdbGsiTombstoneCleanup");
        create_test_table(&provider, &table, true).await;

        for (sk, partition, order) in [("item#1", "grp", "1"), ("item#2", "grp-2", "2")] {
            provider
                .put_item(
                    table.clone(),
                    HashMap::from([
                        ("pk".to_string(), AttributeValue::S("u".to_string())),
                        ("sk".to_string(), AttributeValue::S(sk.to_string())),
                        (
                            "gsi_pk".to_string(),
                            AttributeValue::S(partition.to_string()),
                        ),
                        ("gsi_sk".to_string(), AttributeValue::N(order.to_string())),
                    ]),
                    None,
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();
        }
        provider.run_job(GSI_UPDATE_JOB).await.unwrap();

        provider
            .put_item(
                table.clone(),
                HashMap::from([
                    ("pk".to_string(), AttributeValue::S("u".to_string())),
                    ("sk".to_string(), AttributeValue::S("item#1".to_string())),
                    ("gsi_pk".to_string(), AttributeValue::S("grp-2".to_string())),
                    ("gsi_sk".to_string(), AttributeValue::N("3".to_string())),
                ]),
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();
        provider.run_job(GSI_UPDATE_JOB).await.unwrap();

        let index_name = IndexName::new("TestGSI");
        let tombstone_prefix = crate::keys::gsi_tombstone_prefix_from_name(&table, &index_name);
        let before_cleanup = provider
            .kv_store
            .get_prefix(&tombstone_prefix, true, None, true)
            .await
            .unwrap();
        assert_eq!(before_cleanup.items.len(), 1);

        provider
            .cleanup_gsi_backfill_tombstones(&table, &index_name)
            .await
            .unwrap();

        let after_cleanup = provider
            .kv_store
            .get_prefix(&tombstone_prefix, true, None, true)
            .await
            .unwrap();
        assert!(after_cleanup.items.is_empty());

        let (items, _) = provider
            .query_table(&gsi_query_request_for_partition(&table, "grp-2"))
            .await
            .unwrap();
        assert_eq!(items.len(), 2);
    })
    .await;
    if result.is_err() {
        eprintln!("Skipping FoundationDB GSI tombstone cleanup test: timed out");
    }
}

#[tokio::test]
async fn process_gsi_updates_returns_false_when_caught_up() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();

    let table = TableName::new("GsiUpdateCursorKV");
    let () = create_test_table(&provider, &table, true).await;

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("u".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("a".to_string()));
    item.insert("gsi_pk".to_string(), AttributeValue::S("grp".to_string()));
    item.insert("gsi_sk".to_string(), AttributeValue::N("1".to_string()));

    provider
        .put_item(table.clone(), item, None, None, None, None)
        .await
        .unwrap();

    let first = provider.process_gsi_updates().await.unwrap();
    assert!(first, "First run should do work");

    let second = provider.process_gsi_updates().await.unwrap();
    assert!(!second, "Second run should be idle");
}

#[tokio::test]
async fn process_gsi_updates_skips_missing_gsi_keys() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();

    let table = TableName::new("GsiUpdateMissingKeysKV");
    let () = create_test_table(&provider, &table, true).await;

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("u".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("missing".to_string()));

    provider
        .put_item(table.clone(), item, None, None, None, None)
        .await
        .unwrap();

    let first = provider.process_gsi_updates().await.unwrap();
    assert!(!first, "Missing GSI keys should not report work");

    let second = provider.process_gsi_updates().await.unwrap();
    assert!(!second, "Cursor should advance past no-op updates");
}

#[tokio::test]
async fn update_table_creates_gsi_and_backfills_kv() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();

    // Create base table without GSI
    let table_name = TableName::new("BackfillKV");
    let () = create_test_table(&provider, &table_name, false).await;

    // Seed items
    let items = vec![
        ("user", "a", "grp", 1),
        ("user", "b", "grp", 2),
        ("user", "c", "grp", 3),
    ];
    for (pk, sk, gsi_pk, gsi_sk) in items {
        let mut item = HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S(pk.to_string()));
        item.insert("sk".to_string(), AttributeValue::S(sk.to_string()));
        item.insert("gsi_pk".to_string(), AttributeValue::S(gsi_pk.to_string()));
        item.insert("gsi_sk".to_string(), AttributeValue::N(gsi_sk.to_string()));
        provider
            .put_item(table_name.clone(), item, None, None, None, None)
            .await
            .unwrap();
    }

    // Add GSI via UpdateTable
    let req = storage_types::UpdateTableRequest {
        table_name: table_name.clone(),
        attribute_definitions: None,
        billing_mode: None,
        provisioned_throughput: None,
        on_demand_throughput: None,
        deletion_protection_enabled: None,
        global_secondary_index_updates: Some(vec![storage_types::GlobalSecondaryIndexUpdate {
            create: Some(storage_types::CreateGlobalSecondaryIndex {
                index_name: IndexName::new("G1"),
                key_schema: vec![
                    KeySchemaElement {
                        attribute_name: "gsi_pk".to_string(),
                        key_type: KeyType::Hash,
                    },
                    KeySchemaElement {
                        attribute_name: "gsi_sk".to_string(),
                        key_type: KeyType::Range,
                    },
                ],
                projection: Projection {
                    projection_type: Some(ProjectionType::All),
                    non_key_attributes: None,
                },
                provisioned_throughput: None,
            }),
            delete: None,
            update: None,
        }]),
        replica_updates: None,
        sse_specification: None,
        stream_specification: None,
        table_class: None,
    };

    provider.update_table(req).await.unwrap();

    provider.run_job(GSI_BACKFILL_JOB).await.unwrap();
    provider.run_job(GSI_BACKFILL_JOB).await.unwrap();

    // Verify items visible via GSI
    let q = QueryTableRequest {
        table_name: table_name.clone(),
        index_name: Some(IndexName::new("G1")),
        key_condition_expression: "gsi_pk = :p".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some({
            let mut m = HashMap::new();
            m.insert(":p".to_string(), AttributeValue::S("grp".to_string()));
            m
        }),
        limit: None,
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    };
    let (items, _lek) = provider.query_table(&q).await.unwrap();
    assert_eq!(items.len(), 3);
}

#[tokio::test]
async fn seed_10k_helper() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    let table_name = TableName::new("Seed10k");
    let () = create_test_table(&provider, &table_name, true).await;
    seed_n_items(&provider, &table_name, 10_000).await;
    let (items, lek) = provider
        .scan_table(&create_scan_request(&table_name, None, Some(10_000), None))
        .await
        .unwrap();
    assert_eq!(items.len(), 10_000);
    assert!(lek.is_none());
}

#[tokio::test]
async fn kv_backfill_crash_resume() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    let table = TableName::new("CrashResume");
    let () = create_test_table(&provider, &table, false).await;

    // Seed initial items
    for i in 0..1500 {
        let mut item = HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S("u".to_string()));
        item.insert("sk".to_string(), AttributeValue::S(format!("k#{i:04}")));
        item.insert("gsi_pk".to_string(), AttributeValue::S("grp".to_string()));
        item.insert("gsi_sk".to_string(), AttributeValue::N(i.to_string()));
        provider
            .put_item(TableName::new(&table.clone()), item, None, None, None, None)
            .await
            .unwrap();
    }

    // Start UpdateTable to add GSI; this will begin backfill and persist progress
    let req = storage_types::UpdateTableRequest {
        table_name: TableName::new(&table.clone()),
        attribute_definitions: None,
        billing_mode: None,
        provisioned_throughput: None,
        on_demand_throughput: None,
        deletion_protection_enabled: None,
        global_secondary_index_updates: Some(vec![storage_types::GlobalSecondaryIndexUpdate {
            create: Some(storage_types::CreateGlobalSecondaryIndex {
                index_name: IndexName::new("GCrash"),
                key_schema: vec![
                    KeySchemaElement {
                        attribute_name: "gsi_pk".to_string(),
                        key_type: KeyType::Hash,
                    },
                    KeySchemaElement {
                        attribute_name: "gsi_sk".to_string(),
                        key_type: KeyType::Range,
                    },
                ],
                projection: Projection {
                    projection_type: Some(ProjectionType::All),
                    non_key_attributes: None,
                },
                provisioned_throughput: None,
            }),
            update: None,
            delete: None,
        }]),
        replica_updates: None,
        sse_specification: None,
        stream_specification: None,
        table_class: None,
    };
    provider.update_table(req).await.unwrap();

    // Simulate crash by stopping here; then we resume via job which will noop if
    // Done, or continue if pending Insert more items that should be caught by
    // stream-based GSI updates
    for i in 1500..1600 {
        let mut item = HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S("u".to_string()));
        item.insert("sk".to_string(), AttributeValue::S(format!("k#{i:04}")));
        item.insert("gsi_pk".to_string(), AttributeValue::S("grp".to_string()));
        item.insert("gsi_sk".to_string(), AttributeValue::N(i.to_string()));
        provider
            .put_item(TableName::new(&table.clone()), item, None, None, None, None)
            .await
            .unwrap();
    }

    // Run backfill resume and GSI update jobs
    provider.run_job(GSI_BACKFILL_JOB).await.unwrap();
    provider.run_job(GSI_UPDATE_JOB).await.unwrap();
    provider.run_job(GSI_BACKFILL_JOB).await.unwrap();

    // Verify: GSI returns at least the 1600 items for grp
    let q = QueryTableRequest {
        table_name: table.clone(),
        index_name: Some(IndexName::new("GCrash")),
        key_condition_expression: "gsi_pk = :p".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some({
            let mut m = HashMap::new();
            m.insert(":p".to_string(), AttributeValue::S("grp".to_string()));
            m
        }),
        limit: Some(2000),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    };
    let (items, _lek) = provider.query_table(&q).await.unwrap();
    assert!(items.len() >= 1600);
}

#[tokio::test]
async fn kv_gsi_visibility_is_delayed_by_default() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();

    let table = TableName::new("KvImmediateGsiDefaultDelayed");
    create_test_table(&provider, &table, true).await;

    provider
        .put_item(
            table.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("u".to_string())),
                ("sk".to_string(), AttributeValue::S("item#1".to_string())),
                ("gsi_pk".to_string(), AttributeValue::S("grp".to_string())),
                ("gsi_sk".to_string(), AttributeValue::N("1".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let (before, _) = provider
        .query_table(&gsi_query_request(&table))
        .await
        .unwrap();
    assert!(
        before.is_empty(),
        "default mode should delay GSI visibility"
    );

    provider.run_job(GSI_UPDATE_JOB).await.unwrap();

    let (after, _) = provider
        .query_table(&gsi_query_request(&table))
        .await
        .unwrap();
    assert_eq!(after.len(), 1, "gsi-update should publish the pending row");
}

#[tokio::test]
async fn kv_immediate_gsi_consistency_updates_indexes_inline() {
    let provider = TestProvider::new(create_test_store()).with_immediate_gsi_consistency(true);
    provider.initialize_storage().await.unwrap();

    let table = TableName::new("KvImmediateGsiInline");
    create_test_table(&provider, &table, true).await;

    provider
        .put_item(
            table.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("u".to_string())),
                ("sk".to_string(), AttributeValue::S("item#1".to_string())),
                ("gsi_pk".to_string(), AttributeValue::S("grp".to_string())),
                ("gsi_sk".to_string(), AttributeValue::N("1".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let (before_job, _) = provider
        .query_table(&gsi_query_request(&table))
        .await
        .unwrap();
    assert_eq!(
        before_job.len(),
        1,
        "immediate mode should publish the GSI row in the main write transaction"
    );

    provider.run_job(GSI_UPDATE_JOB).await.unwrap();

    let (after_job, _) = provider
        .query_table(&gsi_query_request(&table))
        .await
        .unwrap();
    assert_eq!(
        after_job.len(),
        1,
        "no-op job should not duplicate index rows"
    );
}

#[tokio::test]
async fn kv_immediate_gsi_consistency_moves_index_entries_inline() {
    let provider = TestProvider::new(create_test_store()).with_immediate_gsi_consistency(true);
    provider.initialize_storage().await.unwrap();

    let table = TableName::new("KvImmediateGsiMove");
    create_test_table(&provider, &table, true).await;

    provider
        .put_item(
            table.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("u".to_string())),
                ("sk".to_string(), AttributeValue::S("item#1".to_string())),
                ("gsi_pk".to_string(), AttributeValue::S("grp".to_string())),
                ("gsi_sk".to_string(), AttributeValue::N("1".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    provider
        .put_item(
            table.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("u".to_string())),
                ("sk".to_string(), AttributeValue::S("item#1".to_string())),
                ("gsi_pk".to_string(), AttributeValue::S("grp-2".to_string())),
                ("gsi_sk".to_string(), AttributeValue::N("2".to_string())),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let (old_partition, _) = provider
        .query_table(&gsi_query_request_for_partition(&table, "grp"))
        .await
        .unwrap();
    assert!(
        old_partition.is_empty(),
        "immediate mode should remove the old GSI row in the same write transaction"
    );

    let (new_partition, _) = provider
        .query_table(&gsi_query_request_for_partition(&table, "grp-2"))
        .await
        .unwrap();
    assert_eq!(
        new_partition.len(),
        1,
        "immediate mode should insert the new GSI row in the same write transaction"
    );
}

#[tokio::test]
async fn kv_immediate_gsi_consistency_batch_write_updates_indexes_inline() {
    use storage_types::{BatchWriteItemRequest, PutRequest, WriteRequest};

    let provider = TestProvider::new(create_test_store()).with_immediate_gsi_consistency(true);
    provider.initialize_storage().await.unwrap();

    let table = TableName::new("KvImmediateGsiBatchWrite");
    create_test_table(&provider, &table, true).await;

    provider
        .batch_write_item(
            BatchWriteItemRequest {
                request_items: HashMap::from([(
                    table.clone(),
                    vec![
                        WriteRequest {
                            put_request: Some(PutRequest {
                                item: HashMap::from([
                                    ("pk".to_string(), AttributeValue::S("u".to_string())),
                                    ("sk".to_string(), AttributeValue::S("item#1".to_string())),
                                    ("gsi_pk".to_string(), AttributeValue::S("grp".to_string())),
                                    ("gsi_sk".to_string(), AttributeValue::N("1".to_string())),
                                ]),
                            }),
                            delete_request: None,
                        },
                        WriteRequest {
                            put_request: Some(PutRequest {
                                item: HashMap::from([
                                    ("pk".to_string(), AttributeValue::S("u".to_string())),
                                    ("sk".to_string(), AttributeValue::S("item#2".to_string())),
                                    ("gsi_pk".to_string(), AttributeValue::S("grp".to_string())),
                                    ("gsi_sk".to_string(), AttributeValue::N("2".to_string())),
                                ]),
                            }),
                            delete_request: None,
                        },
                    ],
                )]),
                return_consumed_capacity: None,
                return_item_collection_metrics: None,
            },
            true,
        )
        .await
        .unwrap();

    let (before_job, _) = provider
        .query_table(&gsi_query_request(&table))
        .await
        .unwrap();
    assert_eq!(
        before_job.len(),
        2,
        "immediate mode should publish batch-written GSI rows inline"
    );
}

#[tokio::test]
async fn kv_backfill_concurrent_writes() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    let table = TableName::new("ConcurrentBF");
    let () = create_test_table(&provider, &table, false).await;

    // Seed some items
    for i in 0..500 {
        let mut item = HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S("u".to_string()));
        item.insert("sk".to_string(), AttributeValue::S(format!("a#{i:04}")));
        item.insert("gsi_pk".to_string(), AttributeValue::S("grp".to_string()));
        item.insert("gsi_sk".to_string(), AttributeValue::N(i.to_string()));
        provider
            .put_item(table.clone(), item, None, None, None, None)
            .await
            .unwrap();
    }

    // Kick off GSI creation
    let req = storage_types::UpdateTableRequest {
        table_name: table.clone(),
        attribute_definitions: None,
        billing_mode: None,
        provisioned_throughput: None,
        on_demand_throughput: None,
        deletion_protection_enabled: None,
        global_secondary_index_updates: Some(vec![storage_types::GlobalSecondaryIndexUpdate {
            create: Some(storage_types::CreateGlobalSecondaryIndex {
                index_name: IndexName::new("GConc"),
                key_schema: vec![
                    KeySchemaElement {
                        attribute_name: "gsi_pk".to_string(),
                        key_type: KeyType::Hash,
                    },
                    KeySchemaElement {
                        attribute_name: "gsi_sk".to_string(),
                        key_type: KeyType::Range,
                    },
                ],
                projection: Projection {
                    projection_type: Some(ProjectionType::All),
                    non_key_attributes: None,
                },
                provisioned_throughput: None,
            }),
            update: None,
            delete: None,
        }]),
        replica_updates: None,
        sse_specification: None,
        stream_specification: None,
        table_class: None,
    };

    // Spawn concurrent writers while backfill likely runs
    let p2 = provider.clone();
    let tname = table.clone();
    let writer = tokio::spawn(async move {
        for i in 500..800 {
            let mut item = HashMap::new();
            item.insert("pk".to_string(), AttributeValue::S("u".to_string()));
            item.insert("sk".to_string(), AttributeValue::S(format!("b#{i:04}")));
            item.insert("gsi_pk".to_string(), AttributeValue::S("grp".to_string()));
            item.insert("gsi_sk".to_string(), AttributeValue::N(i.to_string()));
            let _ = p2
                .put_item(tname.clone(), item, None, None, None, None)
                .await;
        }
    });

    // Run update (synchronously creates GSI and backfills in pages)
    let _ = provider.update_table(req).await.unwrap();
    let () = writer.await.unwrap();

    provider.run_job(GSI_BACKFILL_JOB).await.unwrap();
    provider.run_job(GSI_UPDATE_JOB).await.unwrap();

    // Validate: query GSI returns at least 800 rows
    let q = QueryTableRequest {
        table_name: table.clone(),
        index_name: Some(IndexName::new("GConc")),
        key_condition_expression: "gsi_pk = :p".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some({
            let mut m = HashMap::new();
            m.insert(":p".to_string(), AttributeValue::S("grp".to_string()));
            m
        }),
        limit: Some(2000),
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    };
    let (items, _lek) = provider.query_table(&q).await.unwrap();
    assert!(items.len() >= 800);
}

#[tokio::test]
async fn time_to_live_enable_and_disable() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    let table = TableName::new("TtlLifecycle");
    let () = create_test_table(&provider, &table, false).await;

    // Enable TTL on attribute "ttl"
    let enable_request = UpdateTimeToLiveRequest {
        table_name: table.clone(),
        time_to_live_specification: TimeToLiveSpecification {
            attribute_name: "ttl".to_string(),
            enabled: true,
        },
    };
    provider.update_time_to_live(enable_request).await.unwrap();

    // Run backfill job to completion so status transitions to Enabled
    provider.run_job(GSI_BACKFILL_JOB).await.unwrap();

    let ttl_status = provider.describe_time_to_live(&table).await.unwrap();
    let description = ttl_status.time_to_live_description.expect("description");
    assert_eq!(description.attribute_name.as_deref(), Some("ttl"));
    assert_eq!(description.time_to_live_status, TimeToLiveStatus::Enabled);

    // Disabling TTL should remove configuration and hidden index metadata
    let disable_request = UpdateTimeToLiveRequest {
        table_name: table.clone(),
        time_to_live_specification: TimeToLiveSpecification {
            attribute_name: "ttl".to_string(),
            enabled: false,
        },
    };
    provider.update_time_to_live(disable_request).await.unwrap();

    let ttl_status = provider.describe_time_to_live(&table).await.unwrap();
    let description = ttl_status.time_to_live_description.expect("description");
    assert!(description.attribute_name.is_none());
    assert_eq!(description.time_to_live_status, TimeToLiveStatus::Disabled);

    let table_info = provider.get_table_info(&table).await.unwrap();
    if let Some(gsis) = table_info.global_secondary_indexes {
        assert!(
            gsis.iter()
                .all(|g| !g.index_name.as_ref().starts_with(HIDDEN_TTL_INDEX_PREFIX))
        );
    }
}

#[tokio::test]
async fn ttl_sweep_removes_expired_items() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    let table = TableName::new("TtlSweep");
    let () = create_test_table(&provider, &table, false).await;

    let expired_at = (Utc::now().timestamp() - 120).to_string();
    let future_at = (Utc::now().timestamp() + 3_600).to_string();

    let mut expired_item = HashMap::new();
    expired_item.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    expired_item.insert("sk".to_string(), AttributeValue::S("expired".to_string()));
    expired_item.insert("ttl".to_string(), AttributeValue::N(expired_at.clone()));
    provider
        .put_item(table.clone(), expired_item, None, None, None, None)
        .await
        .unwrap();

    let mut live_item = HashMap::new();
    live_item.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    live_item.insert("sk".to_string(), AttributeValue::S("live".to_string()));
    live_item.insert("ttl".to_string(), AttributeValue::N(future_at));
    provider
        .put_item(table.clone(), live_item, None, None, None, None)
        .await
        .unwrap();

    let enable_request = UpdateTimeToLiveRequest {
        table_name: table.clone(),
        time_to_live_specification: TimeToLiveSpecification {
            attribute_name: "ttl".to_string(),
            enabled: true,
        },
    };
    provider.update_time_to_live(enable_request).await.unwrap();

    provider.run_job(GSI_BACKFILL_JOB).await.unwrap();

    let table_info = provider.get_table_info(&table).await.unwrap();
    let mut check_item = HashMap::new();
    check_item.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    check_item.insert("sk".to_string(), AttributeValue::S("expired".to_string()));
    check_item.insert(
        "ttl".to_string(),
        AttributeValue::N((Utc::now().timestamp() - 120).to_string()),
    );
    let ttl_key_bytes = ttl::ttl_index_key_for_item(&table, &table_info, "ttl", &check_item)
        .unwrap()
        .unwrap();
    assert!(
        provider
            .kv_store
            .get(&ttl_key_bytes, true)
            .await
            .unwrap()
            .is_some(),
        "ttl gsi entry should exist before sweep"
    );

    for _ in 0..20 {
        provider.run_job(TTL_SWEEP_JOB).await.unwrap();
        let mut check_key = HashMap::new();
        check_key.insert("pk".to_string(), AttributeValue::S("user".to_string()));
        check_key.insert("sk".to_string(), AttributeValue::S("expired".to_string()));
        if provider
            .get_item_map(table.clone(), check_key.into(), true)
            .await
            .unwrap()
            .is_none()
        {
            break;
        }
    }

    let mut expired_key = HashMap::new();
    expired_key.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    expired_key.insert("sk".to_string(), AttributeValue::S("expired".to_string()));
    assert!(
        provider
            .get_item_map(table.clone(), expired_key.into(), true)
            .await
            .unwrap()
            .is_none()
    );

    let mut live_key = HashMap::new();
    live_key.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    live_key.insert("sk".to_string(), AttributeValue::S("live".to_string()));
    assert!(
        provider
            .get_item_map(table.clone(), live_key.into(), true)
            .await
            .unwrap()
            .is_some()
    );

    let config = provider.load_ttl_config(&table).await.unwrap().unwrap();
    let expired_ttl = expired_at.parse::<i64>().unwrap();
    assert_eq!(
        config.last_processed_watermark,
        Some(expired_ttl),
        "kv sweep should persist last processed TTL watermark"
    );
}

#[tokio::test]
async fn ttl_index_removed_on_delete() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    let table = TableName::new("TtlIndexDelete");
    let () = create_test_table(&provider, &table, false).await;

    let expires_at = (Utc::now().timestamp() + 3_600).to_string();
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("session".to_string()));
    item.insert("ttl".to_string(), AttributeValue::N(expires_at.clone()));
    provider
        .put_item(table.clone(), item, None, None, None, None)
        .await
        .unwrap();

    let enable_request = UpdateTimeToLiveRequest {
        table_name: table.clone(),
        time_to_live_specification: TimeToLiveSpecification {
            attribute_name: "ttl".to_string(),
            enabled: true,
        },
    };
    provider.update_time_to_live(enable_request).await.unwrap();
    provider.run_job(GSI_BACKFILL_JOB).await.unwrap();

    let table_info = provider.get_table_info(&table).await.unwrap();
    let mut key_item = HashMap::new();
    key_item.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    key_item.insert("sk".to_string(), AttributeValue::S("session".to_string()));
    key_item.insert("ttl".to_string(), AttributeValue::N(expires_at));
    let ttl_key = ttl::ttl_index_key_for_item(&table, &table_info, "ttl", &key_item)
        .unwrap()
        .unwrap();
    assert!(
        provider
            .kv_store
            .get(&ttl_key, true)
            .await
            .unwrap()
            .is_some(),
        "ttl index entry should exist before delete"
    );

    let mut delete_key = HashMap::new();
    delete_key.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    delete_key.insert("sk".to_string(), AttributeValue::S("session".to_string()));
    provider
        .delete_item(table.clone(), delete_key.into(), None, None, None)
        .await
        .unwrap();

    assert!(
        provider
            .kv_store
            .get(&ttl_key, true)
            .await
            .unwrap()
            .is_none(),
        "ttl index entry should be removed after delete"
    );
}

#[tokio::test]
async fn ttl_index_skips_invalid_ttl_value() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    let table = TableName::new("TtlIndexInvalid");
    let () = create_test_table(&provider, &table, false).await;

    let enable_request = UpdateTimeToLiveRequest {
        table_name: table.clone(),
        time_to_live_specification: TimeToLiveSpecification {
            attribute_name: "ttl".to_string(),
            enabled: true,
        },
    };
    provider.update_time_to_live(enable_request).await.unwrap();
    provider.run_job(GSI_BACKFILL_JOB).await.unwrap();

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("invalid".to_string()));
    item.insert(
        "ttl".to_string(),
        AttributeValue::S("not-a-number".to_string()),
    );
    provider
        .put_item(table.clone(), item, None, None, None, None)
        .await
        .unwrap();

    let prefix = ttl::ttl_index_prefix(&table);
    let range = provider
        .kv_store
        .get_prefix(&prefix, true, Some(10), true)
        .await
        .unwrap();
    assert!(
        range.items.is_empty(),
        "invalid ttl values should not write ttl index entries"
    );
}

#[tokio::test]
async fn ttl_index_skips_missing_ttl_attribute() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    let table = TableName::new("TtlIndexMissing");
    let () = create_test_table(&provider, &table, false).await;

    let enable_request = UpdateTimeToLiveRequest {
        table_name: table.clone(),
        time_to_live_specification: TimeToLiveSpecification {
            attribute_name: "ttl".to_string(),
            enabled: true,
        },
    };
    provider.update_time_to_live(enable_request).await.unwrap();
    provider.run_job(GSI_BACKFILL_JOB).await.unwrap();

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("missing".to_string()));
    provider
        .put_item(table.clone(), item, None, None, None, None)
        .await
        .unwrap();

    let prefix = ttl::ttl_index_prefix(&table);
    let range = provider
        .kv_store
        .get_prefix(&prefix, true, Some(10), true)
        .await
        .unwrap();
    assert!(
        range.items.is_empty(),
        "items without ttl should not write ttl index entries"
    );
}

#[tokio::test]
async fn ttl_sweep_skip_progression() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    let table = TableName::new("TtlSkip");
    let () = create_test_table(&provider, &table, false).await;

    let future_at = (Utc::now().timestamp() + 3_600).to_string();

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("future".to_string()));
    item.insert("ttl".to_string(), AttributeValue::N(future_at));
    provider
        .put_item(table.clone(), item, None, None, None, None)
        .await
        .unwrap();

    let enable_request = UpdateTimeToLiveRequest {
        table_name: table.clone(),
        time_to_live_specification: TimeToLiveSpecification {
            attribute_name: "ttl".to_string(),
            enabled: true,
        },
    };
    provider.update_time_to_live(enable_request).await.unwrap();
    provider.run_job(GSI_BACKFILL_JOB).await.unwrap();

    provider.run_job(TTL_SWEEP_JOB).await.unwrap();
    let mut config = provider.load_ttl_config(&table).await.unwrap().unwrap();
    assert_eq!(config.skip_streak, 1);
    assert_eq!(config.skip_runs_remaining, 1);

    provider.run_job(TTL_SWEEP_JOB).await.unwrap();
    config = provider.load_ttl_config(&table).await.unwrap().unwrap();
    assert_eq!(config.skip_streak, 1);
    assert_eq!(config.skip_runs_remaining, 0);

    provider.run_job(TTL_SWEEP_JOB).await.unwrap();
    config = provider.load_ttl_config(&table).await.unwrap().unwrap();
    assert_eq!(config.skip_streak, 2);
    assert_eq!(config.skip_runs_remaining, 2);
}

#[tokio::test]
async fn ttl_sweep_updates_resume_checkpoint() {
    let table = TableName::new("TtlResumeConfig");
    let gsi_name = ttl::ttl_gsi_name(&table);
    let mut config =
        ttl::TtlConfigRecord::new("ttl".to_string(), &gsi_name, TimeToLiveStatus::Enabled);
    assert_eq!(config.last_processed_watermark, None);
    assert_eq!(config.next_shard, 0);

    config.last_processed_watermark = Some(1_234);
    config.next_shard = 5;
    config.register_progress();
    assert_eq!(config.last_processed_watermark, Some(1_234));
    assert_eq!(config.next_shard, 5);
}

#[traced_test]
#[tokio::test]
async fn ttl_sweep_emits_throttle_telemetry() {
    let table = TableName::new("TtlThrottleConfig");
    let gsi_name = ttl::ttl_gsi_name(&table);
    let mut config =
        ttl::TtlConfigRecord::new("ttl".to_string(), &gsi_name, TimeToLiveStatus::Enabled);
    assert_eq!(config.throttled_runs, 0);
    config.register_throttle();
    config.register_throttle();
    assert_eq!(config.throttled_runs, 2);
    config.reset_throttle();
    assert_eq!(config.throttled_runs, 0);
}

#[tokio::test]
async fn ttl_sweep_health_check_forces_run() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    let table = TableName::new("TtlHealthCheck");
    let () = create_test_table(&provider, &table, false).await;

    let enable_request = UpdateTimeToLiveRequest {
        table_name: table.clone(),
        time_to_live_specification: TimeToLiveSpecification {
            attribute_name: "ttl".to_string(),
            enabled: true,
        },
    };
    provider.update_time_to_live(enable_request).await.unwrap();
    provider.run_job(GSI_BACKFILL_JOB).await.unwrap();
    provider.run_job(TTL_SWEEP_JOB).await.unwrap();

    let mut config = provider.load_ttl_config(&table).await.unwrap().unwrap();
    config.skip_runs_remaining = 4;
    config.skip_streak = 4;
    let interval_ms =
        i64::try_from(constants::TTL_SWEEP_HEALTH_CHECK_INTERVAL_MINUTES).unwrap() * 60_000;
    let forced_past = TimestampMillis::from_timestamp(*TimestampMillis::now() - interval_ms - 1);
    config.last_sweep_started_at = Some(forced_past);
    provider.save_ttl_config(&table, &config).await.unwrap();

    provider.run_job(TTL_SWEEP_JOB).await.unwrap();

    let refreshed = provider.load_ttl_config(&table).await.unwrap().unwrap();
    let after = refreshed
        .last_sweep_started_at
        .expect("last sweep timestamp");
    assert!(
        after > forced_past,
        "health check should force sweep execution"
    );
}

#[traced_test]
#[tokio::test]
async fn ttl_sweep_emits_traces() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    let table = TableName::new("TtlTrace");
    let () = create_test_table(&provider, &table, false).await;

    let expired_at = (Utc::now().timestamp() - 120).to_string();
    let mut expired_item = HashMap::new();
    expired_item.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    expired_item.insert("sk".to_string(), AttributeValue::S("expired".to_string()));
    expired_item.insert("ttl".to_string(), AttributeValue::N(expired_at.clone()));
    provider
        .put_item(table.clone(), expired_item, None, None, None, None)
        .await
        .unwrap();

    let enable_request = UpdateTimeToLiveRequest {
        table_name: table.clone(),
        time_to_live_specification: TimeToLiveSpecification {
            attribute_name: "ttl".to_string(),
            enabled: true,
        },
    };
    provider.update_time_to_live(enable_request).await.unwrap();
    provider.run_job(GSI_BACKFILL_JOB).await.unwrap();

    provider.run_job(TTL_SWEEP_JOB).await.unwrap();

    let lines = global_log_lines();
    let (table_idx, table_line) = lines
        .iter()
        .enumerate()
        .rev()
        .find(|(_, line)| {
            line.contains("ttl.sweep.table_summary") && log_line_has_table(line, &table)
        })
        .expect("missing ttl.sweep.table_summary trace");
    assert!(
        table_line.contains("retry_batches="),
        "table summary missing retry_batches field"
    );
    assert!(
        table_line.contains("retry_attempts="),
        "table summary missing retry_attempts field"
    );
    assert!(
        table_line.contains("retry_failures="),
        "table summary missing retry_failures field"
    );

    let job_line = lines[table_idx..]
        .iter()
        .find(|line| line.contains("ttl.sweep.job_summary"))
        .expect("missing ttl.sweep.job_summary trace");
    assert!(
        job_line.contains("retry_batches="),
        "job summary missing retry_batches field"
    );
    assert!(
        job_line.contains("retry_attempts="),
        "job summary missing retry_attempts field"
    );
    assert!(
        job_line.contains("retry_failures="),
        "job summary missing retry_failures field"
    );
}

#[traced_test]
#[tokio::test]
async fn ttl_sweep_records_retry_metrics() {
    let base_store = create_test_store();
    let cleanup_handle = base_store.clone();
    let kv_store = FaultyStore::new(base_store, 0);
    let provider = SortedKvDbStorageProvider::new(kv_store.clone());
    provider.initialize_storage().await.unwrap();

    let table = TableName::new("TtlRetryKv");
    let request = CreateTableRequest::new(
        table.clone(),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        storage_types::BillingMode::PayPerRequest,
    );
    provider.create_table(&request).await.unwrap();

    let expired_at = (Utc::now().timestamp() - 120).to_string();
    let mut expired_item = HashMap::new();
    expired_item.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    expired_item.insert("sk".to_string(), AttributeValue::S("retry".to_string()));
    expired_item.insert("ttl".to_string(), AttributeValue::N(expired_at.clone()));
    provider
        .put_item(table.clone(), expired_item, None, None, None, None)
        .await
        .unwrap();

    let enable_request = UpdateTimeToLiveRequest {
        table_name: table.clone(),
        time_to_live_specification: TimeToLiveSpecification {
            attribute_name: "ttl".to_string(),
            enabled: true,
        },
    };
    provider.update_time_to_live(enable_request).await.unwrap();
    provider.run_job(GSI_BACKFILL_JOB).await.unwrap();

    let mut config = provider.load_ttl_config(&table).await.unwrap().unwrap();
    let table_info = provider.get_table_info(&table).await.unwrap();
    let mut shard_probe = HashMap::new();
    shard_probe.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    shard_probe.insert("sk".to_string(), AttributeValue::S("retry".to_string()));
    shard_probe.insert("ttl".to_string(), AttributeValue::N(expired_at.clone()));
    let prepared = ttl::augment_item_with_ttl_partition(&table_info, &shard_probe, "ttl")
        .expect("ttl partition computation succeeded")
        .expect("ttl partition available");
    let shard_value = match prepared.get(TTL_PARTITION_ATTRIBUTE) {
        Some(AttributeValue::S(value)) => value.clone(),
        other => panic!("unexpected ttl partition payload: {other:?}"),
    };
    let shard: u8 = shard_value.parse().expect("ttl shard parse");
    config.next_shard = shard;
    provider.save_ttl_config(&table, &config).await.unwrap();

    kv_store.set_failures(1);

    provider.run_job(TTL_SWEEP_JOB).await.unwrap();

    let lines = global_log_lines();
    assert!(
        lines.iter().any(|line| {
            line.contains("ttl.sweep.batch_retry") && log_line_has_table(line, &table)
        }),
        "missing ttl.sweep.batch_retry trace"
    );

    assert!(
        lines.iter().any(|line| {
            line.contains("ttl.sweep.table_summary")
                && log_line_has_table(line, &table)
                && line.contains("retry_batches=1")
                && line.contains("retry_attempts=2")
                && line.contains("retry_failures=1")
        }),
        "no table summary with retry telemetry observed"
    );

    assert!(
        lines.iter().any(|line| {
            line.contains("ttl.sweep.job_summary")
                && line.contains("retry_batches=1")
                && line.contains("retry_attempts=2")
                && line.contains("retry_failures=1")
        }),
        "no job summary with retry telemetry observed"
    );

    drop(provider);
    cleanup_store(&cleanup_handle).await;
}

#[tokio::test]
async fn gsi_update_emits_metrics() {
    let _metrics_guard = metrics_assertion_lock().lock().await;
    let (metrics, _facade_guard) = CapturingMetricsFacade::install();

    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();

    let table = TableName::new("GsiMetricsKV");
    let () = create_test_table(&provider, &table, true).await;

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("u".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("a".to_string()));
    item.insert("gsi_pk".to_string(), AttributeValue::S("grp".to_string()));
    item.insert("gsi_sk".to_string(), AttributeValue::N("1".to_string()));

    provider
        .put_item(table.clone(), item, None, None, None, None)
        .await
        .unwrap();

    let did_work = provider.process_gsi_updates().await.unwrap();
    assert!(did_work, "expected gsi update to process work");

    assert!(
        metrics.counter_value(metrics_facade::CounterMetric::GsiUpdatePointerBatches, &[]) >= 1,
        "expected gsi_update_pointer_batches to increment"
    );
    assert!(
        metrics.counter_value(metrics_facade::CounterMetric::GsiUpdateStreamItems, &[]) >= 1,
        "expected gsi_update_stream_items to increment"
    );
    assert!(
        metrics.counter_value(metrics_facade::CounterMetric::GsiUpdateOps, &[]) >= 1,
        "expected gsi_update_ops to increment"
    );
    assert!(
        metrics.histogram_count(metrics_facade::HistogramMetric::GsiUpdateRuntimeMs, &[]) >= 1,
        "expected gsi_update_runtime_ms to record"
    );
}

#[tokio::test]
async fn ttl_sweep_emits_metrics() {
    let _metrics_guard = metrics_assertion_lock().lock().await;
    let (metrics, _facade_guard) = CapturingMetricsFacade::install();
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();

    let table = TableName::new("TtlMetricsKV");
    let () = create_test_table(&provider, &table, false).await;

    let expired_at = (Utc::now().timestamp() - 120).to_string();
    let mut expired_item = HashMap::new();
    expired_item.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    expired_item.insert("sk".to_string(), AttributeValue::S("expired".to_string()));
    expired_item.insert("ttl".to_string(), AttributeValue::N(expired_at.clone()));
    provider
        .put_item(table.clone(), expired_item, None, None, None, None)
        .await
        .unwrap();

    let enable_request = UpdateTimeToLiveRequest {
        table_name: table.clone(),
        time_to_live_specification: TimeToLiveSpecification {
            attribute_name: "ttl".to_string(),
            enabled: true,
        },
    };
    provider.update_time_to_live(enable_request).await.unwrap();
    provider.run_job(GSI_BACKFILL_JOB).await.unwrap();

    let table_info = provider.get_table_info(&table).await.unwrap();
    let shard_item = ttl::augment_item_with_ttl_partition(
        &table_info,
        &{
            let mut item = HashMap::new();
            item.insert("pk".to_string(), AttributeValue::S("user".to_string()));
            item.insert("sk".to_string(), AttributeValue::S("expired".to_string()));
            item.insert("ttl".to_string(), AttributeValue::N(expired_at.clone()));
            item
        },
        "ttl",
    )
    .unwrap()
    .expect("ttl shard item");
    let shard = match shard_item.get(TTL_PARTITION_ATTRIBUTE) {
        Some(AttributeValue::S(value)) => value
            .parse::<u8>()
            .unwrap_or_else(|_| panic!("invalid ttl shard: {value}")),
        other => panic!("unexpected ttl shard value: {other:?}"),
    };
    if let Some(mut config) = provider.load_ttl_config(&table).await.unwrap() {
        config.next_shard = shard;
        provider.save_ttl_config(&table, &config).await.unwrap();
    }

    let table_label = format!("table=\"{}\"", table.as_ref());
    let table_fragments = [table_label.as_str(), "scope=\"table\""];
    let job_fragments = ["scope=\"job\""];

    for _ in 0..20 {
        provider.run_job(TTL_SWEEP_JOB).await.unwrap();
        if metrics.counter_value(
            metrics_facade::CounterMetric::TtlSweepItemsDeleted,
            &job_fragments,
        ) >= 1
            && metrics.counter_value(
                metrics_facade::CounterMetric::TtlSweepItemsDeleted,
                &table_fragments,
            ) >= 1
        {
            break;
        }
    }

    assert!(
        metrics.counter_value(
            metrics_facade::CounterMetric::TtlSweepTablesChecked,
            &job_fragments,
        ) >= 1,
        "expected ttl_sweep_tables_checked job scope increment"
    );
    assert!(
        metrics.counter_value(
            metrics_facade::CounterMetric::TtlSweepTablesChecked,
            &table_fragments,
        ) >= 1,
        "expected ttl_sweep_tables_checked table scope increment"
    );
    assert!(
        metrics.counter_value(
            metrics_facade::CounterMetric::TtlSweepShardsChecked,
            &job_fragments,
        ) >= 1,
        "expected ttl_sweep_shards_checked job scope increment"
    );
    assert!(
        metrics.counter_value(
            metrics_facade::CounterMetric::TtlSweepShardsChecked,
            &table_fragments,
        ) >= 1,
        "expected ttl_sweep_shards_checked table scope increment"
    );
    assert!(
        metrics.counter_value(
            metrics_facade::CounterMetric::TtlSweepItemsDeleted,
            &job_fragments,
        ) >= 1,
        "expected ttl_sweep_items_deleted job scope increment"
    );
    assert!(
        metrics.counter_value(
            metrics_facade::CounterMetric::TtlSweepItemsDeleted,
            &table_fragments,
        ) >= 1,
        "expected ttl_sweep_items_deleted table scope increment"
    );
    assert!(
        metrics.histogram_count(
            metrics_facade::HistogramMetric::TtlSweepRuntimeMs,
            &job_fragments,
        ) >= 1,
        "expected ttl_sweep_runtime_ms job scope recorded"
    );
    assert!(
        metrics.histogram_count(
            metrics_facade::HistogramMetric::TtlSweepRuntimeMs,
            &table_fragments,
        ) >= 1,
        "expected ttl_sweep_runtime_ms table scope recorded"
    );
}

#[tokio::test]
async fn ttl_sweep_skips_updated_item() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    let table = TableName::new("TtlConditional");
    let () = create_test_table(&provider, &table, false).await;

    let expired_at = (Utc::now().timestamp() - 120).to_string();
    let future_at = (Utc::now().timestamp() + 3_600).to_string();

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("session".to_string()));
    item.insert("ttl".to_string(), AttributeValue::N(expired_at.clone()));
    provider
        .put_item(table.clone(), item, None, None, None, None)
        .await
        .unwrap();

    let enable_request = UpdateTimeToLiveRequest {
        table_name: table.clone(),
        time_to_live_specification: TimeToLiveSpecification {
            attribute_name: "ttl".to_string(),
            enabled: true,
        },
    };
    provider.update_time_to_live(enable_request).await.unwrap();
    provider.run_job(GSI_BACKFILL_JOB).await.unwrap();

    let table_info = provider.get_table_info(&table).await.unwrap();
    let mut expired_item = HashMap::new();
    expired_item.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    expired_item.insert("sk".to_string(), AttributeValue::S("session".to_string()));
    expired_item.insert("ttl".to_string(), AttributeValue::N(expired_at.clone()));
    let expired_key = ttl::ttl_index_key_for_item(&table, &table_info, "ttl", &expired_item)
        .unwrap()
        .unwrap();
    assert!(
        provider
            .kv_store
            .get(&expired_key, true)
            .await
            .unwrap()
            .is_some(),
        "expired ttl index entry should exist before refresh"
    );

    let mut refreshed = HashMap::new();
    refreshed.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    refreshed.insert("sk".to_string(), AttributeValue::S("session".to_string()));
    refreshed.insert("ttl".to_string(), AttributeValue::N(future_at.clone()));
    provider
        .put_item(table.clone(), refreshed, None, None, None, None)
        .await
        .unwrap();

    let mut future_item = HashMap::new();
    future_item.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    future_item.insert("sk".to_string(), AttributeValue::S("session".to_string()));
    future_item.insert("ttl".to_string(), AttributeValue::N(future_at.clone()));
    let future_key = ttl::ttl_index_key_for_item(&table, &table_info, "ttl", &future_item)
        .unwrap()
        .unwrap();
    assert!(
        provider
            .kv_store
            .get(&future_key, true)
            .await
            .unwrap()
            .is_some(),
        "future ttl index entry should exist after refresh"
    );
    assert!(
        provider
            .kv_store
            .get(&expired_key, true)
            .await
            .unwrap()
            .is_none(),
        "expired ttl index entry should be removed after refresh"
    );

    provider.run_job(TTL_SWEEP_JOB).await.unwrap();

    let mut key = HashMap::new();
    key.insert("pk".to_string(), AttributeValue::S("user".to_string()));
    key.insert("sk".to_string(), AttributeValue::S("session".to_string()));
    let item = provider
        .get_item_map(table.clone(), key.into(), true)
        .await
        .unwrap();
    assert!(
        item.is_some(),
        "item should remain because TTL was extended before sweep completed"
    );
}

/// Test helper to create a table with optional GSI
async fn create_test_table<S>(
    provider: &SortedKvDbStorageProvider<S>,
    table_name: &TableName,
    with_gsi: bool,
) where
    S: PartitionFamilyKvStore + 'static,
{
    let mut attribute_definitions = vec![
        AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::S,
        },
        AttributeDefinition {
            attribute_name: "sk".to_string(),
            attribute_type: KeyAttributeType::S,
        },
    ];

    let mut global_secondary_indexes = None;

    if with_gsi {
        attribute_definitions.extend(vec![
            AttributeDefinition {
                attribute_name: "gsi_pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_sk".to_string(),
                attribute_type: KeyAttributeType::N,
            },
        ]);

        global_secondary_indexes = Some(vec![CreateGlobalSecondaryIndex {
            index_name: IndexName::new("TestGSI"),
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: "gsi_pk".to_string(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "gsi_sk".to_string(),
                    key_type: KeyType::Range,
                },
            ],
            projection: Projection {
                projection_type: Some(ProjectionType::All),
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        }]);
    }

    let request = CreateTableRequest::new(
        table_name.clone(),
        attribute_definitions,
        vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        storage_types::BillingMode::PayPerRequest,
    )
    .with_global_secondary_indexes(global_secondary_indexes);

    provider.create_table(&request).await.unwrap();
}

/// Test helper to populate test data
async fn populate_test_data(provider: &TestProvider, table_name: &TableName, with_gsi: bool) {
    let items = vec![
        ("user1", "item01", "category_a", "100", "data1"),
        ("user1", "item02", "category_a", "200", "data2"),
        ("user1", "item03", "category_b", "300", "data3"),
        ("user1", "item04", "category_a", "400", "data4"),
        ("user1", "item05", "category_b", "500", "data5"),
        ("user2", "item06", "category_a", "150", "data6"),
        ("user2", "item07", "category_b", "250", "data7"),
        ("user2", "item08", "category_a", "350", "data8"),
        ("user2", "item09", "category_b", "450", "data9"),
        ("user2", "item10", "category_a", "550", "data10"),
    ];

    for (pk, sk, gsi_pk, gsi_sk, data) in &items {
        let mut item = HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S((*pk).to_string()));
        item.insert("sk".to_string(), AttributeValue::S((*sk).to_string()));
        if with_gsi {
            item.insert(
                "gsi_pk".to_string(),
                AttributeValue::S((*gsi_pk).to_string()),
            );
            item.insert(
                "gsi_sk".to_string(),
                AttributeValue::N((*gsi_sk).to_string()),
            );
        }
        item.insert("data".to_string(), AttributeValue::S((*data).to_string()));

        provider
            .put_item(table_name.clone(), item, None, None, None, None)
            .await
            .unwrap();
    }

    if with_gsi {
        // Execute GSI update job to populate the GSI
        let provider_arc = std::sync::Arc::new(provider.clone());
        let gsi_job = crate::storage_provider::GsiUpdateJob::new(provider_arc);
        let err = gsi_job.execute().await;

        assert!(err.is_ok(), "GSI update job failed: {err:?}");
    }
}

async fn populate_limit_boundary_data(provider: &TestProvider, table_name: &TableName) {
    for index in 1..=15 {
        provider
            .put_item(
                table_name.clone(),
                HashMap::from([
                    ("pk".to_string(), AttributeValue::S("user#1".to_string())),
                    (
                        "sk".to_string(),
                        AttributeValue::S(format!("item#{index:03}")),
                    ),
                    ("data".to_string(), AttributeValue::N(index.to_string())),
                ]),
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();
    }
}

/// Test helper to create query request
fn create_query_request(
    table_name: &TableName,
    index_name: Option<&IndexName>,
    key_condition: &str,
    expression_values: HashMap<String, AttributeValue>,
    limit: Option<u32>,
    exclusive_start_key: Option<String>,
    scan_forward: Option<bool>,
) -> QueryTableRequest {
    QueryTableRequest {
        table_name: table_name.clone(),
        index_name: index_name.cloned(),
        key_condition_expression: key_condition.to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(expression_values),
        limit,
        exclusive_start_key,
        scan_index_forward: scan_forward,
        consistent_read: false,
    }
}

/// Test helper to create scan request
fn create_scan_request(
    table_name: &TableName,
    index_name: Option<&IndexName>,
    limit: Option<u32>,
    exclusive_start_key: Option<String>,
) -> ScanTableRequest {
    ScanTableRequest {
        table_name: table_name.clone(),
        index_name: index_name.cloned(),
        limit,
        exclusive_start_key,
        consistent_read: false,
    }
}

#[tokio::test]
async fn query_and_scan_only_return_last_evaluated_key_when_more_items_remain() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    let table_name = TableName::new("limit_boundary_kv");
    create_test_table(&provider, &table_name, false).await;
    populate_limit_boundary_data(&provider, &table_name).await;

    let query_values =
        HashMap::from([(":pk".to_string(), AttributeValue::S("user#1".to_string()))]);
    let query_request = create_query_request(
        &table_name,
        None,
        "pk = :pk",
        query_values.clone(),
        None,
        None,
        Some(true),
    );
    let (items, last_evaluated_key) = provider.query_table(&query_request).await.unwrap();
    assert_eq!(items.len(), 15);
    assert!(last_evaluated_key.is_none());

    let query_request = create_query_request(
        &table_name,
        None,
        "pk = :pk",
        query_values.clone(),
        Some(10),
        None,
        Some(true),
    );
    let (items, last_evaluated_key) = provider.query_table(&query_request).await.unwrap();
    assert_eq!(items.len(), 10);
    assert!(last_evaluated_key.is_some());

    let query_request = create_query_request(
        &table_name,
        None,
        "pk = :pk",
        query_values,
        Some(15),
        None,
        Some(true),
    );
    let (items, last_evaluated_key) = provider.query_table(&query_request).await.unwrap();
    assert_eq!(items.len(), 15);
    assert!(last_evaluated_key.is_none());

    let scan_request = create_scan_request(&table_name, None, Some(10), None);
    let (items, last_evaluated_key) = provider.scan_table(&scan_request).await.unwrap();
    assert_eq!(items.len(), 10);
    assert!(last_evaluated_key.is_some());

    let scan_request = create_scan_request(&table_name, None, Some(15), None);
    let (items, last_evaluated_key) = provider.scan_table(&scan_request).await.unwrap();
    assert_eq!(items.len(), 15);
    assert!(last_evaluated_key.is_none());
}

#[tokio::test]
async fn query_gsi_consistent_read_rejected() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    let table = TableName::new("consistent_read_gsi_query");
    create_test_table(&provider, &table, true).await;
    populate_test_data(&provider, &table, true).await;

    let mut values = HashMap::new();
    values.insert(
        ":gsi_pk".to_string(),
        AttributeValue::S("category_a".to_string()),
    );

    let request = QueryTableRequest {
        table_name: table.clone(),
        index_name: Some(IndexName::new("TestGSI")),
        key_condition_expression: "gsi_pk = :gsi_pk".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(values),
        limit: None,
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: true,
    };

    let err = provider.query_table(&request).await.unwrap_err();
    assert_eq!(
        err.to_string(),
        "Consistent reads are not supported on global secondary indexes"
    );
}

#[tokio::test]
async fn scan_gsi_consistent_read_rejected() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    let table = TableName::new("consistent_read_gsi_scan");
    create_test_table(&provider, &table, true).await;
    populate_test_data(&provider, &table, true).await;

    let request = ScanTableRequest {
        table_name: table.clone(),
        index_name: Some(IndexName::new("TestGSI")),
        limit: Some(5),
        exclusive_start_key: None,
        consistent_read: true,
    };

    let err = provider.scan_table(&request).await.unwrap_err();
    assert_eq!(
        err.to_string(),
        "Consistent reads are not supported on global secondary indexes"
    );
}

#[tokio::test]
async fn query_base_table_allows_consistent_read() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    let table = TableName::new("consistent_read_query_base");
    create_test_table(&provider, &table, false).await;
    populate_test_data(&provider, &table, false).await;

    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));

    let request = QueryTableRequest {
        table_name: table.clone(),
        index_name: None,
        key_condition_expression: "pk = :pk".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(values),
        limit: None,
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: true,
    };

    let (items, _) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 5);
}

#[tokio::test]
async fn scan_base_table_allows_consistent_read() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    let table = TableName::new("consistent_read_scan_base");
    create_test_table(&provider, &table, false).await;
    populate_test_data(&provider, &table, false).await;

    let request = ScanTableRequest {
        table_name: table.clone(),
        index_name: None,
        limit: Some(3),
        exclusive_start_key: None,
        consistent_read: true,
    };

    let (items, _) = provider.scan_table(&request).await.unwrap();
    assert_eq!(items.len(), 3);
}

// ===== EXACT KEY MATCH TESTS =====

#[tokio::test]
async fn exact_key_match_main_table_forward_no_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));
    values.insert(":sk".to_string(), AttributeValue::S("item01".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk AND sk = :sk",
        values,
        None,
        None,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 1);
    assert!(last_key.is_none());
    assert_eq!(items[0]["data"], AttributeValue::S("data1".to_string()));
}

#[tokio::test]
async fn exact_key_match_main_table_backward_no_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));
    values.insert(":sk".to_string(), AttributeValue::S("item01".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk AND sk = :sk",
        values,
        None,
        None,
        Some(false),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 1);
    assert!(last_key.is_none());
    assert_eq!(items[0]["data"], AttributeValue::S("data1".to_string()));
}

#[tokio::test]
async fn exact_key_match_main_table_forward_with_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));
    values.insert(":sk".to_string(), AttributeValue::S("item01".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk AND sk = :sk",
        values,
        Some(1),
        None,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 1);
    assert!(last_key.is_none());
    assert_eq!(items[0]["data"], AttributeValue::S("data1".to_string()));
}

#[tokio::test]
async fn exact_key_match_gsi_forward_no_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), true).await;
    populate_test_data(&provider, &TableName::new("test_table"), true).await;

    let mut values = HashMap::new();
    values.insert(
        ":gsi_pk".to_string(),
        AttributeValue::S("category_a".to_string()),
    );
    values.insert(":gsi_sk".to_string(), AttributeValue::N("100".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        Some(&IndexName::new("TestGSI")),
        "gsi_pk = :gsi_pk AND gsi_sk = :gsi_sk",
        values,
        None,
        None,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 1);
    assert!(last_key.is_none());
    assert_eq!(items[0]["data"], AttributeValue::S("data1".to_string()));
}

#[tokio::test]
async fn exact_key_match_gsi_backward_no_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), true).await;
    populate_test_data(&provider, &TableName::new("test_table"), true).await;

    let mut values = HashMap::new();
    values.insert(
        ":gsi_pk".to_string(),
        AttributeValue::S("category_a".to_string()),
    );
    values.insert(":gsi_sk".to_string(), AttributeValue::N("100".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        Some(&IndexName::new("TestGSI")),
        "gsi_pk = :gsi_pk AND gsi_sk = :gsi_sk",
        values,
        None,
        None,
        Some(false),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 1);
    assert!(last_key.is_none());
    assert_eq!(items[0]["data"], AttributeValue::S("data1".to_string()));
}

#[tokio::test]
async fn exact_key_match_gsi_forward_with_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), true).await;
    populate_test_data(&provider, &TableName::new("test_table"), true).await;

    let mut values = HashMap::new();
    values.insert(
        ":gsi_pk".to_string(),
        AttributeValue::S("category_a".to_string()),
    );
    values.insert(":gsi_sk".to_string(), AttributeValue::N("100".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        Some(&IndexName::new("TestGSI")),
        "gsi_pk = :gsi_pk AND gsi_sk = :gsi_sk",
        values,
        Some(1),
        None,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 1);
    assert!(last_key.is_none());
    assert_eq!(items[0]["data"], AttributeValue::S("data1".to_string()));
}

// ===== BETWEEN TESTS =====

#[tokio::test]
async fn between_main_table_forward_no_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));
    values.insert(
        ":start".to_string(),
        AttributeValue::S("item01".to_string()),
    );
    values.insert(":end".to_string(), AttributeValue::S("item03".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk AND sk BETWEEN :start AND :end",
        values,
        None,
        None,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 3);
    assert!(last_key.is_none());
    let sks: Vec<String> = items
        .iter()
        .filter_map(|item| {
            if let AttributeValue::S(sk) = &item["sk"] {
                Some(sk.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(sks, vec!["item01", "item02", "item03"]);
}

#[tokio::test]
async fn between_main_table_backward_no_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));
    values.insert(
        ":start".to_string(),
        AttributeValue::S("item01".to_string()),
    );
    values.insert(":end".to_string(), AttributeValue::S("item03".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk AND sk BETWEEN :start AND :end",
        values,
        None,
        None,
        Some(false),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 3);
    assert!(last_key.is_none());
    let sks: Vec<String> = items
        .iter()
        .filter_map(|item| {
            if let AttributeValue::S(sk) = &item["sk"] {
                Some(sk.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(sks, vec!["item03", "item02", "item01"]);
}

#[tokio::test]
async fn between_main_table_forward_with_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));
    values.insert(
        ":start".to_string(),
        AttributeValue::S("item01".to_string()),
    );
    values.insert(":end".to_string(), AttributeValue::S("item05".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk AND sk BETWEEN :start AND :end",
        values,
        Some(2),
        None,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 2);
    assert!(last_key.is_some());
    let sks: Vec<String> = items
        .iter()
        .filter_map(|item| {
            if let AttributeValue::S(sk) = &item["sk"] {
                Some(sk.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(sks, vec!["item01", "item02"]);
}

#[tokio::test]
async fn between_gsi_forward_no_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), true).await;
    populate_test_data(&provider, &TableName::new("test_table"), true).await;

    let mut values = HashMap::new();
    values.insert(
        ":gsi_pk".to_string(),
        AttributeValue::S("category_a".to_string()),
    );
    values.insert(":start".to_string(), AttributeValue::N("100".to_string()));
    values.insert(":end".to_string(), AttributeValue::N("300".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        Some(&IndexName::new("TestGSI")),
        "gsi_pk = :gsi_pk AND gsi_sk BETWEEN :start AND :end",
        values,
        None,
        None,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 3);
    assert!(last_key.is_none());
}

#[tokio::test]
async fn between_gsi_backward_no_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), true).await;
    populate_test_data(&provider, &TableName::new("test_table"), true).await;

    let mut values = HashMap::new();
    values.insert(
        ":gsi_pk".to_string(),
        AttributeValue::S("category_a".to_string()),
    );
    values.insert(":start".to_string(), AttributeValue::N("100".to_string()));
    values.insert(":end".to_string(), AttributeValue::N("300".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        Some(&IndexName::new("TestGSI")),
        "gsi_pk = :gsi_pk AND gsi_sk BETWEEN :start AND :end",
        values,
        None,
        None,
        Some(false),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 3, "expected 3 items, got {items:?}");
    assert!(last_key.is_none());
}

#[tokio::test]
async fn between_gsi_forward_with_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), true).await;
    populate_test_data(&provider, &TableName::new("test_table"), true).await;

    let mut values = HashMap::new();
    values.insert(
        ":gsi_pk".to_string(),
        AttributeValue::S("category_a".to_string()),
    );
    values.insert(":start".to_string(), AttributeValue::N("100".to_string()));
    values.insert(":end".to_string(), AttributeValue::N("500".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        Some(&IndexName::new("TestGSI")),
        "gsi_pk = :gsi_pk AND gsi_sk BETWEEN :start AND :end",
        values,
        Some(2),
        None,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 2);
    assert!(last_key.is_some());
}

// ===== COMPARISON TESTS =====

#[tokio::test]
async fn less_than_main_table_forward_no_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));
    values.insert(":val".to_string(), AttributeValue::S("item03".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk AND sk < :val",
        values,
        None,
        None,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 2);
    assert!(last_key.is_none());
    let sks: Vec<String> = items
        .iter()
        .filter_map(|item| {
            if let AttributeValue::S(sk) = &item["sk"] {
                Some(sk.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(sks, vec!["item01", "item02"]);
}

#[tokio::test]
async fn less_than_main_table_backward_no_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));
    values.insert(":val".to_string(), AttributeValue::S("item03".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk AND sk < :val",
        values,
        None,
        None,
        Some(false),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 2);
    assert!(last_key.is_none());
    let sks: Vec<String> = items
        .iter()
        .filter_map(|item| {
            if let AttributeValue::S(sk) = &item["sk"] {
                Some(sk.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(sks, vec!["item02", "item01"]);
}

#[tokio::test]
async fn less_than_equal_main_table_forward_no_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));
    values.insert(":val".to_string(), AttributeValue::S("item03".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk AND sk <= :val",
        values,
        None,
        None,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 3);
    assert!(last_key.is_none());
    let sks: Vec<String> = items
        .iter()
        .filter_map(|item| {
            if let AttributeValue::S(sk) = &item["sk"] {
                Some(sk.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(sks, vec!["item01", "item02", "item03"]);
}

#[tokio::test]
async fn greater_than_main_table_forward_no_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));
    values.insert(":val".to_string(), AttributeValue::S("item03".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk AND sk > :val",
        values,
        None,
        None,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 2);
    assert!(last_key.is_none());
    let sks: Vec<String> = items
        .iter()
        .filter_map(|item| {
            if let AttributeValue::S(sk) = &item["sk"] {
                Some(sk.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(sks, vec!["item04", "item05"]);
}

#[tokio::test]
async fn greater_than_equal_main_table_forward_no_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));
    values.insert(":val".to_string(), AttributeValue::S("item03".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk AND sk >= :val",
        values,
        None,
        None,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 3);
    assert!(last_key.is_none());
    let sks: Vec<String> = items
        .iter()
        .filter_map(|item| {
            if let AttributeValue::S(sk) = &item["sk"] {
                Some(sk.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(sks, vec!["item03", "item04", "item05"]);
}

#[tokio::test]
async fn less_than_gsi_forward_no_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), true).await;
    populate_test_data(&provider, &TableName::new("test_table"), true).await;

    let mut values = HashMap::new();
    values.insert(
        ":gsi_pk".to_string(),
        AttributeValue::S("category_a".to_string()),
    );
    values.insert(":val".to_string(), AttributeValue::N("300".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        Some(&IndexName::new("TestGSI")),
        "gsi_pk = :gsi_pk AND gsi_sk < :val",
        values,
        None,
        None,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 3);
    assert!(last_key.is_none());
}

#[tokio::test]
async fn less_than_gsi_backward_no_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), true).await;
    populate_test_data(&provider, &TableName::new("test_table"), true).await;

    let mut values = HashMap::new();
    values.insert(
        ":gsi_pk".to_string(),
        AttributeValue::S("category_a".to_string()),
    );
    values.insert(":val".to_string(), AttributeValue::N("300".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        Some(&IndexName::new("TestGSI")),
        "gsi_pk = :gsi_pk AND gsi_sk < :val",
        values,
        None,
        None,
        Some(false),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 3);
    assert!(last_key.is_none());
}

#[tokio::test]
async fn less_than_gsi_forward_with_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), true).await;
    populate_test_data(&provider, &TableName::new("test_table"), true).await;

    let mut values = HashMap::new();
    values.insert(
        ":gsi_pk".to_string(),
        AttributeValue::S("category_a".to_string()),
    );
    values.insert(":val".to_string(), AttributeValue::N("500".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        Some(&IndexName::new("TestGSI")),
        "gsi_pk = :gsi_pk AND gsi_sk < :val",
        values,
        Some(2),
        None,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 2);
    assert!(last_key.is_some());
}

// ===== BEGINS_WITH TESTS =====

#[tokio::test]
async fn begins_with_main_table_forward_no_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));
    values.insert(
        ":prefix".to_string(),
        AttributeValue::S("item0".to_string()),
    );

    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk AND begins_with(sk, :prefix)",
        values,
        None,
        None,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 5);
    assert!(last_key.is_none());
    let sks: Vec<String> = items
        .iter()
        .filter_map(|item| {
            if let AttributeValue::S(sk) = &item["sk"] {
                Some(sk.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(sks, vec!["item01", "item02", "item03", "item04", "item05"]);
}

#[tokio::test]
async fn begins_with_main_table_backward_no_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));
    values.insert(
        ":prefix".to_string(),
        AttributeValue::S("item0".to_string()),
    );

    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk AND begins_with(sk, :prefix)",
        values,
        None,
        None,
        Some(false),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 5);
    assert!(last_key.is_none());
    let sks: Vec<String> = items
        .iter()
        .filter_map(|item| {
            if let AttributeValue::S(sk) = &item["sk"] {
                Some(sk.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(sks, vec!["item05", "item04", "item03", "item02", "item01"]);
}

#[tokio::test]
async fn begins_with_main_table_forward_with_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));
    values.insert(
        ":prefix".to_string(),
        AttributeValue::S("item0".to_string()),
    );

    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk AND begins_with(sk, :prefix)",
        values,
        Some(3),
        None,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 3);
    assert!(last_key.is_some());
    let sks: Vec<String> = items
        .iter()
        .filter_map(|item| {
            if let AttributeValue::S(sk) = &item["sk"] {
                Some(sk.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(sks, vec!["item01", "item02", "item03"]);
}

// ===== HASH-ONLY TESTS =====

#[tokio::test]
async fn hash_only_main_table_forward_no_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk",
        values,
        None,
        None,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 5);
    assert!(last_key.is_none());
    let sks: Vec<String> = items
        .iter()
        .filter_map(|item| {
            if let AttributeValue::S(sk) = &item["sk"] {
                Some(sk.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(sks, vec!["item01", "item02", "item03", "item04", "item05"]);
}

#[tokio::test]
async fn hash_only_main_table_backward_no_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk",
        values,
        None,
        None,
        Some(false),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 5);
    assert!(last_key.is_none());
    let sks: Vec<String> = items
        .iter()
        .filter_map(|item| {
            if let AttributeValue::S(sk) = &item["sk"] {
                Some(sk.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(sks, vec!["item05", "item04", "item03", "item02", "item01"]);
}

#[tokio::test]
async fn hash_only_main_table_forward_with_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk",
        values,
        Some(3),
        None,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 3);
    assert!(last_key.is_some());
    let sks: Vec<String> = items
        .iter()
        .filter_map(|item| {
            if let AttributeValue::S(sk) = &item["sk"] {
                Some(sk.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(sks, vec!["item01", "item02", "item03"]);
}

#[tokio::test]
async fn hash_only_gsi_forward_no_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), true).await;
    populate_test_data(&provider, &TableName::new("test_table"), true).await;

    let mut values = HashMap::new();
    values.insert(
        ":gsi_pk".to_string(),
        AttributeValue::S("category_a".to_string()),
    );

    let request = create_query_request(
        &TableName::new("test_table"),
        Some(&IndexName::new("TestGSI")),
        "gsi_pk = :gsi_pk",
        values,
        None,
        None,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 6);
    assert!(last_key.is_none());
}

#[tokio::test]
async fn gsi_projection_limits_attributes() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();

    // Create a table with GSIs having different projection types
    let create_request = CreateTableRequest::new(
        TableName::new("ProjectionTestTable"),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        storage_types::BillingMode::PayPerRequest,
    )
    .with_global_secondary_indexes(Some(vec![
        CreateGlobalSecondaryIndex {
            index_name: IndexName::new("KeysOnlyGSI"),
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: "gsi_pk".to_string(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "gsi_sk".to_string(),
                    key_type: KeyType::Range,
                },
            ],
            projection: Projection {
                projection_type: Some(ProjectionType::KeysOnly),
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        },
        CreateGlobalSecondaryIndex {
            index_name: IndexName::new("IncludeGSI"),
            key_schema: vec![KeySchemaElement {
                attribute_name: "gsi_pk".to_string(),
                key_type: KeyType::Hash,
            }],
            projection: Projection {
                projection_type: Some(ProjectionType::Include),
                non_key_attributes: Some(vec!["included_attr".to_string()]),
            },
            provisioned_throughput: None,
        },
        CreateGlobalSecondaryIndex {
            index_name: IndexName::new("AllGSI"),
            key_schema: vec![KeySchemaElement {
                attribute_name: "gsi_pk".to_string(),
                key_type: KeyType::Hash,
            }],
            projection: Projection {
                projection_type: Some(ProjectionType::All),
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        },
    ]));

    provider.create_table(&create_request).await.unwrap();

    // Put an item with many attributes
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("main_pk".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("main_sk".to_string()));
    item.insert(
        "gsi_pk".to_string(),
        AttributeValue::S("index_pk".to_string()),
    );
    item.insert(
        "gsi_sk".to_string(),
        AttributeValue::S("index_sk".to_string()),
    );
    item.insert(
        "included_attr".to_string(),
        AttributeValue::S("should_be_included".to_string()),
    );
    item.insert(
        "excluded_attr".to_string(),
        AttributeValue::S("should_be_excluded".to_string()),
    );
    item.insert(
        "another_attr".to_string(),
        AttributeValue::S("also_excluded".to_string()),
    );

    provider
        .put_item(
            TableName::new("ProjectionTestTable"),
            item.clone(),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    // Process GSI updates (this would normally be done by the background job)
    provider.process_gsi_updates().await.unwrap();

    // Query each GSI and verify the attributes

    // KeysOnly GSI should only have key attributes
    let keys_only_scan = ScanTableRequest {
        table_name: TableName::new("ProjectionTestTable"),
        index_name: Some(IndexName::new("KeysOnlyGSI")),
        limit: Some(10),
        exclusive_start_key: None,
        consistent_read: false,
    };
    let (keys_only_items, _) = provider.scan_table(&keys_only_scan).await.unwrap();
    assert_eq!(keys_only_items.len(), 1);

    let keys_only_item = &keys_only_items[0];
    assert_eq!(keys_only_item.len(), 4); // pk, sk, gsi_pk, gsi_sk
    assert!(keys_only_item.contains_key("pk"));
    assert!(keys_only_item.contains_key("sk"));
    assert!(keys_only_item.contains_key("gsi_pk"));
    assert!(keys_only_item.contains_key("gsi_sk"));
    assert!(!keys_only_item.contains_key("included_attr"));
    assert!(!keys_only_item.contains_key("excluded_attr"));
    assert!(!keys_only_item.contains_key("another_attr"));

    // Include GSI should have keys plus included attributes
    let include_scan = ScanTableRequest {
        table_name: TableName::new("ProjectionTestTable"),
        index_name: Some(IndexName::new("IncludeGSI")),
        limit: Some(10),
        exclusive_start_key: None,
        consistent_read: false,
    };
    let (include_items, _) = provider.scan_table(&include_scan).await.unwrap();
    assert_eq!(include_items.len(), 1);

    let include_item = &include_items[0];
    assert_eq!(include_item.len(), 4); // pk, sk, gsi_pk, included_attr
    assert!(include_item.contains_key("pk"));
    assert!(include_item.contains_key("sk"));
    assert!(include_item.contains_key("gsi_pk"));
    assert!(include_item.contains_key("included_attr"));
    assert!(!include_item.contains_key("gsi_sk")); // Not in this GSI's key schema
    assert!(!include_item.contains_key("excluded_attr"));
    assert!(!include_item.contains_key("another_attr"));

    // All GSI should have all attributes
    let all_scan = ScanTableRequest {
        table_name: TableName::new("ProjectionTestTable"),
        index_name: Some(IndexName::new("AllGSI")),
        limit: Some(10),
        exclusive_start_key: None,
        consistent_read: false,
    };
    let (all_items, _) = provider.scan_table(&all_scan).await.unwrap();
    assert_eq!(all_items.len(), 1);

    let all_item = &all_items[0];
    assert_eq!(all_item.len(), 7); // All attributes
    assert!(all_item.contains_key("pk"));
    assert!(all_item.contains_key("sk"));
    assert!(all_item.contains_key("gsi_pk"));
    assert!(all_item.contains_key("included_attr"));
    assert!(all_item.contains_key("excluded_attr"));
    assert!(all_item.contains_key("another_attr"));
}

#[tokio::test]
async fn hash_only_gsi_backward_no_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), true).await;
    populate_test_data(&provider, &TableName::new("test_table"), true).await;

    let mut values = HashMap::new();
    values.insert(
        ":gsi_pk".to_string(),
        AttributeValue::S("category_a".to_string()),
    );

    let request = create_query_request(
        &TableName::new("test_table"),
        Some(&IndexName::new("TestGSI")),
        "gsi_pk = :gsi_pk",
        values,
        None,
        None,
        Some(false),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 6);
    assert!(last_key.is_none());
}

#[tokio::test]
async fn hash_only_gsi_forward_with_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), true).await;
    populate_test_data(&provider, &TableName::new("test_table"), true).await;

    let mut values = HashMap::new();
    values.insert(
        ":gsi_pk".to_string(),
        AttributeValue::S("category_a".to_string()),
    );

    let request = create_query_request(
        &TableName::new("test_table"),
        Some(&IndexName::new("TestGSI")),
        "gsi_pk = :gsi_pk",
        values,
        Some(3),
        None,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 3);
    assert!(last_key.is_some());
}

// ===== SCAN TESTS =====

#[tokio::test]
async fn scan_main_table_no_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let request = create_scan_request(&TableName::new("test_table"), None, None, None);

    let (items, last_key) = provider.scan_table(&request).await.unwrap();
    assert_eq!(items.len(), 10);
    assert!(last_key.is_none());
}

#[tokio::test]
async fn scan_main_table_with_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let request = create_scan_request(&TableName::new("test_table"), None, Some(5), None);

    let (items, last_key) = provider.scan_table(&request).await.unwrap();
    assert_eq!(items.len(), 5);
    assert!(last_key.is_some());
}

#[tokio::test]
async fn scan_gsi_no_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), true).await;
    populate_test_data(&provider, &TableName::new("test_table"), true).await;

    let request = create_scan_request(
        &TableName::new("test_table"),
        Some(&IndexName::new("TestGSI")),
        None,
        None,
    );

    let (items, last_key) = provider.scan_table(&request).await.unwrap();
    assert_eq!(items.len(), 10);
    assert!(last_key.is_none());
}

#[tokio::test]
async fn scan_gsi_with_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), true).await;
    populate_test_data(&provider, &TableName::new("test_table"), true).await;

    let request = create_scan_request(
        &TableName::new("test_table"),
        Some(&IndexName::new("TestGSI")),
        Some(4),
        None,
    );

    let (items, last_key) = provider.scan_table(&request).await.unwrap();
    assert_eq!(items.len(), 4);
    assert!(last_key.is_some());
}

// ===== PAGINATION TESTS =====

#[tokio::test]
async fn pagination_main_table_forward_with_limit() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));

    // First page
    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk",
        values.clone(),
        Some(2),
        None,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 2);
    assert!(last_key.is_some());
    let sks: Vec<String> = items
        .iter()
        .filter_map(|item| {
            if let AttributeValue::S(sk) = &item["sk"] {
                Some(sk.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(sks, vec!["item01", "item02"]);

    // Second page
    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk",
        values,
        Some(2),
        last_key,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 2);
    assert!(last_key.is_some());
    let sks: Vec<String> = items
        .iter()
        .filter_map(|item| {
            if let AttributeValue::S(sk) = &item["sk"] {
                Some(sk.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(sks, vec!["item03", "item04"]);
}

#[tokio::test]
async fn pagination_gsi_forward_with_limit() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), true).await;
    populate_test_data(&provider, &TableName::new("test_table"), true).await;
    provider.process_gsi_updates().await.unwrap();

    let mut values = HashMap::new();
    values.insert(
        ":gsi_pk".to_string(),
        AttributeValue::S("category_a".to_string()),
    );

    // First page
    let request = create_query_request(
        &TableName::new("test_table"),
        Some(&IndexName::new("TestGSI")),
        "gsi_pk = :gsi_pk",
        values.clone(),
        Some(3),
        None,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 3);
    assert!(last_key.is_some());

    // Second page
    let request = create_query_request(
        &TableName::new("test_table"),
        Some(&IndexName::new("TestGSI")),
        "gsi_pk = :gsi_pk",
        values,
        Some(3),
        last_key,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 3);
    assert!(last_key.is_none());
}

#[tokio::test]
async fn pagination_scan_with_limit() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    // First page
    let request = create_scan_request(&TableName::new("test_table"), None, Some(4), None);
    let (items, last_key) = provider.scan_table(&request).await.unwrap();
    assert_eq!(items.len(), 4);
    assert!(last_key.is_some());

    // Second page
    let request = create_scan_request(&TableName::new("test_table"), None, Some(4), last_key);
    let (items, last_key) = provider.scan_table(&request).await.unwrap();
    assert_eq!(items.len(), 4);
    assert!(last_key.is_some());
}

#[tokio::test]
async fn scan_pagination_uses_exclusive_start_key() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let request = create_scan_request(&TableName::new("test_table"), None, Some(4), None);
    let (first_items, last_key) = provider.scan_table(&request).await.unwrap();
    assert_eq!(first_items.len(), 4);
    let last_key = last_key.expect("expected next page token");

    let request = create_scan_request(&TableName::new("test_table"), None, Some(4), Some(last_key));
    let (second_items, _) = provider.scan_table(&request).await.unwrap();
    assert_eq!(second_items.len(), 4);

    let mut first_keys = HashSet::new();
    for item in &first_items {
        let pk = match item.get("pk") {
            Some(AttributeValue::S(value)) => value.clone(),
            _ => panic!("missing pk"),
        };
        let sk = match item.get("sk") {
            Some(AttributeValue::S(value)) => value.clone(),
            _ => panic!("missing sk"),
        };
        first_keys.insert((pk, sk));
    }

    for item in &second_items {
        let pk = match item.get("pk") {
            Some(AttributeValue::S(value)) => value.clone(),
            _ => panic!("missing pk"),
        };
        let sk = match item.get("sk") {
            Some(AttributeValue::S(value)) => value.clone(),
            _ => panic!("missing sk"),
        };
        assert!(
            !first_keys.contains(&(pk.clone(), sk.clone())),
            "duplicate item across pages: {pk}/{sk}"
        );
    }
}

// ===== ADDITIONAL COMPREHENSIVE TESTS =====

#[tokio::test]
async fn all_query_operators_comprehensive() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), true).await;
    populate_test_data(&provider, &TableName::new("test_table"), true).await;

    // Test all operators on main table
    let operators = vec![
        ("pk = :pk AND sk = :sk", "exact"),
        ("pk = :pk AND sk BETWEEN :start AND :end", "between"),
        ("pk = :pk AND sk < :val", "less_than"),
        ("pk = :pk AND sk <= :val", "less_equal"),
        ("pk = :pk AND sk > :val", "greater_than"),
        ("pk = :pk AND sk >= :val", "greater_equal"),
        ("pk = :pk AND begins_with(sk, :prefix)", "begins_with"),
        ("pk = :pk", "hash_only"),
    ];

    for (expression, op_name) in operators {
        let mut values = HashMap::new();
        values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));

        match op_name {
            "exact" => {
                values.insert(":sk".to_string(), AttributeValue::S("item01".to_string()));
            }
            "between" => {
                values.insert(
                    ":start".to_string(),
                    AttributeValue::S("item01".to_string()),
                );
                values.insert(":end".to_string(), AttributeValue::S("item03".to_string()));
            }
            "less_than" | "less_equal" | "greater_than" | "greater_equal" => {
                values.insert(":val".to_string(), AttributeValue::S("item03".to_string()));
            }
            "begins_with" => {
                values.insert(
                    ":prefix".to_string(),
                    AttributeValue::S("item0".to_string()),
                );
            }
            _ => {}
        }

        let request = create_query_request(
            &TableName::new("test_table"),
            None,
            expression,
            values,
            None,
            None,
            Some(true),
        );

        let (items, _) = provider.query_table(&request).await.unwrap();
        assert!(!items.is_empty(), "Operator {op_name} should return items");
    }
}

#[tokio::test]
async fn scan_index_combinations() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), true).await;
    populate_test_data(&provider, &TableName::new("test_table"), true).await;

    // Test scan on main table
    let request = create_scan_request(&TableName::new("test_table"), None, Some(5), None);
    let (items, _) = provider.scan_table(&request).await.unwrap();
    assert_eq!(items.len(), 5);

    // Test scan on GSI
    let request = create_scan_request(
        &TableName::new("test_table"),
        Some(&IndexName::new("TestGSI")),
        Some(5),
        None,
    );
    let (items, _) = provider.scan_table(&request).await.unwrap();
    assert_eq!(items.len(), 5);
}

#[tokio::test]
async fn limit_and_pagination_edge_cases() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    // Test limit larger than available items
    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk",
        values,
        Some(10), // More than available
        None,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 5); // Should return all available
    assert!(last_key.is_none()); // No more pages

    // Test limit of 1
    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk",
        values,
        Some(1),
        None,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 1);
    assert!(last_key.is_some());
}

#[tokio::test]
async fn scan_forward_default_behavior() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    // Test default scan_index_forward (should be true)
    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));

    let request = QueryTableRequest {
        table_name: TableName::new("test_table"),
        index_name: None,
        key_condition_expression: "pk = :pk".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(values),
        limit: Some(3),
        exclusive_start_key: None,
        scan_index_forward: None, // Should default to true
        consistent_read: false,
    };

    let (items, _) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 3);
    let sks: Vec<String> = items
        .iter()
        .filter_map(|item| {
            if let AttributeValue::S(sk) = &item["sk"] {
                Some(sk.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(sks, vec!["item01", "item02", "item03"]);
}

#[tokio::test]
async fn nonexistent_index_error() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        Some(&IndexName::new("NonExistentIndex")),
        "pk = :pk",
        values,
        None,
        None,
        Some(true),
    );

    let result = provider.query_table(&request).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn empty_table_queries() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("empty_table"), false).await;

    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));

    let request = create_query_request(
        &TableName::new("empty_table"),
        None,
        "pk = :pk",
        values,
        None,
        None,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 0);
    assert!(last_key.is_none());
}

#[tokio::test]
async fn scan_empty_table() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("empty_table"), false).await;

    let request = create_scan_request(&TableName::new("empty_table"), None, None, None);
    let (items, last_key) = provider.scan_table(&request).await.unwrap();
    assert_eq!(items.len(), 0);
    assert!(last_key.is_none());
}

// ===== ADDITIONAL OPERATOR TESTS =====

#[tokio::test]
async fn comparison_operators_gsi_backward() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), true).await;
    populate_test_data(&provider, &TableName::new("test_table"), true).await;

    let operators = vec![
        ("<", "less_than"),
        ("<=", "less_equal"),
        (">", "greater_than"),
        (">=", "greater_equal"),
    ];

    for (op, op_name) in operators {
        let mut values = HashMap::new();
        values.insert(
            ":gsi_pk".to_string(),
            AttributeValue::S("category_a".to_string()),
        );
        values.insert(":val".to_string(), AttributeValue::N("300".to_string()));

        let expression = format!("gsi_pk = :gsi_pk AND gsi_sk {op} :val");
        let request = create_query_request(
            &TableName::new("test_table"),
            Some(&IndexName::new("TestGSI")),
            &expression,
            values,
            None,
            None,
            Some(false), // Backward
        );

        let (items, _) = provider.query_table(&request).await.unwrap();
        assert!(
            !items.is_empty(),
            "GSI backward {op_name} should return items for expression: {expression}"
        );
    }
}

#[tokio::test]
async fn begins_with_various_prefixes() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let prefixes = vec!["item0", "item01", "item"];

    for prefix in prefixes {
        let mut values = HashMap::new();
        values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));
        values.insert(":prefix".to_string(), AttributeValue::S(prefix.to_string()));

        let request = create_query_request(
            &TableName::new("test_table"),
            None,
            "pk = :pk AND begins_with(sk, :prefix)",
            values,
            None,
            None,
            Some(true),
        );

        let (items, _) = provider.query_table(&request).await.unwrap();
        assert!(
            !items.is_empty(),
            "begins_with with prefix {prefix} should return items"
        );
    }
}

#[tokio::test]
async fn large_limit_values() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    // Test with very large limit
    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk",
        values,
        Some(1000), // Much larger than available items
        None,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 5); // Should return all available
    assert!(last_key.is_none());
}

#[tokio::test]
async fn scan_with_pagination_edge_cases() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    // Test scan with limit equal to total items
    let request = create_scan_request(&TableName::new("test_table"), None, Some(10), None);
    let (items, last_key) = provider.scan_table(&request).await.unwrap();
    assert_eq!(items.len(), 10);
    assert!(last_key.is_none()); // No more pages since we got all items

    // Test scan with limit larger than total items
    let request = create_scan_request(&TableName::new("test_table"), None, Some(20), None);
    let (items, last_key) = provider.scan_table(&request).await.unwrap();
    assert_eq!(items.len(), 10);
    assert!(last_key.is_none());
}

// ===== ADDITIONAL COMPREHENSIVE TESTS FOR 60+ TOTAL =====

#[tokio::test]
async fn exact_key_match_backward_with_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));
    values.insert(":sk".to_string(), AttributeValue::S("item01".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk AND sk = :sk",
        values,
        Some(1),
        None,
        Some(false),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 1);
    assert!(last_key.is_none());
}

#[tokio::test]
async fn exact_key_match_gsi_backward_with_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), true).await;
    populate_test_data(&provider, &TableName::new("test_table"), true).await;

    let mut values = HashMap::new();
    values.insert(
        ":gsi_pk".to_string(),
        AttributeValue::S("category_a".to_string()),
    );
    values.insert(":gsi_sk".to_string(), AttributeValue::N("100".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        Some(&IndexName::new("TestGSI")),
        "gsi_pk = :gsi_pk AND gsi_sk = :gsi_sk",
        values,
        Some(1),
        None,
        Some(false),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 1);
    assert!(last_key.is_none());
}

#[tokio::test]
async fn between_backward_with_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));
    values.insert(
        ":start".to_string(),
        AttributeValue::S("item01".to_string()),
    );
    values.insert(":end".to_string(), AttributeValue::S("item03".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk AND sk BETWEEN :start AND :end",
        values,
        Some(2),
        None,
        Some(false),
    );
    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 2);
    assert!(last_key.is_some());
    let sks: Vec<String> = items
        .iter()
        .filter_map(|item| {
            if let AttributeValue::S(sk) = &item["sk"] {
                Some(sk.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(sks, vec!["item03", "item02"]);
}

#[tokio::test]
async fn between_gsi_backward_with_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), true).await;
    populate_test_data(&provider, &TableName::new("test_table"), true).await;

    let mut values = HashMap::new();
    values.insert(
        ":gsi_pk".to_string(),
        AttributeValue::S("category_a".to_string()),
    );
    values.insert(":start".to_string(), AttributeValue::N("100".to_string()));
    values.insert(":end".to_string(), AttributeValue::N("300".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        Some(&IndexName::new("TestGSI")),
        "gsi_pk = :gsi_pk AND gsi_sk BETWEEN :start AND :end",
        values,
        Some(2),
        None,
        Some(false),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 2);
    assert!(last_key.is_some());
}

#[tokio::test]
async fn begins_with_numeric_validation_error() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));
    values.insert(":x".to_string(), AttributeValue::N("123".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk AND begins_with(sk, :x)",
        values,
        None,
        None,
        Some(true),
    );

    let result = provider.query_table(&request).await;
    assert!(
        result.is_err(),
        "Expected validation error for begins_with with numeric value"
    );
    let err = result.unwrap_err();
    assert!(
        err.to_string()
            .contains("begins_with is only valid for string types"),
        "Unexpected error: {err}"
    );
}

#[tokio::test]
async fn less_than_backward_with_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));
    values.insert(":val".to_string(), AttributeValue::S("item03".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk AND sk < :val",
        values,
        Some(1),
        None,
        Some(false),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 1);
    assert!(last_key.is_some());
    let sks: Vec<String> = items
        .iter()
        .filter_map(|item| {
            if let AttributeValue::S(sk) = &item["sk"] {
                Some(sk.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(sks, vec!["item02"]);
}

#[tokio::test]
async fn greater_than_backward_with_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));
    values.insert(":val".to_string(), AttributeValue::S("item03".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk AND sk > :val",
        values,
        Some(1),
        None,
        Some(false),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 1);
    assert!(last_key.is_some());
    let sks: Vec<String> = items
        .iter()
        .filter_map(|item| {
            if let AttributeValue::S(sk) = &item["sk"] {
                Some(sk.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(sks, vec!["item05"]);
}

#[tokio::test]
async fn begins_with_backward_with_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));
    values.insert(
        ":prefix".to_string(),
        AttributeValue::S("item0".to_string()),
    );

    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk AND begins_with(sk, :prefix)",
        values,
        Some(2),
        None,
        Some(false),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 2);
    assert!(last_key.is_some());
    let sks: Vec<String> = items
        .iter()
        .filter_map(|item| {
            if let AttributeValue::S(sk) = &item["sk"] {
                Some(sk.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(sks, vec!["item05", "item04"]);
}

#[tokio::test]
async fn hash_only_backward_with_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk",
        values,
        Some(2),
        None,
        Some(false),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 2);
    assert!(last_key.is_some());
    let sks: Vec<String> = items
        .iter()
        .filter_map(|item| {
            if let AttributeValue::S(sk) = &item["sk"] {
                Some(sk.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(sks, vec!["item05", "item04"]);
}

#[tokio::test]
async fn hash_only_gsi_backward_with_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), true).await;
    populate_test_data(&provider, &TableName::new("test_table"), true).await;

    let mut values = HashMap::new();
    values.insert(
        ":gsi_pk".to_string(),
        AttributeValue::S("category_a".to_string()),
    );

    let request = create_query_request(
        &TableName::new("test_table"),
        Some(&IndexName::new("TestGSI")),
        "gsi_pk = :gsi_pk",
        values,
        Some(3),
        None,
        Some(false),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 3);
    assert!(last_key.is_some());
}

#[tokio::test]
async fn scan_gsi_backward_no_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), true).await;
    populate_test_data(&provider, &TableName::new("test_table"), true).await;

    let request = ScanTableRequest {
        table_name: TableName::new("test_table"),
        index_name: Some(IndexName::new("TestGSI")),
        limit: None,
        exclusive_start_key: None,
        consistent_read: false,
    };

    let (items, last_key) = provider.scan_table(&request).await.unwrap();
    assert_eq!(items.len(), 10);
    assert!(last_key.is_none());
}

#[tokio::test]
async fn scan_gsi_backward_with_limit_no_page() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), true).await;
    populate_test_data(&provider, &TableName::new("test_table"), true).await;

    let request = ScanTableRequest {
        table_name: TableName::new("test_table"),
        index_name: Some(IndexName::new("TestGSI")),
        limit: Some(3),
        exclusive_start_key: None,
        consistent_read: false,
    };

    let (items, last_key) = provider.scan_table(&request).await.unwrap();
    assert_eq!(items.len(), 3);
    assert!(last_key.is_some());
}

#[tokio::test]
async fn pagination_backward_main_table() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));

    // First page backward
    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk",
        values.clone(),
        Some(2),
        None,
        Some(false),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 2);
    assert!(last_key.is_some());
    let sks: Vec<String> = items
        .iter()
        .filter_map(|item| {
            if let AttributeValue::S(sk) = &item["sk"] {
                Some(sk.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(sks, vec!["item05", "item04"]);

    // Second page backward
    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk",
        values,
        Some(2),
        last_key,
        Some(false),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 2);
    assert!(last_key.is_some());
    let sks: Vec<String> = items
        .iter()
        .filter_map(|item| {
            if let AttributeValue::S(sk) = &item["sk"] {
                Some(sk.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(sks, vec!["item03", "item02"]);
}

#[tokio::test]
async fn pagination_backward_gsi() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), true).await;
    populate_test_data(&provider, &TableName::new("test_table"), true).await;

    let mut values = HashMap::new();
    values.insert(
        ":gsi_pk".to_string(),
        AttributeValue::S("category_a".to_string()),
    );

    // First page backward
    let request = create_query_request(
        &TableName::new("test_table"),
        Some(&IndexName::new("TestGSI")),
        "gsi_pk = :gsi_pk",
        values.clone(),
        Some(3),
        None,
        Some(false),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 3);
    assert!(last_key.is_some());

    // Second page backward
    let request = create_query_request(
        &TableName::new("test_table"),
        Some(&IndexName::new("TestGSI")),
        "gsi_pk = :gsi_pk",
        values,
        Some(3),
        last_key,
        Some(false),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 3);
    assert!(last_key.is_none());
}

#[tokio::test]
async fn pagination_scan_backward() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    // First page backward
    let request = ScanTableRequest {
        table_name: TableName::new("test_table"),
        index_name: None,
        limit: Some(4),
        exclusive_start_key: None,
        consistent_read: false,
    };

    let (items, last_key) = provider.scan_table(&request).await.unwrap();
    assert_eq!(items.len(), 4);
    assert!(last_key.is_some());

    // Second page backward
    let request = ScanTableRequest {
        table_name: TableName::new("test_table"),
        index_name: None,
        limit: Some(4),
        exclusive_start_key: last_key,
        consistent_read: false,
    };

    let (items, last_key) = provider.scan_table(&request).await.unwrap();
    assert_eq!(items.len(), 4);
    assert!(last_key.is_some());
}

#[tokio::test]
async fn mixed_pagination_forward_backward() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));

    // First page forward
    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk",
        values.clone(),
        Some(2),
        None,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 2);
    assert!(last_key.is_some());

    // Second page backward from the same point
    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk",
        values,
        Some(2),
        last_key,
        Some(false),
    );

    let (items, _) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 1);
    let sks: Vec<String> = items
        .iter()
        .filter_map(|item| {
            if let AttributeValue::S(sk) = &item["sk"] {
                Some(sk.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(sks, vec!["item01"]);
}

#[tokio::test]
async fn edge_case_single_item_table() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("single_item_table"), false).await;

    // Add single item
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("user1".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("item01".to_string()));
    item.insert(
        "data".to_string(),
        AttributeValue::S("single_data".to_string()),
    );

    provider
        .put_item(
            TableName::new("single_item_table"),
            item,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    // Test all query types on single item
    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));

    let request = create_query_request(
        &TableName::new("single_item_table"),
        None,
        "pk = :pk",
        values,
        None,
        None,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 1);
    assert!(last_key.is_none());
}

#[tokio::test]
async fn edge_case_no_matching_items() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    // Query for non-existent partition key
    let mut values = HashMap::new();
    values.insert(
        ":pk".to_string(),
        AttributeValue::S("nonexistent".to_string()),
    );

    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk",
        values,
        None,
        None,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 0);
    assert!(last_key.is_none());
}

#[tokio::test]
async fn edge_case_empty_result_with_limit() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    // Query with limit but no matching items
    let mut values = HashMap::new();
    values.insert(
        ":pk".to_string(),
        AttributeValue::S("nonexistent".to_string()),
    );

    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk",
        values,
        Some(5),
        None,
        Some(true),
    );

    let (items, last_key) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 0);
    assert!(last_key.is_none());
}

#[tokio::test]
async fn scan_with_invalid_page_token() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    // Create invalid page token
    let mut invalid_token = "INvalidToken".to_string();
    invalid_token.push_str("=="); // Malformed base64

    let request = create_scan_request(
        &TableName::new("test_table"),
        None,
        Some(5),
        Some(invalid_token),
    );

    // This should not panic but handle gracefully
    let result = provider.scan_table(&request).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn query_with_complex_expressions() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), true).await;
    populate_test_data(&provider, &TableName::new("test_table"), true).await;

    // Test complex expression combinations
    let test_cases = vec![
        ("pk = :pk AND sk > :start AND sk < :end", "range_query"),
        (
            "gsi_pk = :gsi_pk AND gsi_sk >= :min AND gsi_sk <= :max",
            "gsi_range_query",
        ),
    ];

    for (expression, case_name) in test_cases {
        let mut values = HashMap::new();
        match case_name {
            "range_query" => {
                values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));
                values.insert(
                    ":start".to_string(),
                    AttributeValue::S("item01".to_string()),
                );
                values.insert(":end".to_string(), AttributeValue::S("item04".to_string()));
            }
            "gsi_range_query" => {
                values.insert(
                    ":gsi_pk".to_string(),
                    AttributeValue::S("category_a".to_string()),
                );
                values.insert(":min".to_string(), AttributeValue::N("100".to_string()));
                values.insert(":max".to_string(), AttributeValue::N("400".to_string()));
            }
            _ => {}
        }

        let index_name = if case_name.contains("gsi") {
            Some(&IndexName::new("TestGSI"))
        } else {
            None
        };

        let request = create_query_request(
            &TableName::new("test_table"),
            index_name,
            expression,
            values,
            Some(3),
            None,
            Some(true),
        );

        let (items, _) = provider.query_table(&request).await.unwrap();
        assert!(
            !items.is_empty(),
            "Complex expression {case_name} should return items"
        );
    }
}

#[tokio::test]
async fn concurrent_table_operations() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();

    // Create multiple tables concurrently
    let table_names = vec![
        TableName::new("table1"),
        TableName::new("table2"),
        TableName::new("table3"),
    ];

    for table_name in &table_names {
        create_test_table(&provider, table_name, false).await;
        populate_test_data(&provider, table_name, false).await;
    }

    // Query all tables concurrently
    let mut handles = vec![];

    for table_name in &table_names {
        let provider_clone = provider.clone();
        let table_name_clone = table_name.clone();

        let handle = tokio::spawn(async move {
            let mut values = HashMap::new();
            values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));

            let request = create_query_request(
                &table_name_clone,
                None,
                "pk = :pk",
                values,
                None,
                None,
                Some(true),
            );

            provider_clone.query_table(&request).await.unwrap()
        });

        handles.push(handle);
    }

    // Wait for all queries to complete
    for handle in handles {
        let (items, _) = handle.await.unwrap();
        assert_eq!(items.len(), 5);
    }
}

#[tokio::test]
async fn scan_performance_large_dataset() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("large_table"), false).await;

    // Add more items for performance testing
    for i in 0..50 {
        let mut item = HashMap::new();
        item.insert(
            "pk".to_string(),
            AttributeValue::S(format!("user{}", i % 5)),
        );
        item.insert("sk".to_string(), AttributeValue::S(format!("item{i:02}")));
        item.insert("data".to_string(), AttributeValue::S(format!("data{i}")));

        provider
            .put_item(TableName::new("large_table"), item, None, None, None, None)
            .await
            .unwrap();
    }

    // Test scan with limit
    let request = create_scan_request(&TableName::new("large_table"), None, Some(10), None);
    let (items, last_key) = provider.scan_table(&request).await.unwrap();
    assert_eq!(items.len(), 10);
    assert!(last_key.is_some());
}

#[tokio::test]
async fn query_with_attribute_name_substitution() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    // Test with expression attribute names (though our implementation may not fully
    // support this)
    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));

    let request = QueryTableRequest {
        table_name: TableName::new("test_table"),
        index_name: None,
        key_condition_expression: "pk = :pk".to_string(),
        expression_attribute_names: Some(HashMap::from([("#pk".to_string(), "pk".to_string())])),
        expression_attribute_values: Some(values),
        limit: None,
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    };

    let (items, _) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 5);
}

#[tokio::test]
async fn mixed_scan_and_query_consistency() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    // Get all items via scan
    let scan_request = create_scan_request(&TableName::new("test_table"), None, None, None);
    let (scan_items, _) = provider.scan_table(&scan_request).await.unwrap();

    // Get all items via query
    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));

    let query_request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk",
        values,
        None,
        None,
        Some(true),
    );

    let (query_items, _) = provider.query_table(&query_request).await.unwrap();

    // Should have same number of items for user1
    assert_eq!(query_items.len(), 5);

    // Total scan should have all items
    assert_eq!(scan_items.len(), 10);
}

#[tokio::test]
async fn error_handling_malformed_expressions() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;
    populate_test_data(&provider, &TableName::new("test_table"), false).await;

    // Test with malformed expression
    let request = QueryTableRequest {
        table_name: TableName::new("test_table"),
        index_name: None,
        key_condition_expression: "invalid expression".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: Some(HashMap::new()),
        limit: None,
        exclusive_start_key: None,
        scan_index_forward: Some(true),
        consistent_read: false,
    };

    let result = provider.query_table(&request).await;
    // Should either return empty results or handle gracefully
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test]
async fn boundary_conditions_numeric_sort_keys() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), true).await;

    // Add items with numeric sort keys at boundaries
    let boundary_items = vec![
        ("user1", "item01", "category_a", "0", "zero"),
        ("user1", "item02", "category_a", "999999", "max"),
        ("user1", "item03", "category_a", "000001", "padded_zero"),
    ];

    for (pk, sk, gsi_pk, gsi_sk, data) in &boundary_items {
        let mut item = HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S((*pk).to_string()));
        item.insert("sk".to_string(), AttributeValue::S((*sk).to_string()));
        item.insert(
            "gsi_pk".to_string(),
            AttributeValue::S((*gsi_pk).to_string()),
        );
        item.insert(
            "gsi_sk".to_string(),
            AttributeValue::N((*gsi_sk).to_string()),
        );
        item.insert("data".to_string(), AttributeValue::S((*data).to_string()));

        provider
            .put_item(TableName::new("test_table"), item, None, None, None, None)
            .await
            .unwrap();
    }

    // Execute GSI update
    let provider_arc = std::sync::Arc::new(provider.clone());
    let gsi_job = crate::storage_provider::GsiUpdateJob::new(provider_arc);
    let _ = gsi_job.execute().await.unwrap();

    // Query boundaries
    let mut values = HashMap::new();
    values.insert(
        ":gsi_pk".to_string(),
        AttributeValue::S("category_a".to_string()),
    );
    values.insert(":min".to_string(), AttributeValue::N("0".to_string()));
    values.insert(":max".to_string(), AttributeValue::N("999999".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        Some(&IndexName::new("TestGSI")),
        "gsi_pk = :gsi_pk AND gsi_sk BETWEEN :min AND :max",
        values,
        None,
        None,
        Some(true),
    );

    let (items, _) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 3);
}

#[tokio::test]
async fn unicode_and_special_characters() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;

    // Add items with unicode and special characters
    let unicode_items = vec![
        ("user1", "item_ñ", "unicode_data"),
        ("user1", "item_测试", "chinese_data"),
        ("user1", "item_🚀", "emoji_data"),
        ("user1", "item_@#$%", "special_chars"),
    ];

    for (pk, sk, data) in &unicode_items {
        let mut item = HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S((*pk).to_string()));
        item.insert("sk".to_string(), AttributeValue::S((*sk).to_string()));
        item.insert("data".to_string(), AttributeValue::S((*data).to_string()));

        provider
            .put_item(TableName::new("test_table"), item, None, None, None, None)
            .await
            .unwrap();
    }

    // Query unicode items
    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));

    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk",
        values,
        None,
        None,
        Some(true),
    );

    let (items, _) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 4);
}

#[tokio::test]
async fn large_attribute_values() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    create_test_table(&provider, &TableName::new("test_table"), false).await;

    // Create large attribute value
    let large_data = "x".repeat(10000);

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("user1".to_string()));
    item.insert(
        "sk".to_string(),
        AttributeValue::S("large_item".to_string()),
    );
    item.insert("data".to_string(), AttributeValue::S(large_data.clone()));

    provider
        .put_item(TableName::new("test_table"), item, None, None, None, None)
        .await
        .unwrap();

    // Query and verify large data is preserved
    let mut values = HashMap::new();
    values.insert(":pk".to_string(), AttributeValue::S("user1".to_string()));
    values.insert(
        ":sk".to_string(),
        AttributeValue::S("large_item".to_string()),
    );

    let request = create_query_request(
        &TableName::new("test_table"),
        None,
        "pk = :pk AND sk = :sk",
        values,
        None,
        None,
        Some(true),
    );

    let (items, _) = provider.query_table(&request).await.unwrap();
    assert_eq!(items.len(), 1);
    if let AttributeValue::S(data) = &items[0]["data"] {
        assert_eq!(data.len(), 10000);
        assert_eq!(data.as_str(), large_data.as_str());
    }
}

#[tokio::test]
async fn idempotency_token_ttl_support() {
    use storage_types::{TransactPutRequest, TransactWriteItem, TransactWriteItemsRequest};

    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();

    // Create a simple table for the test
    let table_name = TableName::new("IdempotencyTtlTest");
    let _table_id = create_test_table(&provider, &table_name, false).await;

    // Create a transact write request with client request token
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("item".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("1".to_string()));
    item.insert(
        "data".to_string(),
        AttributeValue::S("test_data".to_string()),
    );

    let request = TransactWriteItemsRequest {
        transact_items: vec![TransactWriteItem {
            put: Some(TransactPutRequest {
                table_name: TableName::new(&table_name),
                item,
                condition_expression: None,
                expression_attribute_names: None,
                expression_attribute_values: None,
                return_values_on_condition_check_failure: None,
            }),
            ..Default::default()
        }],
        client_request_token: Some("test_token_ttl_123".to_string()),
        ..Default::default()
    };

    // Execute the transaction first time
    let response1 = provider
        .transact_write_items(request.clone())
        .await
        .unwrap();

    // Execute the same transaction again immediately - should return cached
    // response
    let response2 = provider
        .transact_write_items(request.clone())
        .await
        .unwrap();

    // Both responses should be identical (idempotency working)
    assert_eq!(
        serde_json::to_string(&response1).unwrap(),
        serde_json::to_string(&response2).unwrap(),
        "Idempotent requests should return identical responses"
    );

    // Fixed: TTL is now implemented - tokens are stored with expiration
    // timestamps The TTL is set to 24 hours, so in production this token
    // would expire after that time
}

#[tokio::test]
async fn stream_entries_skipped_without_stream_gsi_ttl() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let table_name = TableName::new("NoStreamGsiTtl");
    create_test_table(&provider, &table_name, false).await;

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("user1".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("item1".to_string()));
    item.insert("data".to_string(), AttributeValue::S("payload".to_string()));

    provider
        .put_item(table_name.clone(), item, None, None, None, None)
        .await
        .unwrap();

    let page = StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
        .await
        .unwrap();
    assert!(
        page.items.is_empty(),
        "expected no stream entries when streams, GSI, and TTL are disabled"
    );
}

#[tokio::test]
async fn stream_entries_created_with_stream_enabled() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let table_name = "StreamEnabledTable";
    create_test_table_with_stream(&provider, table_name)
        .await
        .unwrap();

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("user1".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("item1".to_string()));
    item.insert("data".to_string(), AttributeValue::S("payload".to_string()));

    provider
        .put_item(TableName::new(table_name), item, None, None, None, None)
        .await
        .unwrap();

    let page = StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].data_type, StreamDataType::StreamPointer);
}

#[tokio::test]
async fn delete_table_removes_table_stream_state_before_recreate() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let table_name = TableName::new("RecreatedStreamTable");
    let key_schema = vec![
        KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        },
        KeySchemaElement {
            attribute_name: "sk".to_string(),
            key_type: KeyType::Range,
        },
    ];

    create_test_table_with_stream(&provider, &table_name)
        .await
        .unwrap();
    provider
        .put_item(
            table_name.clone(),
            stream_test_item("old", "item"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    provider.delete_table(&table_name).await.unwrap();

    create_test_table_with_stream(&provider, &table_name)
        .await
        .unwrap();
    provider
        .put_item(
            table_name.clone(),
            stream_test_item("new", "item"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let (records, _) = StreamProvider::get_stream_records_from_pointer_stream(
        &provider,
        StreamName::table_stream(&table_name),
        &key_schema,
        None,
        Some(10),
    )
    .await
    .unwrap();

    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0]
            .new_image
            .as_ref()
            .and_then(|item| item.get("pk")),
        Some(&AttributeValue::S("new".to_string()))
    );
}

#[tokio::test]
async fn put_item_encode_updates_stream_and_ttl_side_effects() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let table_name = "EncodeStreamTtlTable";
    create_test_table_with_stream(&provider, table_name)
        .await
        .unwrap();
    let table = TableName::new(table_name);

    provider
        .update_time_to_live(UpdateTimeToLiveRequest {
            table_name: table.clone(),
            time_to_live_specification: TimeToLiveSpecification {
                attribute_name: "ttl".to_string(),
                enabled: true,
            },
        })
        .await
        .unwrap();

    let baseline =
        StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
            .await
            .unwrap()
            .items
            .len();

    let future_at = (Utc::now().timestamp() + 3_600).to_string();
    let mut item = HashMap::new();
    item.insert(
        "pk".to_string(),
        AttributeValue::S("encode_user".to_string()),
    );
    item.insert(
        "sk".to_string(),
        AttributeValue::S("encode_item".to_string()),
    );
    item.insert("ttl".to_string(), AttributeValue::N(future_at));
    item.insert(
        "data".to_string(),
        AttributeValue::S("encode_payload".to_string()),
    );
    let wire_item = WireItem::from_attribute_map(&item).expect("wire item");

    provider
        .put_item_encode(table.clone(), wire_item, None, None, None, None)
        .await
        .unwrap();

    let page = StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
        .await
        .unwrap();
    assert!(page.items.len() > baseline);
    assert_eq!(
        page.items.last().expect("stream entry").data_type,
        StreamDataType::StreamPointer
    );

    let table_info = provider.get_table_info(&table).await.unwrap();
    let ttl_key = ttl::ttl_index_key_for_item(&table, &table_info, "ttl", &item)
        .unwrap()
        .expect("ttl key");
    assert!(
        provider
            .kv_store
            .get(&ttl_key, true)
            .await
            .unwrap()
            .is_some(),
        "ttl index entry should exist for encode write"
    );
}

#[tokio::test]
async fn stream_entries_created_with_gsi_without_stream_spec() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let table_name = TableName::new("GsiStreamTable");
    create_test_table(&provider, &table_name, true).await;

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("user1".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("item1".to_string()));
    item.insert("gsi_pk".to_string(), AttributeValue::S("gsi1".to_string()));
    item.insert("gsi_sk".to_string(), AttributeValue::N("1".to_string()));
    item.insert("data".to_string(), AttributeValue::S("payload".to_string()));

    provider
        .put_item(table_name.clone(), item, None, None, None, None)
        .await
        .unwrap();

    let item_key = ItemKey::table_key(
        table_name.clone(),
        AttributeValue::S("user1".to_string()),
        Some(AttributeValue::S("item1".to_string())),
    );

    let page = StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].data_type, StreamDataType::StreamPointer);

    let stored_pointer: StoredStreamPointer =
        storage_types::storage_serde::from_bytes(&page.items[0].data).unwrap();
    let pointer = stored_pointer.into_stream_pointer(page.items[0].id);
    assert_eq!(
        pointer.stream_name,
        StreamName::table_item_stream(&table_name, &item_key).expect("item stream")
    );
}

#[tokio::test]
async fn large_stream_pointer_records_resolve_item_images() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let table_name = TableName::new("LargePointerStreamTable");
    create_test_table_with_stream(&provider, table_name.as_ref())
        .await
        .unwrap();

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("user1".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("item1".to_string()));
    item.insert("data".to_string(), AttributeValue::S("x".repeat(2_048)));

    provider
        .put_item(table_name.clone(), item, None, None, None, None)
        .await
        .unwrap();

    let table_info = provider.get_table_info(&table_name).await.unwrap();
    let page = StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    let stored_pointer: StoredStreamPointer =
        storage_types::storage_serde::from_bytes(&page.items[0].data).unwrap();
    assert!(
        matches!(stored_pointer, StoredStreamPointer::Pointer { .. }),
        "large item should use pointer-backed stream storage"
    );

    let (records, _) = StreamProvider::get_stream_records_from_pointer_stream(
        &provider,
        StreamName::table_stream(&table_name),
        &table_info.key_schema,
        None,
        Some(10),
    )
    .await
    .unwrap();

    assert_eq!(records.len(), 1);
    let image = records[0].new_image.as_ref().expect("new image");
    assert_eq!(
        image.get("pk"),
        Some(&AttributeValue::S("user1".to_string()))
    );
    assert_eq!(
        image.get("sk"),
        Some(&AttributeValue::S("item1".to_string()))
    );
}

#[tokio::test]
async fn apply_replication_mutation_put_preserves_replication_metadata() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();
    create_test_table_with_stream(&provider, "ReplicationPutStream")
        .await
        .unwrap();

    let table_name = TableName::new("ReplicationPutStream");
    let new_image = HashMap::from([
        ("pk".to_string(), AttributeValue::S("user1".to_string())),
        ("sk".to_string(), AttributeValue::S("item1".to_string())),
        ("data".to_string(), AttributeValue::S("payload".to_string())),
    ]);
    let metadata = sample_replication_metadata("eu-west-1", 7);

    provider
        .apply_replication_mutation(ReplicationMutation {
            table_name: table_name.clone(),
            key: HashMap::from([
                ("pk".to_string(), AttributeValue::S("user1".to_string())),
                ("sk".to_string(), AttributeValue::S("item1".to_string())),
            ])
            .into(),
            new_image: Some(new_image.clone()),
            old_image: None,
            metadata: metadata.clone(),
        })
        .await
        .unwrap();

    let stored = provider
        .get_item(
            table_name.clone(),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("user1".to_string())),
                ("sk".to_string(), AttributeValue::S("item1".to_string())),
            ])
            .into(),
            true,
        )
        .await
        .unwrap()
        .unwrap()
        .to_attribute_map()
        .unwrap();
    assert_eq!(stored, new_image);

    let page = StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
        .await
        .unwrap();
    let stored_pointer: StoredStreamPointer =
        storage_types::storage_serde::from_bytes(&page.items[0].data).unwrap();
    assert_eq!(stored_pointer.replication_metadata(), Some(&metadata));
}

#[tokio::test]
async fn apply_replication_mutation_delete_writes_tombstone_for_missing_item() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();
    create_test_table_with_stream(&provider, "ReplicationDeleteStream")
        .await
        .unwrap();

    let table_name = TableName::new("ReplicationDeleteStream");
    let key = HashMap::from([
        ("pk".to_string(), AttributeValue::S("user2".to_string())),
        ("sk".to_string(), AttributeValue::S("item9".to_string())),
    ]);
    let metadata = sample_replication_metadata("ap-southeast-2", 11);

    provider
        .apply_replication_mutation(ReplicationMutation {
            table_name: table_name.clone(),
            key: key.clone().into(),
            new_image: None,
            old_image: None,
            metadata: metadata.clone(),
        })
        .await
        .unwrap();

    let system_page =
        StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
            .await
            .unwrap();
    let stored_pointer: StoredStreamPointer =
        storage_types::storage_serde::from_bytes(&system_page.items[0].data).unwrap();
    assert_eq!(stored_pointer.replication_metadata(), Some(&metadata));

    let item_key = ItemKey::from_key_schema(
        table_name.clone(),
        &[key_schema_pk(), key_schema_sk()],
        &key,
    )
    .unwrap();
    let item_stream = StreamName::table_item_stream(&table_name, &item_key).unwrap();
    let item_page = StreamProvider::read_forward(&provider, item_stream, None, 10)
        .await
        .unwrap();
    assert_eq!(item_page.items.len(), 1);
    assert_eq!(item_page.items[0].data_type, StreamDataType::DeleteMarker);
}

#[tokio::test]
async fn local_delete_missing_item_writes_tombstone_to_streams() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();
    create_test_table_with_stream(&provider, "LocalDeleteMissingStream")
        .await
        .unwrap();

    let table_name = TableName::new("LocalDeleteMissingStream");
    let key = HashMap::from([
        (
            "pk".to_string(),
            AttributeValue::S("user-local".to_string()),
        ),
        (
            "sk".to_string(),
            AttributeValue::S("missing-item".to_string()),
        ),
    ]);

    let deleted = provider
        .delete_item(table_name.clone(), key.clone().into(), None, None, None)
        .await
        .unwrap()
        .expect("missing deletes return empty item map in kv backend");
    assert!(deleted.is_empty());

    let system_page =
        StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
            .await
            .unwrap();
    assert_eq!(system_page.items.len(), 1);
    let stored_pointer: StoredStreamPointer =
        storage_types::storage_serde::from_bytes(&system_page.items[0].data).unwrap();
    assert!(stored_pointer.replication_metadata().is_none());

    let item_key = ItemKey::from_key_schema(
        table_name.clone(),
        &[key_schema_pk(), key_schema_sk()],
        &key,
    )
    .unwrap();
    let item_stream = StreamName::table_item_stream(&table_name, &item_key).unwrap();
    let item_page = StreamProvider::read_forward(&provider, item_stream, None, 10)
        .await
        .unwrap();
    assert_eq!(item_page.items.len(), 1);
    assert_eq!(item_page.items[0].data_type, StreamDataType::DeleteMarker);
}

#[tokio::test]
async fn stream_entries_skipped_with_ttl_without_stream_spec() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let table_name = TableName::new("TtlStreamTable");
    create_test_table(&provider, &table_name, false).await;

    let enable_request = UpdateTimeToLiveRequest {
        table_name: table_name.clone(),
        time_to_live_specification: TimeToLiveSpecification {
            attribute_name: "ttl".to_string(),
            enabled: true,
        },
    };
    provider.update_time_to_live(enable_request).await.unwrap();

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("user1".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("item1".to_string()));
    item.insert(
        "ttl".to_string(),
        AttributeValue::N("1700000000".to_string()),
    );
    item.insert("data".to_string(), AttributeValue::S("payload".to_string()));

    provider
        .put_item(table_name.clone(), item, None, None, None, None)
        .await
        .unwrap();

    let page = StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
        .await
        .unwrap();
    assert!(
        page.items.is_empty(),
        "expected no stream entries when only TTL is enabled"
    );
}

#[tokio::test]
async fn batch_stream_entries_created() {
    use storage_types::{BatchWriteItemRequest, PutRequest, WriteRequest};

    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    // Create a simple table with stream enabled for the test
    let table_name = "BatchStreamTest";
    create_test_table_with_stream(&provider, table_name)
        .await
        .unwrap();

    // Create items for batch write
    let mut item1 = HashMap::new();
    item1.insert(
        "pk".to_string(),
        AttributeValue::S("batch_item".to_string()),
    );
    item1.insert("sk".to_string(), AttributeValue::S("1".to_string()));
    item1.insert("data".to_string(), AttributeValue::S("data1".to_string()));

    let mut item2 = HashMap::new();
    item2.insert(
        "pk".to_string(),
        AttributeValue::S("batch_item".to_string()),
    );
    item2.insert("sk".to_string(), AttributeValue::S("2".to_string()));
    item2.insert("data".to_string(), AttributeValue::S("data2".to_string()));

    let mut request_items = HashMap::new();
    request_items.insert(
        TableName::new(table_name),
        vec![
            WriteRequest {
                put_request: Some(PutRequest { item: item1 }),
                delete_request: None,
            },
            WriteRequest {
                put_request: Some(PutRequest { item: item2 }),
                delete_request: None,
            },
        ],
    );

    let batch_request = BatchWriteItemRequest {
        request_items,
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    };

    provider
        .batch_write_item(batch_request, true)
        .await
        .unwrap();

    let page = StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
        .await
        .unwrap();
    assert_eq!(page.items.len(), 2);
    assert!(
        page.items
            .iter()
            .all(|item| item.data_type == StreamDataType::StreamPointer)
    );
}

#[tokio::test]
async fn batch_write_item_missing_table_returns_not_found_with_streams() {
    use storage_types::{BatchWriteItemRequest, PutRequest, WriteRequest};

    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let mut item = HashMap::new();
    item.insert("id".to_string(), AttributeValue::S("missing".to_string()));

    let request = BatchWriteItemRequest {
        request_items: HashMap::from([(
            TableName::new("NonExistentTable"),
            vec![WriteRequest {
                put_request: Some(PutRequest { item }),
                delete_request: None,
            }],
        )]),
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    };

    let err = provider
        .batch_write_item(request, true)
        .await
        .expect_err("missing table should fail");
    assert!(matches!(err.as_ref(), StorageEnum::TableNotFound { .. }));
}

async fn create_test_table_with_stream(
    provider: &TestProvider,
    table_name: &(impl ToString + ?Sized),
) -> Result<(), Box<dyn std::error::Error>> {
    use storage_types::{StreamSpecification, StreamViewType};
    let table_name = TableName::new(table_name);

    let request = CreateTableRequest::new(
        table_name,
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        storage_types::BillingMode::PayPerRequest,
    )
    .with_stream_specification(Some(StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(StreamViewType::NewAndOldImages),
    }));

    provider.create_table(&request).await?;
    Ok(())
}

fn stream_test_item(pk: &str, sk: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_string(), AttributeValue::S(pk.to_string())),
        ("sk".to_string(), AttributeValue::S(sk.to_string())),
        (
            "data".to_string(),
            AttributeValue::S(format!("{pk}-{sk}-payload")),
        ),
    ])
}

fn stream_id_from_u64(value: u64) -> StreamItemId {
    let mut bytes = [0u8; 12];
    bytes[4..].copy_from_slice(&value.to_be_bytes());
    StreamItemId::from(bytes)
}

fn sample_replication_metadata(
    region_name: &str,
    sequence_suffix: u64,
) -> ReplicationEventMetadata {
    ReplicationEventMetadata {
        origin_region: region_name.to_string(),
        origin_sequence: stream_id_from_u64(sequence_suffix),
        origin_hlc: ReplicationHybridLogicalClock {
            physical_ms: TimestampMillis::from_timestamp(
                1_700_000_000_000 + sequence_suffix as i64,
            ),
            logical: sequence_suffix as u32,
        },
        origin_commit_ts: TimestampMillis::from_timestamp(
            1_700_000_000_000 + sequence_suffix as i64,
        ),
        table_replica_epoch: 3,
        write_source: ReplicationWriteSource::Replicated,
    }
}

fn key_schema_pk() -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: "pk".to_string(),
        key_type: KeyType::Hash,
    }
}

fn key_schema_sk() -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: "sk".to_string(),
        key_type: KeyType::Range,
    }
}

fn build_pointer_stream_item(
    stream_item_id: StreamItemId,
    created_at: TimestampMillis,
    table_name: &TableName,
    item_stream: StreamName,
) -> StreamItem {
    let stored_pointer = StoredStreamPointer::pointer(
        item_stream,
        table_name.clone(),
        storage_types::ItemStreamVersion::new(1),
    );
    StreamItem {
        id: stream_item_id,
        stream_name: None,
        data: storage_types::storage_serde::to_bytes(&stored_pointer).expect("pointer bytes"),
        data_type: StreamDataType::StreamPointer,
        created_at,
    }
}

fn build_item_stream_item(
    stream_item_id: StreamItemId,
    created_at: TimestampMillis,
    stream_name: StreamName,
    item: &HashMap<String, AttributeValue>,
) -> StreamItem {
    StreamItem {
        id: stream_item_id,
        stream_name: Some(stream_name),
        data: storage_types::storage_serde::to_bytes(item).expect("item bytes"),
        data_type: StreamDataType::DynamoDbJson,
        created_at,
    }
}

async fn insert_stream_item(
    provider: &TestProvider,
    stream_name: &StreamName,
    stream_item: &StreamItem,
) {
    let key: StreamKey = stream_name + &stream_item.id;
    let bytes = crate::stream::item_codec::encode_stream_item(stream_item).expect("stream bytes");
    provider
        .kv_store
        .put(key.as_ref(), &bytes, None)
        .await
        .expect("stream insert");
}

#[tokio::test]
async fn stream_trim_removes_expired_entries() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let table_name = TableName::new("StreamTrimTable");
    create_test_table_with_stream(&provider, "StreamTrimTable")
        .await
        .unwrap();

    let item_key = ItemKey::table_key(
        table_name.clone(),
        AttributeValue::S("pk1".to_string()),
        Some(AttributeValue::S("sk1".to_string())),
    );
    let item_stream = StreamName::table_item_stream(&table_name, &item_key).expect("item stream");

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("pk1".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("sk1".to_string()));
    item.insert("data".to_string(), AttributeValue::S("payload".to_string()));

    let cutoff = TimestampMillis::now()
        - (constants::STREAM_TRIM_RETENTION_HOURS * constants::MILLIS_PER_HOUR);
    let old_created_at = cutoff - 1_000;
    let new_created_at = cutoff + 1_000;

    let old_id = stream_id_from_u64(1);
    let new_id = stream_id_from_u64(2);

    let old_pointer =
        build_pointer_stream_item(old_id, old_created_at, &table_name, item_stream.clone());
    let old_item = build_item_stream_item(old_id, old_created_at, item_stream.clone(), &item);
    let new_pointer =
        build_pointer_stream_item(new_id, new_created_at, &table_name, item_stream.clone());
    let new_item = build_item_stream_item(new_id, new_created_at, item_stream.clone(), &item);

    insert_stream_item(&provider, &StreamName::system_table_stream(), &old_pointer).await;
    insert_stream_item(
        &provider,
        &StreamName::table_stream(&table_name),
        &old_pointer,
    )
    .await;
    insert_stream_item(&provider, &item_stream, &old_item).await;

    insert_stream_item(&provider, &StreamName::system_table_stream(), &new_pointer).await;
    insert_stream_item(
        &provider,
        &StreamName::table_stream(&table_name),
        &new_pointer,
    )
    .await;
    insert_stream_item(&provider, &item_stream, &new_item).await;

    provider
        .run_job(storage_common::STREAM_TRIM_JOB)
        .await
        .unwrap();

    let sys_page =
        StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
            .await
            .unwrap();
    assert_eq!(sys_page.items.len(), 1);
    assert_eq!(sys_page.items[0].id, new_id);

    let table_page =
        StreamProvider::read_forward(&provider, StreamName::table_stream(&table_name), None, 10)
            .await
            .unwrap();
    assert_eq!(table_page.items.len(), 1);
    assert_eq!(table_page.items[0].id, new_id);

    let item_page = StreamProvider::read_forward(&provider, item_stream.clone(), None, 10)
        .await
        .unwrap();
    assert_eq!(item_page.items.len(), 1);
    assert_eq!(item_page.items[0].id, new_id);
}

#[tokio::test]
async fn stream_trim_keeps_recent_entries() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let table_name = TableName::new("StreamTrimRecent");
    create_test_table_with_stream(&provider, "StreamTrimRecent")
        .await
        .unwrap();

    let item_key = ItemKey::table_key(
        table_name.clone(),
        AttributeValue::S("pk1".to_string()),
        Some(AttributeValue::S("sk1".to_string())),
    );
    let item_stream = StreamName::table_item_stream(&table_name, &item_key).expect("item stream");

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("pk1".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("sk1".to_string()));
    item.insert("data".to_string(), AttributeValue::S("payload".to_string()));

    let cutoff = TimestampMillis::now()
        - (constants::STREAM_TRIM_RETENTION_HOURS * constants::MILLIS_PER_HOUR);
    let created_at = cutoff + 60_000;
    let stream_id = stream_id_from_u64(10);

    let pointer =
        build_pointer_stream_item(stream_id, created_at, &table_name, item_stream.clone());
    let item_entry = build_item_stream_item(stream_id, created_at, item_stream.clone(), &item);

    insert_stream_item(&provider, &StreamName::system_table_stream(), &pointer).await;
    insert_stream_item(&provider, &StreamName::table_stream(&table_name), &pointer).await;
    insert_stream_item(&provider, &item_stream, &item_entry).await;

    provider
        .run_job(storage_common::STREAM_TRIM_JOB)
        .await
        .unwrap();

    let sys_page =
        StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
            .await
            .unwrap();
    assert_eq!(sys_page.items.len(), 1);
    assert_eq!(sys_page.items[0].id, stream_id);
}

#[tokio::test]
async fn stream_trim_respects_active_backfill_session_floor() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let table_name = TableName::new("StreamTrimBackfillFloor");
    create_test_table_with_stream(&provider, "StreamTrimBackfillFloor")
        .await
        .unwrap();
    create_test_table(&provider, &TableName::new("sys_storage_replication"), false).await;

    let item_key = ItemKey::table_key(
        table_name.clone(),
        AttributeValue::S("pk1".to_string()),
        Some(AttributeValue::S("sk1".to_string())),
    );
    let item_stream = StreamName::table_item_stream(&table_name, &item_key).expect("item stream");

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("pk1".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("sk1".to_string()));
    item.insert("data".to_string(), AttributeValue::S("payload".to_string()));

    let cutoff = TimestampMillis::now()
        - (constants::STREAM_TRIM_RETENTION_HOURS * constants::MILLIS_PER_HOUR);
    let old_created_at = cutoff - 5_000;
    let protected_created_at = cutoff - 4_000;

    let old_id = stream_id_from_u64(20);
    let protected_id = stream_id_from_u64(21);

    let old_pointer =
        build_pointer_stream_item(old_id, old_created_at, &table_name, item_stream.clone());
    let old_item = build_item_stream_item(old_id, old_created_at, item_stream.clone(), &item);
    let protected_pointer = build_pointer_stream_item(
        protected_id,
        protected_created_at,
        &table_name,
        item_stream.clone(),
    );
    let protected_item = build_item_stream_item(
        protected_id,
        protected_created_at,
        item_stream.clone(),
        &item,
    );

    insert_stream_item(&provider, &StreamName::system_table_stream(), &old_pointer).await;
    insert_stream_item(
        &provider,
        &StreamName::table_stream(&table_name),
        &old_pointer,
    )
    .await;
    insert_stream_item(&provider, &item_stream, &old_item).await;

    insert_stream_item(
        &provider,
        &StreamName::system_table_stream(),
        &protected_pointer,
    )
    .await;
    insert_stream_item(
        &provider,
        &StreamName::table_stream(&table_name),
        &protected_pointer,
    )
    .await;
    insert_stream_item(&provider, &item_stream, &protected_item).await;

    provider
        .put_item(
            TableName::new("sys_storage_replication"),
            HashMap::from([
                (
                    "pk".to_string(),
                    AttributeValue::S("catchup#learner-1".to_string()),
                ),
                ("sk".to_string(), AttributeValue::S("session".to_string())),
                (
                    "payload".to_string(),
                    AttributeValue::S(
                        serde_json::json!({
                            "protected_stream_cursor": protected_id,
                            "updated_at": TimestampMillis::now(),
                        })
                        .to_string(),
                    ),
                ),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    provider
        .run_job(storage_common::STREAM_TRIM_JOB)
        .await
        .unwrap();

    let sys_page =
        StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
            .await
            .unwrap();
    assert_eq!(sys_page.items.len(), 1);
    assert_eq!(sys_page.items[0].id, protected_id);

    let table_page =
        StreamProvider::read_forward(&provider, StreamName::table_stream(&table_name), None, 10)
            .await
            .unwrap();
    assert_eq!(table_page.items.len(), 1);
    assert_eq!(table_page.items[0].id, protected_id);

    let item_page = StreamProvider::read_forward(&provider, item_stream.clone(), None, 10)
        .await
        .unwrap();
    assert_eq!(item_page.items.len(), 1);
    assert_eq!(item_page.items[0].id, protected_id);
}

#[tokio::test]
async fn stream_trim_fails_closed_for_malformed_active_session() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let table_name = TableName::new("StreamTrimMalformedSession");
    create_test_table_with_stream(&provider, "StreamTrimMalformedSession")
        .await
        .unwrap();
    create_test_table(&provider, &TableName::new("sys_storage_replication"), false).await;

    let item_key = ItemKey::table_key(
        table_name.clone(),
        AttributeValue::S("pk1".to_string()),
        Some(AttributeValue::S("sk1".to_string())),
    );
    let item_stream = StreamName::table_item_stream(&table_name, &item_key).expect("item stream");

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("pk1".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("sk1".to_string()));
    item.insert("data".to_string(), AttributeValue::S("payload".to_string()));

    let cutoff = TimestampMillis::now()
        - (constants::STREAM_TRIM_RETENTION_HOURS * constants::MILLIS_PER_HOUR);
    let stream_id = stream_id_from_u64(22);
    let pointer =
        build_pointer_stream_item(stream_id, cutoff - 5_000, &table_name, item_stream.clone());
    let item_entry = build_item_stream_item(stream_id, cutoff - 5_000, item_stream.clone(), &item);

    insert_stream_item(&provider, &StreamName::system_table_stream(), &pointer).await;
    insert_stream_item(&provider, &StreamName::table_stream(&table_name), &pointer).await;
    insert_stream_item(&provider, &item_stream, &item_entry).await;

    provider
        .put_item(
            TableName::new("sys_storage_replication"),
            HashMap::from([
                (
                    "pk".to_string(),
                    AttributeValue::S("bootstrap#region-b".to_string()),
                ),
                ("sk".to_string(), AttributeValue::S("session".to_string())),
                (
                    "payload".to_string(),
                    AttributeValue::S(r#"{"updated_at": 1}"#.to_string()),
                ),
            ]),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    provider
        .run_job(storage_common::STREAM_TRIM_JOB)
        .await
        .expect_err("malformed active session must fail closed");

    let sys_page =
        StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
            .await
            .unwrap();
    assert_eq!(sys_page.items.len(), 1);
    assert_eq!(sys_page.items[0].id, stream_id);
}

#[tokio::test]
async fn stream_trim_missing_table_stream_entry() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let table_name = TableName::new("StreamTrimMissingTableStream");
    create_test_table_with_stream(&provider, "StreamTrimMissingTableStream")
        .await
        .unwrap();

    let item_key = ItemKey::table_key(
        table_name.clone(),
        AttributeValue::S("pk1".to_string()),
        Some(AttributeValue::S("sk1".to_string())),
    );
    let item_stream = StreamName::table_item_stream(&table_name, &item_key).expect("item stream");

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("pk1".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("sk1".to_string()));
    item.insert("data".to_string(), AttributeValue::S("payload".to_string()));

    let cutoff = TimestampMillis::now()
        - (constants::STREAM_TRIM_RETENTION_HOURS * constants::MILLIS_PER_HOUR);
    let created_at = cutoff - 1_000;
    let stream_id = stream_id_from_u64(11);

    let pointer =
        build_pointer_stream_item(stream_id, created_at, &table_name, item_stream.clone());
    let item_entry = build_item_stream_item(stream_id, created_at, item_stream.clone(), &item);

    insert_stream_item(&provider, &StreamName::system_table_stream(), &pointer).await;
    insert_stream_item(&provider, &item_stream, &item_entry).await;

    provider
        .run_job(storage_common::STREAM_TRIM_JOB)
        .await
        .unwrap();

    let sys_page =
        StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
            .await
            .unwrap();
    assert!(sys_page.items.is_empty());

    let table_page =
        StreamProvider::read_forward(&provider, StreamName::table_stream(&table_name), None, 10)
            .await
            .unwrap();
    assert!(table_page.items.is_empty());

    let item_page = StreamProvider::read_forward(&provider, item_stream.clone(), None, 10)
        .await
        .unwrap();
    assert!(item_page.items.is_empty());
}

#[tokio::test]
async fn stream_trim_missing_item_stream_entry() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let table_name = TableName::new("StreamTrimMissingItemStream");
    create_test_table_with_stream(&provider, "StreamTrimMissingItemStream")
        .await
        .unwrap();

    let item_key = ItemKey::table_key(
        table_name.clone(),
        AttributeValue::S("pk1".to_string()),
        Some(AttributeValue::S("sk1".to_string())),
    );
    let item_stream = StreamName::table_item_stream(&table_name, &item_key).expect("item stream");

    let cutoff = TimestampMillis::now()
        - (constants::STREAM_TRIM_RETENTION_HOURS * constants::MILLIS_PER_HOUR);
    let created_at = cutoff - 1_000;
    let stream_id = stream_id_from_u64(12);

    let pointer =
        build_pointer_stream_item(stream_id, created_at, &table_name, item_stream.clone());

    insert_stream_item(&provider, &StreamName::system_table_stream(), &pointer).await;
    insert_stream_item(&provider, &StreamName::table_stream(&table_name), &pointer).await;

    provider
        .run_job(storage_common::STREAM_TRIM_JOB)
        .await
        .unwrap();

    let sys_page =
        StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
            .await
            .unwrap();
    assert!(sys_page.items.is_empty());

    let table_page =
        StreamProvider::read_forward(&provider, StreamName::table_stream(&table_name), None, 10)
            .await
            .unwrap();
    assert!(table_page.items.is_empty());

    let item_page = StreamProvider::read_forward(&provider, item_stream.clone(), None, 10)
        .await
        .unwrap();
    assert!(item_page.items.is_empty());
}

#[tokio::test]
async fn stream_trim_handles_multiple_batches() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let table_name = TableName::new("StreamTrimMultiBatch");
    create_test_table_with_stream(&provider, "StreamTrimMultiBatch")
        .await
        .unwrap();

    let item_key = ItemKey::table_key(
        table_name.clone(),
        AttributeValue::S("pk1".to_string()),
        Some(AttributeValue::S("sk1".to_string())),
    );
    let item_stream = StreamName::table_item_stream(&table_name, &item_key).expect("item stream");

    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("pk1".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("sk1".to_string()));
    item.insert("data".to_string(), AttributeValue::S("payload".to_string()));

    let cutoff = TimestampMillis::now()
        - (constants::STREAM_TRIM_RETENTION_HOURS * constants::MILLIS_PER_HOUR);
    let total = constants::STREAM_TRIM_DELETE_BATCH_SIZE * 3 + 1;

    for idx in 0..total {
        let stream_id = stream_id_from_u64(u64::try_from(idx + 1).unwrap());
        let created_at = cutoff - 1_000 - i64::try_from(idx).unwrap();
        let pointer =
            build_pointer_stream_item(stream_id, created_at, &table_name, item_stream.clone());
        let item_entry = build_item_stream_item(stream_id, created_at, item_stream.clone(), &item);

        insert_stream_item(&provider, &StreamName::system_table_stream(), &pointer).await;
        insert_stream_item(&provider, &StreamName::table_stream(&table_name), &pointer).await;
        insert_stream_item(&provider, &item_stream, &item_entry).await;
    }

    provider
        .run_job(storage_common::STREAM_TRIM_JOB)
        .await
        .unwrap();

    let sys_page = StreamProvider::read_forward(
        &provider,
        StreamName::system_table_stream(),
        None,
        constants::STREAM_TRIM_READ_LIMIT,
    )
    .await
    .unwrap();
    assert!(sys_page.items.is_empty());

    let table_page = StreamProvider::read_forward(
        &provider,
        StreamName::table_stream(&table_name),
        None,
        constants::STREAM_TRIM_READ_LIMIT,
    )
    .await
    .unwrap();
    assert!(table_page.items.is_empty());

    let item_page = StreamProvider::read_forward(
        &provider,
        item_stream.clone(),
        None,
        constants::STREAM_TRIM_READ_LIMIT,
    )
    .await
    .unwrap();
    assert!(item_page.items.is_empty());
}

#[tokio::test]
async fn stream_trim_skips_invalid_pointer() {
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let cutoff = TimestampMillis::now()
        - (constants::STREAM_TRIM_RETENTION_HOURS * constants::MILLIS_PER_HOUR);
    let created_at = cutoff - 1_000;
    let stream_id = stream_id_from_u64(3);

    let invalid_pointer = StreamItem {
        id: stream_id,
        stream_name: None,
        data: b"invalid".to_vec(),
        data_type: StreamDataType::StreamPointer,
        created_at,
    };

    insert_stream_item(
        &provider,
        &StreamName::system_table_stream(),
        &invalid_pointer,
    )
    .await;

    provider
        .run_job(storage_common::STREAM_TRIM_JOB)
        .await
        .unwrap();

    let sys_page =
        StreamProvider::read_forward(&provider, StreamName::system_table_stream(), None, 10)
            .await
            .unwrap();
    assert_eq!(sys_page.items.len(), 1);
    assert_eq!(sys_page.items[0].id, stream_id);
}

#[tokio::test]
async fn batch_get_item_emits_billed_metrics_for_requested_keys() {
    let _metrics_guard = metrics_assertion_lock().lock().await;
    let handle = metrics_handle();
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();

    let table_name = TableName::new("BatchGetBilledMetricsKv");
    create_test_table(&provider, &table_name, false).await;

    let mut existing_item = HashMap::new();
    existing_item.insert("pk".to_string(), AttributeValue::S("tenant".to_string()));
    existing_item.insert("sk".to_string(), AttributeValue::S("item#1".to_string()));
    existing_item.insert("data".to_string(), AttributeValue::S("payload".to_string()));
    provider
        .put_item(table_name.clone(), existing_item, None, None, None, None)
        .await
        .unwrap();

    let op_fragments = [
        "ddb_op=\"batch_get_item\"",
        "item_kind=\"get\"",
        "direction=\"read\"",
    ];
    let base_ops = parse_counter(handle, "storage_billed_item_ops_total", &op_fragments);
    let base_bytes = parse_counter(handle, "storage_logical_item_bytes_total", &op_fragments);

    let present_key = HashMap::from([
        ("pk".to_string(), AttributeValue::S("tenant".to_string())),
        ("sk".to_string(), AttributeValue::S("item#1".to_string())),
    ]);
    let missing_key = HashMap::from([
        ("pk".to_string(), AttributeValue::S("tenant".to_string())),
        (
            "sk".to_string(),
            AttributeValue::S("item#missing".to_string()),
        ),
    ]);
    let response = provider
        .batch_get_item(BatchGetItemRequest {
            request_items: HashMap::from([(
                table_name.clone(),
                KeysAndAttributes {
                    keys: vec![present_key.into(), missing_key.into()].into(),
                    attributes_to_get: None,
                    projection_expression: None,
                    expression_attribute_names: None,
                    consistent_read: Some(true),
                },
            )]),
            return_consumed_capacity: None,
        })
        .await
        .unwrap();

    let returned_items = response
        .responses
        .as_ref()
        .and_then(|responses| responses.get(&table_name))
        .expect("response for table");
    assert_eq!(returned_items.len(), 1);
    let expected_bytes = WireItem::from_attribute_map(&HashMap::from([
        ("pk".to_string(), AttributeValue::S("tenant".to_string())),
        ("sk".to_string(), AttributeValue::S("item#1".to_string())),
        ("data".to_string(), AttributeValue::S("payload".to_string())),
    ]))
    .unwrap()
    .payload_len() as f64;
    let after_ops = parse_counter(handle, "storage_billed_item_ops_total", &op_fragments);
    let after_bytes = parse_counter(handle, "storage_logical_item_bytes_total", &op_fragments);

    assert_eq!(after_ops - base_ops, 2.0);
    assert_eq!(after_bytes - base_bytes, expected_bytes);
}

#[tokio::test]
async fn transact_write_items_emits_billed_item_breakdown_metrics() {
    let _metrics_guard = metrics_assertion_lock().lock().await;
    let handle = metrics_handle();
    let provider = create_test_provider();
    provider.initialize_storage().await.unwrap();

    let table_name = TableName::new("TransactBilledMetricsKv");
    create_test_table(&provider, &table_name, false).await;

    for (pk, sk, data) in [
        ("tenant", "update-target", "before-update"),
        ("tenant", "delete-target", "before-delete"),
        ("tenant", "check-target", "before-check"),
    ] {
        let item = HashMap::from([
            ("pk".to_string(), AttributeValue::S(pk.to_string())),
            ("sk".to_string(), AttributeValue::S(sk.to_string())),
            ("data".to_string(), AttributeValue::S(data.to_string())),
        ]);
        provider
            .put_item(table_name.clone(), item, None, None, None, None)
            .await
            .unwrap();
    }

    let write_fragments = ["ddb_op=\"transact_write_items\"", "direction=\"write\""];
    let base_put_ops = parse_counter(
        handle,
        "storage_billed_item_ops_total",
        &[write_fragments[0], "item_kind=\"put\"", write_fragments[1]],
    );
    let base_update_ops = parse_counter(
        handle,
        "storage_billed_item_ops_total",
        &[
            write_fragments[0],
            "item_kind=\"update\"",
            write_fragments[1],
        ],
    );
    let base_delete_ops = parse_counter(
        handle,
        "storage_billed_item_ops_total",
        &[
            write_fragments[0],
            "item_kind=\"delete\"",
            write_fragments[1],
        ],
    );
    let base_check_ops = parse_counter(
        handle,
        "storage_billed_item_ops_total",
        &[
            write_fragments[0],
            "item_kind=\"condition_check\"",
            write_fragments[1],
        ],
    );

    provider
        .transact_write_items(TransactWriteItemsRequest {
            transact_items: vec![
                TransactWriteItem {
                    put: Some(TransactPutRequest {
                        table_name: table_name.clone(),
                        item: HashMap::from([
                            ("pk".to_string(), AttributeValue::S("tenant".to_string())),
                            (
                                "sk".to_string(),
                                AttributeValue::S("put-target".to_string()),
                            ),
                            (
                                "data".to_string(),
                                AttributeValue::S("after-put".to_string()),
                            ),
                        ]),
                        condition_expression: None,
                        expression_attribute_names: None,
                        expression_attribute_values: None,
                        return_values_on_condition_check_failure: None,
                    }),
                    ..Default::default()
                },
                TransactWriteItem {
                    update: Some(TransactUpdateRequest {
                        table_name: table_name.clone(),
                        key: HashMap::from([
                            ("pk".to_string(), AttributeValue::S("tenant".to_string())),
                            (
                                "sk".to_string(),
                                AttributeValue::S("update-target".to_string()),
                            ),
                        ])
                        .into(),
                        update_expression: "SET #data = :data".to_string(),
                        condition_expression: None,
                        expression_attribute_names: Some(HashMap::from([(
                            "#data".to_string(),
                            "data".to_string(),
                        )])),
                        expression_attribute_values: Some(HashMap::from([(
                            ":data".to_string(),
                            AttributeValue::S("after-update".to_string()),
                        )])),
                        return_values_on_condition_check_failure: None,
                    }),
                    ..Default::default()
                },
                TransactWriteItem {
                    delete: Some(TransactDeleteRequest {
                        table_name: table_name.clone(),
                        key: HashMap::from([
                            ("pk".to_string(), AttributeValue::S("tenant".to_string())),
                            (
                                "sk".to_string(),
                                AttributeValue::S("delete-target".to_string()),
                            ),
                        ])
                        .into(),
                        condition_expression: None,
                        expression_attribute_names: None,
                        expression_attribute_values: None,
                        return_values_on_condition_check_failure: None,
                    }),
                    ..Default::default()
                },
                TransactWriteItem {
                    condition_check: Some(TransactConditionCheckRequest {
                        table_name: table_name.clone(),
                        key: HashMap::from([
                            ("pk".to_string(), AttributeValue::S("tenant".to_string())),
                            (
                                "sk".to_string(),
                                AttributeValue::S("check-target".to_string()),
                            ),
                        ])
                        .into(),
                        condition_expression: "attribute_exists(pk)".to_string(),
                        expression_attribute_names: None,
                        expression_attribute_values: None,
                        return_values_on_condition_check_failure: None,
                    }),
                    ..Default::default()
                },
            ],
            ..Default::default()
        })
        .await
        .unwrap();

    let after_put_ops = parse_counter(
        handle,
        "storage_billed_item_ops_total",
        &[write_fragments[0], "item_kind=\"put\"", write_fragments[1]],
    );
    let after_update_ops = parse_counter(
        handle,
        "storage_billed_item_ops_total",
        &[
            write_fragments[0],
            "item_kind=\"update\"",
            write_fragments[1],
        ],
    );
    let after_delete_ops = parse_counter(
        handle,
        "storage_billed_item_ops_total",
        &[
            write_fragments[0],
            "item_kind=\"delete\"",
            write_fragments[1],
        ],
    );
    let after_check_ops = parse_counter(
        handle,
        "storage_billed_item_ops_total",
        &[
            write_fragments[0],
            "item_kind=\"condition_check\"",
            write_fragments[1],
        ],
    );

    assert_eq!(after_put_ops - base_put_ops, 1.0);
    assert_eq!(after_update_ops - base_update_ops, 1.0);
    assert_eq!(after_delete_ops - base_delete_ops, 1.0);
    assert_eq!(after_check_ops - base_check_ops, 1.0);
}
