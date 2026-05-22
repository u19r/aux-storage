use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use tokio::sync::Mutex;

use crate::{
    jitter::jittered,
    worker::{
        DistributedWorker, LeaseResult, LeaseUpdateBuilder, WorkItemProcessor, WorkItemStore,
        WorkerConfig, is_conditional_check_failure,
    },
};

// ============================================================================
// Mock Store
// ============================================================================

#[derive(Debug, Clone)]
struct MockItem {
    id: String,
    status: String,
    lease_until_ms: Option<i64>,
}

struct MockStore {
    items: Arc<Mutex<Vec<MockItem>>>,
    lease_conflict: bool,
}

impl MockStore {
    fn new(items: Vec<MockItem>) -> Self {
        Self {
            items: Arc::new(Mutex::new(items)),
            lease_conflict: false,
        }
    }

    fn with_lease_conflict(mut self) -> Self {
        self.lease_conflict = true;
        self
    }
}

#[derive(Debug, thiserror::Error)]
#[error("mock error: {0}")]
struct MockError(String);

#[async_trait::async_trait]
impl WorkItemStore<MockItem> for MockStore {
    type Error = MockError;

    async fn query_due_items(
        &self,
        _shard: Option<u8>,
        now_ms: i64,
        _limit: u32,
    ) -> Result<Vec<MockItem>, Self::Error> {
        let items = self.items.lock().await;
        Ok(items
            .iter()
            .filter(|item| {
                item.status == "queued" && item.lease_until_ms.is_none_or(|lease| lease < now_ms)
            })
            .cloned()
            .collect())
    }

    async fn acquire_lease(
        &self,
        item: &MockItem,
        _worker_id: &str,
        lease_until_ms: i64,
        _now_ms: i64,
    ) -> Result<LeaseResult, Self::Error> {
        if self.lease_conflict {
            return Ok(LeaseResult::Conflict);
        }

        let mut items = self.items.lock().await;
        if let Some(found) = items.iter_mut().find(|i| i.id == item.id) {
            found.status = "in_flight".to_string();
            found.lease_until_ms = Some(lease_until_ms);
            Ok(LeaseResult::Acquired)
        } else {
            Ok(LeaseResult::Conflict)
        }
    }

    async fn mark_completed(&self, item: &MockItem) -> Result<(), Self::Error> {
        let mut items = self.items.lock().await;
        if let Some(found) = items.iter_mut().find(|i| i.id == item.id) {
            found.status = "completed".to_string();
            found.lease_until_ms = None;
        }
        Ok(())
    }

    async fn mark_failed(&self, item: &MockItem, _error: &str) -> Result<(), Self::Error> {
        let mut items = self.items.lock().await;
        if let Some(found) = items.iter_mut().find(|i| i.id == item.id) {
            found.status = "failed".to_string();
            found.lease_until_ms = None;
        }
        Ok(())
    }
}

// ============================================================================
// Mock Processor
// ============================================================================

struct MockProcessor {
    process_count: AtomicU32,
    should_fail: bool,
}

impl MockProcessor {
    fn new() -> Self {
        Self {
            process_count: AtomicU32::new(0),
            should_fail: false,
        }
    }

    fn failing() -> Self {
        Self {
            process_count: AtomicU32::new(0),
            should_fail: true,
        }
    }

    fn count(&self) -> u32 {
        self.process_count.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl WorkItemProcessor<MockItem> for MockProcessor {
    type Error = MockError;

    async fn process(&self, _item: &MockItem) -> Result<(), Self::Error> {
        self.process_count.fetch_add(1, Ordering::SeqCst);
        if self.should_fail {
            Err(MockError("intentional failure".to_string()))
        } else {
            Ok(())
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[tokio::test]
async fn worker_processes_items() {
    let items = vec![
        MockItem {
            id: "1".to_string(),
            status: "queued".to_string(),
            lease_until_ms: None,
        },
        MockItem {
            id: "2".to_string(),
            status: "queued".to_string(),
            lease_until_ms: None,
        },
    ];

    let store = Arc::new(MockStore::new(items));
    let processor = Arc::new(MockProcessor::new());

    let config = WorkerConfig::new("test-worker")
        .with_lease_duration(Duration::from_secs(60))
        .with_poll_interval(Duration::from_millis(10));

    let worker = DistributedWorker::new(config, store.clone(), processor.clone());

    let processed = worker.run_once().await.unwrap();

    assert_eq!(processed, 2);
    assert_eq!(processor.count(), 2);

    // Verify items are marked completed
    let items = store.items.lock().await;
    assert!(items.iter().all(|i| i.status == "completed"));
}

#[tokio::test]
async fn worker_handles_lease_conflict() {
    let items = vec![MockItem {
        id: "1".to_string(),
        status: "queued".to_string(),
        lease_until_ms: None,
    }];

    let store = Arc::new(MockStore::new(items).with_lease_conflict());
    let processor = Arc::new(MockProcessor::new());

    let config = WorkerConfig::new("test-worker");
    let worker = DistributedWorker::new(config, store, processor.clone());

    let processed = worker.run_once().await.unwrap();

    // Should not process due to lease conflict
    assert_eq!(processed, 0);
    assert_eq!(processor.count(), 0);
}

#[tokio::test]
async fn worker_handles_processing_failure() {
    let items = vec![MockItem {
        id: "1".to_string(),
        status: "queued".to_string(),
        lease_until_ms: None,
    }];

    let store = Arc::new(MockStore::new(items));
    let processor = Arc::new(MockProcessor::failing());

    let config = WorkerConfig::new("test-worker");
    let worker = DistributedWorker::new(config, store.clone(), processor.clone());

    let result = worker.run_once().await;

    // run_once returns Ok even if individual items fail (logs the error)
    // This allows the worker to continue processing other items
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 0); // 0 successfully processed
    assert_eq!(processor.count(), 1); // Processing was attempted before the item was requeued.

    // Item should be marked failed
    let items = store.items.lock().await;
    assert_eq!(items[0].status, "failed");
}

#[tokio::test]
async fn worker_skips_leased_items() {
    let now = chrono::Utc::now().timestamp_millis();
    let items = vec![
        MockItem {
            id: "1".to_string(),
            status: "queued".to_string(),
            lease_until_ms: Some(now + 60_000), // Active lease
        },
        MockItem {
            id: "2".to_string(),
            status: "queued".to_string(),
            lease_until_ms: Some(now - 1000), // Expired lease
        },
    ];

    let store = Arc::new(MockStore::new(items));
    let processor = Arc::new(MockProcessor::new());

    let config = WorkerConfig::new("test-worker");
    let worker = DistributedWorker::new(config, store, processor.clone());

    let processed = worker.run_once().await.unwrap();

    // Should only process item with expired lease
    assert_eq!(processed, 1);
    assert_eq!(processor.count(), 1);
}

#[tokio::test]
async fn worker_run_with_shutdown() {
    let items = vec![MockItem {
        id: "1".to_string(),
        status: "queued".to_string(),
        lease_until_ms: None,
    }];

    let store = Arc::new(MockStore::new(items));
    let processor = Arc::new(MockProcessor::new());

    let config = WorkerConfig::new("test-worker").with_poll_interval(Duration::from_millis(10));

    let worker = DistributedWorker::new(config, store, processor.clone());

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Start worker in background
    let worker_handle = tokio::spawn(async move {
        worker.run(shutdown_rx).await;
    });

    // Give it time to process
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Signal shutdown
    shutdown_tx.send(true).unwrap();

    // Wait for worker to stop
    tokio::time::timeout(Duration::from_secs(1), worker_handle)
        .await
        .expect("Worker should stop within timeout")
        .expect("Worker task should complete");

    // Should have processed the item
    assert!(processor.count() >= 1);
}

// ============================================================================
// Multi-Runner Tests - Simulating Multiple Nodes
// ============================================================================

/// A more realistic mock store that simulates `DynamoDB` conditional writes.
/// It properly tracks which worker holds a lease and rejects concurrent
/// acquires.
struct ConcurrentMockStore {
    items: Arc<Mutex<Vec<ConcurrentMockItem>>>,
    /// Track which items have been processed (for verification)
    processed_items: Arc<Mutex<HashSet<String>>>,
    /// Track how many lease conflicts occurred
    conflict_count: Arc<AtomicU32>,
}

#[derive(Debug, Clone)]
struct ConcurrentMockItem {
    id: String,
    status: String,
    leased_by: Option<String>,
    lease_until_ms: Option<i64>,
}

impl ConcurrentMockStore {
    fn new(item_count: usize) -> Self {
        let items: Vec<_> = (0..item_count)
            .map(|i| ConcurrentMockItem {
                id: format!("item-{i}"),
                status: "queued".to_string(),
                leased_by: None,
                lease_until_ms: None,
            })
            .collect();
        Self {
            items: Arc::new(Mutex::new(items)),
            processed_items: Arc::new(Mutex::new(HashSet::new())),
            conflict_count: Arc::new(AtomicU32::new(0)),
        }
    }

    fn conflicts(&self) -> u32 {
        self.conflict_count.load(Ordering::SeqCst)
    }

    async fn all_completed(&self) -> bool {
        let items = self.items.lock().await;
        items.iter().all(|i| i.status == "completed")
    }
}

#[async_trait::async_trait]
impl WorkItemStore<ConcurrentMockItem> for ConcurrentMockStore {
    type Error = MockError;

    async fn query_due_items(
        &self,
        _shard: Option<u8>,
        now_ms: i64,
        limit: u32,
    ) -> Result<Vec<ConcurrentMockItem>, Self::Error> {
        let items = self.items.lock().await;
        Ok(items
            .iter()
            .filter(|item| {
                item.status == "queued" && item.lease_until_ms.is_none_or(|lease| lease < now_ms)
            })
            .take(limit as usize)
            .cloned()
            .collect())
    }

    async fn acquire_lease(
        &self,
        item: &ConcurrentMockItem,
        worker_id: &str,
        lease_until_ms: i64,
        now_ms: i64,
    ) -> Result<LeaseResult, Self::Error> {
        let mut items = self.items.lock().await;
        if let Some(found) = items.iter_mut().find(|i| i.id == item.id) {
            // Simulate DynamoDB conditional check:
            // Only acquire if no active lease exists
            let can_acquire = match (found.leased_by.as_ref(), found.lease_until_ms) {
                (None, _) => true,
                (Some(_), Some(until)) if until < now_ms => true, // Expired lease
                _ => false,
            };

            if can_acquire && found.status == "queued" {
                found.leased_by = Some(worker_id.to_string());
                found.lease_until_ms = Some(lease_until_ms);
                Ok(LeaseResult::Acquired)
            } else {
                self.conflict_count.fetch_add(1, Ordering::SeqCst);
                Ok(LeaseResult::Conflict)
            }
        } else {
            Ok(LeaseResult::Conflict)
        }
    }

    async fn mark_completed(&self, item: &ConcurrentMockItem) -> Result<(), Self::Error> {
        let mut items = self.items.lock().await;
        if let Some(found) = items.iter_mut().find(|i| i.id == item.id) {
            found.status = "completed".to_string();
            found.leased_by = None;
            found.lease_until_ms = None;
        }
        self.processed_items.lock().await.insert(item.id.clone());
        Ok(())
    }

    async fn mark_failed(
        &self,
        item: &ConcurrentMockItem,
        _error: &str,
    ) -> Result<(), Self::Error> {
        let mut items = self.items.lock().await;
        if let Some(found) = items.iter_mut().find(|i| i.id == item.id) {
            // Release lease so item can be retried
            found.leased_by = None;
            found.lease_until_ms = None;
        }
        Ok(())
    }
}

/// A processor that tracks which items were processed and by which worker.
struct TrackingProcessor {
    worker_id: String,
    processed: Arc<Mutex<Vec<(String, String)>>>, // (item_id, worker_id)
    process_delay: Option<Duration>,
}

impl TrackingProcessor {
    fn new(worker_id: &str, processed: Arc<Mutex<Vec<(String, String)>>>) -> Self {
        Self {
            worker_id: worker_id.to_string(),
            processed,
            process_delay: None,
        }
    }

    fn with_delay(mut self, delay: Duration) -> Self {
        self.process_delay = Some(delay);
        self
    }
}

#[async_trait::async_trait]
impl WorkItemProcessor<ConcurrentMockItem> for TrackingProcessor {
    type Error = MockError;

    async fn process(&self, item: &ConcurrentMockItem) -> Result<(), Self::Error> {
        if let Some(delay) = self.process_delay {
            tokio::time::sleep(delay).await;
        }
        self.processed
            .lock()
            .await
            .push((item.id.clone(), self.worker_id.clone()));
        Ok(())
    }
}

#[tokio::test]
async fn multiple_workers_no_double_processing() {
    // Create 20 items to be processed
    let store = Arc::new(ConcurrentMockStore::new(20));
    let processed_log = Arc::new(Mutex::new(Vec::new()));

    // Spawn 5 workers that all compete for the same items
    let mut handles = Vec::new();
    let mut shutdown_txs = Vec::new();

    for i in 0..5 {
        let store = store.clone();
        let processed = processed_log.clone();
        let worker_id = format!("worker-{i}");

        let processor = Arc::new(TrackingProcessor::new(&worker_id, processed));
        let config = WorkerConfig::new(&worker_id)
            .with_poll_interval(Duration::from_millis(5))
            .with_lease_duration(Duration::from_secs(60))
            .with_batch_size(10);

        let worker = DistributedWorker::new(config, store, processor);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        shutdown_txs.push(shutdown_tx);

        handles.push(tokio::spawn(async move {
            worker.run(shutdown_rx).await;
        }));
    }

    // Wait for all items to be processed
    for _ in 0..100 {
        if store.all_completed().await {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Signal all workers to shut down
    for tx in shutdown_txs {
        tx.send(true).ok();
    }

    // Wait for all workers to finish
    for handle in handles {
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("Worker should stop")
            .expect("Worker should complete");
    }

    // Verify: each item was processed exactly once
    let log = processed_log.lock().await;
    let processed_ids: HashSet<_> = log.iter().map(|(id, _)| id.clone()).collect();

    assert_eq!(processed_ids.len(), 20, "All 20 items should be processed");
    assert_eq!(log.len(), 20, "No item should be processed more than once");

    // Log some stats
    println!("Total conflicts: {}", store.conflicts());
    println!("Items processed: {}", log.len());
}

#[tokio::test]
async fn workers_handle_slow_processing_with_lease_expiry() {
    // Create items that take a while to process
    let store = Arc::new(ConcurrentMockStore::new(5));
    let processed_log = Arc::new(Mutex::new(Vec::new()));

    // Two workers with different processing speeds
    // Worker 1: fast (10ms per item)
    // Worker 2: slow (but should still get some work)
    let mut handles = Vec::new();
    let mut shutdown_txs = Vec::new();

    for (i, delay_ms) in [(0, 10), (1, 20)] {
        let store = store.clone();
        let processed = processed_log.clone();
        let worker_id = format!("worker-{i}");

        let processor = Arc::new(
            TrackingProcessor::new(&worker_id, processed)
                .with_delay(Duration::from_millis(delay_ms)),
        );
        let config = WorkerConfig::new(&worker_id)
            .with_poll_interval(Duration::from_millis(5))
            .with_lease_duration(Duration::from_secs(60))
            .with_batch_size(3);

        let worker = DistributedWorker::new(config, store, processor);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        shutdown_txs.push(shutdown_tx);

        handles.push(tokio::spawn(async move {
            worker.run(shutdown_rx).await;
        }));
    }

    // Wait for all items to be processed
    for _ in 0..200 {
        if store.all_completed().await {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Shutdown
    for tx in shutdown_txs {
        tx.send(true).ok();
    }
    for handle in handles {
        handle.await.ok();
    }

    // Verify no double processing
    let log = processed_log.lock().await;
    let processed_ids: HashSet<_> = log.iter().map(|(id, _)| id.clone()).collect();

    assert_eq!(processed_ids.len(), 5);
    assert_eq!(log.len(), 5, "No double processing should occur");

    // Check work distribution - both workers should have done some work
    let worker0_count = log.iter().filter(|(_, w)| w == "worker-0").count();
    let worker1_count = log.iter().filter(|(_, w)| w == "worker-1").count();
    println!("Worker-0 processed: {worker0_count}, Worker-1 processed: {worker1_count}");

    // At least verify total is correct
    assert_eq!(worker0_count + worker1_count, 5);
}

#[tokio::test]
async fn run_once_multiple_workers_partition_work() {
    // Test that run_once correctly partitions work across multiple workers
    let store = Arc::new(ConcurrentMockStore::new(10));
    let processed_log = Arc::new(Mutex::new(Vec::new()));

    // Create 3 workers
    let workers: Vec<_> = (0..3)
        .map(|i| {
            let worker_id = format!("worker-{i}");
            let processor = Arc::new(TrackingProcessor::new(&worker_id, processed_log.clone()));
            let config = WorkerConfig::new(&worker_id)
                .with_lease_duration(Duration::from_secs(60))
                .with_batch_size(5);
            DistributedWorker::new(config, store.clone(), processor)
        })
        .collect();

    // Run all workers once concurrently
    let handles: Vec<_> = workers
        .into_iter()
        .map(|w| tokio::spawn(async move { w.run_once().await }))
        .collect();

    let mut total_processed = 0;
    for handle in handles {
        let result = handle.await.expect("Task should complete");
        total_processed += result.expect("Should succeed");
    }

    // Verify all items processed exactly once
    assert_eq!(total_processed, 10);

    let log = processed_log.lock().await;
    let unique_ids: HashSet<_> = log.iter().map(|(id, _)| id.clone()).collect();
    assert_eq!(unique_ids.len(), 10, "All items should be processed");
    assert_eq!(log.len(), 10, "No double processing");
}

#[tokio::test]
async fn high_contention_many_workers_few_items() {
    // Stress test: many workers competing for few items
    let store = Arc::new(ConcurrentMockStore::new(3));
    let processed_log = Arc::new(Mutex::new(Vec::new()));

    let mut handles = Vec::new();
    let mut shutdown_txs = Vec::new();

    // 10 workers for 3 items - high contention
    for i in 0..10 {
        let store = store.clone();
        let processed = processed_log.clone();
        let worker_id = format!("worker-{i}");

        let processor = Arc::new(TrackingProcessor::new(&worker_id, processed));
        let config = WorkerConfig::new(&worker_id)
            .with_poll_interval(Duration::from_millis(2))
            .with_lease_duration(Duration::from_secs(60))
            .with_batch_size(5);

        let worker = DistributedWorker::new(config, store, processor);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        shutdown_txs.push(shutdown_tx);

        handles.push(tokio::spawn(async move {
            worker.run(shutdown_rx).await;
        }));
    }

    // Wait for processing
    for _ in 0..100 {
        if store.all_completed().await {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Shutdown
    for tx in shutdown_txs {
        tx.send(true).ok();
    }
    for handle in handles {
        tokio::time::timeout(Duration::from_millis(500), handle)
            .await
            .ok();
    }

    // Verify correctness
    let log = processed_log.lock().await;
    assert_eq!(
        log.len(),
        3,
        "Exactly 3 items should be processed, got {}",
        log.len()
    );

    let unique_ids: HashSet<_> = log.iter().map(|(id, _)| id.clone()).collect();
    assert_eq!(unique_ids.len(), 3);

    println!("High contention test - conflicts: {}", store.conflicts());
}

#[test]
fn default_worker_config() {
    let config = WorkerConfig::default();
    assert_eq!(config.lease_duration, Duration::from_secs(60));
    assert_eq!(config.poll_interval, Duration::from_secs(5));
    assert_eq!(config.batch_size, 50);
    assert_eq!(config.jitter_percent, 20);
    assert!(!config.worker_id.is_empty());
}

#[test]
fn worker_config_builder() {
    let config = WorkerConfig::new("test-worker")
        .with_lease_duration(Duration::from_secs(120))
        .with_poll_interval(Duration::from_secs(10))
        .with_batch_size(100)
        .with_jitter_percent(30);

    assert_eq!(config.worker_id, "test-worker");
    assert_eq!(config.lease_duration, Duration::from_secs(120));
    assert_eq!(config.poll_interval, Duration::from_secs(10));
    assert_eq!(config.batch_size, 100);
    assert_eq!(config.jitter_percent, 30);
}

#[test]
fn lease_until_ms() {
    let config = WorkerConfig::new("test").with_lease_duration(Duration::from_secs(60));
    let now = chrono::Utc::now().timestamp_millis();
    let lease_until = config.lease_until_ms(now);

    // Should be approximately 60 seconds in the future
    assert!(lease_until > now);
    assert!(lease_until <= now + 60_001);
}

#[test]
fn jittered_within_bounds() {
    let base = Duration::from_secs(10);
    let percent = 20u8;

    for _ in 0..100 {
        let result = jittered(base, percent);
        let min = Duration::from_secs(8);
        let max = Duration::from_secs(12);
        assert!(result >= min && result <= max);
    }
}

#[test]
fn jittered_zero_percent() {
    let base = Duration::from_secs(10);
    let result = jittered(base, 0);
    assert_eq!(result, base);
}

#[test]
fn jittered_zero_base() {
    let result = jittered(Duration::ZERO, 50);
    assert_eq!(result, Duration::ZERO);
}

#[test]
fn lease_update_builder() {
    let builder = LeaseUpdateBuilder::new();

    let statement = builder.build_update_statement("worker-1", 1000, 500);
    assert!(statement.update_expression.contains("lease_until_ms"));
    assert!(statement.update_expression.contains("leased_by"));
    assert!(
        statement
            .condition_expression
            .contains("attribute_not_exists")
    );
    assert!(statement.condition_expression.contains("#status"));
    assert_eq!(
        statement.expression_attribute_names.get("#status"),
        Some(&"status".to_string())
    );
    assert!(statement.expression_attribute_values.contains_key(":lease"));
    assert!(
        statement
            .expression_attribute_values
            .contains_key(":worker")
    );
    assert!(statement.expression_attribute_values.contains_key(":now"));
}

#[test]
fn conditional_check_failure_detection_matches_supported_messages() {
    assert!(is_conditional_check_failure(
        "ConditionalCheckFailedException"
    ));
    assert!(is_conditional_check_failure("conditional check failed"));
    assert!(is_conditional_check_failure("ConditionCheck failed"));
    assert!(!is_conditional_check_failure("Some other error"));
}
