use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use storage_types::{StorageError, StorageResult};
use tokio::task::JoinSet;

#[cfg(feature = "rocksdb-backend")]
use crate::kv_support_tests::rocksdb_test_path;
use crate::sorted_kv_store::{BatchItem, DirectWriteOperation, SortedKvStore};

const WARMUP_OPS: usize = 200;
const CONCURRENCY_LEVELS: &[usize] = &[50, 100, 250, 500, 1000];
const OPS_PER_WORKER: usize = 10;
const KEY_SIZE: usize = 48;
const VALUE_SIZE: usize = 512;

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[ignore = "fast high-concurrency KV backend probe; run explicitly with --ignored --nocapture"]
async fn kv_put_high_concurrency_fast_loop() -> StorageResult<()> {
    #[cfg(feature = "rocksdb-backend")]
    {
        let rocksdb = create_rocksdb_store()?;
        run_store_probe("rocksdb", Arc::new(rocksdb.store)).await?;
    }

    #[cfg(feature = "foundationdb-backend")]
    if let Some(fdb) = create_foundationdb_store().await {
        run_store_probe("foundationdb", Arc::new(fdb)).await?;
    }

    Ok(())
}

#[cfg(feature = "rocksdb-backend")]
struct RocksFixture {
    store: crate::RocksDbKvStore,
}

#[cfg(feature = "rocksdb-backend")]
fn create_rocksdb_store() -> StorageResult<RocksFixture> {
    let store = crate::RocksDbKvStore::new(rocksdb_test_path("kv-perf-rocksdb"))?;
    Ok(RocksFixture { store })
}

#[cfg(feature = "foundationdb-backend")]
async fn create_foundationdb_store() -> Option<crate::FoundationDbKvStore> {
    crate::backends::fdb::fdb_support_tests::connect_fdb_store("kv-perf").await
}

async fn run_store_probe<S>(name: &'static str, store: Arc<S>) -> StorageResult<()>
where S: SortedKvStore + 'static {
    run_unchecked_serial_puts(Arc::clone(&store), name, WARMUP_OPS, 0).await?;

    for &workers in CONCURRENCY_LEVELS {
        let unchecked = run_unchecked_concurrent_puts(
            Arc::clone(&store),
            name,
            workers,
            OPS_PER_WORKER,
            1_000_000 + workers * OPS_PER_WORKER,
        )
        .await?;
        print_report(name, "transact_write_unchecked", workers, &unchecked);

        let batch = run_batch_write_concurrent_puts(
            Arc::clone(&store),
            name,
            workers,
            OPS_PER_WORKER,
            2_000_000 + workers * OPS_PER_WORKER,
        )
        .await?;
        print_report(name, "batch_write", workers, &batch);

        let put = run_put_concurrent_puts(
            Arc::clone(&store),
            name,
            workers,
            OPS_PER_WORKER,
            3_000_000 + workers * OPS_PER_WORKER,
        )
        .await?;
        print_report(name, "put", workers, &put);
    }

    Ok(())
}

async fn run_unchecked_serial_puts<S>(
    store: Arc<S>,
    provider: &'static str,
    count: usize,
    start_id: usize,
) -> StorageResult<ProbeReport>
where
    S: SortedKvStore + 'static,
{
    let started = Instant::now();
    let mut latencies = Vec::with_capacity(count);
    for offset in 0..count {
        let operation_started = Instant::now();
        put_one_unchecked(store.as_ref(), provider, start_id + offset).await?;
        latencies.push(operation_started.elapsed());
    }
    Ok(ProbeReport::new(count, started.elapsed(), latencies))
}

async fn run_unchecked_concurrent_puts<S>(
    store: Arc<S>,
    provider: &'static str,
    workers: usize,
    ops_per_worker: usize,
    start_id: usize,
) -> StorageResult<ProbeReport>
where
    S: SortedKvStore + 'static,
{
    run_concurrent(workers, ops_per_worker, move |worker, offset| {
        let store = Arc::clone(&store);
        async move {
            let id = start_id + worker * ops_per_worker + offset;
            put_one_unchecked(store.as_ref(), provider, id).await
        }
    })
    .await
}

async fn run_batch_write_concurrent_puts<S>(
    store: Arc<S>,
    provider: &'static str,
    workers: usize,
    ops_per_worker: usize,
    start_id: usize,
) -> StorageResult<ProbeReport>
where
    S: SortedKvStore + 'static,
{
    run_concurrent(workers, ops_per_worker, move |worker, offset| {
        let store = Arc::clone(&store);
        async move {
            let id = start_id + worker * ops_per_worker + offset;
            put_one_batch(store.as_ref(), provider, id).await
        }
    })
    .await
}

async fn run_put_concurrent_puts<S>(
    store: Arc<S>,
    provider: &'static str,
    workers: usize,
    ops_per_worker: usize,
    start_id: usize,
) -> StorageResult<ProbeReport>
where
    S: SortedKvStore + 'static,
{
    run_concurrent(workers, ops_per_worker, move |worker, offset| {
        let store = Arc::clone(&store);
        async move {
            let id = start_id + worker * ops_per_worker + offset;
            put_one_direct(store.as_ref(), provider, id).await
        }
    })
    .await
}

async fn run_concurrent<F, Fut>(
    workers: usize,
    ops_per_worker: usize,
    operation: F,
) -> StorageResult<ProbeReport>
where
    F: Fn(usize, usize) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = StorageResult<()>> + Send + 'static,
{
    let operation = Arc::new(operation);
    let started = Instant::now();
    let mut tasks = JoinSet::new();
    for worker in 0..workers {
        let operation = Arc::clone(&operation);
        tasks.spawn(async move {
            let mut latencies = Vec::with_capacity(ops_per_worker);
            for offset in 0..ops_per_worker {
                let operation_started = Instant::now();
                operation(worker, offset).await?;
                latencies.push(operation_started.elapsed());
            }
            StorageResult::Ok(latencies)
        });
    }

    let mut latencies = Vec::with_capacity(workers * ops_per_worker);
    while let Some(result) = tasks.join_next().await {
        let worker_latencies = result
            .map_err(|error| StorageError::internal(&format!("probe task failed: {error}")))??;
        latencies.extend(worker_latencies);
    }

    Ok(ProbeReport::new(
        workers * ops_per_worker,
        started.elapsed(),
        latencies,
    ))
}

async fn put_one_unchecked<S>(store: &S, provider: &str, id: usize) -> StorageResult<()>
where S: SortedKvStore {
    let operation = DirectWriteOperation::Put {
        key: shaped_bytes(provider, "unchecked-key", id, KEY_SIZE),
        value: shaped_bytes(provider, "unchecked-value", id, VALUE_SIZE),
    };
    store.transact_write_unchecked(vec![operation]).await
}

async fn put_one_direct<S>(store: &S, provider: &str, id: usize) -> StorageResult<()>
where S: SortedKvStore {
    store
        .put(
            &shaped_bytes(provider, "put-key", id, KEY_SIZE),
            &shaped_bytes(provider, "put-value", id, VALUE_SIZE),
            None,
        )
        .await
}

async fn put_one_batch<S>(store: &S, provider: &str, id: usize) -> StorageResult<()>
where S: SortedKvStore {
    store
        .batch_write(vec![BatchItem {
            key: shaped_bytes(provider, "batch-key", id, KEY_SIZE),
            value: Some(shaped_bytes(provider, "batch-value", id, VALUE_SIZE)),
        }])
        .await
}

fn shaped_bytes(provider: &str, prefix: &str, id: usize, size: usize) -> Vec<u8> {
    let mut value = format!("{provider}/{prefix}/{id:020}").into_bytes();
    while value.len() < size {
        value.push(b'x');
    }
    value.truncate(size);
    value
}

#[derive(Debug)]
struct ProbeReport {
    operations: usize,
    elapsed: Duration,
    p50: Duration,
    p90: Duration,
    p99: Duration,
}

impl ProbeReport {
    fn new(operations: usize, elapsed: Duration, mut latencies: Vec<Duration>) -> Self {
        latencies.sort_unstable();
        Self {
            operations,
            elapsed,
            p50: percentile(&latencies, 50),
            p90: percentile(&latencies, 90),
            p99: percentile(&latencies, 99),
        }
    }

    fn ops_per_second(&self) -> f64 {
        self.operations as f64 / self.elapsed.as_secs_f64().max(0.001)
    }
}

fn percentile(latencies: &[Duration], percentile: usize) -> Duration {
    if latencies.is_empty() {
        return Duration::ZERO;
    }
    let index = ((latencies.len() - 1) * percentile) / 100;
    latencies[index]
}

fn print_report(provider: &str, phase: &str, workers: usize, report: &ProbeReport) {
    println!(
        "kv_probe provider={} phase={} workers={} ops={} elapsed_ms={:.1} ops_sec={:.1} \
         p50_ms={:.3} p90_ms={:.3} p99_ms={:.3}",
        provider,
        phase,
        workers,
        report.operations,
        report.elapsed.as_secs_f64() * 1000.0,
        report.ops_per_second(),
        millis(report.p50),
        millis(report.p90),
        millis(report.p99)
    );
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}
