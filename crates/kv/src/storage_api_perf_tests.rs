use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use storage_provider::StorageProvider;
use storage_types::{
    AttributeDefinition, AttributeValue, BatchGetItemRequest, BatchWriteItemRequest, BillingMode,
    CreateGlobalSecondaryIndex, CreateTableRequest, IndexName, KeyAttributeType, KeySchemaElement,
    KeyType, KeysAndAttributes, Projection, ProjectionType, PutRequest, QueryTableRequest,
    StorageError, StorageResult, TableName, TransactPutRequest, TransactWriteItem,
    TransactWriteItemsRequest, UpdateItemRequest, WriteRequest,
};
use tokio::task::JoinSet;

use crate::{FoundationDbKvStore, SortedKvDbStorageProvider};

const CONCURRENCY_LEVELS: &[usize] = &[50, 100, 250, 500, 1000];
const OPS_PER_WORKER: usize = 3;
const READ_SEED_ITEMS: usize = 2_500;
const WRITE_SEED_ITEMS: usize = 4_000;
const BATCH_WIDTH: usize = 4;
const BATCH_WRITE_WIDTH: usize = 4;
const TRANSACT_WIDTH: usize = 3;
const GSI_COUNT: usize = 5;
const UPDATE_THROUGHPUT_WORKERS: usize = 500;
const UPDATE_THROUGHPUT_OPS_PER_WORKER: usize = 20;
const UPDATE_THROUGHPUT_SEED_ITEMS: usize =
    UPDATE_THROUGHPUT_WORKERS * UPDATE_THROUGHPUT_OPS_PER_WORKER;

type FdbProvider = SortedKvDbStorageProvider<FoundationDbKvStore>;

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[ignore = "fast high-concurrency FoundationDB storage API probe; run explicitly with --ignored \
            --nocapture"]
async fn foundationdb_storage_api_fast_loop() -> StorageResult<()> {
    let Some(store) =
        crate::backends::fdb::fdb_support_tests::connect_fdb_store("storage-api-perf").await
    else {
        println!(
            "api_probe provider=foundationdb skipped=true reason=missing_or_unreachable_local_fdb"
        );
        return Ok(());
    };

    let provider = Arc::new(
        SortedKvDbStorageProvider::new(store)
            .with_immediate_gsi_consistency(true)
            .with_database_jobs_enabled(false),
    );
    provider.initialize_storage().await?;

    let table = TableName::new("FdbApiPerf");
    provider.create_table(&create_table_request(&table)).await?;
    seed_read_items(Arc::clone(&provider), &table).await?;
    seed_write_items(Arc::clone(&provider), &table).await?;

    storage_common::provider_perf::reset_provider("foundationdb");

    run_method_probe(Arc::clone(&provider), table.clone(), "get", api_get).await?;
    run_method_probe(Arc::clone(&provider), table.clone(), "query", api_query).await?;
    run_method_probe(
        Arc::clone(&provider),
        table.clone(),
        "batch_get",
        api_batch_get,
    )
    .await?;
    run_method_probe(Arc::clone(&provider), table.clone(), "put", api_put).await?;
    run_method_probe(Arc::clone(&provider), table.clone(), "update", api_update).await?;
    run_method_probe(
        Arc::clone(&provider),
        table.clone(),
        "batch_write",
        api_batch_write,
    )
    .await?;
    run_method_probe(
        Arc::clone(&provider),
        table.clone(),
        "transact_write",
        api_transact_write,
    )
    .await?;
    run_method_probe(Arc::clone(&provider), table, "delete", api_delete).await?;
    print_phase_counters("foundationdb");
    print_phase_counters("storage_provider");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[ignore = "fast FoundationDB update-only storage API probe; run explicitly with --ignored \
            --nocapture"]
async fn foundationdb_update_api_fast_loop() -> StorageResult<()> {
    let Some(store) =
        crate::backends::fdb::fdb_support_tests::connect_fdb_store("storage-api-update-perf").await
    else {
        println!(
            "api_probe provider=foundationdb skipped=true reason=missing_or_unreachable_local_fdb"
        );
        return Ok(());
    };

    let provider = Arc::new(
        SortedKvDbStorageProvider::new(store)
            .with_immediate_gsi_consistency(true)
            .with_database_jobs_enabled(false),
    );
    provider.initialize_storage().await?;

    let table = TableName::new("FdbUpdateApiPerf");
    provider.create_table(&create_table_request(&table)).await?;
    seed_write_items(Arc::clone(&provider), &table).await?;

    storage_common::provider_perf::reset_provider("foundationdb");
    run_method_probe(Arc::clone(&provider), table, "update", api_update).await?;
    print_phase_counters("foundationdb");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[ignore = "longer FoundationDB update throughput probe for profiling; run explicitly with \
            --ignored --nocapture"]
async fn foundationdb_update_throughput_profile_loop() -> StorageResult<()> {
    let Some(store) =
        crate::backends::fdb::fdb_support_tests::connect_fdb_store("storage-api-update-throughput")
            .await
    else {
        println!(
            "api_probe provider=foundationdb skipped=true reason=missing_or_unreachable_local_fdb"
        );
        return Ok(());
    };

    let provider = Arc::new(
        SortedKvDbStorageProvider::new(store)
            .with_immediate_gsi_consistency(true)
            .with_database_jobs_enabled(false),
    );
    provider.initialize_storage().await?;

    let table = TableName::new("FdbUpdateThroughputPerf");
    provider.create_table(&create_table_request(&table)).await?;
    seed_write_items_count(Arc::clone(&provider), &table, UPDATE_THROUGHPUT_SEED_ITEMS).await?;

    storage_common::provider_perf::reset_provider("foundationdb");
    let contended_report = run_concurrent(
        UPDATE_THROUGHPUT_WORKERS,
        UPDATE_THROUGHPUT_OPS_PER_WORKER,
        {
            let provider = Arc::clone(&provider);
            let table = table.clone();
            move |worker, offset| {
                let provider = Arc::clone(&provider);
                let table = table.clone();
                async move {
                    let id = worker * UPDATE_THROUGHPUT_OPS_PER_WORKER + offset;
                    api_update_in_keyspace(provider, table, id, WRITE_SEED_ITEMS).await
                }
            }
        },
    )
    .await?;
    print_report(
        "update_throughput_contended",
        UPDATE_THROUGHPUT_WORKERS,
        &contended_report,
    );
    print_phase_counters("foundationdb");

    storage_common::provider_perf::reset_provider("foundationdb");
    let wide_report = run_concurrent(
        UPDATE_THROUGHPUT_WORKERS,
        UPDATE_THROUGHPUT_OPS_PER_WORKER,
        {
            let provider = Arc::clone(&provider);
            let table = table.clone();
            move |worker, offset| {
                let provider = Arc::clone(&provider);
                let table = table.clone();
                async move {
                    let id = worker * UPDATE_THROUGHPUT_OPS_PER_WORKER + offset;
                    api_update_in_keyspace(provider, table, id, UPDATE_THROUGHPUT_SEED_ITEMS).await
                }
            }
        },
    )
    .await?;
    print_report(
        "update_throughput_wide",
        UPDATE_THROUGHPUT_WORKERS,
        &wide_report,
    );
    print_phase_counters("foundationdb");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[ignore = "projection-aware FoundationDB GSI write-pressure probe; run explicitly with --ignored \
            --nocapture"]
async fn foundationdb_projection_gsi_fast_loop() -> StorageResult<()> {
    let Some(store) =
        crate::backends::fdb::fdb_support_tests::connect_fdb_store("storage-api-projection-perf")
            .await
    else {
        println!(
            "api_probe provider=foundationdb skipped=true reason=missing_or_unreachable_local_fdb"
        );
        return Ok(());
    };

    let provider = Arc::new(
        SortedKvDbStorageProvider::new(store)
            .with_immediate_gsi_consistency(true)
            .with_database_jobs_enabled(false),
    );
    provider.initialize_storage().await?;

    let table = TableName::new("FdbProjectionApiPerf");
    provider
        .create_table(&create_projection_table_request(&table))
        .await?;
    seed_write_items(Arc::clone(&provider), &table).await?;

    storage_common::provider_perf::reset_provider("foundationdb");
    run_method_probe(
        Arc::clone(&provider),
        table,
        "projection_update",
        api_update,
    )
    .await?;
    print_phase_counters("foundationdb");

    Ok(())
}

fn create_table_request(table: &TableName) -> CreateTableRequest {
    create_table_request_with_projection(table, |_| Projection {
        projection_type: Some(ProjectionType::All),
        non_key_attributes: None,
    })
}

fn create_projection_table_request(table: &TableName) -> CreateTableRequest {
    create_table_request_with_projection(table, |index| match index {
        0 | 1 => Projection {
            projection_type: Some(ProjectionType::KeysOnly),
            non_key_attributes: None,
        },
        _ => Projection {
            projection_type: Some(ProjectionType::Include),
            non_key_attributes: Some(vec!["included".to_string()]),
        },
    })
}

fn create_table_request_with_projection(
    table: &TableName,
    projection_for_index: impl Fn(usize) -> Projection,
) -> CreateTableRequest {
    let mut attributes = vec![
        AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::S,
        },
        AttributeDefinition {
            attribute_name: "sk".to_string(),
            attribute_type: KeyAttributeType::S,
        },
    ];
    let mut global_secondary_indexes = Vec::with_capacity(GSI_COUNT);

    for index in 0..GSI_COUNT {
        let gsi_pk = format!("gsi{index}_pk");
        let gsi_sk = format!("gsi{index}_sk");
        attributes.push(AttributeDefinition {
            attribute_name: gsi_pk.clone(),
            attribute_type: KeyAttributeType::S,
        });
        attributes.push(AttributeDefinition {
            attribute_name: gsi_sk.clone(),
            attribute_type: KeyAttributeType::S,
        });
        global_secondary_indexes.push(CreateGlobalSecondaryIndex {
            index_name: IndexName::new(&format!("gsi{index}")),
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: gsi_pk,
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: gsi_sk,
                    key_type: KeyType::Range,
                },
            ],
            projection: projection_for_index(index),
            provisioned_throughput: None,
        });
    }

    CreateTableRequest::new(
        table.clone(),
        attributes,
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
        BillingMode::PayPerRequest,
    )
    .with_global_secondary_indexes(Some(global_secondary_indexes))
}

async fn seed_read_items(provider: Arc<FdbProvider>, table: &TableName) -> StorageResult<()> {
    let mut tasks = JoinSet::new();
    for id in 0..READ_SEED_ITEMS {
        let provider = Arc::clone(&provider);
        let table = table.clone();
        tasks.spawn(async move {
            provider
                .put_item(table, item_for("read", id), None, None, None, None)
                .await
                .map(|_| ())
        });
    }
    drain_tasks(tasks).await
}

async fn seed_write_items(provider: Arc<FdbProvider>, table: &TableName) -> StorageResult<()> {
    seed_write_items_count(provider, table, WRITE_SEED_ITEMS).await
}

async fn seed_write_items_count(
    provider: Arc<FdbProvider>,
    table: &TableName,
    count: usize,
) -> StorageResult<()> {
    let mut tasks = JoinSet::new();
    for id in 0..count {
        let provider = Arc::clone(&provider);
        let table = table.clone();
        tasks.spawn(async move {
            provider
                .put_item(table, item_for("write", id), None, None, None, None)
                .await
                .map(|_| ())
        });
    }
    drain_tasks(tasks).await
}

async fn run_method_probe<F, Fut>(
    provider: Arc<FdbProvider>,
    table: TableName,
    method: &'static str,
    operation: F,
) -> StorageResult<()>
where
    F: Fn(Arc<FdbProvider>, TableName, usize) -> Fut + Send + Sync + Copy + 'static,
    Fut: std::future::Future<Output = StorageResult<()>> + Send + 'static,
{
    for &workers in CONCURRENCY_LEVELS {
        let report = run_concurrent(workers, OPS_PER_WORKER, {
            let provider = Arc::clone(&provider);
            let table = table.clone();
            move |worker, offset| {
                let provider = Arc::clone(&provider);
                let table = table.clone();
                async move {
                    let id = worker * OPS_PER_WORKER + offset;
                    operation(provider, table, id).await
                }
            }
        })
        .await?;
        print_report(method, workers, &report);
    }
    Ok(())
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
        latencies.extend(result.map_err(|error| {
            StorageError::internal(&format!("api probe task failed: {error}"))
        })??);
    }

    Ok(ProbeReport::new(
        workers * ops_per_worker,
        started.elapsed(),
        latencies,
    ))
}

async fn drain_tasks(mut tasks: JoinSet<StorageResult<()>>) -> StorageResult<()> {
    while let Some(result) = tasks.join_next().await {
        result.map_err(|error| StorageError::internal(&format!("seed task failed: {error}")))??;
    }
    Ok(())
}

fn item_for(prefix: &str, id: usize) -> HashMap<String, AttributeValue> {
    let mut item = key_for(prefix, id);
    item.insert(
        "payload".to_string(),
        AttributeValue::S(format!("payload-{id:020}")),
    );
    item.insert("counter".to_string(), AttributeValue::N("0".to_string()));
    item.insert(
        "included".to_string(),
        AttributeValue::S("stable".to_string()),
    );
    for index in 0..GSI_COUNT {
        item.insert(
            format!("gsi{index}_pk"),
            AttributeValue::S(format!("group-{}", id % 64)),
        );
        item.insert(
            format!("gsi{index}_sk"),
            AttributeValue::S(format!("{prefix}-{id:020}")),
        );
    }
    item
}

fn key_for(prefix: &str, id: usize) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_string(), AttributeValue::S(prefix.to_string())),
        ("sk".to_string(), AttributeValue::S(format!("{id:020}"))),
    ])
}

async fn api_get(provider: Arc<FdbProvider>, table: TableName, id: usize) -> StorageResult<()> {
    provider
        .get_item(table, key_for("read", id % READ_SEED_ITEMS).into(), true)
        .await
        .map(|_| ())
}

async fn api_query(provider: Arc<FdbProvider>, table: TableName, id: usize) -> StorageResult<()> {
    provider
        .query_table(&QueryTableRequest {
            table_name: table,
            index_name: None,
            key_condition_expression: "pk = :pk".to_string(),
            expression_attribute_names: None,
            expression_attribute_values: Some(HashMap::from([(
                ":pk".to_string(),
                AttributeValue::S("read".to_string()),
            )])),
            limit: Some(8 + u32::try_from(id % 8).unwrap_or(0)),
            exclusive_start_key: None,
            scan_index_forward: Some(true),
            consistent_read: true,
        })
        .await
        .map(|_| ())
}

async fn api_batch_get(
    provider: Arc<FdbProvider>,
    table: TableName,
    id: usize,
) -> StorageResult<()> {
    let keys = (0..BATCH_WIDTH)
        .map(|offset| key_for("read", (id * BATCH_WIDTH + offset) % READ_SEED_ITEMS))
        .map(Into::into)
        .collect::<Vec<_>>();
    provider
        .batch_get_item(BatchGetItemRequest {
            request_items: HashMap::from([(
                table,
                KeysAndAttributes {
                    keys: keys.into(),
                    attributes_to_get: None,
                    projection_expression: None,
                    expression_attribute_names: None,
                    consistent_read: Some(true),
                },
            )]),
            return_consumed_capacity: None,
        })
        .await
        .map(|_| ())
}

async fn api_put(provider: Arc<FdbProvider>, table: TableName, id: usize) -> StorageResult<()> {
    provider
        .put_item(table, item_for("put", id), None, None, None, None)
        .await
        .map(|_| ())
}

async fn api_update(provider: Arc<FdbProvider>, table: TableName, id: usize) -> StorageResult<()> {
    api_update_in_keyspace(provider, table, id, WRITE_SEED_ITEMS).await
}

async fn api_update_in_keyspace(
    provider: Arc<FdbProvider>,
    table: TableName,
    id: usize,
    keyspace: usize,
) -> StorageResult<()> {
    let target = id % keyspace;
    provider
        .update_item(UpdateItemRequest {
            table_name: table,
            key: key_for("write", target).into(),
            update_expression: Some("SET payload = :payload, counter = :counter".to_string()),
            attribute_updates: None,
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: Some(HashMap::from([
                (
                    ":payload".to_string(),
                    AttributeValue::S(format!("updated-{id:020}")),
                ),
                (":counter".to_string(), AttributeValue::N(id.to_string())),
            ])),
            expected: None,
            conditional_operator: None,
            return_values: None,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
            return_values_on_condition_check_failure: None,
            aux_item_stream_ttl_hours: None,
        })
        .await
        .map(|_| ())
}

async fn api_batch_write(
    provider: Arc<FdbProvider>,
    table: TableName,
    id: usize,
) -> StorageResult<()> {
    let writes = (0..BATCH_WRITE_WIDTH)
        .map(|offset| WriteRequest {
            put_request: Some(PutRequest {
                item: item_for("batch", id * BATCH_WRITE_WIDTH + offset),
                aux_item_stream_ttl_hours: None,
            }),
            delete_request: None,
        })
        .collect::<Vec<_>>();
    provider
        .batch_write_item(
            BatchWriteItemRequest {
                request_items: HashMap::from([(table, writes)]),
                return_consumed_capacity: None,
                return_item_collection_metrics: None,
            },
            false,
        )
        .await
        .map(|_| ())
}

async fn api_transact_write(
    provider: Arc<FdbProvider>,
    table: TableName,
    id: usize,
) -> StorageResult<()> {
    let transact_items = (0..TRANSACT_WIDTH)
        .map(|offset| TransactWriteItem {
            put: Some(TransactPutRequest {
                table_name: table.clone(),
                item: item_for("transact", id * TRANSACT_WIDTH + offset),
                condition_expression: None,
                expression_attribute_names: None,
                expression_attribute_values: None,
                return_values_on_condition_check_failure: None,
                aux_item_stream_ttl_hours: None,
            }),
            update: None,
            delete: None,
            condition_check: None,
        })
        .collect();
    provider
        .transact_write_items(TransactWriteItemsRequest {
            transact_items,
            client_request_token: None,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
        })
        .await
        .map(|_| ())
}

async fn api_delete(provider: Arc<FdbProvider>, table: TableName, id: usize) -> StorageResult<()> {
    provider
        .delete_item(
            table,
            key_for("write", id % WRITE_SEED_ITEMS).into(),
            None,
            None,
            None,
        )
        .await
        .map(|_| ())
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

fn print_report(method: &str, workers: usize, report: &ProbeReport) {
    println!(
        "api_probe provider=foundationdb method={} workers={} ops={} elapsed_ms={:.1} \
         ops_sec={:.1} p50_ms={:.3} p90_ms={:.3} p99_ms={:.3}",
        method,
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

fn print_phase_counters(provider: &'static str) {
    for counter in storage_common::provider_perf::snapshot_provider(provider) {
        println!(
            "api_probe_phase provider={} name={} calls={} total_ms={:.3} max_ms={:.3} \
             total_amount={} max_amount={}",
            provider,
            counter.name,
            counter.calls,
            millis(counter.total),
            millis(counter.max),
            counter.total_amount,
            counter.max_amount
        );
    }
}
