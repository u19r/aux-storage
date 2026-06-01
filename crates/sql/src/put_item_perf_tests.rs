use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use storage_common::provider_perf::{reset_provider, snapshot_provider};
use storage_provider::{SqliteSettings, StorageProvider};
use storage_types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateGlobalSecondaryIndex,
    CreateTableRequest, IndexName, KeyAttributeType, KeySchemaElement, KeyType, Projection,
    ProjectionType, StorageError, StorageResult, TableName, UpdateItemRequest,
};
use tempfile::TempDir;
use tokio::task::JoinSet;

#[cfg(feature = "postgres-backend")]
use crate::PostgresStorageProvider;
use crate::{
    SQLiteStorageProvider, TursoStorageProvider,
    backends::turso::{reset_turso_statement_counters, turso_statement_counters},
};

const WARMUP_PUTS: u64 = 100;
const SERIAL_PUTS: u64 = 750;
const CONCURRENT_WORKERS: u64 = 64;
const PUTS_PER_WORKER: u64 = 20;
const KEY_SIZE: usize = 40;
const VALUE_SIZE: usize = 512;
const GSI_COUNT: usize = 5;
const SHAPE_SERIAL_PUTS: u64 = 300;
const SHAPE_CONCURRENT_WORKERS: u64 = 32;
const SHAPE_PUTS_PER_WORKER: u64 = 10;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "fast provider-level performance probe; run explicitly with --ignored --nocapture"]
async fn put_item_with_conditions_immediate_gsi_fast_loop() -> StorageResult<()> {
    let sqlite = create_sqlite_provider().await?;
    let turso = create_turso_provider().await?;

    run_provider_probe("sqlite", sqlite.provider, sqlite.table_name).await?;

    reset_turso_statement_counters();
    run_provider_probe("turso", turso.provider, turso.table_name).await?;

    #[cfg(feature = "postgres-backend")]
    if let Some(postgres) = create_postgres_provider().await? {
        run_provider_probe("postgres", postgres.provider, postgres.table_name).await?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "fast Turso/SQLite write-shape isolation probe; run explicitly with --ignored \
            --nocapture"]
async fn put_item_shape_isolation_fast_loop() -> StorageResult<()> {
    for shape in [
        ProbeShape {
            name: "no_gsi_condition",
            gsi_count: 0,
            condition_expression: true,
        },
        ProbeShape {
            name: "no_gsi_no_condition",
            gsi_count: 0,
            condition_expression: false,
        },
        ProbeShape {
            name: "gsi5_condition",
            gsi_count: GSI_COUNT,
            condition_expression: true,
        },
        ProbeShape {
            name: "gsi5_no_condition",
            gsi_count: GSI_COUNT,
            condition_expression: false,
        },
    ] {
        let sqlite = create_sqlite_provider_for_shape(shape).await?;
        let turso = create_turso_provider_for_shape(shape).await?;

        run_shape_probe("sqlite", sqlite.provider, sqlite.table_name, shape).await?;

        reset_turso_statement_counters();
        run_shape_probe("turso", turso.provider, turso.table_name, shape).await?;
        let (queries, executes) = turso_statement_counters();
        let measured_puts = SHAPE_SERIAL_PUTS + SHAPE_CONCURRENT_WORKERS * SHAPE_PUTS_PER_WORKER;
        println!(
            "put_shape_probe provider=turso shape={} sql_queries={} sql_executes={} \
             sql_calls_per_put={:.2}",
            shape.name,
            queries,
            executes,
            (queries + executes) as f64 / measured_puts as f64
        );
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "fast Turso update-path isolation probe; run explicitly with --ignored --nocapture"]
async fn update_item_shape_isolation_fast_loop() -> StorageResult<()> {
    let shape = ProbeShape {
        name: "gsi5_update",
        gsi_count: GSI_COUNT,
        condition_expression: false,
    };
    let turso = create_turso_provider_for_shape(shape).await?;
    seed_items(
        Arc::clone(&turso.provider),
        turso.table_name.clone(),
        1_000,
        shape,
    )
    .await?;

    reset_turso_statement_counters();
    reset_provider("turso");
    let serial = run_serial_updates(
        Arc::clone(&turso.provider),
        turso.table_name.clone(),
        0,
        SHAPE_SERIAL_PUTS,
    )
    .await?;
    print_shape_report("turso", shape.name, "update_serial", &serial);
    print_perf_counters("turso", "update_serial");

    reset_provider("turso");
    let concurrent = run_concurrent_updates(
        Arc::clone(&turso.provider),
        turso.table_name,
        0,
        SHAPE_CONCURRENT_WORKERS,
        SHAPE_PUTS_PER_WORKER,
    )
    .await?;
    print_shape_report("turso", shape.name, "update_concurrent", &concurrent);
    print_perf_counters("turso", "update_concurrent");

    let (queries, executes) = turso_statement_counters();
    let measured_updates = SHAPE_SERIAL_PUTS + SHAPE_CONCURRENT_WORKERS * SHAPE_PUTS_PER_WORKER;
    println!(
        "update_shape_probe provider=turso shape={} sql_queries={} sql_executes={} \
         sql_calls_per_update={:.2}",
        shape.name,
        queries,
        executes,
        (queries + executes) as f64 / measured_updates as f64
    );

    Ok(())
}

#[derive(Clone, Copy)]
struct ProbeShape {
    name: &'static str,
    gsi_count: usize,
    condition_expression: bool,
}

struct ProviderFixture<P> {
    provider: Arc<P>,
    table_name: TableName,
    _temp_dir: TempDir,
}

async fn create_sqlite_provider() -> StorageResult<ProviderFixture<SQLiteStorageProvider>> {
    create_sqlite_provider_for_shape(ProbeShape {
        name: "default",
        gsi_count: GSI_COUNT,
        condition_expression: true,
    })
    .await
}

async fn create_sqlite_provider_for_shape(
    shape: ProbeShape,
) -> StorageResult<ProviderFixture<SQLiteStorageProvider>> {
    let temp_dir = tempfile::tempdir().map_err(|error| {
        StorageError::internal(&format!("create SQLite temporary directory: {error}"))
    })?;
    let db_path = temp_dir
        .path()
        .join(format!("put-item-probe-{}.sqlite", shape.name));
    let provider = SQLiteStorageProvider::new_with_settings(
        path_to_str(&db_path)?,
        SqliteSettings {
            immediate_gsi_consistency: true,
            force_file_backed_database: true,
        },
    )
    .await?;
    provider.initialize_storage().await?;
    let table_name = TableName::new(&format!("sqlite_put_item_probe_{}", shape.name));
    provider
        .create_table(&probe_table_request(&table_name, shape.gsi_count))
        .await?;

    Ok(ProviderFixture {
        provider: Arc::new(provider),
        table_name,
        _temp_dir: temp_dir,
    })
}

async fn create_turso_provider() -> StorageResult<ProviderFixture<TursoStorageProvider>> {
    create_turso_provider_for_shape(ProbeShape {
        name: "default",
        gsi_count: GSI_COUNT,
        condition_expression: true,
    })
    .await
}

async fn create_turso_provider_for_shape(
    shape: ProbeShape,
) -> StorageResult<ProviderFixture<TursoStorageProvider>> {
    let temp_dir = tempfile::tempdir().map_err(|error| {
        StorageError::internal(&format!("create Turso temporary directory: {error}"))
    })?;
    let db_path = temp_dir
        .path()
        .join(format!("put-item-probe-{}.turso", shape.name));
    let provider = TursoStorageProvider::new(path_to_str(&db_path)?)
        .await?
        .with_immediate_gsi_consistency(true);
    provider.initialize_storage().await?;
    let table_name = TableName::new(&format!("turso_put_item_probe_{}", shape.name));
    provider
        .create_table(&probe_table_request(&table_name, shape.gsi_count))
        .await?;

    Ok(ProviderFixture {
        provider: Arc::new(provider),
        table_name,
        _temp_dir: temp_dir,
    })
}

#[cfg(feature = "postgres-backend")]
async fn create_postgres_provider()
-> StorageResult<Option<ProviderFixture<PostgresStorageProvider>>> {
    let dsn = std::env::var("POSTGRES_TEST_DSN")
        .ok()
        .or_else(|| std::env::var("TEST_POSTGRES_DSN").ok())
        .or_else(|| std::env::var("AUX_STORAGE_POSTGRES_DSN").ok())
        .unwrap_or_else(default_postgres_test_dsn);

    println!("put_item_probe provider=postgres setup=connect");
    let provider =
        PostgresStorageProvider::new_with_tls(&dsn, CONCURRENT_WORKERS as usize, 4, false)
            .await?
            .with_immediate_gsi_consistency(true);
    println!("put_item_probe provider=postgres setup=initialize_storage");
    provider.initialize_storage().await?;
    let uuid_text = uuid::Uuid::now_v7().to_string();
    let table_name = TableName::new(&format!(
        "pgpip_{}",
        uuid_text.chars().take(8).collect::<String>()
    ));
    println!("put_item_probe provider=postgres setup=create_table table={table_name}");
    provider
        .create_table(&probe_table_request(&table_name, GSI_COUNT))
        .await?;

    let temp_dir = tempfile::tempdir().map_err(|error| {
        StorageError::internal(&format!("create Postgres temporary directory: {error}"))
    })?;
    Ok(Some(ProviderFixture {
        provider: Arc::new(provider),
        table_name,
        _temp_dir: temp_dir,
    }))
}

#[cfg(feature = "postgres-backend")]
fn default_postgres_test_dsn() -> String {
    format!(
        "postgresql://{}@localhost/postgres",
        std::env::var("USER").unwrap_or_else(|_| "postgres".to_string())
    )
}

async fn run_provider_probe<P>(
    name: &'static str,
    provider: Arc<P>,
    table_name: TableName,
) -> StorageResult<()>
where
    P: StorageProvider + 'static,
{
    run_serial_puts(Arc::clone(&provider), table_name.clone(), 0, WARMUP_PUTS).await?;

    reset_provider(name);
    let serial = run_serial_puts(
        Arc::clone(&provider),
        table_name.clone(),
        10_000,
        SERIAL_PUTS,
    )
    .await?;
    print_report(name, "serial", &serial);
    print_perf_counters(name, "serial");

    reset_provider(name);
    let concurrent = run_concurrent_puts(
        Arc::clone(&provider),
        table_name,
        100_000,
        CONCURRENT_WORKERS,
        PUTS_PER_WORKER,
    )
    .await?;
    print_report(name, "concurrent", &concurrent);
    print_perf_counters(name, "concurrent");

    if name == "turso" {
        let (queries, executes) = turso_statement_counters();
        let measured_puts = WARMUP_PUTS + SERIAL_PUTS + CONCURRENT_WORKERS * PUTS_PER_WORKER;
        println!(
            "put_item_probe provider=turso sql_queries={} sql_executes={} sql_calls_per_put={:.2}",
            queries,
            executes,
            (queries + executes) as f64 / measured_puts as f64
        );
    }

    Ok(())
}

async fn run_shape_probe<P>(
    name: &'static str,
    provider: Arc<P>,
    table_name: TableName,
    shape: ProbeShape,
) -> StorageResult<()>
where
    P: StorageProvider + 'static,
{
    reset_provider(name);
    let serial = run_serial_puts_with_shape(
        Arc::clone(&provider),
        table_name.clone(),
        10_000,
        SHAPE_SERIAL_PUTS,
        shape,
    )
    .await?;
    print_shape_report(name, shape.name, "serial", &serial);
    print_perf_counters(name, "serial");

    reset_provider(name);
    let concurrent = run_concurrent_puts_with_shape(
        Arc::clone(&provider),
        table_name,
        100_000,
        SHAPE_CONCURRENT_WORKERS,
        SHAPE_PUTS_PER_WORKER,
        shape,
    )
    .await?;
    print_shape_report(name, shape.name, "concurrent", &concurrent);
    print_perf_counters(name, "concurrent");

    Ok(())
}

async fn run_serial_puts<P>(
    provider: Arc<P>,
    table_name: TableName,
    start_id: u64,
    count: u64,
) -> StorageResult<ProbeReport>
where
    P: StorageProvider + 'static,
{
    run_serial_puts_with_shape(
        provider,
        table_name,
        start_id,
        count,
        ProbeShape {
            name: "default",
            gsi_count: GSI_COUNT,
            condition_expression: true,
        },
    )
    .await
}

async fn run_serial_puts_with_shape<P>(
    provider: Arc<P>,
    table_name: TableName,
    start_id: u64,
    count: u64,
    shape: ProbeShape,
) -> StorageResult<ProbeReport>
where
    P: StorageProvider + 'static,
{
    let started = Instant::now();
    let mut latencies = Vec::with_capacity(count as usize);
    for offset in 0..count {
        let operation_started = Instant::now();
        put_one(
            provider.as_ref(),
            table_name.clone(),
            start_id + offset,
            offset,
            shape,
        )
        .await?;
        latencies.push(operation_started.elapsed());
    }
    Ok(ProbeReport::new(count, started.elapsed(), latencies))
}

async fn run_concurrent_puts<P>(
    provider: Arc<P>,
    table_name: TableName,
    start_id: u64,
    workers: u64,
    puts_per_worker: u64,
) -> StorageResult<ProbeReport>
where
    P: StorageProvider + 'static,
{
    run_concurrent_puts_with_shape(
        provider,
        table_name,
        start_id,
        workers,
        puts_per_worker,
        ProbeShape {
            name: "default",
            gsi_count: GSI_COUNT,
            condition_expression: true,
        },
    )
    .await
}

async fn run_concurrent_puts_with_shape<P>(
    provider: Arc<P>,
    table_name: TableName,
    start_id: u64,
    workers: u64,
    puts_per_worker: u64,
    shape: ProbeShape,
) -> StorageResult<ProbeReport>
where
    P: StorageProvider + 'static,
{
    let started = Instant::now();
    let mut tasks = JoinSet::new();
    for worker in 0..workers {
        let provider = Arc::clone(&provider);
        let table_name = table_name.clone();
        tasks.spawn(async move {
            let mut latencies = Vec::with_capacity(puts_per_worker as usize);
            for offset in 0..puts_per_worker {
                let id = start_id + worker * puts_per_worker + offset;
                let operation_started = Instant::now();
                put_one(provider.as_ref(), table_name.clone(), id, offset, shape).await?;
                latencies.push(operation_started.elapsed());
            }
            StorageResult::Ok(latencies)
        });
    }

    let mut latencies = Vec::with_capacity((workers * puts_per_worker) as usize);
    while let Some(result) = tasks.join_next().await {
        let worker_latencies = result
            .map_err(|error| StorageError::internal(&format!("probe task failed: {error}")))??;
        latencies.extend(worker_latencies);
    }

    Ok(ProbeReport::new(
        workers * puts_per_worker,
        started.elapsed(),
        latencies,
    ))
}

async fn seed_items<P>(
    provider: Arc<P>,
    table_name: TableName,
    count: u64,
    shape: ProbeShape,
) -> StorageResult<()>
where
    P: StorageProvider + 'static,
{
    for id in 0..count {
        put_one(provider.as_ref(), table_name.clone(), id, id, shape).await?;
    }
    Ok(())
}

async fn run_serial_updates<P>(
    provider: Arc<P>,
    table_name: TableName,
    start_id: u64,
    count: u64,
) -> StorageResult<ProbeReport>
where
    P: StorageProvider + 'static,
{
    let started = Instant::now();
    let mut latencies = Vec::with_capacity(count as usize);
    for offset in 0..count {
        let operation_started = Instant::now();
        update_one(
            provider.as_ref(),
            table_name.clone(),
            start_id + offset,
            offset,
        )
        .await?;
        latencies.push(operation_started.elapsed());
    }
    Ok(ProbeReport::new(count, started.elapsed(), latencies))
}

async fn run_concurrent_updates<P>(
    provider: Arc<P>,
    table_name: TableName,
    start_id: u64,
    workers: u64,
    updates_per_worker: u64,
) -> StorageResult<ProbeReport>
where
    P: StorageProvider + 'static,
{
    let started = Instant::now();
    let mut tasks = JoinSet::new();
    for worker in 0..workers {
        let provider = Arc::clone(&provider);
        let table_name = table_name.clone();
        tasks.spawn(async move {
            let mut latencies = Vec::with_capacity(updates_per_worker as usize);
            for offset in 0..updates_per_worker {
                let id = start_id + ((worker * updates_per_worker + offset) % 1_000);
                let operation_started = Instant::now();
                update_one(provider.as_ref(), table_name.clone(), id, offset).await?;
                latencies.push(operation_started.elapsed());
            }
            StorageResult::Ok(latencies)
        });
    }

    let mut latencies = Vec::with_capacity((workers * updates_per_worker) as usize);
    while let Some(result) = tasks.join_next().await {
        let worker_latencies = result
            .map_err(|error| StorageError::internal(&format!("probe task failed: {error}")))??;
        latencies.extend(worker_latencies);
    }

    Ok(ProbeReport::new(
        workers * updates_per_worker,
        started.elapsed(),
        latencies,
    ))
}

async fn put_one<P>(
    provider: &P,
    table_name: TableName,
    id: u64,
    payload_seed: u64,
    shape: ProbeShape,
) -> StorageResult<()>
where
    P: StorageProvider + ?Sized,
{
    let response = provider
        .put_item(
            table_name,
            probe_item(id, payload_seed),
            shape
                .condition_expression
                .then(|| "attribute_not_exists(pk)".to_string()),
            None,
            None,
            None,
        )
        .await?;
    if response.attributes.is_some() {
        return Err(StorageError::internal(
            "put_item probe unexpectedly returned old attributes",
        ));
    }
    Ok(())
}

async fn update_one<P>(
    provider: &P,
    table_name: TableName,
    id: u64,
    payload_seed: u64,
) -> StorageResult<()>
where
    P: StorageProvider + ?Sized,
{
    let request = UpdateItemRequest {
        table_name,
        key: HashMap::from([
            (
                "pk".to_string(),
                AttributeValue::S(shaped("pk", id, KEY_SIZE)),
            ),
            (
                "sk".to_string(),
                AttributeValue::S(shaped("sk", id, KEY_SIZE)),
            ),
        ])
        .into(),
        update_expression: Some("SET payload = :payload, gsi0pk = :gsi0pk".to_string()),
        attribute_updates: None,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: Some(HashMap::from([
            (
                ":payload".to_string(),
                AttributeValue::S(shaped("payload-updated", payload_seed, VALUE_SIZE)),
            ),
            (
                ":gsi0pk".to_string(),
                AttributeValue::S(shaped("gsi0pk-updated", payload_seed, KEY_SIZE)),
            ),
        ])),
        expected: None,
        conditional_operator: None,
        return_values: None,
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
        return_values_on_condition_check_failure: None,
    };
    provider.update_item(request).await.map(|_| ())
}

fn probe_table_request(table_name: &TableName, gsi_count: usize) -> CreateTableRequest {
    let mut attribute_definitions = vec![attribute_definition("pk"), attribute_definition("sk")];
    let mut gsis = Vec::new();
    for index in 0..gsi_count {
        let gsi_pk = format!("gsi{index}pk");
        let gsi_sk = format!("gsi{index}sk");
        attribute_definitions.push(attribute_definition(&gsi_pk));
        attribute_definitions.push(attribute_definition(&gsi_sk));
        gsis.push(CreateGlobalSecondaryIndex {
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
            projection: Projection {
                projection_type: Some(ProjectionType::All),
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        });
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
        BillingMode::PayPerRequest,
    );
    if gsis.is_empty() {
        request
    } else {
        request.with_global_secondary_indexes(Some(gsis))
    }
}

fn probe_item(id: u64, payload_seed: u64) -> HashMap<String, AttributeValue> {
    let mut item = HashMap::from([
        (
            "pk".to_string(),
            AttributeValue::S(shaped("pk", id, KEY_SIZE)),
        ),
        (
            "sk".to_string(),
            AttributeValue::S(shaped("sk", id, KEY_SIZE)),
        ),
        (
            "payload".to_string(),
            AttributeValue::S(shaped("payload", payload_seed, VALUE_SIZE)),
        ),
        ("n".to_string(), AttributeValue::N(id.to_string())),
    ]);

    for index in 0..GSI_COUNT {
        item.insert(
            format!("gsi{index}pk"),
            AttributeValue::S(shaped(&format!("gsi{index}pk"), id % 128, KEY_SIZE)),
        );
        item.insert(
            format!("gsi{index}sk"),
            AttributeValue::S(shaped(&format!("gsi{index}sk"), id, KEY_SIZE)),
        );
    }

    item
}

fn attribute_definition(attribute_name: &str) -> AttributeDefinition {
    AttributeDefinition {
        attribute_name: attribute_name.to_string(),
        attribute_type: KeyAttributeType::S,
    }
}

fn shaped(prefix: &str, id: u64, size: usize) -> String {
    let mut value = format!("{prefix}-{id:020}");
    while value.len() < size {
        value.push('x');
    }
    value.truncate(size);
    value
}

fn path_to_str(path: &std::path::Path) -> StorageResult<&str> {
    path.to_str()
        .ok_or_else(|| StorageError::internal("probe path is not utf-8"))
}

#[derive(Debug)]
struct ProbeReport {
    operations: u64,
    elapsed: Duration,
    p50: Duration,
    p90: Duration,
    p99: Duration,
}

impl ProbeReport {
    fn new(operations: u64, elapsed: Duration, mut latencies: Vec<Duration>) -> Self {
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

fn print_report(provider: &str, phase: &str, report: &ProbeReport) {
    println!(
        "put_item_probe provider={} phase={} ops={} elapsed_ms={:.1} ops_sec={:.1} p50_ms={:.3} \
         p90_ms={:.3} p99_ms={:.3}",
        provider,
        phase,
        report.operations,
        report.elapsed.as_secs_f64() * 1000.0,
        report.ops_per_second(),
        millis(report.p50),
        millis(report.p90),
        millis(report.p99)
    );
}

fn print_shape_report(provider: &str, shape: &str, phase: &str, report: &ProbeReport) {
    println!(
        "put_shape_probe provider={} shape={} phase={} ops={} elapsed_ms={:.1} ops_sec={:.1} \
         p50_ms={:.3} p90_ms={:.3} p99_ms={:.3}",
        provider,
        shape,
        phase,
        report.operations,
        report.elapsed.as_secs_f64() * 1000.0,
        report.ops_per_second(),
        millis(report.p50),
        millis(report.p90),
        millis(report.p99)
    );
}

fn print_perf_counters(provider: &'static str, phase: &str) {
    for counter in snapshot_provider(provider) {
        let avg = counter.total.as_secs_f64() * 1000.0 / counter.calls as f64;
        println!(
            "put_item_probe provider={} phase={} hotspot={} calls={} total_ms={:.3} avg_ms={:.3} \
             max_ms={:.3} total_amount={} max_amount={}",
            provider,
            phase,
            counter.name,
            counter.calls,
            counter.total.as_secs_f64() * 1000.0,
            avg,
            counter.max.as_secs_f64() * 1000.0,
            counter.total_amount,
            counter.max_amount
        );
    }
    reset_provider(provider);
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}
