use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use alloc_counter::AllocationGuard;
use storage_common::provider_perf::emit_runtime_report;
use storage_condition::{
    evaluate_condition, parse_condition_expression, try_evaluate_condition_with_cached_roots,
    try_evaluate_condition_with_root,
};
use storage_provider::StorageProvider as _;
use storage_types::{
    AttributeDefinition, AttributeValue, CreateTableRequest, KeyAttributeType, KeyAttributes,
    KeySchemaElement, KeyType, StreamName, StreamSpecification, StreamViewType, TableName,
    TimeToLiveSpecification, UpdateTimeToLiveRequest, WireItem, WireItemKeyAttributes,
};
use stream_provider::StreamProvider as _;

use super::{
    put_item_impl::condition_item_ref as put_condition_item_ref,
    transact_write_impl::condition_item_ref as transact_condition_item_ref,
};
use crate::{
    SQLiteStorageProvider, naming,
    utils::{SqliteConn, call_sqlite},
};

const ITEM_COUNT: usize = 96;
const STREAM_READ_LIMIT: u32 = 256;
const TTL_ATTRIBUTE: &str = "ttl";
const TABLE_NAME_ENCODE_BASELINE: &str = "alloc_write_encode_ttl_stream_sqlite";
const CONDITION_EVAL_ITERATIONS: usize = 2048;
const MACRO_CONDITION_ITERATIONS: usize = 128;

struct PerfMeasurement {
    report: alloc_counter::AllocationReport<'static>,
    elapsed: Duration,
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current-thread runtime")
}

fn create_table_request(table_name: &TableName) -> CreateTableRequest {
    CreateTableRequest::new(
        table_name.clone(),
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
    }))
}

fn create_table_request_without_stream(table_name: &TableName) -> CreateTableRequest {
    CreateTableRequest::new(
        table_name.clone(),
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
}

fn sample_item(index: usize) -> HashMap<String, AttributeValue> {
    let ttl = 2_200_000_000_u64 + u64::try_from(index).unwrap_or(0);
    HashMap::from([
        ("pk".to_string(), AttributeValue::S("ORG#ALLOC".to_string())),
        (
            "sk".to_string(),
            AttributeValue::S(format!("ITEM#{index:04}")),
        ),
        (
            "entity_type".to_string(),
            AttributeValue::S("ALLOC_PROFILE".to_string()),
        ),
        ("revision".to_string(), AttributeValue::N(index.to_string())),
        (
            TTL_ATTRIBUTE.to_string(),
            AttributeValue::N(ttl.to_string()),
        ),
        (
            "payload".to_string(),
            AttributeValue::M(HashMap::from([
                (
                    "status".to_string(),
                    AttributeValue::S("active".to_string()),
                ),
                (
                    "flags".to_string(),
                    AttributeValue::L(vec![
                        AttributeValue::S("stream".to_string()),
                        AttributeValue::S("ttl".to_string()),
                    ]),
                ),
            ])),
        ),
    ])
}

fn sample_items() -> Vec<HashMap<String, AttributeValue>> {
    (0..ITEM_COUNT).map(sample_item).collect()
}

fn realistic_condition_item() -> HashMap<String, AttributeValue> {
    let mut item = sample_item(42);
    item.insert(
        "status".to_string(),
        AttributeValue::S("active".to_string()),
    );
    item.insert(
        "owner".to_string(),
        AttributeValue::S("tenant-a".repeat(16)),
    );
    item.insert(
        "search".to_string(),
        AttributeValue::S("prefix-value".to_string()),
    );
    item.insert(
        "tags".to_string(),
        AttributeValue::SS(vec!["hot".to_string(), "tracked".to_string()]),
    );
    item.insert(
        "notes".to_string(),
        AttributeValue::L(vec![
            AttributeValue::S("first".to_string()),
            AttributeValue::S("second".to_string()),
        ]),
    );
    item.insert(
        "payload".to_string(),
        AttributeValue::M(HashMap::from([
            (
                "status".to_string(),
                AttributeValue::S("active".to_string()),
            ),
            (
                "search".to_string(),
                AttributeValue::S("prefix-value".to_string()),
            ),
            ("body".to_string(), AttributeValue::S("x".repeat(1024))),
        ])),
    );
    item
}

fn condition() -> storage_condition::Condition {
    parse_condition_expression(
        "#status = :active AND begins_with(#search, :prefix)",
        Some(&HashMap::from([
            ("#status".to_string(), "status".to_string()),
            ("#search".to_string(), "search".to_string()),
        ])),
        Some(&HashMap::from([
            (
                ":active".to_string(),
                AttributeValue::S("active".to_string()),
            ),
            (
                ":prefix".to_string(),
                AttributeValue::S("prefix".to_string()),
            ),
        ])),
    )
    .expect("parse condition")
}

fn repeated_root_condition() -> storage_condition::Condition {
    parse_condition_expression(
        "payload.status = :active AND begins_with(payload.search, :prefix) AND size(payload.body) \
         > :min",
        None,
        Some(&HashMap::from([
            (
                ":active".to_string(),
                AttributeValue::S("active".to_string()),
            ),
            (
                ":prefix".to_string(),
                AttributeValue::S("prefix".to_string()),
            ),
            (":min".to_string(), AttributeValue::N("100".to_string())),
        ])),
    )
    .expect("parse repeated-root condition")
}

fn sample_wire_items() -> Vec<WireItem> {
    sample_items()
        .into_iter()
        .map(|item| WireItem::from_attribute_map(&item).expect("wire item"))
        .collect()
}

fn realistic_local_split_wire_item() -> WireItem {
    let mut item = realistic_condition_item();
    let pk = item.remove("pk").expect("pk");
    let sk = item.remove("sk").expect("sk");
    let blob = serde_json::to_vec(&item).expect("encode non-key blob");
    WireItem::local_split(
        WireItemKeyAttributes::new("pk".to_string(), pk, Some("sk".to_string()), Some(sk)),
        None,
        Some(blob),
    )
}

async fn setup_provider(table_name: &TableName) -> SQLiteStorageProvider {
    let provider = SQLiteStorageProvider::new(":memory:")
        .await
        .expect("create sqlite provider");
    provider
        .initialize_storage()
        .await
        .expect("initialize storage");
    provider
        .initialize_stream()
        .await
        .expect("initialize stream");
    provider
        .create_table(&create_table_request(table_name))
        .await
        .expect("create table");
    provider
        .update_time_to_live(UpdateTimeToLiveRequest {
            table_name: table_name.clone(),
            time_to_live_specification: TimeToLiveSpecification {
                attribute_name: TTL_ATTRIBUTE.to_string(),
                enabled: true,
            },
        })
        .await
        .expect("enable ttl");
    provider
}

async fn setup_provider_without_stream(table_name: &TableName) -> SQLiteStorageProvider {
    let provider = SQLiteStorageProvider::new(":memory:")
        .await
        .expect("create sqlite provider");
    provider
        .initialize_storage()
        .await
        .expect("initialize storage");
    provider
        .create_table(&create_table_request_without_stream(table_name))
        .await
        .expect("create table");
    provider
}

async fn assert_stream_entries(provider: &SQLiteStorageProvider) {
    let page = provider
        .read_forward(StreamName::system_table_stream(), None, STREAM_READ_LIMIT)
        .await
        .expect("read stream entries");
    assert!(
        page.items.len() >= ITEM_COUNT,
        "expected at least {ITEM_COUNT} stream entries, got {}",
        page.items.len()
    );
}

async fn assert_ttl_row_count(
    provider: &SQLiteStorageProvider,
    table_name: &TableName,
    expected_rows: usize,
) {
    let ttl_table = naming::physical_ttl_index_table_name(table_name);
    let row_count = provider
        .connection
        .call_unwrap(move |conn| {
            let sql = format!("SELECT COUNT(*) FROM \"{ttl_table}\"");
            let mut stmt = conn.prepare(&sql)?;
            let count: i64 = stmt.query_row([], |row| row.get(0))?;
            Ok::<_, rusqlite::Error>(count)
        })
        .await
        .expect("count ttl rows");
    assert_eq!(
        usize::try_from(row_count).unwrap_or(0),
        expected_rows,
        "unexpected ttl row count"
    );
}

fn measure_put_item_encode_stream_ttl_baseline() -> alloc_counter::AllocationReport<'static> {
    let runtime = runtime();
    let table_name = TableName::new(TABLE_NAME_ENCODE_BASELINE);
    let provider = runtime.block_on(setup_provider(&table_name));
    let write_items = sample_wire_items();

    let guard = AllocationGuard::start(
        module_path!(),
        "sqlite_put_item_encode_stream_ttl_baseline",
        file!(),
        line!(),
        Some("baseline"),
    );
    runtime.block_on(async {
        for item in write_items {
            provider
                .put_item_encode(table_name.clone(), item, None, None, None, None)
                .await
                .expect("put item encode");
        }
    });
    let report = guard.finish();

    runtime.block_on(async {
        assert_stream_entries(&provider).await;
        assert_ttl_row_count(&provider, &table_name, ITEM_COUNT).await;
    });
    report
}

fn measure_transact_condition_old_item_clone_baseline() -> alloc_counter::AllocationReport<'static>
{
    let item = realistic_condition_item();
    let condition = parse_condition_expression(
        "#status = :active AND begins_with(#search, :prefix)",
        Some(&HashMap::from([
            ("#status".to_string(), "status".to_string()),
            ("#search".to_string(), "search".to_string()),
        ])),
        Some(&HashMap::from([
            (
                ":active".to_string(),
                AttributeValue::S("active".to_string()),
            ),
            (
                ":prefix".to_string(),
                AttributeValue::S("prefix".to_string()),
            ),
        ])),
    )
    .expect("parse condition");

    let guard = AllocationGuard::start(
        module_path!(),
        "sqlite_transact_condition_old_item_clone_baseline",
        file!(),
        line!(),
        Some("clone_baseline"),
    );
    for _ in 0..CONDITION_EVAL_ITERATIONS {
        let old_item = Some(item.clone());
        let condition_item = old_item.clone().unwrap_or_default();
        let matched = evaluate_condition(&condition_item, &condition);
        std::hint::black_box(matched);
    }
    guard.finish()
}

fn measure_transact_condition_old_item_borrowed() -> alloc_counter::AllocationReport<'static> {
    let item = realistic_condition_item();
    let condition = parse_condition_expression(
        "#status = :active AND begins_with(#search, :prefix)",
        Some(&HashMap::from([
            ("#status".to_string(), "status".to_string()),
            ("#search".to_string(), "search".to_string()),
        ])),
        Some(&HashMap::from([
            (
                ":active".to_string(),
                AttributeValue::S("active".to_string()),
            ),
            (
                ":prefix".to_string(),
                AttributeValue::S("prefix".to_string()),
            ),
        ])),
    )
    .expect("parse condition");

    let guard = AllocationGuard::start(
        module_path!(),
        "sqlite_transact_condition_old_item_borrowed",
        file!(),
        line!(),
        Some("borrowed"),
    );
    for _ in 0..CONDITION_EVAL_ITERATIONS {
        let old_item = Some(item.clone());
        let matched =
            evaluate_condition(transact_condition_item_ref(old_item.as_ref()), &condition);
        std::hint::black_box(matched);
    }
    guard.finish()
}

fn measure_put_condition_old_item_clone_baseline() -> alloc_counter::AllocationReport<'static> {
    let item = realistic_condition_item();
    let condition = condition();

    let guard = AllocationGuard::start(
        module_path!(),
        "sqlite_put_condition_old_item_clone_baseline",
        file!(),
        line!(),
        Some("clone_baseline"),
    );
    for _ in 0..CONDITION_EVAL_ITERATIONS {
        let old_item = Some(item.clone());
        let matched = evaluate_condition(&old_item.clone().unwrap_or(HashMap::new()), &condition);
        std::hint::black_box(matched);
    }
    guard.finish()
}

fn measure_put_condition_old_item_borrowed() -> alloc_counter::AllocationReport<'static> {
    let item = realistic_condition_item();
    let condition = condition();

    let guard = AllocationGuard::start(
        module_path!(),
        "sqlite_put_condition_old_item_borrowed",
        file!(),
        line!(),
        Some("borrowed"),
    );
    for _ in 0..CONDITION_EVAL_ITERATIONS {
        let old_item = Some(item.clone());
        let matched = evaluate_condition(put_condition_item_ref(old_item.as_ref()), &condition);
        std::hint::black_box(matched);
    }
    guard.finish()
}

fn measure_wire_condition_to_attribute_map_baseline() -> PerfMeasurement {
    let item = realistic_local_split_wire_item();
    let condition = condition();

    let guard = AllocationGuard::start(
        module_path!(),
        "sqlite_wire_condition_to_attribute_map_baseline",
        file!(),
        line!(),
        Some("wire_to_attribute_map"),
    );
    let started = Instant::now();
    for _ in 0..CONDITION_EVAL_ITERATIONS {
        let condition_item = item.to_attribute_map().expect("decode wire item");
        let matched = evaluate_condition(&condition_item, &condition);
        std::hint::black_box(matched);
    }
    let elapsed = started.elapsed();
    PerfMeasurement {
        report: guard.finish(),
        elapsed,
    }
}

fn measure_wire_condition_root_lookup() -> PerfMeasurement {
    let item = realistic_local_split_wire_item();
    let condition = condition();

    let guard = AllocationGuard::start(
        module_path!(),
        "sqlite_wire_condition_root_lookup",
        file!(),
        line!(),
        Some("wire_root_lookup"),
    );
    let started = Instant::now();
    for _ in 0..CONDITION_EVAL_ITERATIONS {
        let mut root_value = |field: &str| item.attribute_value(field);
        let matched = try_evaluate_condition_with_root(&condition, &mut root_value)
            .expect("evaluate wire condition");
        std::hint::black_box(matched);
    }
    let elapsed = started.elapsed();
    PerfMeasurement {
        report: guard.finish(),
        elapsed,
    }
}

fn measure_wire_repeated_condition_no_cache() -> PerfMeasurement {
    let item = realistic_local_split_wire_item();
    let condition = repeated_root_condition();

    let guard = AllocationGuard::start(
        module_path!(),
        "sqlite_wire_repeated_condition_no_cache",
        file!(),
        line!(),
        Some("wire_repeated_no_cache"),
    );
    let started = Instant::now();
    for _ in 0..CONDITION_EVAL_ITERATIONS {
        let mut root_value = |field: &str| item.attribute_value(field);
        let matched = try_evaluate_condition_with_root(&condition, &mut root_value)
            .expect("evaluate repeated condition");
        std::hint::black_box(matched);
    }
    let elapsed = started.elapsed();
    PerfMeasurement {
        report: guard.finish(),
        elapsed,
    }
}

fn measure_wire_repeated_condition_cached() -> PerfMeasurement {
    let item = realistic_local_split_wire_item();
    let condition = repeated_root_condition();

    let guard = AllocationGuard::start(
        module_path!(),
        "sqlite_wire_repeated_condition_cached",
        file!(),
        line!(),
        Some("wire_repeated_cached"),
    );
    let started = Instant::now();
    for _ in 0..CONDITION_EVAL_ITERATIONS {
        let mut root_value = |field: &str| item.attribute_value(field);
        let matched = try_evaluate_condition_with_cached_roots(&condition, &mut root_value)
            .expect("evaluate repeated condition");
        std::hint::black_box(matched);
    }
    let elapsed = started.elapsed();
    PerfMeasurement {
        report: guard.finish(),
        elapsed,
    }
}

fn emit_perf_measurement(
    test_name: &str,
    label: &str,
    measurement: &PerfMeasurement,
    mutation_count: usize,
) {
    alloc_counter::emit_report(&measurement.report);
    emit_runtime_report(
        module_path!(),
        test_name,
        label,
        mutation_count,
        measurement.elapsed,
    );
}

fn condition_key() -> KeyAttributes {
    let mut key = KeyAttributes::new();
    key.insert("pk", AttributeValue::S("ORG#ALLOC".to_string()));
    key.insert("sk", AttributeValue::S("ITEM#0042".to_string()));
    key
}

fn condition_expression_inputs() -> (
    String,
    Option<HashMap<String, String>>,
    Option<HashMap<String, AttributeValue>>,
) {
    (
        "payload.status = :active AND begins_with(payload.search, :prefix) AND size(payload.body) \
         > :min"
            .to_string(),
        None,
        Some(HashMap::from([
            (
                ":active".to_string(),
                AttributeValue::S("active".to_string()),
            ),
            (
                ":prefix".to_string(),
                AttributeValue::S("prefix".to_string()),
            ),
            (":min".to_string(), AttributeValue::N("100".to_string())),
        ])),
    )
}

fn measure_conditional_put_macro_full_map_baseline() -> PerfMeasurement {
    let runtime = runtime();
    let table_name = TableName::new("macro_conditional_put_full_map_sqlite");
    let provider = runtime.block_on(setup_provider_without_stream(&table_name));
    let wire_item = realistic_local_split_wire_item();
    let (condition_expression, names, values) = condition_expression_inputs();
    let key = condition_key();

    runtime.block_on(async {
        provider
            .put_item_encode(
                table_name.clone(),
                wire_item.clone(),
                None,
                None,
                None,
                None,
            )
            .await
            .expect("seed item");
    });

    let guard = AllocationGuard::start(
        module_path!(),
        "sqlite_conditional_put_macro_full_map_baseline",
        file!(),
        line!(),
        Some("macro_full_map_before"),
    );
    let started = Instant::now();
    runtime.block_on(async {
        for _ in 0..MACRO_CONDITION_ITERATIONS {
            let old_item = call_sqlite(&provider.connection, {
                let table_name = table_name.clone();
                let key = key.clone();
                move |conn| {
                    let sqlite = SqliteConn::Connection(conn);
                    SQLiteStorageProvider::do_get_wire_item(&table_name, &key, &sqlite)
                }
            })
            .await
            .expect("get old wire item")
            .expect("old item");
            let condition =
                parse_condition_expression(&condition_expression, names.as_ref(), values.as_ref())
                    .expect("parse condition");
            let old_map = old_item.to_attribute_map().expect("decode old item");
            assert!(evaluate_condition(&old_map, &condition));
            provider
                .put_item_encode(
                    table_name.clone(),
                    wire_item.clone(),
                    None,
                    None,
                    None,
                    None,
                )
                .await
                .expect("put item");
        }
    });
    let elapsed = started.elapsed();
    PerfMeasurement {
        report: guard.finish(),
        elapsed,
    }
}

fn measure_conditional_put_macro_cached() -> PerfMeasurement {
    let runtime = runtime();
    let table_name = TableName::new("macro_conditional_put_cached_sqlite");
    let provider = runtime.block_on(setup_provider_without_stream(&table_name));
    let wire_item = realistic_local_split_wire_item();
    let (condition_expression, names, values) = condition_expression_inputs();

    runtime.block_on(async {
        provider
            .put_item_encode(
                table_name.clone(),
                wire_item.clone(),
                None,
                None,
                None,
                None,
            )
            .await
            .expect("seed item");
    });

    let guard = AllocationGuard::start(
        module_path!(),
        "sqlite_conditional_put_macro_cached",
        file!(),
        line!(),
        Some("macro_cached_after"),
    );
    let started = Instant::now();
    runtime.block_on(async {
        for _ in 0..MACRO_CONDITION_ITERATIONS {
            provider
                .put_item_encode(
                    table_name.clone(),
                    wire_item.clone(),
                    Some(condition_expression.clone()),
                    names.clone(),
                    values.clone(),
                    None,
                )
                .await
                .expect("conditional put item");
        }
    });
    let elapsed = started.elapsed();
    PerfMeasurement {
        report: guard.finish(),
        elapsed,
    }
}

#[test]
fn sqlite_write_path_allocation_profile_tests() {
    assert_sqlite_put_item_encode_stream_ttl_allocation_baseline();
    assert_sqlite_transact_condition_evaluation_borrows_old_item();
    assert_sqlite_put_condition_evaluation_borrows_old_item();
    assert_sqlite_wire_condition_uses_root_lookup();
    assert_sqlite_repeated_wire_condition_uses_cache();
    assert_sqlite_conditional_put_macro_uses_cached_wire_condition();
}

fn assert_sqlite_put_item_encode_stream_ttl_allocation_baseline() {
    // Snapshot (2026-02-18, `cargo test -p sqlite write_path_alloc_tests --
    // --nocapture`): allocation_count=16707, allocated_bytes=3144704.
    let report = measure_put_item_encode_stream_ttl_baseline();
    alloc_counter::emit_report(&report);
    assert!(report.allocation_count > 0);
    assert!(report.allocated_bytes > 0);
}

fn assert_sqlite_transact_condition_evaluation_borrows_old_item() {
    let baseline = measure_transact_condition_old_item_clone_baseline();
    let borrowed = measure_transact_condition_old_item_borrowed();

    alloc_counter::emit_report(&baseline);
    alloc_counter::emit_report(&borrowed);

    assert!(borrowed.allocation_count < baseline.allocation_count);
    assert!(borrowed.allocated_bytes < baseline.allocated_bytes);
}

fn assert_sqlite_put_condition_evaluation_borrows_old_item() {
    let baseline = measure_put_condition_old_item_clone_baseline();
    let borrowed = measure_put_condition_old_item_borrowed();

    alloc_counter::emit_report(&baseline);
    alloc_counter::emit_report(&borrowed);

    assert!(borrowed.allocation_count < baseline.allocation_count);
    assert!(borrowed.allocated_bytes < baseline.allocated_bytes);
}

fn assert_sqlite_wire_condition_uses_root_lookup() {
    let baseline = measure_wire_condition_to_attribute_map_baseline();
    let root_lookup = measure_wire_condition_root_lookup();

    emit_perf_measurement(
        "sqlite_wire_condition_uses_root_lookup",
        "before",
        &baseline,
        CONDITION_EVAL_ITERATIONS,
    );
    emit_perf_measurement(
        "sqlite_wire_condition_uses_root_lookup",
        "after",
        &root_lookup,
        CONDITION_EVAL_ITERATIONS,
    );

    assert!(
        root_lookup.report.allocation_count < baseline.report.allocation_count,
        "expected root lookup to allocate less often, baseline={} root_lookup={}",
        baseline.report.allocation_count,
        root_lookup.report.allocation_count
    );
    assert!(
        root_lookup.report.allocated_bytes < baseline.report.allocated_bytes,
        "expected root lookup to allocate fewer bytes, baseline={} root_lookup={}",
        baseline.report.allocated_bytes,
        root_lookup.report.allocated_bytes
    );
}

fn assert_sqlite_repeated_wire_condition_uses_cache() {
    let baseline = measure_wire_repeated_condition_no_cache();
    let cached = measure_wire_repeated_condition_cached();

    emit_perf_measurement(
        "sqlite_repeated_wire_condition_uses_cache",
        "before",
        &baseline,
        CONDITION_EVAL_ITERATIONS,
    );
    emit_perf_measurement(
        "sqlite_repeated_wire_condition_uses_cache",
        "after",
        &cached,
        CONDITION_EVAL_ITERATIONS,
    );

    assert!(
        cached.report.allocation_count < baseline.report.allocation_count,
        "expected cached root lookup to allocate less often, baseline={} cached={}",
        baseline.report.allocation_count,
        cached.report.allocation_count
    );
    assert!(
        cached.report.allocated_bytes < baseline.report.allocated_bytes,
        "expected cached root lookup to allocate fewer bytes, baseline={} cached={}",
        baseline.report.allocated_bytes,
        cached.report.allocated_bytes
    );
}

fn assert_sqlite_conditional_put_macro_uses_cached_wire_condition() {
    let baseline = measure_conditional_put_macro_full_map_baseline();
    let cached = measure_conditional_put_macro_cached();

    emit_perf_measurement(
        "sqlite_conditional_put_macro_uses_cached_wire_condition",
        "before",
        &baseline,
        MACRO_CONDITION_ITERATIONS,
    );
    emit_perf_measurement(
        "sqlite_conditional_put_macro_uses_cached_wire_condition",
        "after",
        &cached,
        MACRO_CONDITION_ITERATIONS,
    );

    assert!(
        cached.report.allocation_count < baseline.report.allocation_count,
        "expected cached macro path to allocate less often, baseline={} cached={}",
        baseline.report.allocation_count,
        cached.report.allocation_count
    );
    assert!(
        cached.report.allocated_bytes < baseline.report.allocated_bytes,
        "expected cached macro path to allocate fewer bytes, baseline={} cached={}",
        baseline.report.allocated_bytes,
        cached.report.allocated_bytes
    );
}
