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
    AttributeDefinition, AttributeValue, BillingMode, CreateTableRequest, KeyAttributeType,
    KeyAttributes, KeySchemaElement, KeyType, TableName, TransactUpdateRequest, TransactWriteItem,
    TransactWriteItemsRequest, WireItem, WireItemKeyAttributes,
};

use super::{
    PostgresStorageProvider,
    transaction_helpers::{condition_item_ref, evaluate_wire_condition},
};
use crate::provider_core::transaction::preflight_transact_item_key_with_table_info;

const CONDITION_EVAL_ITERATIONS: usize = 2048;
const MACRO_CONDITION_ITERATIONS: usize = 64;
const TRANSACT_ITEM_ITERATIONS: usize = 512;
const TRANSACT_ITEM_COUNT: usize = 25;

type ConditionExpressionInputs = (
    String,
    Option<HashMap<String, String>>,
    Option<HashMap<String, AttributeValue>>,
);

struct PerfMeasurement {
    report: alloc_counter::AllocationReport<'static>,
    elapsed: Duration,
}

#[test]
fn postgres_transact_condition_allocation_profile_tests() {
    assert_postgres_transact_condition_evaluation_borrows_old_item();
    assert_postgres_transact_update_condition_failure_delays_old_item_clone();
    assert_postgres_transact_write_loop_moves_attempt_items();
    assert_postgres_transact_condition_failure_uses_wire_item();
    assert_postgres_wire_condition_uses_root_lookup();
    assert_postgres_repeated_wire_condition_uses_cache();
}

#[tokio::test]
#[ignore = "live Postgres macro perf probe; requires TEST_POSTGRES_DSN or local host=/tmp \
            dbname=postgres"]
async fn postgres_conditional_put_macro_profile_tests() {
    assert_postgres_conditional_put_macro_uses_cached_wire_condition().await;
}

#[tokio::test]
#[ignore = "live Postgres macro perf probe; requires TEST_POSTGRES_DSN or fresh local Postgres DB"]
async fn postgres_transact_condition_failure_macro_profile_tests() {
    assert_postgres_transact_condition_failure_macro_uses_wire_condition().await;
}

#[tokio::test]
#[ignore = "live Postgres macro perf probe; requires TEST_POSTGRES_DSN or fresh local Postgres DB"]
async fn postgres_transact_write_loop_macro_profile_tests() {
    assert_postgres_transact_write_loop_macro_moves_attempt_items().await;
}

#[tokio::test]
#[ignore = "live Postgres macro perf probe; requires TEST_POSTGRES_DSN or fresh local Postgres DB"]
async fn postgres_transact_preflight_table_cache_macro_profile_tests() {
    assert_postgres_transact_preflight_macro_uses_request_table_cache().await;
}

fn assert_postgres_transact_condition_evaluation_borrows_old_item() {
    let baseline = measure_transact_condition_old_item_clone_baseline();
    let borrowed = measure_transact_condition_old_item_borrowed();

    alloc_counter::emit_report(&baseline);
    alloc_counter::emit_report(&borrowed);

    assert!(
        borrowed.allocation_count < baseline.allocation_count,
        "expected borrowed condition evaluation to allocate less often, baseline={} borrowed={}",
        baseline.allocation_count,
        borrowed.allocation_count
    );
    assert!(
        borrowed.allocated_bytes < baseline.allocated_bytes,
        "expected borrowed condition evaluation to allocate fewer bytes, baseline={} borrowed={}",
        baseline.allocated_bytes,
        borrowed.allocated_bytes
    );
}

fn assert_postgres_transact_update_condition_failure_delays_old_item_clone() {
    let baseline = measure_transact_update_failed_condition_clone_baseline();
    let delayed = measure_transact_update_failed_condition_delayed_clone();

    alloc_counter::emit_report(&baseline);
    alloc_counter::emit_report(&delayed);

    assert!(
        delayed.allocation_count < baseline.allocation_count,
        "expected delayed update clone to allocate less often, baseline={} delayed={}",
        baseline.allocation_count,
        delayed.allocation_count
    );
    assert!(
        delayed.allocated_bytes < baseline.allocated_bytes,
        "expected delayed update clone to allocate fewer bytes, baseline={} delayed={}",
        baseline.allocated_bytes,
        delayed.allocated_bytes
    );
}

fn assert_postgres_transact_write_loop_moves_attempt_items() {
    let baseline = measure_transact_write_loop_clone_items_baseline();
    let moved = measure_transact_write_loop_move_items();

    emit_perf_measurement(
        "postgres_transact_write_loop_moves_attempt_items",
        "before",
        &baseline,
        TRANSACT_ITEM_ITERATIONS * TRANSACT_ITEM_COUNT,
    );
    emit_perf_measurement(
        "postgres_transact_write_loop_moves_attempt_items",
        "after",
        &moved,
        TRANSACT_ITEM_ITERATIONS * TRANSACT_ITEM_COUNT,
    );

    assert!(
        moved.report.allocation_count < baseline.report.allocation_count,
        "expected moved transaction items to allocate less often, baseline={} moved={}",
        baseline.report.allocation_count,
        moved.report.allocation_count
    );
    assert!(
        moved.report.allocated_bytes < baseline.report.allocated_bytes,
        "expected moved transaction items to allocate fewer bytes, baseline={} moved={}",
        baseline.report.allocated_bytes,
        moved.report.allocated_bytes
    );
}

fn measure_transact_condition_old_item_clone_baseline() -> alloc_counter::AllocationReport<'static>
{
    let item = realistic_condition_item();
    let condition = condition();

    let guard = AllocationGuard::start(
        module_path!(),
        "postgres_transact_condition_old_item_clone_baseline",
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
    let condition = condition();

    let guard = AllocationGuard::start(
        module_path!(),
        "postgres_transact_condition_old_item_borrowed",
        file!(),
        line!(),
        Some("borrowed"),
    );
    for _ in 0..CONDITION_EVAL_ITERATIONS {
        let old_item = Some(item.clone());
        let matched = evaluate_condition(condition_item_ref(old_item.as_ref()), &condition);
        std::hint::black_box(matched);
    }
    guard.finish()
}

fn measure_transact_update_failed_condition_clone_baseline()
-> alloc_counter::AllocationReport<'static> {
    let item = failed_condition_item();
    let condition = condition();

    let guard = AllocationGuard::start(
        module_path!(),
        "postgres_transact_update_failed_condition_clone_baseline",
        file!(),
        line!(),
        Some("failed_condition_clone_baseline"),
    );
    for _ in 0..CONDITION_EVAL_ITERATIONS {
        let existing_item = Some(item.clone());
        let item_to_update = existing_item.clone().unwrap_or_default();
        let matched = evaluate_condition(condition_item_ref(existing_item.as_ref()), &condition);
        std::hint::black_box((matched, item_to_update.len()));
    }
    guard.finish()
}

fn measure_transact_update_failed_condition_delayed_clone()
-> alloc_counter::AllocationReport<'static> {
    let item = failed_condition_item();
    let condition = condition();

    let guard = AllocationGuard::start(
        module_path!(),
        "postgres_transact_update_failed_condition_delayed_clone",
        file!(),
        line!(),
        Some("failed_condition_delayed_clone"),
    );
    for _ in 0..CONDITION_EVAL_ITERATIONS {
        let existing_item = Some(item.clone());
        let matched = evaluate_condition(condition_item_ref(existing_item.as_ref()), &condition);
        if matched {
            let item_to_update = existing_item.clone().unwrap_or_default();
            std::hint::black_box(item_to_update.len());
        }
        std::hint::black_box(matched);
    }
    guard.finish()
}

fn measure_transact_write_loop_clone_items_baseline() -> PerfMeasurement {
    let request = realistic_transact_write_request();

    let guard = AllocationGuard::start(
        module_path!(),
        "postgres_transact_write_loop_clone_items_baseline",
        file!(),
        line!(),
        Some("clone_items_before"),
    );
    let started = Instant::now();
    for _ in 0..TRANSACT_ITEM_ITERATIONS {
        let attempt_request = request.clone();
        let mut action_count_sum = 0;
        for item in attempt_request.transact_items.clone() {
            action_count_sum += usize::from(item.put.is_some())
                + usize::from(item.update.is_some())
                + usize::from(item.delete.is_some())
                + usize::from(item.condition_check.is_some());
            std::hint::black_box(item);
        }
        std::hint::black_box(action_count_sum);
    }
    let elapsed = started.elapsed();
    PerfMeasurement {
        report: guard.finish(),
        elapsed,
    }
}

fn measure_transact_write_loop_move_items() -> PerfMeasurement {
    let request = realistic_transact_write_request();

    let guard = AllocationGuard::start(
        module_path!(),
        "postgres_transact_write_loop_move_items",
        file!(),
        line!(),
        Some("move_items_after"),
    );
    let started = Instant::now();
    for _ in 0..TRANSACT_ITEM_ITERATIONS {
        let attempt_request = request.clone();
        let mut action_count_sum = 0;
        for item in attempt_request.transact_items {
            action_count_sum += usize::from(item.put.is_some())
                + usize::from(item.update.is_some())
                + usize::from(item.delete.is_some())
                + usize::from(item.condition_check.is_some());
            std::hint::black_box(item);
        }
        std::hint::black_box(action_count_sum);
    }
    let elapsed = started.elapsed();
    PerfMeasurement {
        report: guard.finish(),
        elapsed,
    }
}

fn measure_transact_condition_failure_full_map_baseline() -> PerfMeasurement {
    let item = realistic_local_split_wire_item();
    let condition = failing_condition();

    let guard = AllocationGuard::start(
        module_path!(),
        "postgres_transact_condition_failure_full_map_baseline",
        file!(),
        line!(),
        Some("condition_failure_full_map_before"),
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

fn measure_transact_condition_failure_wire_item() -> PerfMeasurement {
    let item = realistic_local_split_wire_item();
    let condition = failing_condition();

    let guard = AllocationGuard::start(
        module_path!(),
        "postgres_transact_condition_failure_wire_item",
        file!(),
        line!(),
        Some("condition_failure_wire_after"),
    );
    let started = Instant::now();
    for _ in 0..CONDITION_EVAL_ITERATIONS {
        let matched =
            evaluate_wire_condition(Some(&item), &condition).expect("evaluate wire condition");
        std::hint::black_box(matched);
    }
    let elapsed = started.elapsed();
    PerfMeasurement {
        report: guard.finish(),
        elapsed,
    }
}

fn measure_wire_condition_to_attribute_map_baseline() -> PerfMeasurement {
    let item = realistic_local_split_wire_item();
    let condition = condition();

    let guard = AllocationGuard::start(
        module_path!(),
        "postgres_wire_condition_to_attribute_map_baseline",
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
        "postgres_wire_condition_root_lookup",
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
        "postgres_wire_repeated_condition_no_cache",
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
        "postgres_wire_repeated_condition_cached",
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

fn assert_postgres_wire_condition_uses_root_lookup() {
    let baseline = measure_wire_condition_to_attribute_map_baseline();
    let root_lookup = measure_wire_condition_root_lookup();

    emit_perf_measurement(
        "postgres_wire_condition_uses_root_lookup",
        "before",
        &baseline,
        CONDITION_EVAL_ITERATIONS,
    );
    emit_perf_measurement(
        "postgres_wire_condition_uses_root_lookup",
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

fn assert_postgres_repeated_wire_condition_uses_cache() {
    let baseline = measure_wire_repeated_condition_no_cache();
    let cached = measure_wire_repeated_condition_cached();

    emit_perf_measurement(
        "postgres_repeated_wire_condition_uses_cache",
        "before",
        &baseline,
        CONDITION_EVAL_ITERATIONS,
    );
    emit_perf_measurement(
        "postgres_repeated_wire_condition_uses_cache",
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

fn assert_postgres_transact_condition_failure_uses_wire_item() {
    let baseline = measure_transact_condition_failure_full_map_baseline();
    let wire_item = measure_transact_condition_failure_wire_item();

    emit_perf_measurement(
        "postgres_transact_condition_failure_uses_wire_item",
        "before",
        &baseline,
        CONDITION_EVAL_ITERATIONS,
    );
    emit_perf_measurement(
        "postgres_transact_condition_failure_uses_wire_item",
        "after",
        &wire_item,
        CONDITION_EVAL_ITERATIONS,
    );

    assert!(
        wire_item.report.allocation_count < baseline.report.allocation_count,
        "expected wire condition failure to allocate less often, baseline={} wire={}",
        baseline.report.allocation_count,
        wire_item.report.allocation_count
    );
    assert!(
        wire_item.report.allocated_bytes < baseline.report.allocated_bytes,
        "expected wire condition failure to allocate fewer bytes, baseline={} wire={}",
        baseline.report.allocated_bytes,
        wire_item.report.allocated_bytes
    );
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

fn failing_condition() -> storage_condition::Condition {
    parse_condition_expression(
        "#status = :inactive AND begins_with(#search, :prefix)",
        Some(&HashMap::from([
            ("#status".to_string(), "status".to_string()),
            ("#search".to_string(), "search".to_string()),
        ])),
        Some(&HashMap::from([
            (
                ":inactive".to_string(),
                AttributeValue::S("inactive".to_string()),
            ),
            (
                ":prefix".to_string(),
                AttributeValue::S("prefix".to_string()),
            ),
        ])),
    )
    .expect("parse failing condition")
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

fn repeated_root_condition_expression_inputs() -> ConditionExpressionInputs {
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

fn postgres_macro_table_request(table_name: &TableName) -> CreateTableRequest {
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
        BillingMode::PayPerRequest,
    )
}

fn condition_key() -> KeyAttributes {
    let mut key = KeyAttributes::new();
    key.insert("pk", AttributeValue::S("ORG#ALLOC".to_string()));
    key.insert("sk", AttributeValue::S("ITEM#0042".to_string()));
    key
}

fn realistic_transact_write_request() -> TransactWriteItemsRequest {
    realistic_transact_write_request_for_table(&TableName::new("pg_transact_clone_profile"))
}

fn realistic_transact_write_request_for_table(table_name: &TableName) -> TransactWriteItemsRequest {
    TransactWriteItemsRequest {
        transact_items: (0..TRANSACT_ITEM_COUNT)
            .map(|index| TransactWriteItem {
                update: Some(TransactUpdateRequest {
                    table_name: table_name.clone(),
                    key: transact_key(index),
                    update_expression: "SET #payload = :payload, #counter = #counter + :inc"
                        .to_string(),
                    condition_expression: Some(
                        "#status = :active AND begins_with(#search, :prefix)".to_string(),
                    ),
                    expression_attribute_names: Some(HashMap::from([
                        ("#payload".to_string(), "payload".to_string()),
                        ("#counter".to_string(), "counter".to_string()),
                        ("#status".to_string(), "status".to_string()),
                        ("#search".to_string(), "search".to_string()),
                    ])),
                    expression_attribute_values: Some(HashMap::from([
                        (":payload".to_string(), AttributeValue::S("y".repeat(1024))),
                        (":inc".to_string(), AttributeValue::N("1".to_string())),
                        (
                            ":active".to_string(),
                            AttributeValue::S("active".to_string()),
                        ),
                        (
                            ":prefix".to_string(),
                            AttributeValue::S("prefix".to_string()),
                        ),
                    ])),
                    return_values_on_condition_check_failure: None,
                    aux_item_stream_ttl_hours: None,
                }),
                ..TransactWriteItem::default()
            })
            .collect(),
        client_request_token: None,
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    }
}

fn transact_key(index: usize) -> KeyAttributes {
    let mut key = KeyAttributes::new();
    key.insert("pk", AttributeValue::S(format!("ORG#ALLOC#{index:04}")));
    key.insert("sk", AttributeValue::S("ITEM#0042".to_string()));
    key
}

fn live_postgres_dsn() -> String {
    std::env::var("TEST_POSTGRES_DSN")
        .or_else(|_| std::env::var("CUCUMBER_POSTGRES_DSN"))
        .unwrap_or_else(|_| "host=/tmp dbname=postgres".to_string())
}

async fn setup_live_postgres_macro_table(
    provider: &PostgresStorageProvider,
    suffix: &str,
) -> TableName {
    let table_name = TableName::new(&format!(
        "pg_cond_macro_{}_{}",
        suffix,
        uuid::Uuid::now_v7().simple()
    ));
    provider
        .create_table(&postgres_macro_table_request(&table_name))
        .await
        .expect("create postgres macro table");
    provider
        .put_item(
            table_name.clone(),
            realistic_condition_item(),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("seed postgres macro table");
    table_name
}

async fn setup_live_postgres_transact_loop_table(provider: &PostgresStorageProvider) -> TableName {
    let table_name = TableName::new(&format!("pg_txloop_{}", uuid::Uuid::now_v7().simple()));
    provider
        .create_table(&postgres_macro_table_request(&table_name))
        .await
        .expect("create postgres transaction loop table");
    for index in 0..TRANSACT_ITEM_COUNT {
        provider
            .put_item(
                table_name.clone(),
                realistic_condition_item_for_index(index),
                None,
                None,
                None,
                None,
            )
            .await
            .expect("seed postgres transaction loop table");
    }
    table_name
}

async fn measure_postgres_conditional_put_macro_full_map_baseline(
    provider: &PostgresStorageProvider,
    table_name: &TableName,
) -> PerfMeasurement {
    let item = realistic_condition_item();
    let (condition_expression, names, values) = repeated_root_condition_expression_inputs();
    let key = condition_key();

    let guard = AllocationGuard::start(
        module_path!(),
        "postgres_conditional_put_macro_full_map_baseline",
        file!(),
        line!(),
        Some("macro_full_map_before"),
    );
    let started = Instant::now();
    for _ in 0..MACRO_CONDITION_ITERATIONS {
        let old_item = provider
            .get_item(table_name.clone(), key.clone(), true)
            .await
            .expect("get old postgres wire item")
            .expect("old postgres wire item");
        let condition =
            parse_condition_expression(&condition_expression, names.as_ref(), values.as_ref())
                .expect("parse postgres condition");
        let old_map = old_item.to_attribute_map().expect("decode old item");
        assert!(evaluate_condition(&old_map, &condition));
        provider
            .put_item(table_name.clone(), item.clone(), None, None, None, None)
            .await
            .expect("put postgres item");
    }
    let elapsed = started.elapsed();
    PerfMeasurement {
        report: guard.finish(),
        elapsed,
    }
}

async fn measure_postgres_conditional_put_macro_cached(
    provider: &PostgresStorageProvider,
    table_name: &TableName,
) -> PerfMeasurement {
    let item = realistic_condition_item();
    let (condition_expression, names, values) = repeated_root_condition_expression_inputs();

    let guard = AllocationGuard::start(
        module_path!(),
        "postgres_conditional_put_macro_cached",
        file!(),
        line!(),
        Some("macro_cached_after"),
    );
    let started = Instant::now();
    for _ in 0..MACRO_CONDITION_ITERATIONS {
        provider
            .put_item(
                table_name.clone(),
                item.clone(),
                Some(condition_expression.clone()),
                names.clone(),
                values.clone(),
                None,
            )
            .await
            .expect("conditional put postgres item");
    }
    let elapsed = started.elapsed();
    PerfMeasurement {
        report: guard.finish(),
        elapsed,
    }
}

async fn assert_postgres_conditional_put_macro_uses_cached_wire_condition() {
    let provider = PostgresStorageProvider::new_with_tls(&live_postgres_dsn(), 8, 2, false)
        .await
        .expect("postgres provider");
    provider
        .initialize_storage()
        .await
        .expect("initialize postgres storage");

    let baseline_table = setup_live_postgres_macro_table(&provider, "before").await;
    let cached_table = setup_live_postgres_macro_table(&provider, "after").await;
    let baseline =
        measure_postgres_conditional_put_macro_full_map_baseline(&provider, &baseline_table).await;
    let cached = measure_postgres_conditional_put_macro_cached(&provider, &cached_table).await;

    emit_perf_measurement(
        "postgres_conditional_put_macro_uses_cached_wire_condition",
        "before",
        &baseline,
        MACRO_CONDITION_ITERATIONS,
    );
    emit_perf_measurement(
        "postgres_conditional_put_macro_uses_cached_wire_condition",
        "after",
        &cached,
        MACRO_CONDITION_ITERATIONS,
    );

    provider
        .delete_table(&baseline_table)
        .await
        .expect("delete baseline postgres macro table");
    provider
        .delete_table(&cached_table)
        .await
        .expect("delete cached postgres macro table");

    assert!(
        cached.report.allocation_count < baseline.report.allocation_count,
        "expected cached Postgres macro path to allocate less often, baseline={} cached={}",
        baseline.report.allocation_count,
        cached.report.allocation_count
    );
    assert!(
        cached.report.allocated_bytes < baseline.report.allocated_bytes,
        "expected cached Postgres macro path to allocate fewer bytes, baseline={} cached={}",
        baseline.report.allocated_bytes,
        cached.report.allocated_bytes
    );
}

async fn measure_postgres_transact_condition_failure_full_map_baseline(
    provider: &PostgresStorageProvider,
    table_name: &TableName,
) -> PerfMeasurement {
    let (condition_expression, names, values) = failing_condition_expression_inputs();
    let key = condition_key();

    let guard = AllocationGuard::start(
        module_path!(),
        "postgres_transact_condition_failure_full_map_baseline",
        file!(),
        line!(),
        Some("transact_failure_full_map_before"),
    );
    let started = Instant::now();
    for _ in 0..MACRO_CONDITION_ITERATIONS {
        let old_item = provider
            .get_item(table_name.clone(), key.clone(), true)
            .await
            .expect("get old postgres wire item")
            .expect("old postgres wire item");
        let condition =
            parse_condition_expression(&condition_expression, names.as_ref(), values.as_ref())
                .expect("parse postgres condition");
        let old_map = old_item.to_attribute_map().expect("decode old item");
        assert!(!evaluate_condition(&old_map, &condition));
    }
    let elapsed = started.elapsed();
    PerfMeasurement {
        report: guard.finish(),
        elapsed,
    }
}

async fn measure_postgres_transact_condition_failure_wire_path(
    provider: &PostgresStorageProvider,
    table_name: &TableName,
) -> PerfMeasurement {
    let (condition_expression, names, values) = failing_condition_expression_inputs();
    let key = condition_key();

    let guard = AllocationGuard::start(
        module_path!(),
        "postgres_transact_condition_failure_wire_path",
        file!(),
        line!(),
        Some("transact_failure_wire_after"),
    );
    let started = Instant::now();
    for _ in 0..MACRO_CONDITION_ITERATIONS {
        let old_item = provider
            .get_item(table_name.clone(), key.clone(), true)
            .await
            .expect("get old postgres wire item")
            .expect("old postgres wire item");
        let condition =
            parse_condition_expression(&condition_expression, names.as_ref(), values.as_ref())
                .expect("parse postgres condition");
        let matched =
            evaluate_wire_condition(Some(&old_item), &condition).expect("evaluate wire condition");
        assert!(!matched);
    }
    let elapsed = started.elapsed();
    PerfMeasurement {
        report: guard.finish(),
        elapsed,
    }
}

async fn assert_postgres_transact_condition_failure_macro_uses_wire_condition() {
    let provider = PostgresStorageProvider::new_with_tls(&live_postgres_dsn(), 8, 2, false)
        .await
        .expect("postgres provider");
    provider
        .initialize_storage()
        .await
        .expect("initialize postgres storage");

    let table = setup_live_postgres_macro_table(&provider, "txfail").await;
    let baseline =
        measure_postgres_transact_condition_failure_full_map_baseline(&provider, &table).await;
    let wire_path = measure_postgres_transact_condition_failure_wire_path(&provider, &table).await;

    emit_perf_measurement(
        "postgres_transact_condition_failure_macro_uses_wire_condition",
        "before",
        &baseline,
        MACRO_CONDITION_ITERATIONS,
    );
    emit_perf_measurement(
        "postgres_transact_condition_failure_macro_uses_wire_condition",
        "after",
        &wire_path,
        MACRO_CONDITION_ITERATIONS,
    );

    provider
        .delete_table(&table)
        .await
        .expect("delete transact failure postgres macro table");
}

async fn measure_postgres_transact_write_loop_live_clone_items(
    provider: &PostgresStorageProvider,
    request: &TransactWriteItemsRequest,
) -> PerfMeasurement {
    let guard = AllocationGuard::start(
        module_path!(),
        "postgres_transact_write_loop_live_clone_items",
        file!(),
        line!(),
        Some("live_clone_items_before"),
    );
    let started = Instant::now();
    for _ in 0..MACRO_CONDITION_ITERATIONS {
        let attempt_request = request.clone();
        for item in attempt_request.transact_items.clone() {
            let Some(update) = item.update else {
                continue;
            };
            let old_item = provider
                .get_item(update.table_name.clone(), update.key.clone(), true)
                .await
                .expect("get live transaction loop item")
                .expect("live transaction loop item");
            let condition = parse_condition_expression(
                update.condition_expression.as_deref().expect("condition"),
                update.expression_attribute_names.as_ref(),
                update.expression_attribute_values.as_ref(),
            )
            .expect("parse live transaction loop condition");
            assert!(evaluate_wire_condition(Some(&old_item), &condition).expect("evaluate"));
        }
    }
    let elapsed = started.elapsed();
    PerfMeasurement {
        report: guard.finish(),
        elapsed,
    }
}

async fn measure_postgres_transact_write_loop_live_move_items(
    provider: &PostgresStorageProvider,
    request: &TransactWriteItemsRequest,
) -> PerfMeasurement {
    let guard = AllocationGuard::start(
        module_path!(),
        "postgres_transact_write_loop_live_move_items",
        file!(),
        line!(),
        Some("live_move_items_after"),
    );
    let started = Instant::now();
    for _ in 0..MACRO_CONDITION_ITERATIONS {
        let attempt_request = request.clone();
        for item in attempt_request.transact_items {
            let Some(update) = item.update else {
                continue;
            };
            let old_item = provider
                .get_item(update.table_name.clone(), update.key.clone(), true)
                .await
                .expect("get live transaction loop item")
                .expect("live transaction loop item");
            let condition = parse_condition_expression(
                update.condition_expression.as_deref().expect("condition"),
                update.expression_attribute_names.as_ref(),
                update.expression_attribute_values.as_ref(),
            )
            .expect("parse live transaction loop condition");
            assert!(evaluate_wire_condition(Some(&old_item), &condition).expect("evaluate"));
        }
    }
    let elapsed = started.elapsed();
    PerfMeasurement {
        report: guard.finish(),
        elapsed,
    }
}

async fn assert_postgres_transact_write_loop_macro_moves_attempt_items() {
    let provider = PostgresStorageProvider::new_with_tls(&live_postgres_dsn(), 8, 2, false)
        .await
        .expect("postgres provider");
    provider
        .initialize_storage()
        .await
        .expect("initialize postgres storage");

    let table = setup_live_postgres_transact_loop_table(&provider).await;
    let request = realistic_transact_write_request_for_table(&table);
    let baseline = measure_postgres_transact_write_loop_live_clone_items(&provider, &request).await;
    let moved = measure_postgres_transact_write_loop_live_move_items(&provider, &request).await;

    emit_perf_measurement(
        "postgres_transact_write_loop_macro_moves_attempt_items",
        "before",
        &baseline,
        MACRO_CONDITION_ITERATIONS * TRANSACT_ITEM_COUNT,
    );
    emit_perf_measurement(
        "postgres_transact_write_loop_macro_moves_attempt_items",
        "after",
        &moved,
        MACRO_CONDITION_ITERATIONS * TRANSACT_ITEM_COUNT,
    );

    provider
        .delete_table(&table)
        .await
        .expect("delete transaction loop postgres macro table");
}

async fn measure_postgres_transact_preflight_repeated_provider_cache(
    provider: &PostgresStorageProvider,
    request: &TransactWriteItemsRequest,
    table_name: &TableName,
) -> PerfMeasurement {
    let guard = AllocationGuard::start(
        module_path!(),
        "postgres_transact_preflight_repeated_provider_cache",
        file!(),
        line!(),
        Some("repeated_provider_cache_before"),
    );
    let started = Instant::now();
    for _ in 0..MACRO_CONDITION_ITERATIONS {
        for item in &request.transact_items {
            let table_info = provider
                .get_table_info_cached_arc(table_name)
                .await
                .expect("cached table info");
            let preflight = preflight_transact_item_key_with_table_info(item, &table_info)
                .expect("preflight transaction item");
            std::hint::black_box(preflight);
        }
    }
    let elapsed = started.elapsed();
    PerfMeasurement {
        report: guard.finish(),
        elapsed,
    }
}

async fn measure_postgres_transact_preflight_request_table_cache(
    provider: &PostgresStorageProvider,
    request: &TransactWriteItemsRequest,
    table_name: &TableName,
) -> PerfMeasurement {
    let guard = AllocationGuard::start(
        module_path!(),
        "postgres_transact_preflight_request_table_cache",
        file!(),
        line!(),
        Some("request_table_cache_after"),
    );
    let started = Instant::now();
    for _ in 0..MACRO_CONDITION_ITERATIONS {
        let table_info = provider
            .get_table_info_cached_arc(table_name)
            .await
            .expect("cached table info");
        for item in &request.transact_items {
            let preflight = preflight_transact_item_key_with_table_info(item, &table_info)
                .expect("preflight transaction item");
            std::hint::black_box(preflight);
        }
    }
    let elapsed = started.elapsed();
    PerfMeasurement {
        report: guard.finish(),
        elapsed,
    }
}

async fn assert_postgres_transact_preflight_macro_uses_request_table_cache() {
    let provider = PostgresStorageProvider::new_with_tls(&live_postgres_dsn(), 8, 2, false)
        .await
        .expect("postgres provider");
    provider
        .initialize_storage()
        .await
        .expect("initialize postgres storage");

    let table = setup_live_postgres_transact_loop_table(&provider).await;
    let request = realistic_transact_write_request_for_table(&table);
    let baseline =
        measure_postgres_transact_preflight_repeated_provider_cache(&provider, &request, &table)
            .await;
    let cached =
        measure_postgres_transact_preflight_request_table_cache(&provider, &request, &table).await;

    emit_perf_measurement(
        "postgres_transact_preflight_macro_uses_request_table_cache",
        "before",
        &baseline,
        MACRO_CONDITION_ITERATIONS * TRANSACT_ITEM_COUNT,
    );
    emit_perf_measurement(
        "postgres_transact_preflight_macro_uses_request_table_cache",
        "after",
        &cached,
        MACRO_CONDITION_ITERATIONS * TRANSACT_ITEM_COUNT,
    );

    provider
        .delete_table(&table)
        .await
        .expect("delete transaction preflight postgres macro table");

    assert!(
        cached.report.allocation_count <= baseline.report.allocation_count,
        "expected request table cache not to allocate more often, baseline={} cached={}",
        baseline.report.allocation_count,
        cached.report.allocation_count
    );
    assert!(
        cached.elapsed <= baseline.elapsed.mul_f32(1.05),
        "expected request table cache not to regress CPU by more than 5%, baseline={:?} \
         cached={:?}",
        baseline.elapsed,
        cached.elapsed
    );
}

fn failing_condition_expression_inputs() -> ConditionExpressionInputs {
    (
        "#status = :inactive AND begins_with(#search, :prefix)".to_string(),
        Some(HashMap::from([
            ("#status".to_string(), "status".to_string()),
            ("#search".to_string(), "search".to_string()),
        ])),
        Some(HashMap::from([
            (
                ":inactive".to_string(),
                AttributeValue::S("inactive".to_string()),
            ),
            (
                ":prefix".to_string(),
                AttributeValue::S("prefix".to_string()),
            ),
        ])),
    )
}

fn realistic_condition_item() -> HashMap<String, AttributeValue> {
    let mut item = HashMap::from([
        ("pk".to_string(), AttributeValue::S("ORG#ALLOC".to_string())),
        ("sk".to_string(), AttributeValue::S("ITEM#0042".to_string())),
        (
            "entity_type".to_string(),
            AttributeValue::S("ALLOC_PROFILE".to_string()),
        ),
        ("revision".to_string(), AttributeValue::N("42".to_string())),
        (
            "ttl".to_string(),
            AttributeValue::N("2200000042".to_string()),
        ),
        (
            "status".to_string(),
            AttributeValue::S("active".to_string()),
        ),
        (
            "owner".to_string(),
            AttributeValue::S("tenant-a".repeat(16)),
        ),
        (
            "search".to_string(),
            AttributeValue::S("prefix-value".to_string()),
        ),
        (
            "tags".to_string(),
            AttributeValue::SS(vec!["hot".to_string(), "tracked".to_string()]),
        ),
    ]);
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

fn realistic_condition_item_for_index(index: usize) -> HashMap<String, AttributeValue> {
    let mut item = realistic_condition_item();
    item.insert(
        "pk".to_string(),
        AttributeValue::S(format!("ORG#ALLOC#{index:04}")),
    );
    item
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

fn failed_condition_item() -> HashMap<String, AttributeValue> {
    let mut item = realistic_condition_item();
    item.insert(
        "status".to_string(),
        AttributeValue::S("inactive".to_string()),
    );
    item
}
