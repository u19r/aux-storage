use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use alloc_counter::AllocationGuard;
use storage::DatabaseManager;
use storage_types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateGlobalSecondaryIndex,
    CreateTableRequest, IndexName, KeyAttributeType, KeyAttributes, KeySchemaElement, KeyType,
    Projection, ProjectionType, QueryRequest, TableName, TransactGetItem, TransactGetItemsRequest,
    TransactGetRequest,
};

use crate::{
    manager::{StorageApiManager, StorageApiManagerImpl, StorageApiManagerOptions},
    types::Response,
};

const ITEM_COUNT: usize = 60;
const QUERY_ITERATIONS: usize = 80;
const TRANSACT_GET_ITERATIONS: usize = 120;

struct ReadPerfFixture {
    manager: StorageApiManagerImpl,
    table_name: TableName,
}

#[tokio::test(flavor = "current_thread")]
async fn realistic_query_manager_allocation_profile_tests() {
    let fixture = ReadPerfFixture::new("ReadPerfQueryAlloc").await;
    let request = query_request(&fixture.table_name);
    let guard = AllocationGuard::start(
        module_path!(),
        "realistic_query_manager_allocation_profile_tests",
        file!(),
        line!(),
        Some("items_60_attrs_10_gsis_2_ttl"),
    );

    for _ in 0..QUERY_ITERATIONS {
        let response = fixture
            .manager
            .query(request.clone())
            .await
            .expect("query succeeds");
        assert_query_count(response, ITEM_COUNT as u32);
    }

    let report = guard.finish();
    alloc_counter::emit_report(&report);
    assert!(report.allocation_count > 0);
    assert!(report.allocated_bytes > 0);
}

#[tokio::test(flavor = "current_thread")]
async fn realistic_transact_get_manager_allocation_profile_tests() {
    let fixture = ReadPerfFixture::new("ReadPerfTxnGetAlloc").await;
    let request = transact_get_request(&fixture.table_name);
    let guard = AllocationGuard::start(
        module_path!(),
        "realistic_transact_get_manager_allocation_profile_tests",
        file!(),
        line!(),
        Some("items_25_projected_attrs_10_gsis_2_ttl"),
    );

    for _ in 0..TRANSACT_GET_ITERATIONS {
        let response = fixture
            .manager
            .transact_get_items(request.clone())
            .await
            .expect("transact get succeeds");
        assert_transact_get_count(response, 25);
    }

    let report = guard.finish();
    alloc_counter::emit_report(&report);
    assert!(report.allocation_count > 0);
    assert!(report.allocated_bytes > 0);
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "manual runtime perf probe; run with --ignored --nocapture --test-threads=1"]
async fn realistic_query_manager_runtime_perf_probe() {
    let fixture = ReadPerfFixture::new("ReadPerfQueryRuntime").await;
    let request = query_request(&fixture.table_name);
    let elapsed = measure_query_runtime(&fixture.manager, &request).await;
    println!(
        "realistic_query_manager iterations={QUERY_ITERATIONS} items_per_iter={ITEM_COUNT} \
         elapsed_ms={:.3} ns_per_iter={:.2}",
        elapsed.as_secs_f64() * 1_000.0,
        elapsed.as_nanos() as f64 / QUERY_ITERATIONS as f64
    );
    assert!(elapsed.as_nanos() > 0);
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "manual runtime perf probe; run with --ignored --nocapture --test-threads=1"]
async fn realistic_transact_get_manager_runtime_perf_probe() {
    let fixture = ReadPerfFixture::new("ReadPerfTxnGetRuntime").await;
    let request = transact_get_request(&fixture.table_name);
    let elapsed = measure_transact_get_runtime(&fixture.manager, &request).await;
    println!(
        "realistic_transact_get_manager iterations={TRANSACT_GET_ITERATIONS} items_per_iter=25 \
         elapsed_ms={:.3} ns_per_iter={:.2}",
        elapsed.as_secs_f64() * 1_000.0,
        elapsed.as_nanos() as f64 / TRANSACT_GET_ITERATIONS as f64
    );
    assert!(elapsed.as_nanos() > 0);
}

async fn measure_query_runtime(
    manager: &StorageApiManagerImpl,
    request: &QueryRequest,
) -> Duration {
    let started = Instant::now();
    for _ in 0..QUERY_ITERATIONS {
        let response = manager
            .query(request.clone())
            .await
            .expect("query succeeds");
        assert_query_count(response, ITEM_COUNT as u32);
    }
    started.elapsed()
}

async fn measure_transact_get_runtime(
    manager: &StorageApiManagerImpl,
    request: &TransactGetItemsRequest,
) -> Duration {
    let started = Instant::now();
    for _ in 0..TRANSACT_GET_ITERATIONS {
        let response = manager
            .transact_get_items(request.clone())
            .await
            .expect("transact get succeeds");
        assert_transact_get_count(response, 25);
    }
    started.elapsed()
}

impl ReadPerfFixture {
    async fn new(table_name: &str) -> Self {
        let db = Arc::new(DatabaseManager::new_for_test().await.expect("db"));
        let table_name = TableName::new(table_name);
        db.create_table(&create_table_request(&table_name))
            .await
            .expect("create table");
        for index in 0..ITEM_COUNT {
            db.put_item(storage::PutItemInput {
                table_name: table_name.clone(),
                item: realistic_item(index).into(),
                condition_expression: None,
                expression_attribute_names: None,
                expression_attribute_values: None,
                return_values: None,
                aux_item_stream_ttl_hours: None,
            })
            .await
            .expect("seed item");
        }
        let manager =
            StorageApiManagerImpl::new_with_options(db, StorageApiManagerOptions::default());
        Self {
            manager,
            table_name,
        }
    }
}

fn create_table_request(table_name: &TableName) -> CreateTableRequest {
    CreateTableRequest::new(
        table_name.clone(),
        vec![
            attr("pk", KeyAttributeType::S),
            attr("sk", KeyAttributeType::S),
            attr("gsi1pk", KeyAttributeType::S),
            attr("gsi1sk", KeyAttributeType::S),
            attr("gsi2pk", KeyAttributeType::S),
            attr("gsi2sk", KeyAttributeType::S),
        ],
        vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)],
        BillingMode::PayPerRequest,
    )
    .with_global_secondary_indexes(Some(vec![
        gsi("gsi1", "gsi1pk", "gsi1sk"),
        gsi("gsi2", "gsi2pk", "gsi2sk"),
    ]))
}

fn attr(name: &str, attribute_type: KeyAttributeType) -> AttributeDefinition {
    AttributeDefinition {
        attribute_name: name.to_string(),
        attribute_type,
    }
}

fn key(name: &str, key_type: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_string(),
        key_type,
    }
}

fn gsi(name: &str, pk: &str, sk: &str) -> CreateGlobalSecondaryIndex {
    CreateGlobalSecondaryIndex {
        index_name: IndexName::new(name),
        key_schema: vec![key(pk, KeyType::Hash), key(sk, KeyType::Range)],
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    }
}

fn realistic_item(index: usize) -> HashMap<String, AttributeValue> {
    let mut item = HashMap::with_capacity(14);
    item.insert("pk".to_string(), AttributeValue::S(partition_key()));
    item.insert("sk".to_string(), AttributeValue::S(sort_key(index)));
    item.insert(
        "gsi1pk".to_string(),
        AttributeValue::S(gsi_key("acct", index)),
    );
    item.insert(
        "gsi1sk".to_string(),
        AttributeValue::S(gsi_key("created", index)),
    );
    item.insert(
        "gsi2pk".to_string(),
        AttributeValue::S(gsi_key("status", index)),
    );
    item.insert(
        "gsi2sk".to_string(),
        AttributeValue::S(gsi_key("bucket", index)),
    );
    item.insert(
        "ttl".to_string(),
        AttributeValue::N((1_900_000_000 + index as u64).to_string()),
    );
    for attr_index in 0..7 {
        item.insert(
            format!("attr_{attr_index}"),
            AttributeValue::S(realistic_value(index, attr_index)),
        );
    }
    item.insert("meta".to_string(), realistic_nested_value(index));
    item
}

fn partition_key() -> String {
    format!("tenant#{}", "p".repeat(92))
}

fn sort_key(index: usize) -> String {
    format!("item#{index:04}#{}", "s".repeat(90))
}

fn gsi_key(prefix: &str, index: usize) -> String {
    format!("{prefix}#{index:04}#{}", "g".repeat(88))
}

fn realistic_value(item_index: usize, attr_index: usize) -> String {
    let target_len = 800 + ((item_index + attr_index) % 8) * 100;
    format!(
        "value#{item_index:04}#{attr_index:02}#{}",
        "v".repeat(target_len)
    )
}

fn realistic_nested_value(index: usize) -> AttributeValue {
    let mut map = HashMap::with_capacity(3);
    map.insert(
        "child".to_string(),
        AttributeValue::S(format!("nested#{index}#{}", "n".repeat(900))),
    );
    map.insert("count".to_string(), AttributeValue::N(index.to_string()));
    map.insert(
        "events".to_string(),
        AttributeValue::L(vec![
            event_value(index, "created"),
            event_value(index, "updated"),
        ]),
    );
    AttributeValue::M(map)
}

fn event_value(index: usize, event_type: &str) -> AttributeValue {
    let mut map = HashMap::with_capacity(2);
    map.insert(
        "name".to_string(),
        AttributeValue::S(event_type.to_string()),
    );
    map.insert(
        "payload".to_string(),
        AttributeValue::S(realistic_value(index, 0)),
    );
    AttributeValue::M(map)
}

fn query_request(table_name: &TableName) -> QueryRequest {
    let mut request = QueryRequest::new(
        table_name.clone(),
        "#pk = :pk AND begins_with(#sk, :sk_prefix)".to_string(),
    );
    request.expression_attribute_names = Some(HashMap::from([
        ("#pk".to_string(), "pk".to_string()),
        ("#sk".to_string(), "sk".to_string()),
    ]));
    request.expression_attribute_values = Some(HashMap::from([
        (":pk".to_string(), AttributeValue::S(partition_key())),
        (
            ":sk_prefix".to_string(),
            AttributeValue::S("item#".to_string()),
        ),
    ]));
    request
}

fn transact_get_request(table_name: &TableName) -> TransactGetItemsRequest {
    TransactGetItemsRequest {
        transact_items: (0..25)
            .map(|index| TransactGetItem {
                get: TransactGetRequest {
                    table_name: table_name.clone(),
                    key: KeyAttributes::from([
                        ("pk".to_string(), AttributeValue::S(partition_key())),
                        ("sk".to_string(), AttributeValue::S(sort_key(index))),
                    ]),
                    projection_expression: Some(
                        "pk, sk, ttl, gsi1pk, gsi1sk, gsi2pk, gsi2sk, #meta.child, #meta.count"
                            .to_string(),
                    ),
                    expression_attribute_names: Some(HashMap::from([(
                        "#meta".to_string(),
                        "meta".to_string(),
                    )])),
                },
            })
            .collect(),
        return_consumed_capacity: None,
    }
}

fn assert_query_count(response: Response, expected: u32) {
    let Response::QueryWire(response) = response else {
        panic!("expected wire query response");
    };
    assert_eq!(response.count, expected);
}

fn assert_transact_get_count(response: Response, expected: usize) {
    let Response::TransactGetItems(response) = response else {
        panic!("expected transact get response");
    };
    assert_eq!(response.responses.len(), expected);
}
