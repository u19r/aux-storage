//! AWS emulator behavioural suite ports.
//!
//! Table lifecycle, global secondary indexes, time to live and DynamoDB
//! Streams. Expected values are AWS's, taken from the behavioural scripts;
//! a deliberate aux difference is named where it is asserted instead.

use std::{
    collections::BTreeSet,
    panic::{AssertUnwindSafe, resume_unwind},
    time::Duration,
};

use futures::{FutureExt as _, future::BoxFuture};
use serde_json::{Value, json};
use storage_types::{
    CreateTableRequest, DescribeStreamRequest, DescribeTableRequest, DescribeTimeToLiveRequest,
    GetRecordsRequest, GetShardIteratorRequest, ListStreamsRequest, StorageError, TableName,
    TimeToLiveStatus, TimestampMillis, UpdateTimeToLiveRequest,
};

use super::{
    dynamodb_stream_history_tests::{
        create_stream_table, describe_shards, find_populated_shard, iterator, put_value,
    },
    test_support::{TestStore, assert_dynamo_error_response_exact, unique_table_name},
};

const NOT_FOUND: &str = "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException";
const IN_USE: &str = "com.amazonaws.dynamodb.v20120810#ResourceInUseException";
const LIMIT: &str = "com.amazonaws.dynamodb.v20120810#LimitExceededException";
const CONDITIONAL: &str = "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException";
const VALIDATION: &str = "com.amazon.coral.validate#ValidationException";
const ONE_ONLINE_INDEX: &str = "Subscriber limit exceeded: Only 1 online index can be created or \
                                deleted simultaneously per table";
/// A wait for asynchronous publication gives up after this long.
const POLL_BOUND: Duration = Duration::from_secs(10);

/// Run `body` and release the store even when an assertion fails.
async fn with_store(body: impl for<'a> FnOnce(&'a TestStore) -> BoxFuture<'a, ()>) {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(body(&store)).catch_unwind().await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

fn assert_error(error: StorageError, error_type: &str, message: &str) {
    assert_dynamo_error_response_exact(
        error,
        axum::http::StatusCode::BAD_REQUEST,
        &json!({"__type": error_type, "message": message}).to_string(),
    );
}

fn expect_err<T>(result: Result<T, StorageError>) -> StorageError {
    match result {
        Ok(_) => panic!("operation must be rejected"),
        Err(error) => error,
    }
}

fn ok<T>(result: Result<T, StorageError>, what: &str) -> T {
    result.unwrap_or_else(|error| panic!("{what}: {:?}", error.as_ref()))
}

fn table_not_found(table: &TableName) -> String {
    format!("Requested resource not found: Table: {table} not found")
}

/// An item operation on a missing table answers the bare text AWS answers.
fn data_plane_not_found(_table: &TableName) -> String {
    "Requested resource not found".to_owned()
}

fn tf<T>(value: Value) -> T
where
    T: TryFrom<Value>,
    T::Error: std::fmt::Debug,
{
    T::try_from(value).unwrap()
}

fn attrs(defs: &[(&str, &str)]) -> Value {
    Value::Array(
        defs.iter()
            .map(|(name, kind)| json!({"AttributeName": name, "AttributeType": kind}))
            .collect(),
    )
}

fn keys(schema: &[(&str, &str)]) -> Value {
    Value::Array(
        schema
            .iter()
            .map(|(name, kind)| json!({"AttributeName": name, "KeyType": kind}))
            .collect(),
    )
}

fn index(name: &str, schema: &[(&str, &str)], projection: Value) -> Value {
    json!({"IndexName": name, "KeySchema": keys(schema), "Projection": projection})
}

fn all() -> Value {
    json!({"ProjectionType": "ALL"})
}

fn keys_only() -> Value {
    json!({"ProjectionType": "KEYS_ONLY"})
}

async fn create(
    store: &TestStore,
    table: &TableName,
    defs: &[(&str, &str)],
    schema: &[(&str, &str)],
    indexes: Vec<Value>,
) -> Value {
    let mut request = json!({
        "TableName": table.as_ref(),
        "AttributeDefinitions": attrs(defs),
        "KeySchema": keys(schema),
        "BillingMode": "PAY_PER_REQUEST",
    });
    if !indexes.is_empty() {
        request["GlobalSecondaryIndexes"] = Value::Array(indexes);
    }
    let created = ok(
        store
            .operations
            .create_table(tf::<CreateTableRequest>(request))
            .await,
        "create table",
    );
    serde_json::to_value(created.table_description).unwrap()
}

async fn describe(store: &TestStore, table: &TableName) -> Value {
    let described = ok(
        store
            .operations
            .describe_table(DescribeTableRequest {
                table_name: table.clone(),
            })
            .await,
        "describe table",
    );
    serde_json::to_value(described.table).unwrap()
}

async fn update(store: &TestStore, request: Value) -> Result<Value, StorageError> {
    store
        .operations
        .update_table(tf(request))
        .await
        .map(|response| serde_json::to_value(response.table_description).unwrap())
}

async fn put(store: &TestStore, table: &TableName, item: Value) {
    let request = tf(json!({"TableName": table.as_ref(), "Item": item}));
    ok(store.operations.put_item(request).await, "put item");
}

async fn get(store: &TestStore, table: &TableName, key: Value) -> Value {
    let request = tf(json!({"TableName": table.as_ref(), "Key": key}));
    serde_json::to_value(ok(store.operations.get_item(request).await, "get item")).unwrap()
}

async fn query(
    store: &TestStore,
    table: &TableName,
    index: Option<&str>,
    condition: &str,
    values: Value,
    extra: Value,
) -> Value {
    let mut request = json!({
        "TableName": table.as_ref(),
        "KeyConditionExpression": condition,
        "ExpressionAttributeValues": values,
    });
    if let Some(index) = index {
        request["IndexName"] = json!(index);
    }
    if let Value::Object(map) = extra {
        for (name, value) in map {
            request[name] = value;
        }
    }
    let response = ok(store.operations.query(tf(request)).await, "query");
    serde_json::to_value(response).unwrap()
}

async fn scan(store: &TestStore, table: &TableName, index: Option<&str>, extra: Value) -> Value {
    let mut request = json!({"TableName": table.as_ref()});
    if let Some(index) = index {
        request["IndexName"] = json!(index);
    }
    if let Value::Object(map) = extra {
        for (name, value) in map {
            request[name] = value;
        }
    }
    let response = ok(store.operations.scan(tf(request)).await, "scan");
    serde_json::to_value(response).unwrap()
}

async fn query_pages(
    store: &TestStore,
    table: &TableName,
    index: &str,
    condition: &str,
    values: Value,
    forward: bool,
    limit: u32,
) -> Vec<Value> {
    let mut pages = Vec::new();
    let mut start = Value::Null;
    for _ in 0..50 {
        let mut extra = json!({"Limit": limit, "ScanIndexForward": forward});
        if !start.is_null() {
            extra["ExclusiveStartKey"] = start.clone();
        }
        let page = query(store, table, Some(index), condition, values.clone(), extra).await;
        start = page.get("LastEvaluatedKey").cloned().unwrap_or(Value::Null);
        pages.push(page);
        if start.is_null() {
            return pages;
        }
    }
    panic!("paged query did not finish within 50 pages");
}

fn items(response: &Value) -> Vec<Value> {
    response["Items"].as_array().cloned().unwrap_or_default()
}

fn strings(items: &[Value], attribute: &str) -> Vec<String> {
    items
        .iter()
        .map(|item| item[attribute]["S"].as_str().unwrap().to_owned())
        .collect()
}

fn sorted(mut values: Vec<String>) -> Vec<String> {
    values.sort();
    values
}

fn field_names(value: &Value) -> BTreeSet<String> {
    value.as_object().unwrap().keys().cloned().collect()
}

fn names(list: &Value, attribute: &str) -> Vec<String> {
    list.as_array()
        .map(|entries| {
            entries
                .iter()
                .map(|entry| entry[attribute].as_str().unwrap().to_owned())
                .collect()
        })
        .unwrap_or_default()
}

fn index_names(description: &Value) -> Vec<String> {
    names(&description["GlobalSecondaryIndexes"], "IndexName")
}

fn attribute_definitions(description: &Value) -> Vec<String> {
    sorted(names(&description["AttributeDefinitions"], "AttributeName"))
}

async fn enable_stream(store: &TestStore, table: &TableName) {
    super::table_stream_eligibility_tests::set_stream(store, table, true)
        .await
        .unwrap();
}

fn create_index_request(table: &TableName, name: &str, attribute: &str) -> Value {
    json!({
        "TableName": table.as_ref(),
        "AttributeDefinitions": [{"AttributeName": attribute, "AttributeType": "S"}],
        "GlobalSecondaryIndexUpdates": [{"Create": index(name, &[(attribute, "HASH")], all())}],
    })
}

fn delete_index_request(table: &TableName, names: &[&str]) -> Value {
    json!({
        "TableName": table.as_ref(),
        "GlobalSecondaryIndexUpdates": names
            .iter()
            .map(|name| json!({"Delete": {"IndexName": name}}))
            .collect::<Vec<_>>(),
    })
}

async fn finish_backfill(store: &TestStore, page_limit: usize) {
    super::gsi_lifecycle_tests::finish_backfill(store, page_limit)
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Table lifecycle
// ---------------------------------------------------------------------------

const FANOUT_TABLES: usize = 8;

/// AWS behavioural: check_50_table_fanout_item_isolation_and_scan_exactness.py
///
/// The script's 50 tables and wall-clock budgets are reduced to a bounded
/// count.
#[tokio::test]
async fn given_tables_with_identical_item_keys_when_written_concurrently_then_each_scan_holds_one_item_and_duplicate_creates_fail()
 {
    with_store(|store| {
        Box::pin(async move {
            let tables: Vec<TableName> = (0..FANOUT_TABLES).map(|_| unique_table_name()).collect();
            let created = futures::future::join_all(tables.iter().map(|table| {
                store
                    .operations
                    .create_table(super::test_support::hash_table_request(table.clone()))
            }))
            .await;
            for result in created {
                ok(result, "concurrent create");
            }
            let written = futures::future::join_all(tables.iter().map(|table| {
                let request = tf(json!({
                    "TableName": table.as_ref(),
                    "Item": {"pk": {"S": "shared-key"}, "owner": {"S": table.as_ref()}},
                }));
                store.operations.put_item(request)
            }))
            .await;
            for result in written {
                ok(result, "concurrent put");
            }
            for table in &tables {
                let scanned = scan(store, table, None, json!({"ConsistentRead": true})).await;
                assert_eq!(scanned["Count"], 1);
                assert_eq!(scanned["ScannedCount"], 1);
                assert_eq!(items(&scanned)[0]["owner"], json!({"S": table.as_ref()}));
            }

            let duplicates = futures::future::join_all(tables.iter().map(|table| {
                store
                    .operations
                    .create_table(super::test_support::hash_table_request(table.clone()))
            }))
            .await;
            for (table, result) in tables.iter().zip(duplicates) {
                assert_error(
                    expect_err(result),
                    IN_USE,
                    &format!("Table already exists: {table}"),
                );
            }

            for table in &tables {
                ok(
                    store
                        .operations
                        .delete_table(tf(json!({"TableName": table.as_ref()})))
                        .await,
                    "delete table",
                );
                let error = expect_err(
                    store
                        .operations
                        .describe_table(DescribeTableRequest {
                            table_name: table.clone(),
                        })
                        .await,
                );
                assert_error(error, NOT_FOUND, &table_not_found(table));
                let error = expect_err(
                    store
                        .operations
                        .put_item(tf(json!({
                            "TableName": table.as_ref(),
                            "Item": {"pk": {"S": "shared-key"}},
                        })))
                        .await,
                );
                assert_error(error, NOT_FOUND, &data_plane_not_found(table));
            }
        })
    })
    .await;
}

/// AWS behavioural: check_concurrent_create_table_single_winner.py
#[tokio::test]
async fn given_eight_concurrent_creates_of_one_name_when_resolved_then_one_wins_and_seven_report_creating()
 {
    with_store(|store| {
        Box::pin(async move {
            let table = unique_table_name();
            let results = futures::future::join_all((0..8).map(|_| {
                store
                    .operations
                    .create_table(super::test_support::hash_table_request(table.clone()))
            }))
            .await;
            let mut winners = 0;
            for result in results {
                match result {
                    Ok(created) => {
                        winners += 1;
                        assert_eq!(created.table_description.table_name, table);
                    }
                    // Aux creates synchronously, so a loser always finds an
                    // existing table (AWS may instead report "Table is being
                    // created").
                    Err(error) => {
                        assert_error(error, IN_USE, &format!("Table already exists: {table}"));
                    }
                }
            }
            assert_eq!(winners, 1);
            let listed = ok(
                store
                    .operations
                    .list_tables(tf(json!({"Limit": 100})))
                    .await,
                "list tables",
            );
            assert_eq!(
                listed
                    .table_names
                    .iter()
                    .filter(|name| **name == table)
                    .count(),
                1
            );
        })
    })
    .await;
}

/// AWS behavioural:
/// check_binary_range_key_item_set_and_nested_path_updates_then_table_refuses_reads.
/// py AWS behavioural: check_create_describe_delete.py
/// AWS behavioural: check_delete_table_twice_rejected_not_silently_succeeds.py
/// AWS behavioural: check_put_item_on_deleting_table_rejected.py
///
/// Capacity arguments and the typed UpdateItem steps of the scripts are
/// dropped (capacity is intentionally unsupported; UpdateItem is covered by
/// the item tests).
#[tokio::test]
async fn given_numeric_hash_binary_range_table_when_deleted_then_every_access_reports_not_found_and_name_is_reusable()
 {
    with_store(|store| {
        Box::pin(async move {
            let table = unique_table_name();
            let created = create(
                store,
                &table,
                &[("tenant", "N"), ("blob", "B")],
                &[("tenant", "HASH"), ("blob", "RANGE")],
                vec![],
            )
            .await;
            assert_eq!(created["TableName"], table.as_ref());
            let described = describe(store, &table).await;
            assert_eq!(described["TableStatus"], "ACTIVE");
            assert_eq!(
                described["KeySchema"],
                keys(&[("tenant", "HASH"), ("blob", "RANGE")])
            );
            assert_eq!(
                sorted(
                    described["AttributeDefinitions"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|definition| format!(
                            "{}:{}",
                            definition["AttributeName"].as_str().unwrap(),
                            definition["AttributeType"].as_str().unwrap()
                        ))
                        .collect()
                ),
                ["blob:B", "tenant:N"]
            );
            // aux reports a fixed on-demand summary for every table.
            assert_eq!(
                described["BillingModeSummary"]["BillingMode"],
                "PAY_PER_REQUEST"
            );
            let item = json!({"tenant": {"N": "7"}, "blob": {"B": "AAEC"}, "v": {"S": "typed"}});
            put(store, &table, item.clone()).await;
            let key = json!({"tenant": {"N": "7"}, "blob": {"B": "AAEC"}});
            assert_eq!(get(store, &table, key.clone()).await["Item"], item);

            ok(
                store
                    .operations
                    .delete_table(tf(json!({"TableName": table.as_ref()})))
                    .await,
                "delete table",
            );
            let not_found = table_not_found(&table);
            let data_plane = data_plane_not_found(&table);
            assert_error(
                expect_err(
                    store
                        .operations
                        .describe_table(DescribeTableRequest {
                            table_name: table.clone(),
                        })
                        .await,
                ),
                NOT_FOUND,
                &not_found,
            );
            assert_error(
                expect_err(
                    store
                        .operations
                        .get_item(tf(json!({"TableName": table.as_ref(), "Key": key})))
                        .await,
                ),
                NOT_FOUND,
                &data_plane,
            );
            assert_error(
                expect_err(
                    store
                        .operations
                        .query(tf(json!({
                            "TableName": table.as_ref(),
                            "KeyConditionExpression": "tenant = :t",
                            "ExpressionAttributeValues": {":t": {"N": "7"}},
                        })))
                        .await,
                ),
                NOT_FOUND,
                &data_plane,
            );
            assert_error(
                expect_err(
                    store
                        .operations
                        .put_item(tf(json!({"TableName": table.as_ref(), "Item": item})))
                        .await,
                ),
                NOT_FOUND,
                &data_plane,
            );
            let listed = ok(
                store.operations.list_tables(tf(json!({}))).await,
                "list tables",
            );
            assert!(!listed.table_names.contains(&table));
            assert_error(
                expect_err(
                    store
                        .operations
                        .delete_table(tf(json!({"TableName": table.as_ref()})))
                        .await,
                ),
                NOT_FOUND,
                &not_found,
            );

            let recreated = create(store, &table, &[("pk", "S")], &[("pk", "HASH")], vec![]).await;
            assert_eq!(recreated["TableName"], table.as_ref());
        })
    })
    .await;
}

/// AWS behavioural: check_table_with_sort_key.py
#[tokio::test]
async fn given_hash_and_range_string_keys_when_described_then_key_schema_lists_both() {
    with_store(|store| {
        Box::pin(async move {
            let table = unique_table_name();
            create(
                store,
                &table,
                &[("pk", "S"), ("sk", "S")],
                &[("pk", "HASH"), ("sk", "RANGE")],
                vec![],
            )
            .await;
            assert_eq!(
                describe(store, &table).await["KeySchema"],
                keys(&[("pk", "HASH"), ("sk", "RANGE")])
            );
        })
    })
    .await;
}

/// AWS behavioural:
/// check_deletion_protection_permits_item_deletes_and_survives_a_stream_update.
/// py
#[tokio::test]
async fn given_protected_table_when_items_are_deleted_and_stream_enabled_then_table_delete_is_refused_until_unprotected()
 {
    with_store(|store| {
        Box::pin(async move {
            let table = unique_table_name();
            let request = tf::<CreateTableRequest>(json!({
                "TableName": table.as_ref(),
                "AttributeDefinitions": attrs(&[("pk", "S")]),
                "KeySchema": keys(&[("pk", "HASH")]),
                "BillingMode": "PAY_PER_REQUEST",
                "DeletionProtectionEnabled": true,
            }));
            ok(store.operations.create_table(request).await, "create table");
            for ordinal in 0..2 {
                put(
                    store,
                    &table,
                    json!({"pk": {"S": format!("key{ordinal}")}, "v": {"S": format!("ROW-{ordinal}")}}),
                )
                .await;
            }
            let deleted = ok(
                store
                    .operations
                    .delete_item(tf(json!({
                        "TableName": table.as_ref(),
                        "Key": {"pk": {"S": "key0"}},
                        "ReturnValues": "ALL_OLD",
                    })))
                    .await,
                "delete item",
            );
            assert_eq!(
                serde_json::to_value(deleted).unwrap()["Attributes"],
                json!({"pk": {"S": "key0"}, "v": {"S": "ROW-0"}})
            );
            let batch = ok(
                store
                    .operations
                    .batch_write_item(tf(json!({"RequestItems": {
                        table.as_ref(): [{"DeleteRequest": {"Key": {"pk": {"S": "key1"}}}}]
                    }})))
                    .await,
                "batch delete",
            );
            assert_eq!(
                serde_json::to_value(batch).unwrap()["UnprocessedItems"],
                json!({})
            );

            let streamed = update(
                store,
                json!({
                    "TableName": table.as_ref(),
                    "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "KEYS_ONLY"},
                }),
            )
            .await
            .unwrap();
            assert_eq!(streamed["DeletionProtectionEnabled"], true);
            assert_eq!(
                streamed["StreamSpecification"],
                json!({"StreamEnabled": true, "StreamViewType": "KEYS_ONLY"})
            );

            assert_error(
                expect_err(
                    store
                        .operations
                        .delete_table(tf(json!({"TableName": table.as_ref()})))
                        .await,
                ),
                VALIDATION,
                "Resource cannot be deleted as it is currently protected against deletion. Disable deletion protection first.",
            );
            update(
                store,
                json!({"TableName": table.as_ref(), "DeletionProtectionEnabled": false}),
            )
            .await
            .unwrap();
            ok(
                store
                    .operations
                    .delete_table(tf(json!({"TableName": table.as_ref()})))
                    .await,
                "delete unprotected table",
            );
            assert_error(
                expect_err(
                    store
                        .operations
                        .describe_table(DescribeTableRequest {
                            table_name: table.clone(),
                        })
                        .await,
                ),
                NOT_FOUND,
                &table_not_found(&table),
            );
            assert_error(
                expect_err(
                    store
                        .operations
                        .delete_item(tf(json!({
                            "TableName": table.as_ref(),
                            "Key": {"pk": {"S": "key0"}},
                        })))
                        .await,
                ),
                NOT_FOUND,
                &data_plane_not_found(&table),
            );
        })
    })
    .await;
}

/// AWS behavioural: check_deep_dynamodb_2_describe_item_count_lags_scan.py
///
/// Documented aux difference (writing/dynamodb_parity.md, DescribeTable
/// estimates): ItemCount and TableSizeBytes are always 0, the first of the
/// two values AWS accepts. The consistent Scan counts every write.
#[tokio::test]
async fn given_twenty_five_writes_when_described_then_estimates_are_zero_and_consistent_scan_counts_all()
 {
    with_store(|store| {
        Box::pin(async move {
            let table = store.create_standard_table().await;
            for ordinal in 0..25 {
                put(
                    store,
                    &table,
                    json!({"pk": {"S": format!("k{ordinal:02}")}}),
                )
                .await;
            }
            let before = scan(store, &table, None, json!({"ConsistentRead": true})).await;
            assert_eq!(before["Count"], 25);
            let described = describe(store, &table).await;
            assert_eq!(described["TableStatus"], "ACTIVE");
            assert_eq!(described["ItemCount"], 0);
            assert_eq!(described["TableSizeBytes"], 0);
            let after = scan(store, &table, None, json!({"ConsistentRead": true})).await;
            assert_eq!(after["Count"], 25);
        })
    })
    .await;
}

/// AWS behavioural:
/// check_list_tables_pages_names_in_byte_order_across_punctuation.py
#[tokio::test]
async fn given_names_differing_in_punctuation_when_listed_in_pages_then_utf8_byte_order_holds() {
    with_store(|store| {
        Box::pin(async move {
            let prefix = unique_table_name();
            let expected: Vec<String> = ["-a-z", "-a.z", "-aZ", "-a_z", "-ab"]
                .iter()
                .map(|suffix| format!("{prefix}{suffix}"))
                .collect();
            for name in [
                &expected[3],
                &expected[0],
                &expected[4],
                &expected[2],
                &expected[1],
            ] {
                store.create_hash_table(TableName::new(name)).await;
            }
            for limit in [1_u32, 100] {
                let mut seen = Vec::new();
                let mut start = Value::Null;
                for _ in 0..20 {
                    let mut request = json!({"Limit": limit});
                    if !start.is_null() {
                        request["ExclusiveStartTableName"] = start.clone();
                    }
                    let page = serde_json::to_value(ok(
                        store.operations.list_tables(tf(request)).await,
                        "list tables",
                    ))
                    .unwrap();
                    let page_names = names_of(&page["TableNames"]);
                    assert!(page_names.len() <= limit as usize);
                    assert_eq!(page_names, sorted(page_names.clone()));
                    seen.extend(page_names);
                    start = page
                        .get("LastEvaluatedTableName")
                        .cloned()
                        .unwrap_or(Value::Null);
                    if start.is_null() {
                        break;
                    }
                }
                let prefix_text: &str = prefix.as_ref();
                let ours: Vec<String> = seen
                    .into_iter()
                    .filter(|name| name.starts_with(prefix_text))
                    .collect();
                assert_eq!(ours, expected, "limit {limit}");
            }
        })
    })
    .await;
}

fn names_of(list: &Value) -> Vec<String> {
    list.as_array()
        .unwrap()
        .iter()
        .map(|name| name.as_str().unwrap().to_owned())
        .collect()
}

// ---------------------------------------------------------------------------
// Global secondary indexes: foreground maintenance
// ---------------------------------------------------------------------------

/// AWS behavioural:
/// check_ddbgsi_base_item_delete_removes_the_index_entry_while_a_sibling_survives.
/// py
#[tokio::test]
async fn given_two_items_in_one_bucket_when_one_is_deleted_then_index_holds_exactly_the_sibling() {
    with_store(|store| {
        Box::pin(async move {
            let table = unique_table_name();
            create(
                store,
                &table,
                &[("pk", "S"), ("gsi1pk", "S")],
                &[("pk", "HASH")],
                vec![index("by_bucket", &[("gsi1pk", "HASH")], all())],
            )
            .await;
            for (pk, body) in [
                ("ITEM#DOOMED", "doomed-payload"),
                ("ITEM#SIBLING", "sibling-payload"),
            ] {
                put(
                    store,
                    &table,
                    json!({"pk": {"S": pk}, "gsi1pk": {"S": "BUCKET#A"}, "body": {"S": body}}),
                )
                .await;
            }
            let values = json!({":g": {"S": "BUCKET#A"}});
            let before = query(
                store,
                &table,
                Some("by_bucket"),
                "gsi1pk = :g",
                values.clone(),
                json!({}),
            )
            .await;
            assert_eq!(before["ScannedCount"], 2);
            assert_eq!(
                sorted(strings(&items(&before), "pk")),
                ["ITEM#DOOMED", "ITEM#SIBLING"]
            );

            ok(
                store
                    .operations
                    .delete_item(tf(json!({
                        "TableName": table.as_ref(),
                        "Key": {"pk": {"S": "ITEM#DOOMED"}},
                    })))
                    .await,
                "delete item",
            );
            let after = query(
                store,
                &table,
                Some("by_bucket"),
                "gsi1pk = :g",
                values,
                json!({}),
            )
            .await;
            assert_eq!(after["ScannedCount"], 1);
            assert_eq!(strings(&items(&after), "pk"), ["ITEM#SIBLING"]);
            assert_eq!(strings(&items(&after), "body"), ["sibling-payload"]);
            assert!(
                get(store, &table, json!({"pk": {"S": "ITEM#DOOMED"}}))
                    .await
                    .get("Item")
                    .is_none()
            );
            assert!(
                get(store, &table, json!({"pk": {"S": "ITEM#SIBLING"}}))
                    .await
                    .get("Item")
                    .is_some()
            );
        })
    })
    .await;
}

/// AWS behavioural:
/// check_ddbgsi_index_gains_no_entry_when_a_conditional_write_is_rejected.py
#[tokio::test]
async fn given_rejected_conditional_put_when_accepted_update_follows_then_index_holds_only_the_moved_entry()
 {
    with_store(|store| {
        Box::pin(async move {
            let table = unique_table_name();
            create(
                store,
                &table,
                &[("pk", "S"), ("gsi1pk", "S")],
                &[("pk", "HASH")],
                vec![index("by_bucket", &[("gsi1pk", "HASH")], all())],
            )
            .await;
            put(
                store,
                &table,
                json!({"pk": {"S": "ITEM#SUBJECT"}, "gsi1pk": {"S": "BUCKET#ORIGINAL"}, "body": {"S": "original"}}),
            )
            .await;
            let rejected = expect_err(
                store
                    .operations
                    .put_item(tf(json!({
                        "TableName": table.as_ref(),
                        "Item": {"pk": {"S": "ITEM#SUBJECT"}, "gsi1pk": {"S": "BUCKET#REFUSED"}, "body": {"S": "refused"}},
                        "ConditionExpression": "attribute_not_exists(pk)",
                    })))
                    .await,
            );
            assert_error(rejected, CONDITIONAL, "The conditional request failed");
            let base = get(store, &table, json!({"pk": {"S": "ITEM#SUBJECT"}})).await;
            assert_eq!(base["Item"]["gsi1pk"], json!({"S": "BUCKET#ORIGINAL"}));
            assert_eq!(base["Item"]["body"], json!({"S": "original"}));
            let refused = query(store, &table, Some("by_bucket"), "gsi1pk = :g", json!({":g": {"S": "BUCKET#REFUSED"}}), json!({})).await;
            assert_eq!(refused["Count"], 0);

            // The barrier write separates a refused key from an unprocessed one.
            put(
                store,
                &table,
                json!({"pk": {"S": "ITEM#BARRIER"}, "gsi1pk": {"S": "BUCKET#BARRIER"}, "body": {"S": "barrier"}}),
            )
            .await;
            ok(
                store
                    .operations
                    .update_item(tf(json!({
                        "TableName": table.as_ref(),
                        "Key": {"pk": {"S": "ITEM#SUBJECT"}},
                        "UpdateExpression": "SET gsi1pk = :g, body = :b",
                        "ConditionExpression": "body = :o",
                        "ExpressionAttributeValues": {
                            ":g": {"S": "BUCKET#MOVED"}, ":b": {"S": "moved"}, ":o": {"S": "original"}
                        },
                    })))
                    .await,
                "conditional update",
            );
            let moved = query(store, &table, Some("by_bucket"), "gsi1pk = :g", json!({":g": {"S": "BUCKET#MOVED"}}), json!({})).await;
            assert_eq!(strings(&items(&moved), "pk"), ["ITEM#SUBJECT"]);
            assert_eq!(strings(&items(&moved), "body"), ["moved"]);
            let original = query(store, &table, Some("by_bucket"), "gsi1pk = :g", json!({":g": {"S": "BUCKET#ORIGINAL"}}), json!({})).await;
            assert_eq!(original["Count"], 0);
            let everything = scan(store, &table, Some("by_bucket"), json!({})).await;
            assert_eq!(
                sorted(strings(&items(&everything), "gsi1pk")),
                ["BUCKET#BARRIER", "BUCKET#MOVED"]
            );
        })
    })
    .await;
}

/// AWS behavioural:
/// check_ddbgsi_projected_value_converges_in_the_index_after_an_update.py
#[tokio::test]
async fn given_include_projection_when_label_and_notes_are_updated_then_index_entry_never_shows_notes()
 {
    with_store(|store| {
        Box::pin(async move {
            let table = unique_table_name();
            create(
                store,
                &table,
                &[("pk", "S"), ("sk", "S"), ("gsi1pk", "S")],
                &[("pk", "HASH"), ("sk", "RANGE")],
                vec![index(
                    "by_group",
                    &[("gsi1pk", "HASH")],
                    json!({"ProjectionType": "INCLUDE", "NonKeyAttributes": ["label"]}),
                )],
            )
            .await;
            put(
                store,
                &table,
                json!({
                    "pk": {"S": "p"}, "sk": {"S": "s"}, "gsi1pk": {"S": "G"},
                    "label": {"S": "v1"}, "notes": {"S": "n1"}
                }),
            )
            .await;
            let expected_names: BTreeSet<String> = ["pk", "sk", "gsi1pk", "label"]
                .iter()
                .map(|name| (*name).to_owned())
                .collect();
            let values = json!({":g": {"S": "G"}});
            let first = items(
                &query(
                    store,
                    &table,
                    Some("by_group"),
                    "gsi1pk = :g",
                    values.clone(),
                    json!({}),
                )
                .await,
            );
            assert_eq!(first.len(), 1);
            assert_eq!(field_names(&first[0]), expected_names);
            assert_eq!(first[0]["label"], json!({"S": "v1"}));

            ok(
                store
                    .operations
                    .update_item(tf(json!({
                        "TableName": table.as_ref(),
                        "Key": {"pk": {"S": "p"}, "sk": {"S": "s"}},
                        "UpdateExpression": "SET label = :l, notes = :n",
                        "ExpressionAttributeValues": {":l": {"S": "v2"}, ":n": {"S": "n2"}},
                    })))
                    .await,
                "update item",
            );
            let second = items(
                &query(
                    store,
                    &table,
                    Some("by_group"),
                    "gsi1pk = :g",
                    values,
                    json!({}),
                )
                .await,
            );
            assert_eq!(second.len(), 1);
            assert_eq!(field_names(&second[0]), expected_names);
            assert_eq!(second[0]["label"], json!({"S": "v2"}));
            let base = get(store, &table, json!({"pk": {"S": "p"}, "sk": {"S": "s"}})).await;
            assert_eq!(base["Item"]["label"], json!({"S": "v2"}));
            assert_eq!(base["Item"]["notes"], json!({"S": "n2"}));
        })
    })
    .await;
}

/// AWS behavioural: check_gsi_duplicate_composite_keys_do_not_collapse.py
#[tokio::test]
async fn given_items_sharing_an_index_key_pair_when_one_is_deleted_or_moved_then_only_its_slot_changes()
 {
    with_store(|store| {
        Box::pin(async move {
            let table = unique_table_name();
            create(
                store,
                &table,
                &[("pk", "S"), ("gsi_pk", "S"), ("gsi_sk", "S")],
                &[("pk", "HASH")],
                vec![index(
                    "by_pair",
                    &[("gsi_pk", "HASH"), ("gsi_sk", "RANGE")],
                    all(),
                )],
            )
            .await;
            for ordinal in 0..3 {
                put(
                    store,
                    &table,
                    json!({
                        "pk": {"S": format!("base-{ordinal}")}, "gsi_pk": {"S": "SHARED"},
                        "gsi_sk": {"S": "SAME"}, "body": {"S": format!("payload-{ordinal}")}
                    }),
                )
                .await;
            }
            let slot = |sort: &'static str| json!({":p": {"S": "SHARED"}, ":s": {"S": sort}});
            let condition = "gsi_pk = :p AND gsi_sk = :s";
            let all_rows = query(
                store,
                &table,
                Some("by_pair"),
                condition,
                slot("SAME"),
                json!({}),
            )
            .await;
            assert_eq!(
                sorted(strings(&items(&all_rows), "pk")),
                ["base-0", "base-1", "base-2"]
            );
            ok(
                store
                    .operations
                    .delete_item(tf(
                        json!({"TableName": table.as_ref(), "Key": {"pk": {"S": "base-1"}}}),
                    ))
                    .await,
                "delete item",
            );
            let survivors = query(
                store,
                &table,
                Some("by_pair"),
                condition,
                slot("SAME"),
                json!({}),
            )
            .await;
            assert_eq!(
                sorted(strings(&items(&survivors), "pk")),
                ["base-0", "base-2"]
            );

            ok(
                store
                    .operations
                    .update_item(tf(json!({
                        "TableName": table.as_ref(),
                        "Key": {"pk": {"S": "base-0"}},
                        "UpdateExpression": "SET gsi_sk = :m",
                        "ExpressionAttributeValues": {":m": {"S": "MOVED"}},
                    })))
                    .await,
                "move item",
            );
            let old_slot = query(
                store,
                &table,
                Some("by_pair"),
                condition,
                slot("SAME"),
                json!({}),
            )
            .await;
            assert_eq!(items(&old_slot)[0]["pk"], json!({"S": "base-2"}));
            assert_eq!(old_slot["Count"], 1);
            let new_slot = query(
                store,
                &table,
                Some("by_pair"),
                condition,
                slot("MOVED"),
                json!({}),
            )
            .await;
            assert_eq!(items(&new_slot)[0]["pk"], json!({"S": "base-0"}));
            assert_eq!(new_slot["Count"], 1);
        })
    })
    .await;
}

/// AWS behavioural:
/// check_gsi_query_paging_over_tied_index_keys_matches_unpaged_read.py
#[tokio::test]
async fn given_items_tied_on_the_index_key_when_paged_with_a_limit_then_pages_equal_the_unpaged_read()
 {
    with_store(|store| {
        Box::pin(async move {
            let table = unique_table_name();
            create(
                store,
                &table,
                &[("pk", "S"), ("gsi_pk", "S")],
                &[("pk", "HASH")],
                vec![index("by_tie", &[("gsi_pk", "HASH")], all())],
            )
            .await;
            for ordinal in 0..7 {
                put(
                    store,
                    &table,
                    json!({"pk": {"S": format!("t-{ordinal}")}, "gsi_pk": {"S": "TIE"}}),
                )
                .await;
            }
            let values = json!({":g": {"S": "TIE"}});
            let unpaged = query(
                store,
                &table,
                Some("by_tie"),
                "gsi_pk = :g",
                values.clone(),
                json!({}),
            )
            .await;
            let unpaged = strings(&items(&unpaged), "pk");
            assert_eq!(unpaged.len(), 7);
            let pages = query_pages(store, &table, "by_tie", "gsi_pk = :g", values, true, 2).await;
            let paged: Vec<String> = pages
                .iter()
                .flat_map(|page| strings(&items(page), "pk"))
                .collect();
            assert_eq!(paged, unpaged);
        })
    })
    .await;
}

/// AWS behavioural: check_keys_only_gsi_string_sort_key_pages_in_byte_order.py
#[tokio::test]
async fn given_keys_only_index_with_string_sort_key_when_paged_both_ways_then_utf8_byte_order_holds()
 {
    with_store(|store| {
        Box::pin(async move {
            let table = unique_table_name();
            create(
                store,
                &table,
                &[("id", "N"), ("grp", "N"), ("label", "S")],
                &[("id", "HASH")],
                vec![index(
                    "by_label",
                    &[("grp", "HASH"), ("label", "RANGE")],
                    keys_only(),
                )],
            )
            .await;
            let labels = [
                "b",
                "B",
                "_x",
                "a.b",
                "a-b",
                "aa",
                "Z",
                "\u{e9}t\u{e9}",
                "a",
                "10",
                "9",
            ];
            for (ordinal, label) in labels.iter().enumerate() {
                put(
                    store,
                    &table,
                    json!({
                        "id": {"N": ordinal.to_string()}, "grp": {"N": "1"},
                        "label": {"S": label}, "extra": {"S": "not projected"}
                    }),
                )
                .await;
            }
            // An item without the sort key is not indexed.
            put(store, &table, json!({"id": {"N": "99"}, "grp": {"N": "1"}})).await;

            let expected_names: BTreeSet<String> = ["id", "grp", "label"]
                .iter()
                .map(|name| (*name).to_owned())
                .collect();
            let mut forward_labels: Vec<String> =
                labels.iter().map(|label| (*label).to_owned()).collect();
            forward_labels.sort();
            for forward in [true, false] {
                let pages = query_pages(
                    store,
                    &table,
                    "by_label",
                    "grp = :g",
                    json!({":g": {"N": "1"}}),
                    forward,
                    3,
                )
                .await;
                let mut seen = Vec::new();
                for page in &pages {
                    let page_items = items(page);
                    assert_eq!(page["Count"], page_items.len());
                    assert!(page_items.len() <= 3);
                    for item in &page_items {
                        assert_eq!(field_names(item), expected_names);
                    }
                    if let Some(last) = page.get("LastEvaluatedKey") {
                        assert_eq!(field_names(last), expected_names);
                    }
                    seen.extend(strings(&page_items, "label"));
                }
                let mut expected = forward_labels.clone();
                if !forward {
                    expected.reverse();
                }
                assert_eq!(seen, expected, "forward={forward}");
            }
        })
    })
    .await;
}

/// AWS behavioural:
/// check_keys_only_and_all_projections_are_exempt_from_the_attribute_budget.py
#[tokio::test]
async fn given_six_keys_only_or_all_indexes_when_table_is_created_then_all_are_accepted_in_order() {
    with_store(|store| {
        Box::pin(async move {
            let table = unique_table_name();
            let mut defs = vec![("pk".to_owned(), "S")];
            let mut indexes = Vec::new();
            for ordinal in 0..6 {
                let attribute = format!("g{ordinal}");
                indexes.push(index(
                    &format!("idx{ordinal}"),
                    &[(attribute.as_str(), "HASH")],
                    if ordinal % 2 == 0 { keys_only() } else { all() },
                ));
                defs.push((attribute, "S"));
            }
            let defs: Vec<(&str, &str)> = defs
                .iter()
                .map(|(name, kind)| (name.as_str(), *kind))
                .collect();
            let created = create(store, &table, &defs, &[("pk", "HASH")], indexes).await;
            let expected = ["idx0", "idx1", "idx2", "idx3", "idx4", "idx5"];
            assert_eq!(index_names(&created), expected);
            assert_eq!(index_names(&describe(store, &table).await), expected);
        })
    })
    .await;
}

/// AWS behavioural: check_put_item_omitting_a_gsi_key_is_a_sparse_write.py
#[tokio::test]
async fn given_item_without_the_index_key_when_put_then_it_is_stored_and_absent_from_the_index() {
    with_store(|store| {
        Box::pin(async move {
            let table = unique_table_name();
            create(
                store,
                &table,
                &[("pk", "S"), ("gpk", "S")],
                &[("pk", "HASH")],
                vec![index("gsi1", &[("gpk", "HASH")], all())],
            )
            .await;
            put(store, &table, json!({"pk": {"S": "sparse"}})).await;
            put(
                store,
                &table,
                json!({"pk": {"S": "dense"}, "gpk": {"S": "G"}}),
            )
            .await;
            assert!(
                get(store, &table, json!({"pk": {"S": "sparse"}}))
                    .await
                    .get("Item")
                    .is_some()
            );
            let queried = query(
                store,
                &table,
                Some("gsi1"),
                "gpk = :g",
                json!({":g": {"S": "G"}}),
                json!({}),
            )
            .await;
            assert_eq!(queried["Count"], 1);
            let scanned = scan(store, &table, Some("gsi1"), json!({})).await;
            assert_eq!(strings(&items(&scanned), "pk"), ["dense"]);
        })
    })
    .await;
}

/// AWS behavioural:
/// check_update_item_lsi_sort_key_move_and_combined_gsi_lsi_move.py
///
/// Local secondary indexes are validated, not created, so only the
/// GSI-key half of the script is ported.
#[tokio::test]
async fn given_indexed_item_when_its_index_key_is_updated_then_old_entry_is_gone_and_payload_is_kept()
 {
    with_store(|store| {
        Box::pin(async move {
            let table = unique_table_name();
            create(
                store,
                &table,
                &[("pk", "S"), ("gsi1pk", "S")],
                &[("pk", "HASH")],
                vec![index("gsi1", &[("gsi1pk", "HASH")], all())],
            )
            .await;
            put(
                store,
                &table,
                json!({"pk": {"S": "P"}, "gsi1pk": {"S": "X"}, "payload": {"S": "payload-31"}}),
            )
            .await;
            ok(
                store
                    .operations
                    .update_item(tf(json!({
                        "TableName": table.as_ref(),
                        "Key": {"pk": {"S": "P"}},
                        "UpdateExpression": "SET gsi1pk = :y",
                        "ExpressionAttributeValues": {":y": {"S": "Y"}},
                    })))
                    .await,
                "update item",
            );
            let old = query(
                store,
                &table,
                Some("gsi1"),
                "gsi1pk = :g",
                json!({":g": {"S": "X"}}),
                json!({}),
            )
            .await;
            assert_eq!(old["Count"], 0);
            let new = query(
                store,
                &table,
                Some("gsi1"),
                "gsi1pk = :g",
                json!({":g": {"S": "Y"}}),
                json!({}),
            )
            .await;
            assert_eq!(strings(&items(&new), "pk"), ["P"]);
            assert_eq!(strings(&items(&new), "payload"), ["payload-31"]);
            let base = get(store, &table, json!({"pk": {"S": "P"}})).await;
            assert_eq!(base["Item"]["payload"], json!({"S": "payload-31"}));
        })
    })
    .await;
}

/// AWS behavioural:
/// check_gsi_delete_prunes_unshared_attribute_definitions_while_lsi_survives.py
///
/// The script shares `shared_attr` with a local secondary index, which aux
/// does not create. A second global index keeps `shared_attr` instead.
///
/// The script waits until the index is gone and then sees `gsi_only` pruned.
/// Recorded AWS (ddbpar5b `gsi_delete_deleting`) still lists the deleted
/// index's attribute right after the delete, and aux has no completion step,
/// so aux keeps every definition.
#[tokio::test]
async fn given_two_indexes_sharing_an_attribute_when_one_is_deleted_then_attribute_definitions_stay_declared()
 {
    with_store(|store| {
        Box::pin(async move {
            let table = unique_table_name();
            create(
                store,
                &table,
                &[
                    ("pk", "S"),
                    ("sk", "S"),
                    ("gsi_only", "S"),
                    ("shared_attr", "S"),
                ],
                &[("pk", "HASH"), ("sk", "RANGE")],
                vec![
                    index(
                        "gsi-main",
                        &[("gsi_only", "HASH"), ("shared_attr", "RANGE")],
                        all(),
                    ),
                    index("gsi-keep", &[("shared_attr", "HASH")], all()),
                ],
            )
            .await;
            let item = json!({
                "pk": {"S": "p"}, "sk": {"S": "s"},
                "gsi_only": {"S": "g"}, "shared_attr": {"S": "x"}
            });
            put(store, &table, item.clone()).await;
            update(store, delete_index_request(&table, &["gsi-main"]))
                .await
                .unwrap();
            let described = describe(store, &table).await;
            assert_eq!(
                attribute_definitions(&described),
                ["gsi_only", "pk", "shared_attr", "sk"]
            );
            assert_eq!(
                get(store, &table, json!({"pk": {"S": "p"}, "sk": {"S": "s"}})).await["Item"],
                item
            );
            let queried = query(
                store,
                &table,
                None,
                "pk = :p AND sk = :s",
                json!({":p": {"S": "p"}, ":s": {"S": "s"}}),
                json!({}),
            )
            .await;
            assert_eq!(queried["Count"], 1);
            assert_eq!(items(&queried)[0], item);
        })
    })
    .await;
}

// ---------------------------------------------------------------------------
// Global secondary indexes: UpdateTable and backfill
// ---------------------------------------------------------------------------

/// AWS behavioural:
/// check_gsi_added_online_indexes_items_written_while_it_builds.py
#[tokio::test]
async fn given_index_added_online_when_items_are_written_during_the_build_then_index_serves_all_keyed_items()
 {
    with_store(|store| {
        Box::pin(async move {
            let table = store.create_standard_table().await;
            for (pk, color) in [("r1", Some("red")), ("b1", Some("blue")), ("u1", None)] {
                let mut item = json!({"pk": {"S": pk}});
                if let Some(color) = color {
                    item["color"] = json!({"S": color});
                }
                put(store, &table, item).await;
            }
            enable_stream(store, &table).await;
            update(store, create_index_request(&table, "by_color", "color"))
                .await
                .unwrap();
            for (pk, color) in [("r2", Some("red")), ("b2", Some("blue")), ("u2", None)] {
                let mut item = json!({"pk": {"S": pk}});
                if let Some(color) = color {
                    item["color"] = json!({"S": color});
                }
                put(store, &table, item).await;
            }
            finish_backfill(store, 1).await;

            let described = describe(store, &table).await;
            assert_eq!(index_names(&described), ["by_color"]);
            assert_eq!(
                described["GlobalSecondaryIndexes"][0]["IndexStatus"],
                "ACTIVE"
            );
            let red = query(
                store,
                &table,
                Some("by_color"),
                "color = :c",
                json!({":c": {"S": "red"}}),
                json!({}),
            )
            .await;
            assert_eq!(sorted(strings(&items(&red), "pk")), ["r1", "r2"]);
            let indexed = scan(store, &table, Some("by_color"), json!({})).await;
            assert_eq!(
                sorted(strings(&items(&indexed), "pk")),
                ["b1", "b2", "r1", "r2"]
            );
        })
    })
    .await;
}

/// AWS behavioural:
/// check_gsi_added_to_a_populated_table_backfills_every_preexisting_item.py
#[tokio::test]
async fn given_populated_table_when_index_is_added_then_backfill_indexes_every_item() {
    with_store(|store| {
        Box::pin(async move {
            let table = store.create_standard_table().await;
            for ordinal in 0..50 {
                put(
                    store,
                    &table,
                    json!({"pk": {"S": format!("k-{ordinal:02}")}, "gpk": {"S": format!("g{}", ordinal % 5)}}),
                )
                .await;
            }
            enable_stream(store, &table).await;
            update(store, create_index_request(&table, "by_group", "gpk"))
                .await
                .unwrap();
            finish_backfill(store, 8).await;

            let described = describe(store, &table).await;
            let index = &described["GlobalSecondaryIndexes"][0];
            assert_eq!(index["IndexStatus"], "ACTIVE");
            assert!(matches!(index.get("Backfilling"), None | Some(Value::Bool(false))));
            let mut indexed = Vec::new();
            for group in 0..5 {
                let rows = query(
                    store,
                    &table,
                    Some("by_group"),
                    "gpk = :g",
                    json!({":g": {"S": format!("g{group}")}}),
                    json!({}),
                )
                .await;
                assert_eq!(rows["Count"], 10, "group g{group}");
                indexed.extend(strings(&items(&rows), "pk"));
            }
            let expected: Vec<String> = (0..50).map(|ordinal| format!("k-{ordinal:02}")).collect();
            assert_eq!(sorted(indexed), expected);
        })
    })
    .await;
}

/// AWS behavioural:
/// check_gsi_added_to_a_populated_table_excludes_a_type_mismatched_item.py
#[tokio::test]
async fn given_item_with_a_mistyped_index_key_when_index_is_added_then_it_is_skipped_and_base_item_stays()
 {
    with_store(|store| {
        Box::pin(async move {
            let table = store.create_standard_table().await;
            put(
                store,
                &table,
                json!({"pk": {"S": "valid-1"}, "gpk": {"S": "g"}}),
            )
            .await;
            put(
                store,
                &table,
                json!({"pk": {"S": "valid-2"}, "gpk": {"S": "g"}}),
            )
            .await;
            put(
                store,
                &table,
                json!({"pk": {"S": "violator"}, "gpk": {"N": "5"}}),
            )
            .await;
            enable_stream(store, &table).await;
            update(store, create_index_request(&table, "gsi1", "gpk"))
                .await
                .unwrap();
            finish_backfill(store, 8).await;

            let queried = query(
                store,
                &table,
                Some("gsi1"),
                "gpk = :g",
                json!({":g": {"S": "g"}}),
                json!({}),
            )
            .await;
            assert_eq!(
                sorted(strings(&items(&queried), "pk")),
                ["valid-1", "valid-2"]
            );
            let scanned = scan(store, &table, Some("gsi1"), json!({})).await;
            assert_eq!(
                sorted(strings(&items(&scanned), "pk")),
                ["valid-1", "valid-2"]
            );
            assert!(
                get(store, &table, json!({"pk": {"S": "violator"}}))
                    .await
                    .get("Item")
                    .is_some()
            );
        })
    })
    .await;
}

/// AWS behavioural:
/// check_gsi_creating_blocks_other_index_but_accepts_self_delete.py
///
/// The self-delete half is the next test.
#[tokio::test]
async fn given_index_still_creating_when_another_is_added_then_limit_applies() {
    with_store(|store| {
        Box::pin(async move {
            let table = store.create_standard_table().await;
            for ordinal in 0..40 {
                put(
                    store,
                    &table,
                    json!({"pk": {"S": format!("k{ordinal:02}")}, "a_attr": {"S": "a"}, "b_attr": {"S": "b"}}),
                )
                .await;
            }
            enable_stream(store, &table).await;
            let creating = update(store, create_index_request(&table, "idx-a", "a_attr"))
                .await
                .unwrap();
            assert_eq!(creating["GlobalSecondaryIndexes"][0]["IndexStatus"], "CREATING");

            let second = expect_err(update(store, create_index_request(&table, "idx-b", "b_attr")).await);
            assert_error(second, LIMIT, ONE_ONLINE_INDEX);
            assert_eq!(index_names(&describe(store, &table).await), ["idx-a"]);
        })
    })
    .await;
}

/// AWS behavioural: check_update_table_creates_at_most_one_index_per_call.py
/// AWS behavioural: check_update_table_exceeds_limit_rejected.py
#[tokio::test]
async fn given_two_index_creates_in_one_call_when_updated_then_limit_applies_and_a_lone_create_is_accepted()
 {
    with_store(|store| {
        Box::pin(async move {
            let table = store.create_standard_table().await;
            enable_stream(store, &table).await;
            let both = json!({
                "TableName": table.as_ref(),
                "AttributeDefinitions": attrs(&[("g1", "S"), ("g2", "S")]),
                "GlobalSecondaryIndexUpdates": [
                    {"Create": index("gsi-neg", &[("g1", "HASH")], all())},
                    {"Create": index("gsi-pos", &[("g2", "HASH")], all())},
                ],
            });
            assert_error(expect_err(update(store, both).await), LIMIT, ONE_ONLINE_INDEX);
            assert!(index_names(&describe(store, &table).await).is_empty());

            let lone = json!({
                "TableName": table.as_ref(),
                "AttributeDefinitions": attrs(&[("g2", "S")]),
                "GlobalSecondaryIndexUpdates": [{"Create": index("gsi-pos", &[("g2", "HASH")], all())}],
            });
            update(store, lone).await.unwrap();
            assert_eq!(index_names(&describe(store, &table).await), ["gsi-pos"]);
        })
    })
    .await;
}

/// AWS behavioural: check_update_table_delete_of_an_absent_gsi_rejected.py
#[tokio::test]
async fn given_absent_index_name_when_deleted_then_not_found_and_existing_index_is_kept() {
    with_store(|store| {
        Box::pin(async move {
            let table = unique_table_name();
            create(
                store,
                &table,
                &[("pk", "S"), ("keep_attr", "S")],
                &[("pk", "HASH")],
                vec![index("by-keep", &[("keep_attr", "HASH")], all())],
            )
            .await;
            let error =
                expect_err(update(store, delete_index_request(&table, &["by-nothing"])).await);
            assert_error(
                error,
                NOT_FOUND,
                &format!("Requested resource not found: Index by-nothing for table {table}"),
            );
            assert_eq!(index_names(&describe(store, &table).await), ["by-keep"]);

            update(store, delete_index_request(&table, &["by-keep"]))
                .await
                .unwrap();
            assert!(index_names(&describe(store, &table).await).is_empty());
        })
    })
    .await;
}

/// AWS behavioural:
/// check_update_table_duplicate_and_bulk_gsi_delete_rejected.py
#[tokio::test]
async fn given_duplicate_or_bulk_index_deletes_when_updated_then_rejected_and_one_delete_succeeds()
{
    with_store(|store| {
        Box::pin(async move {
            let table = unique_table_name();
            let letters = ["a", "b", "c", "d", "e", "f"];
            let attribute_names: Vec<String> = letters
                .iter()
                .map(|letter| format!("attr_{letter}"))
                .collect();
            let mut defs = vec![("pk", "S")];
            defs.extend(attribute_names.iter().map(|name| (name.as_str(), "S")));
            let indexes = letters
                .iter()
                .zip(&attribute_names)
                .map(|(letter, attribute)| {
                    index(
                        &format!("idx-{letter}"),
                        &[(attribute.as_str(), "HASH")],
                        all(),
                    )
                })
                .collect();
            create(store, &table, &defs, &[("pk", "HASH")], indexes).await;
            let expected: Vec<String> = letters
                .iter()
                .map(|letter| format!("idx-{letter}"))
                .collect();

            let duplicate =
                expect_err(update(store, delete_index_request(&table, &["idx-a", "idx-a"])).await);
            assert_error(
                duplicate,
                VALIDATION,
                "One or more parameter values were invalid: Only one global secondary index \
                 update per index is allowed simultaneously. Index: idx-a",
            );
            assert_eq!(index_names(&describe(store, &table).await), expected);

            let all_names: Vec<&str> = expected.iter().map(String::as_str).collect();
            let bulk = expect_err(update(store, delete_index_request(&table, &all_names)).await);
            assert_error(bulk, LIMIT, ONE_ONLINE_INDEX);
            assert_eq!(index_names(&describe(store, &table).await), expected);

            update(store, delete_index_request(&table, &["idx-a"]))
                .await
                .unwrap();
            assert_eq!(index_names(&describe(store, &table).await), &expected[1..]);
        })
    })
    .await;
}

/// AWS behavioural:
/// check_update_table_gsi_attribute_definitions_track_live_keys.py
///
/// The provisioned-throughput no-op step is dropped (capacity unsupported).
#[tokio::test]
async fn given_index_creates_and_deletes_when_updated_then_attribute_definitions_grow_with_creates_and_stay_after_deletes()
 {
    with_store(|store| {
        Box::pin(async move {
            let table = store.create_standard_table().await;
            enable_stream(store, &table).await;
            let undeclared = json!({
                "TableName": table.as_ref(),
                "GlobalSecondaryIndexUpdates": [{"Create": index("gsi-bad", &[("undeclared_attr", "HASH")], all())}],
            });
            assert_error(
                expect_err(update(store, undeclared).await),
                VALIDATION,
                "One or more parameter values were invalid: Some index key attributes are not defined in AttributeDefinitions. Keys: [undeclared_attr], AttributeDefinitions: [pk]",
            );

            let declared = json!({
                "TableName": table.as_ref(),
                "AttributeDefinitions": attrs(&[("gk", "S"), ("unused", "S")]),
                "GlobalSecondaryIndexUpdates": [{"Create": index("gsi-live", &[("gk", "HASH")], all())}],
            });
            update(store, declared).await.unwrap();
            assert_eq!(attribute_definitions(&describe(store, &table).await), ["gk", "pk"]);

            finish_backfill(store, 8).await;
            update(store, delete_index_request(&table, &["gsi-live"]))
                .await
                .unwrap();
            // The script sees `gk` pruned once the index is gone; recorded AWS (ddbpar5b
            // `gsi_delete_deleting`) still lists it right after the delete, and aux has no
            // completion step, so aux keeps it.
            assert_eq!(attribute_definitions(&describe(store, &table).await), ["gk", "pk"]);
        })
    })
    .await;
}

/// AWS behavioural: check_update_table_rejects_a_duplicate_index_name.py
#[tokio::test]
async fn given_existing_index_name_when_created_again_then_rejected_and_original_key_is_unchanged()
{
    with_store(|store| {
        Box::pin(async move {
            let table = unique_table_name();
            create(
                store,
                &table,
                &[("pk", "S"), ("dup_attr", "S")],
                &[("pk", "HASH")],
                vec![index("by-dup", &[("dup_attr", "HASH")], all())],
            )
            .await;
            enable_stream(store, &table).await;
            let again = json!({
                "TableName": table.as_ref(),
                "AttributeDefinitions": attrs(&[("other_attr", "S")]),
                "GlobalSecondaryIndexUpdates": [{"Create": index("by-dup", &[("other_attr", "HASH")], all())}],
            });
            assert_error(
                expect_err(update(store, again).await),
                VALIDATION,
                "Attempting to create an index which already exists",
            );
            let described = describe(store, &table).await;
            assert_eq!(index_names(&described), ["by-dup"]);
            assert_eq!(
                described["GlobalSecondaryIndexes"][0]["KeySchema"],
                keys(&[("dup_attr", "HASH")])
            );
        })
    })
    .await;
}

// ---------------------------------------------------------------------------
// Time to live
// ---------------------------------------------------------------------------

async fn ttl_update(
    store: &TestStore,
    table: &TableName,
    attribute: &str,
    enabled: bool,
) -> Result<(), StorageError> {
    let request = serde_json::from_value::<UpdateTimeToLiveRequest>(json!({
        "TableName": table.as_ref(),
        "TimeToLiveSpecification": {"AttributeName": attribute, "Enabled": enabled},
    }))
    .unwrap();
    store
        .operations
        .update_time_to_live(request)
        .await
        .map(|_| ())
}

async fn ttl_state(store: &TestStore, table: &TableName) -> (TimeToLiveStatus, Option<String>) {
    let described = ok(
        store
            .operations
            .describe_time_to_live(DescribeTimeToLiveRequest {
                table_name: table.clone(),
            })
            .await,
        "describe ttl",
    );
    let description = described.time_to_live_description.unwrap();
    (description.time_to_live_status, description.attribute_name)
}

fn assert_ttl_enabled(state: (TimeToLiveStatus, Option<String>), attribute: &str) {
    assert!(matches!(
        state.0,
        TimeToLiveStatus::Enabling | TimeToLiveStatus::Enabled
    ));
    assert_eq!(state.1.as_deref(), Some(attribute));
}

/// AWS behavioural: check_ttl_rename_while_enabled_rejected.py
#[tokio::test]
async fn given_ttl_enabled_on_one_attribute_when_another_is_enabled_then_rejected_and_fresh_table_accepts()
 {
    with_store(|store| {
        Box::pin(async move {
            let table = store.create_standard_table().await;
            ttl_update(store, &table, "expires_at", true).await.unwrap();
            let error = expect_err(ttl_update(store, &table, "purge_after", true).await);
            assert_error(
                error,
                VALIDATION,
                "TimeToLive is active on a different AttributeName: current AttributeName is \
                 expires_at",
            );
            assert_ttl_enabled(ttl_state(store, &table).await, "expires_at");

            let control = store.create_standard_table().await;
            ttl_update(store, &control, "purge_after", true)
                .await
                .unwrap();
            assert_ttl_enabled(ttl_state(store, &control).await, "purge_after");
        })
    })
    .await;
}

/// AWS behavioural:
/// check_update_ttl_attribute_name_exceeding_max_length_rejected.py
/// AWS behavioural: check_update_ttl_invalid_attribute.py
#[tokio::test]
async fn given_ttl_attribute_name_outside_one_to_255_characters_when_enabled_then_rejected_and_255_is_accepted()
 {
    with_store(|store| {
        Box::pin(async move {
            let table = store.create_standard_table().await;
            for (name, bound) in [
                (String::new(), "greater than or equal to 1"),
                ("a".repeat(256), "less than or equal to 255"),
            ] {
                let error = expect_err(ttl_update(store, &table, &name, true).await);
                assert_error(
                    error,
                    VALIDATION,
                    &format!(
                        "1 validation error detected: Value '{name}' at \
                         'timeToLiveSpecification.attributeName' failed to satisfy constraint: \
                         Member must have length {bound}"
                    ),
                );
                assert_eq!(ttl_state(store, &table).await.0, TimeToLiveStatus::Disabled);
            }
            let longest = "a".repeat(255);
            ttl_update(store, &table, &longest, true).await.unwrap();
            assert_ttl_enabled(ttl_state(store, &table).await, &longest);
        })
    })
    .await;
}

// ---------------------------------------------------------------------------
// DynamoDB Streams
// ---------------------------------------------------------------------------

/// Read every shard of `arn` from its start in pages of `limit` records,
/// repeating until `expected` records are visible. Returns the size of every
/// page read in the final round and the serialized records.
async fn drain_stream(
    store: &TestStore,
    arn: &str,
    limit: u32,
    expected: usize,
) -> (Vec<usize>, Vec<Value>) {
    let started = tokio::time::Instant::now();
    loop {
        store.wait_until_changes_are_readable().await;
        let mut sizes = Vec::new();
        let mut records = Vec::new();
        for shard in describe_shards(store, arn).await {
            let mut position = iterator(store, arn, &shard.shard_id, "TRIM_HORIZON", None).await;
            for _ in 0..200 {
                let page = ok(
                    store
                        .operations
                        .get_records(GetRecordsRequest {
                            shard_iterator: position.clone(),
                            limit: Some(limit),
                        })
                        .await,
                    "get records",
                );
                sizes.push(page.records.len());
                if page.records.is_empty() {
                    break;
                }
                records.extend(
                    page.records
                        .iter()
                        .map(|record| serde_json::to_value(record).unwrap()),
                );
                match page.next_shard_iterator {
                    Some(next) => position = next,
                    None => break,
                }
            }
        }
        if records.len() >= expected {
            return (sizes, records);
        }
        assert!(
            started.elapsed() < POLL_BOUND,
            "waited for {expected} stream records, saw {}",
            records.len()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn record_key(record: &Value) -> String {
    record["dynamodb"]["Keys"]["pk"]["S"]
        .as_str()
        .unwrap()
        .to_owned()
}

/// AWS behavioural: check_concurrent_writes_each_appear_exactly_once.py
#[tokio::test]
async fn given_sixteen_concurrent_puts_to_distinct_keys_when_stream_is_drained_then_each_key_has_one_distinct_insert()
 {
    with_store(|store| {
        Box::pin(async move {
            let (table, arn) = create_stream_table(store, "NEW_AND_OLD_IMAGES").await;
            let expected: Vec<String> = (0..16)
                .map(|ordinal| format!("conc-{ordinal:02}"))
                .collect();
            futures::future::join_all(
                expected
                    .iter()
                    .map(|key| put_value(store, &table, key, key)),
            )
            .await;
            let (_, records) = drain_stream(store, &arn, 100, 16).await;
            assert_eq!(records.len(), 16);
            assert!(records.iter().all(|record| record["eventName"] == "INSERT"));
            assert_eq!(sorted(records.iter().map(record_key).collect()), expected);
            let event_ids: BTreeSet<&str> = records
                .iter()
                .map(|record| record["eventID"].as_str().unwrap())
                .collect();
            assert_eq!(event_ids.len(), 16);
            let sequences: BTreeSet<&str> = records
                .iter()
                .map(|record| record["dynamodb"]["SequenceNumber"].as_str().unwrap())
                .collect();
            assert_eq!(sequences.len(), 16);
            for record in &records {
                assert_eq!(
                    record["dynamodb"]["NewImage"]["value"]["S"],
                    record["dynamodb"]["Keys"]["pk"]["S"]
                );
            }
        })
    })
    .await;
}

/// AWS behavioural:
/// check_stream_small_limit_walk_sees_every_record_once_and_unknown_shard_not_found.
/// py
#[tokio::test]
async fn given_records_read_two_at_a_time_when_every_shard_is_walked_then_each_key_is_seen_once_and_unknown_shard_is_refused()
 {
    with_store(|store| {
        Box::pin(async move {
            let (table, arn) = create_stream_table(store, "NEW_AND_OLD_IMAGES").await;
            let expected: Vec<String> = (0..7).map(|ordinal| format!("walk-{ordinal}")).collect();
            for key in &expected {
                put_value(store, &table, key, "v").await;
            }
            let (sizes, records) = drain_stream(store, &arn, 2, 7).await;
            assert!(sizes.iter().all(|size| *size <= 2), "page sizes {sizes:?}");
            assert!(records.iter().all(|record| record["eventName"] == "INSERT"));
            assert_eq!(sorted(records.iter().map(record_key).collect()), expected);

            let error = expect_err(
                store
                    .operations
                    .get_shard_iterator(GetShardIteratorRequest {
                        stream_arn: arn.clone(),
                        shard_id: "shardId-00000000000000000000-00000000".into(),
                        shard_iterator_type: "TRIM_HORIZON".into(),
                        sequence_number: None,
                    })
                    .await,
            );
            assert_error(
                error,
                NOT_FOUND,
                "Requested resource not found: Shard does not exist",
            );
        })
    })
    .await;
}

/// AWS behavioural: check_describe_stream_shard_pagination_covers_all_shards.py
#[tokio::test]
async fn given_describe_stream_limited_to_one_shard_when_walked_then_pages_cover_the_unpaged_shard_set_once()
 {
    with_store(|store| {
        Box::pin(async move {
            let (table, arn) = create_stream_table(store, "NEW_AND_OLD_IMAGES").await;
            put_value(store, &table, "shard-walk", "v").await;
            let describe_page = |start: Option<String>, limit: Option<u32>| {
                let arn = arn.clone();
                async move {
                    ok(
                        store
                            .operations
                            .describe_stream(DescribeStreamRequest {
                                stream_arn: arn,
                                exclusive_start_shard_id: start,
                                limit,
                                shard_filter: None,
                            })
                            .await,
                        "describe stream",
                    )
                    .stream_description
                }
            };
            let full = describe_page(None, None).await;
            assert_eq!(full.stream_arn, arn);
            assert_eq!(full.table_name, table);
            assert_eq!(full.stream_status, "ENABLED");
            assert_eq!(
                serde_json::to_value(&full.stream_view_type).unwrap(),
                "NEW_AND_OLD_IMAGES"
            );
            assert_eq!(
                serde_json::to_value(&full.key_schema).unwrap(),
                keys(&[("pk", "HASH")])
            );
            assert_eq!(arn.rsplit_once("/stream/").unwrap().1, full.stream_label);
            assert!(full.last_evaluated_shard_id.is_none());
            let expected: Vec<String> = full
                .shards
                .iter()
                .map(|shard| shard.shard_id.clone())
                .collect();

            let mut walked = Vec::new();
            let mut start = None;
            for _ in 0..64 {
                let page = describe_page(start.clone(), Some(1)).await;
                assert_eq!(page.shards.len(), 1);
                walked.push(page.shards[0].shard_id.clone());
                match page.last_evaluated_shard_id {
                    Some(token) => {
                        assert_eq!(token, page.shards[0].shard_id);
                        start = Some(token);
                    }
                    None => break,
                }
            }
            assert_eq!(walked, expected);
        })
    })
    .await;
}

async fn update_stream(store: &TestStore, table: &TableName, specification: Value) {
    update(
        store,
        json!({"TableName": table.as_ref(), "StreamSpecification": specification}),
    )
    .await
    .unwrap();
}

/// AWS behavioural:
/// check_disable_closes_shards_and_reenable_mints_new_stream.py
///
/// Documented aux difference (writing/dynamodb_parity.md, table streams): a
/// disabled stream is unreadable, so the reads of the disabled stream and the
/// both-ARNs ListStreams step are dropped.
#[tokio::test]
async fn given_stream_disabled_and_reenabled_with_another_view_then_new_stream_is_open_and_table_delete_reports_not_found()
 {
    with_store(|store| {
        Box::pin(async move {
            let (table, first_arn) = create_stream_table(store, "NEW_AND_OLD_IMAGES").await;
            let first_label = describe(store, &table).await["LatestStreamLabel"].clone();
            put_value(store, &table, "item", "one").await;

            update_stream(store, &table, json!({"StreamEnabled": false})).await;
            assert!(
                describe(store, &table)
                    .await
                    .get("StreamSpecification")
                    .is_none()
            );

            update_stream(
                store,
                &table,
                json!({"StreamEnabled": true, "StreamViewType": "KEYS_ONLY"}),
            )
            .await;
            let described = describe(store, &table).await;
            assert_eq!(
                described["StreamSpecification"],
                json!({"StreamEnabled": true, "StreamViewType": "KEYS_ONLY"})
            );
            let arn = described["LatestStreamArn"].as_str().unwrap().to_owned();
            assert_ne!(arn, first_arn);
            assert_ne!(described["LatestStreamLabel"], first_label);
            let stream = ok(
                store
                    .operations
                    .describe_stream(DescribeStreamRequest {
                        stream_arn: arn.clone(),
                        exclusive_start_shard_id: None,
                        limit: None,
                        shard_filter: None,
                    })
                    .await,
                "describe new stream",
            )
            .stream_description;
            assert_eq!(stream.stream_status, "ENABLED");
            assert_eq!(
                serde_json::to_value(&stream.stream_view_type).unwrap(),
                "KEYS_ONLY"
            );
            assert!(
                stream
                    .shards
                    .iter()
                    .all(|shard| shard.sequence_number_range.ending_sequence_number.is_none())
            );

            ok(
                store
                    .operations
                    .delete_table(tf(json!({"TableName": table.as_ref()})))
                    .await,
                "delete table",
            );
            assert_error(
                expect_err(
                    store
                        .operations
                        .describe_table(DescribeTableRequest {
                            table_name: table.clone(),
                        })
                        .await,
                ),
                NOT_FOUND,
                &table_not_found(&table),
            );
            assert_error(
                expect_err(
                    store
                        .operations
                        .put_item(tf(
                            json!({"TableName": table.as_ref(), "Item": {"pk": {"S": "item"}}}),
                        ))
                        .await,
                ),
                NOT_FOUND,
                &data_plane_not_found(&table),
            );
        })
    })
    .await;
}

/// AWS behavioural: check_foreign_sequence_number_rejected.py
#[tokio::test]
async fn given_foreign_or_missing_sequence_number_when_iterator_is_requested_then_rejected_and_own_sequence_returns_its_record()
 {
    with_store(|store| {
        Box::pin(async move {
            let (table, arn) = create_stream_table(store, "NEW_AND_OLD_IMAGES").await;
            put_value(store, &table, "seq-item", "7").await;
            store.wait_until_changes_are_readable().await;
            let shard = find_populated_shard(store, &arn).await;
            let start = iterator(store, &arn, &shard, "TRIM_HORIZON", None).await;
            let page = ok(
                store
                    .operations
                    .get_records(GetRecordsRequest {
                        shard_iterator: start,
                        limit: Some(10),
                    })
                    .await,
                "get records",
            );
            let own = page.records[0].dynamodb.sequence_number.clone();

            for (sequence, message) in [
                (
                    Some("000000000000000000000".to_owned()),
                    "Invalid SequenceNumber for the shard",
                ),
                (
                    None,
                    "One or more parameter values were invalid: SequenceNumber cannot be null",
                ),
            ] {
                let error = expect_err(
                    store
                        .operations
                        .get_shard_iterator(GetShardIteratorRequest {
                            stream_arn: arn.clone(),
                            shard_id: shard.clone(),
                            shard_iterator_type: "AT_SEQUENCE_NUMBER".into(),
                            sequence_number: sequence,
                        })
                        .await,
                );
                assert_error(error, VALIDATION, message);
            }
            let shards = describe_shards(store, &arn).await;
            let open = shards
                .iter()
                .find(|candidate| candidate.shard_id == shard)
                .unwrap();
            assert!(open.sequence_number_range.ending_sequence_number.is_none());

            let at_own =
                iterator(store, &arn, &shard, "AT_SEQUENCE_NUMBER", Some(own.clone())).await;
            let again = ok(
                store
                    .operations
                    .get_records(GetRecordsRequest {
                        shard_iterator: at_own,
                        limit: Some(1),
                    })
                    .await,
                "get records at own sequence",
            );
            let record = serde_json::to_value(&again.records[0]).unwrap();
            assert_eq!(record["dynamodb"]["NewImage"]["value"], json!({"S": "7"}));
            assert_eq!(record["dynamodb"]["SequenceNumber"], own);
        })
    })
    .await;
}

/// AWS behavioural: check_malformed_shard_iterator_rejected.py
#[tokio::test]
async fn given_arbitrary_or_truncated_shard_iterator_when_records_are_read_then_rejected_and_intact_iterator_replays()
 {
    with_store(|store| {
        Box::pin(async move {
            let (table, arn) = create_stream_table(store, "NEW_AND_OLD_IMAGES").await;
            put_value(store, &table, "iterator-item", "1").await;
            store.wait_until_changes_are_readable().await;
            let shard = find_populated_shard(store, &arn).await;
            let real = iterator(store, &arn, &shard, "TRIM_HORIZON", None).await;
            let truncated: String = real.chars().take(real.chars().count() - 10).collect();
            for bad in ["sim2real-behav-not-an-iterator".to_owned(), truncated] {
                let error = expect_err(
                    store
                        .operations
                        .get_records(GetRecordsRequest {
                            shard_iterator: bad,
                            limit: Some(10),
                        })
                        .await,
                );
                assert_error(error, VALIDATION, "Invalid ShardIterator");
            }
            let replay = ok(
                store
                    .operations
                    .get_records(GetRecordsRequest {
                        shard_iterator: real,
                        limit: Some(10),
                    })
                    .await,
                "replay intact iterator",
            );
            assert_eq!(replay.records.len(), 1);
            assert_eq!(
                serde_json::to_value(&replay.records[0]).unwrap()["dynamodb"]["Keys"],
                json!({"pk": {"S": "iterator-item"}})
            );
            let fresh = iterator(store, &arn, &shard, "TRIM_HORIZON", None).await;
            let fresh = ok(
                store
                    .operations
                    .get_records(GetRecordsRequest {
                        shard_iterator: fresh,
                        limit: Some(10),
                    })
                    .await,
                "fresh iterator",
            );
            assert_eq!(
                serde_json::to_value(&fresh.records[0]).unwrap()["dynamodb"]["NewImage"]["value"],
                json!({"S": "1"})
            );
        })
    })
    .await;
}

/// AWS behavioural: check_nonexistent_stream_arn_rejected.py
#[tokio::test]
async fn given_well_formed_arn_of_an_absent_table_when_stream_is_used_then_not_found_and_control_stream_works()
 {
    with_store(|store| {
        Box::pin(async move {
            let (table, arn) = create_stream_table(store, "NEW_AND_OLD_IMAGES").await;
            put_value(store, &table, "control", "1").await;
            let absent = unique_table_name();
            let bogus = format!(
                "arn:aws:dynamodb:us-east-1:123456789012:table/{absent}/stream/2026-01-01T00:00:\
                 00.000"
            );
            let error = expect_err(
                store
                    .operations
                    .describe_stream(DescribeStreamRequest {
                        stream_arn: bogus.clone(),
                        exclusive_start_shard_id: None,
                        limit: None,
                        shard_filter: None,
                    })
                    .await,
            );
            assert_error(
                error,
                NOT_FOUND,
                &format!("Requested resource not found: Stream: {bogus} not found"),
            );
            let error = expect_err(
                store
                    .operations
                    .get_shard_iterator(GetShardIteratorRequest {
                        stream_arn: bogus,
                        shard_id: "shardId-00000000000000000000-00000000".into(),
                        shard_iterator_type: "TRIM_HORIZON".into(),
                        sequence_number: None,
                    })
                    .await,
            );
            assert_error(
                error,
                NOT_FOUND,
                "Requested resource not found: Stream does not exist",
            );
            let listed = ok(
                store
                    .operations
                    .list_streams(ListStreamsRequest {
                        table_name: Some(absent.clone()),
                        exclusive_start_stream_arn: None,
                        limit: None,
                    })
                    .await,
                "list streams",
            );
            assert!(listed.streams.is_empty());
            assert_error(
                expect_err(
                    store
                        .operations
                        .describe_table(DescribeTableRequest {
                            table_name: absent.clone(),
                        })
                        .await,
                ),
                NOT_FOUND,
                &table_not_found(&absent),
            );

            let (_, records) = drain_stream(store, &arn, 10, 1).await;
            assert_eq!(records.len(), 1);
        })
    })
    .await;
}

/// AWS behavioural: check_stream_record_creation_time_tracks_the_live_clock.py
///
/// The script's real waits are replaced by the fixture clock: INSERT is
/// written at 400 ms into a second, MODIFY 2.5 s later.
#[tokio::test]
async fn given_writes_at_known_clock_times_when_records_are_read_then_creation_times_are_whole_seconds_of_those_times()
 {
    with_store(|store| {
        Box::pin(async move {
            let (table, arn) = create_stream_table(store, "NEW_AND_OLD_IMAGES").await;
            let start_millis = 1_800_000_000_400;
            store.set_time(TimestampMillis::from_timestamp(start_millis));
            put_value(store, &table, "clocked", "one").await;
            store.advance_time(Duration::from_millis(2_500));
            put_value(store, &table, "clocked", "two").await;

            let (_, records) = drain_stream(store, &arn, 10, 2).await;
            let created = |name: &str| -> f64 {
                records
                    .iter()
                    .find(|record| record["eventName"] == name)
                    .unwrap_or_else(|| panic!("no {name} record"))["dynamodb"]
                    ["ApproximateCreationDateTime"]
                    .as_f64()
                    .unwrap()
            };
            let (insert, modify) = (created("INSERT"), created("MODIFY"));
            assert_eq!(insert.fract(), 0.0, "INSERT time must be whole seconds");
            assert_eq!(modify.fract(), 0.0, "MODIFY time must be whole seconds");
            assert_eq!(insert, 1_800_000_000.0);
            assert_eq!(modify, 1_800_000_002.0);
            let gap = modify - insert;
            assert!((1.0..=2.5 + 2.0).contains(&gap), "gap {gap}");
        })
    })
    .await;
}
