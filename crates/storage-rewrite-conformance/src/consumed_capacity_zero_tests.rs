//! Consumed capacity is always zero: a request that asks for `TOTAL` or
//! `INDEXES` gets the AWS shape with every unit `0.0`; a request that omits
//! the parameter or sends `NONE` gets no `ConsumedCapacity`.

use serde_json::{Value, json};
use storage_types::{
    BatchGetItemRequest, BatchWriteItemRequest, CreateTableRequest, DeleteItemRequest,
    GetItemRequest, PutItemRequest, QueryRequest, ScanRequest, TransactGetItemsRequest,
    TransactWriteItemsRequest, UpdateItemRequest,
};

use super::test_support::TestStore;

const TABLE: &str = "capacity_zero";

fn request<T>(value: Value) -> T
where
    T: TryFrom<Value>,
    T::Error: std::fmt::Debug,
{
    T::try_from(value).expect("valid capacity request")
}

/// The request with `ReturnConsumedCapacity` set to `mode`, when present.
fn with_mode(mut value: Value, mode: Option<&str>) -> Value {
    if let Some(mode) = mode {
        value["ReturnConsumedCapacity"] = json!(mode);
    }
    value
}

fn asked(mode: Option<&str>) -> bool {
    matches!(mode, Some("TOTAL" | "INDEXES"))
}

fn indexes(mode: Option<&str>) -> bool {
    mode == Some("INDEXES")
}

/// One table entry: `extra` names the unit field next to `CapacityUnits`.
fn entry(table: &str, mode: Option<&str>, extra: Option<&str>) -> Value {
    let mut units = json!({"CapacityUnits": 0.0});
    if let Some(extra) = extra {
        units[extra] = json!(0.0);
    }
    let mut value = units.clone();
    value["TableName"] = json!(table);
    if indexes(mode) {
        value["Table"] = units;
    }
    value
}

/// The expected `ConsumedCapacity` member: `Null` when not asked.
fn single(mode: Option<&str>) -> Value {
    if asked(mode) {
        entry(TABLE, mode, None)
    } else {
        Value::Null
    }
}

fn array(mode: Option<&str>, extra: Option<&str>) -> Value {
    if asked(mode) {
        json!([entry(TABLE, mode, extra)])
    } else {
        Value::Null
    }
}

async fn create_table(store: &TestStore) {
    store
        .operations
        .create_table(request::<CreateTableRequest>(json!({
            "TableName": TABLE,
            "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "g", "AttributeType": "S"}
            ],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "GlobalSecondaryIndexes": [{
                "IndexName": "by_g",
                "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }]
        })))
        .await
        .expect("create table");
}

macro_rules! capacity {
    ($response:expr) => {
        serde_json::to_value(&$response).expect("response json")["ConsumedCapacity"].clone()
    };
}

#[tokio::test]
async fn given_point_operations_when_capacity_is_asked_then_every_unit_is_zero() {
    let store = TestStore::new().await;
    create_table(&store).await;
    for mode in [None, Some("NONE"), Some("TOTAL"), Some("INDEXES")] {
        let put = store
            .operations
            .put_item(request::<PutItemRequest>(with_mode(
                json!({"TableName": TABLE, "Item": {"pk": {"S": "a"}, "g": {"S": "g"}}}),
                mode,
            )))
            .await
            .expect("put");
        assert_eq!(capacity!(put), single(mode), "put {mode:?}");

        let get = store
            .operations
            .get_item(request::<GetItemRequest>(with_mode(
                json!({"TableName": TABLE, "Key": {"pk": {"S": "a"}}}),
                mode,
            )))
            .await
            .expect("get");
        assert_eq!(capacity!(get), single(mode), "get {mode:?}");

        let update = store
            .operations
            .update_item(request::<UpdateItemRequest>(with_mode(
                json!({"TableName": TABLE, "Key": {"pk": {"S": "a"}},
                    "UpdateExpression": "SET n = :n",
                    "ExpressionAttributeValues": {":n": {"N": "1"}}}),
                mode,
            )))
            .await
            .expect("update");
        assert_eq!(capacity!(update), single(mode), "update {mode:?}");

        let delete = store
            .operations
            .delete_item(request::<DeleteItemRequest>(with_mode(
                json!({"TableName": TABLE, "Key": {"pk": {"S": "a"}}}),
                mode,
            )))
            .await
            .expect("delete");
        assert_eq!(capacity!(delete), single(mode), "delete {mode:?}");
    }
    store.finish().await;
}

#[tokio::test]
async fn given_query_and_scan_when_capacity_is_asked_then_every_unit_is_zero() {
    let store = TestStore::new().await;
    create_table(&store).await;
    store
        .operations
        .put_item(request::<PutItemRequest>(
            json!({"TableName": TABLE, "Item": {"pk": {"S": "a"}, "g": {"S": "g"}}}),
        ))
        .await
        .expect("seed");
    for mode in [None, Some("NONE"), Some("TOTAL"), Some("INDEXES")] {
        let table_query = store
            .operations
            .query(request::<QueryRequest>(with_mode(
                json!({"TableName": TABLE, "KeyConditionExpression": "pk = :p",
                    "ExpressionAttributeValues": {":p": {"S": "a"}}}),
                mode,
            )))
            .await
            .expect("table query");
        assert_eq!(capacity!(table_query), single(mode), "query {mode:?}");

        let index_query = store
            .operations
            .query(request::<QueryRequest>(with_mode(
                json!({"TableName": TABLE, "IndexName": "by_g",
                    "KeyConditionExpression": "g = :g",
                    "ExpressionAttributeValues": {":g": {"S": "g"}}}),
                mode,
            )))
            .await
            .expect("index query");
        let mut expected = single(mode);
        if indexes(mode) {
            expected["GlobalSecondaryIndexes"] = json!({"by_g": {"CapacityUnits": 0.0}});
        }
        assert_eq!(capacity!(index_query), expected, "gsi query {mode:?}");

        let scan = store
            .operations
            .scan(request::<ScanRequest>(with_mode(
                json!({"TableName": TABLE}),
                mode,
            )))
            .await
            .expect("scan");
        assert_eq!(capacity!(scan), single(mode), "scan {mode:?}");
    }
    store.finish().await;
}

#[tokio::test]
async fn given_batches_and_transactions_when_capacity_is_asked_then_every_unit_is_zero() {
    let store = TestStore::new().await;
    create_table(&store).await;
    for mode in [None, Some("NONE"), Some("TOTAL"), Some("INDEXES")] {
        let write = store
            .operations
            .batch_write_item(request::<BatchWriteItemRequest>(with_mode(
                json!({"RequestItems": {TABLE: [
                    {"PutRequest": {"Item": {"pk": {"S": "b"}, "g": {"S": "g"}}}}
                ]}}),
                mode,
            )))
            .await
            .expect("batch write");
        assert_eq!(capacity!(write), array(mode, None), "batch write {mode:?}");

        let read = store
            .operations
            .batch_get_item(request::<BatchGetItemRequest>(with_mode(
                json!({"RequestItems": {TABLE: {"Keys": [{"pk": {"S": "b"}}]}}}),
                mode,
            )))
            .await
            .expect("batch get");
        assert_eq!(capacity!(read), array(mode, None), "batch get {mode:?}");

        let transact_write = store
            .operations
            .transact_write_items(request::<TransactWriteItemsRequest>(with_mode(
                json!({"TransactItems": [
                    {"Put": {"TableName": TABLE, "Item": {"pk": {"S": "c"}, "g": {"S": "g"}}}},
                    {"Put": {"TableName": TABLE, "Item": {"pk": {"S": "d"}, "g": {"S": "g"}}}}
                ]}),
                mode,
            )))
            .await
            .expect("transact write");
        assert_eq!(
            capacity!(transact_write),
            array(mode, Some("WriteCapacityUnits")),
            "transact write {mode:?}"
        );

        let transact_read = store
            .operations
            .transact_get_items(request::<TransactGetItemsRequest>(with_mode(
                json!({"TransactItems": [
                    {"Get": {"TableName": TABLE, "Key": {"pk": {"S": "c"}}}},
                    {"Get": {"TableName": TABLE, "Key": {"pk": {"S": "d"}}}}
                ]}),
                mode,
            )))
            .await
            .expect("transact get");
        assert_eq!(
            capacity!(transact_read),
            array(mode, Some("ReadCapacityUnits")),
            "transact get {mode:?}"
        );
    }
    store.finish().await;
}
