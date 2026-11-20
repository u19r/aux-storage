//! PERF-FDB-1: a warm operation reads the catalog version in its first data
//! wave. Two stores over one database prove that a moved version, a recreated
//! table and a deleted table are still decided by the stamps (stale-incarnation
//! rule). These tests need a shared database (FoundationDB or Postgres) and
//! return at once on every other provider.

use std::sync::Arc;

use futures::FutureExt as _;
use serde_json::{Value, json};
use storage_types::{
    GetItemRequest, PutItemRequest, QueryRequest, StorageError, StorageResult, TableName,
    context::WrappedError as _,
};

use super::{
    native_census_test_support::NativeCensus,
    test_support::{TestStore, unique_table_name},
};

const OPERATIONS: [&str; 3] = ["get", "query", "put"];

fn request<T>(value: Value) -> T
where
    T: TryFrom<Value>,
    T::Error: std::fmt::Debug,
{
    T::try_from(value).expect("valid request")
}

fn get(table: &TableName, key: &str) -> GetItemRequest {
    request(json!({"TableName": table, "Key": {"pk": {"S": key}}}))
}

fn query(table: &TableName, key: &str) -> QueryRequest {
    request(json!({
        "TableName": table,
        "KeyConditionExpression": "pk = :p",
        "ExpressionAttributeValues": {":p": {"S": key}}
    }))
}

fn put(table: &TableName, key: &str, value: &str) -> PutItemRequest {
    request(json!({"TableName": table, "Item": {"pk": {"S": key}, "v": {"S": value}}}))
}

async fn move_catalog_version(other: &TestStore) {
    let name = unique_table_name();
    other.create_hash_table(name.clone()).await;
    other
        .operations
        .delete_table(storage_types::DeleteTableRequest { table_name: name })
        .await
        .unwrap();
}

fn is_table_not_found(error: &StorageError) -> bool {
    matches!(
        error.to_enum(),
        storage_types::StorageEnum::ResourceNotFound { .. }
    )
}

/// One public operation; the response as JSON (`Null` for a put).
async fn run(
    store: &TestStore,
    operation: &str,
    table: &TableName,
    key: &str,
    value: &str,
) -> StorageResult<Value> {
    Ok(match operation {
        "get" => serde_json::to_value(store.operations.get_item(get(table, key)).await?).unwrap(),
        "query" => serde_json::to_value(store.operations.query(query(table, key)).await?).unwrap(),
        _ => {
            store.operations.put_item(put(table, key, value)).await?;
            Value::Null
        }
    })
}

fn selected_database_is_shared() -> bool {
    matches!(
        TestStore::selected_backend().as_str(),
        "foundationdb" | "postgres"
    )
}

async fn delete_table(store: &TestStore, table: &TableName) {
    store
        .operations
        .delete_table(storage_types::DeleteTableRequest {
            table_name: table.clone(),
        })
        .await
        .unwrap();
}

#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires native fixture"
)]
async fn given_moved_catalog_version_when_warm_operations_run_then_stamps_confirm_and_cache_retags()
{
    if !selected_database_is_shared() {
        return;
    }
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let other = store.open_same_database().await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let table = unique_table_name();
        store.create_hash_table(table.clone()).await;
        run(&store, "put", &table, "a", "seed").await.unwrap();
        for operation in OPERATIONS {
            run(&store, operation, &table, "a", "seed").await.unwrap();
        }
        let provider = TestStore::selected_backend();
        for operation in OPERATIONS {
            move_catalog_version(&other).await;
            census.reset();
            let first_response = run(&store, operation, &table, "a", "seed").await.unwrap();
            let first = census.snapshot();
            census.reset();
            run(&store, operation, &table, "a", "seed").await.unwrap();
            let second = census.snapshot();
            match operation {
                "get" => assert_eq!(first_response["Item"]["v"]["S"], "seed"),
                "query" => assert_eq!(first_response["Items"].as_array().unwrap().len(), 1),
                _ => {}
            }
            println!("PERF_FDB_1_MOVED {provider} {operation} {first} {second}");
            if provider == "foundationdb" {
                assert_eq!(first.begins, 1, "{first} / {second}");
                match operation {
                    "get" => {
                        // GRV, item and version, stamps, item again.
                        assert!(first.waves == 4 && second.waves == 2, "{first} / {second}");
                        assert!(first.keys == 4 && second.keys == 2, "{first} / {second}");
                    }
                    "query" => {
                        assert!(first.waves == 4 && second.waves == 2, "{first} / {second}");
                    }
                    _ => assert!(first.waves == 5 && second.waves == 3, "{first} / {second}"),
                }
            }
        }
    })
    .catch_unwind()
    .await;
    drop(other);
    store.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires native fixture"
)]
async fn given_other_store_recreates_table_when_warm_operations_run_then_current_incarnation_answers()
 {
    if !selected_database_is_shared() {
        return;
    }
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let other = store.open_same_database().await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let table = unique_table_name();
        for operation in OPERATIONS {
            store.create_hash_table(table.clone()).await;
            run(&store, "put", &table, "a", "old").await.unwrap();
            run(&store, operation, &table, "a", "old").await.unwrap();
            delete_table(&other, &table).await;
            other.create_hash_table(table.clone()).await;
            run(&other, "put", &table, "a", "new").await.unwrap();
            match operation {
                "get" => {
                    let response = run(&store, "get", &table, "a", "").await.unwrap();
                    assert_eq!(response["Item"]["v"]["S"], "new");
                }
                "query" => {
                    let response = run(&store, "query", &table, "a", "").await.unwrap();
                    let items = response["Items"].as_array().unwrap();
                    assert_eq!(items.len(), 1);
                    assert_eq!(items[0]["v"]["S"], "new");
                }
                _ => {
                    run(&store, "put", &table, "b", "from_store").await.unwrap();
                    let response = run(&other, "get", &table, "b", "").await.unwrap();
                    assert_eq!(response["Item"]["v"]["S"], "from_store");
                }
            }
            delete_table(&store, &table).await;
        }
    })
    .catch_unwind()
    .await;
    drop(other);
    store.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires native fixture"
)]
async fn given_other_store_deletes_table_when_warm_operations_run_then_table_not_found() {
    if !selected_database_is_shared() {
        return;
    }
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let other = store.open_same_database().await;
    let outcome = std::panic::AssertUnwindSafe(async {
        for operation in OPERATIONS {
            let table = unique_table_name();
            store.create_hash_table(table.clone()).await;
            run(&store, "put", &table, "a", "seed").await.unwrap();
            run(&store, operation, &table, "a", "seed").await.unwrap();
            delete_table(&other, &table).await;
            let error = run(&store, operation, &table, "a", "seed")
                .await
                .expect_err("the table is gone");
            assert!(is_table_not_found(&error), "{operation}: {error}");
        }
    })
    .catch_unwind()
    .await;
    drop(other);
    store.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}
