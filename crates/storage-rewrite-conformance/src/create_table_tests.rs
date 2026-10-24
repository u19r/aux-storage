//! Customer-visible CreateTable lifecycle rules retained from the legacy
//! provider suites.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{
    CreateTableRequest, DescribeTableRequest, GetItemRequest, ListTablesRequest, PutItemRequest,
    QueryRequest, TableStatus,
};

use super::test_support::{TestStore, unique_table_name};

// Business rule: creation publishes a usable schema, description, and catalog
// entry.
#[tokio::test]
async fn given_new_table_when_created_then_description_listing_and_item_round_trip_succeed() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        let request = CreateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .unwrap();
        let created = observe(store.operations.create_table(request)).await;
        created.assert_no_warning_or_error();
        let description = created.result.unwrap().table_description;
        assert_eq!(description.table_name, table);
        assert_eq!(description.table_status, TableStatus::Active);

        let stored = store
            .operations
            .get_table_info(table.clone())
            .await
            .unwrap();
        assert_eq!(stored.table_name, table);
        assert_eq!(stored.table_status, TableStatus::Active);
        assert_eq!(stored.key_schema.len(), 1);

        let described = store
            .operations
            .describe_table(DescribeTableRequest {
                table_name: table.clone(),
            })
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(&described.table).unwrap(),
            serde_json::to_value(&description).unwrap(),
            "create and describe must show the same table"
        );
        let listed = store
            .operations
            .list_tables(ListTablesRequest {
                exclusive_start_table_name: None,
                limit: None,
            })
            .await
            .unwrap();
        assert_eq!(listed.table_names, vec![table.clone()]);

        let put = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Item": {"pk": {"S": "customer-1"}}
        }))
        .unwrap();
        store.operations.put_item(put).await.unwrap();
        let get = GetItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"S": "customer-1"}}
        }))
        .unwrap();
        let fetched = store.operations.get_item(get).await.unwrap();
        assert_eq!(
            serde_json::to_value(fetched).unwrap(),
            json!({"Item": {"pk": {"S": "customer-1"}}})
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Business and format rules: a new GSI is published in table descriptions and
// accepts a query.
#[tokio::test]
async fn given_table_with_gsi_when_created_then_active_index_is_described_and_queryable() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        let request = CreateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "gpk", "AttributeType": "S"}
            ],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "GlobalSecondaryIndexes": [{
                "IndexName": "ByGroup",
                "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .unwrap();
        let created = observe(store.operations.create_table(request)).await;
        created.assert_no_warning_or_error();
        let description = created.result.unwrap().table_description;
        let indexes = description.global_secondary_indexes.as_ref().unwrap();
        assert_eq!(indexes.len(), 1);
        assert_eq!(
            serde_json::to_value(&indexes[0]).unwrap(),
            json!({
                "IndexName": "ByGroup",
                "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
                "IndexStatus": "ACTIVE",
                "Backfilling": false
            })
        );

        let described = store
            .operations
            .describe_table(DescribeTableRequest {
                table_name: table.clone(),
            })
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(described.table.global_secondary_indexes).unwrap(),
            serde_json::to_value(description.global_secondary_indexes).unwrap(),
        );

        let query = QueryRequest::try_from(json!({
            "TableName": table.as_ref(),
            "IndexName": "ByGroup",
            "KeyConditionExpression": "gpk = :group",
            "ExpressionAttributeValues": {":group": {"S": "group-1"}}
        }))
        .unwrap();
        let queried = store.operations.query(query).await.unwrap();
        assert_eq!(
            serde_json::to_value(queried).unwrap(),
            json!({
                "Items": [], "Count": 0, "ScannedCount": 0
            })
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Boundary and validation rules from the Cucumber CreateTable matrix: an
// omitted capacity is zero, while a capacity above 32 is rejected before I/O.
#[tokio::test]
async fn given_default_and_oversized_indexer_capacity_when_tables_are_created_then_zero_is_reported_and_invalid_request_does_no_io()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        let valid_request = CreateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .unwrap();
        let created = observe(store.operations.create_table(valid_request)).await;
        created.assert_no_warning_or_error();
        let span = created.one_span_with("dynamo_operation", &[("operation", "CreateTable")]);
        assert_eq!(span.fields.len(), 1);
        assert_eq!(
            serde_json::to_value(created.result.unwrap()).unwrap()["TableDescription"]
                ["MaxIndexers"],
            0,
        );

        let described = store
            .operations
            .describe_table(DescribeTableRequest {
                table_name: table.clone(),
            })
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(described).unwrap()["Table"]["MaxIndexers"],
            0,
        );

        let rejected_name = unique_table_name();
        let rejected = observe(async {
            CreateTableRequest::try_from(json!({
                "TableName": rejected_name.as_ref(),
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "BillingMode": "PAY_PER_REQUEST",
                "MaxIndexers": 33
            }))
        })
        .await;
        rejected.assert_no_warning_or_error();
        rejected.assert_database_calls_exact(&[]);
        assert_eq!(
            rejected.result.unwrap_err(),
            "Invalid request format: MaxIndexers:too_many",
        );

        let listed = store
            .operations
            .list_tables(ListTablesRequest {
                exclusive_start_table_name: None,
                limit: None,
            })
            .await
            .unwrap();
        assert_eq!(listed.table_names, vec![table]);
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
