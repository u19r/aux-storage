//! Customer-visible DeleteTable behavior across replacement providers.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_provider::TableReclaimStep;
use storage_types::{
    DeleteTableRequest, DescribeTableRequest, GetItemRequest, ListTablesRequest, PutItemRequest,
    TableStatus, UpdateTableRequest,
};

use super::test_support::{TestStore, assert_dynamo_error_response_exact};

#[tokio::test]
async fn given_a_table_with_an_item_when_deleted_and_recreated_then_old_data_is_absent() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        let put = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Item": {"pk": {"S": "customer-1"}, "value": {"S": "before-delete"}}
        }))
        .unwrap();
        store.operations.put_item(put).await.unwrap();
        let create_index = UpdateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "AttributeDefinitions": [{"AttributeName": "gpk", "AttributeType": "S"}],
            "GlobalSecondaryIndexUpdates": [{"Create": {
                "IndexName": "ByGroup",
                "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }}]
        }))
        .unwrap();
        store.operations.update_table(create_index).await.unwrap();

        let deleted = observe(store.operations.delete_table(DeleteTableRequest {
            table_name: table.clone(),
        }))
        .await;
        deleted.assert_no_warning_or_error();
        let span = deleted.one_span_with("dynamo_operation", &[("operation", "DeleteTable")]);
        assert_eq!(span.fields.len(), 1);
        let description = deleted.result.unwrap().table_description;
        assert_eq!(description.table_name, table);
        assert_eq!(description.table_status, TableStatus::Deleting);
        let missing = observe(store.operations.describe_table(DescribeTableRequest {
            table_name: table.clone(),
        }))
        .await;
        missing.assert_no_warning_or_error();
        let missing_error = json!({
            "__type": "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException",
            "message": format!("Requested resource not found: Table: {} not found", table.as_ref()),
        })
        .to_string();
        assert_dynamo_error_response_exact(
            missing.result.unwrap_err(),
            axum::http::StatusCode::BAD_REQUEST,
            &missing_error,
        );
        let listed = store
            .operations
            .list_tables(ListTablesRequest {
                exclusive_start_table_name: None,
                limit: None,
            })
            .await
            .unwrap();
        assert!(
            !listed.table_names.contains(&table),
            "deleted table remains listed"
        );

        let repeated = observe(store.operations.delete_table(DeleteTableRequest {
            table_name: table.clone(),
        }))
        .await;
        repeated.assert_no_warning_or_error();
        let expected_error = json!({
            "__type": "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException",
            "message": format!("Requested resource not found: Table: {} not found", table.as_ref()),
        })
        .to_string();
        assert_dynamo_error_response_exact(
            repeated.result.unwrap_err(),
            axum::http::StatusCode::BAD_REQUEST,
            &expected_error,
        );

        let rejected = observe(
            store
                .operations
                .create_table(super::test_support::hash_table_request(table.clone())),
        )
        .await;
        rejected.assert_no_warning_or_error();
        let reserved_error = json!({
            "__type": "com.amazonaws.dynamodb.v20120810#ResourceInUseException",
            "message": format!("Table already exists: {}", table.as_ref()),
        })
        .to_string();
        assert_dynamo_error_response_exact(
            rejected.result.unwrap_err(),
            axum::http::StatusCode::BAD_REQUEST,
            &reserved_error,
        );
        store.advance_time(std::time::Duration::from_secs(29));
        let still_reserved = observe(
            store
                .operations
                .create_table(super::test_support::hash_table_request(table.clone())),
        )
        .await;
        still_reserved.assert_no_warning_or_error();
        assert_dynamo_error_response_exact(
            still_reserved.result.unwrap_err(),
            axum::http::StatusCode::BAD_REQUEST,
            &reserved_error,
        );
        store.advance_time(std::time::Duration::from_secs(1));
        store.create_hash_table(table.clone()).await;
        let described = store
            .operations
            .describe_table(DescribeTableRequest {
                table_name: table.clone(),
            })
            .await
            .unwrap();
        assert_eq!(described.table.table_name, table);
        let get = GetItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"S": "customer-1"}}
        }))
        .unwrap();
        let fetched = store.operations.get_item(get).await.unwrap();
        assert!(
            fetched.item.is_none(),
            "item from deleted table became visible again"
        );

        let replacement = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Item": {"pk": {"S": "customer-1"}, "value": {"S": "after-recreate"}}
        }))
        .unwrap();
        store.operations.put_item(replacement).await.unwrap();
        let mut advanced = 0;
        for _ in 0..1000 {
            match store
                .table_reclaim_repository
                .reclaim_table_page_at(store.now())
                .await
                .unwrap()
            {
                TableReclaimStep::Advanced => advanced += 1,
                TableReclaimStep::Complete => break,
            }
        }
        assert!(advanced > 0, "deleted table should have reclaim work");
        assert_eq!(
            store
                .table_reclaim_repository
                .reclaim_table_page_at(store.now())
                .await
                .unwrap(),
            TableReclaimStep::Complete,
            "deleted table reclaim must finish within a bounded number of pages"
        );
        let fetched = store
            .operations
            .get_item(
                GetItemRequest::try_from(json!({
                    "TableName": table.as_ref(),
                    "Key": {"pk": {"S": "customer-1"}}
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(fetched).unwrap()["Item"]["value"],
            json!({"S": "after-recreate"}),
            "reclaiming the deleted incarnation must preserve replacement items"
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
