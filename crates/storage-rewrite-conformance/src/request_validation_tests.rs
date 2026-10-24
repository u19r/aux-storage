//! Request-only failures precede metadata access for every native provider.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::json;
use storage_types::{
    BatchGetItemRequest, BatchWriteItemRequest, DeleteItemRequest, DynamoRequestValidate,
    GetItemRequest, PutItemRequest, QueryRequest, ScanRequest, StorageError,
    TransactGetItemsRequest, TransactWriteItemsRequest, UpdateItemRequest,
};

use super::test_support::{TestStore, unique_table_name};

fn assert_request_error(error: StorageError, expected: String) {
    let response = http_error::HttpApiError::from(error);
    assert_eq!(response.status_code, 400);
    assert_eq!(
        response.error_type,
        "com.amazon.coral.validate#ValidationException"
    );
    assert_eq!(response.message, expected);
}

#[tokio::test]
async fn given_invalid_raw_requests_and_missing_tables_when_executed_then_request_errors_win() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        let operations = &store.operations;
        let key = json!({"pk": {"S": "p"}});
        let put: PutItemRequest = serde_json::from_value(json!({
            "TableName": table, "Item": key, "ReturnItemCollectionMetrics": "invalid"
        })).unwrap();
        let expected = put.validate_for_dynamodb().unwrap_err();
        assert_request_error(operations.put_item(put).await.unwrap_err(), expected);

        let get: GetItemRequest = serde_json::from_value(json!({
            "TableName": table, "Key": key, "ProjectionExpression": "#missing"
        })).unwrap();
        let expected = get.validate_for_dynamodb().unwrap_err();
        assert_request_error(operations.get_item(get).await.unwrap_err(), expected);

        let delete: DeleteItemRequest = serde_json::from_value(json!({
            "TableName": table, "Key": key, "ReturnValuesOnConditionCheckFailure": "invalid"
        })).unwrap();
        let expected = delete.validate_for_dynamodb().unwrap_err();
        assert_request_error(operations.delete_item(delete).await.unwrap_err(), expected);

        let update: UpdateItemRequest = serde_json::from_value(json!({
            "TableName": table, "Key": key, "UpdateExpression": "SET note = :missing"
        })).unwrap();
        let expected = update.validate_for_dynamodb().unwrap_err();
        assert_request_error(operations.update_item(update).await.unwrap_err(), expected);

        let query: QueryRequest = serde_json::from_value(json!({
            "TableName": table, "KeyConditionExpression": "pk = :missing"
        })).unwrap();
        let expected = query.validate_for_dynamodb().unwrap_err();
        assert_request_error(operations.query(query).await.unwrap_err(), expected);

        let scan: ScanRequest = serde_json::from_value(json!({
            "TableName": table, "ProjectionExpression": "#missing"
        })).unwrap();
        let expected = scan.validate_for_dynamodb().unwrap_err();
        assert_request_error(operations.scan(scan).await.unwrap_err(), expected);

        let batch_get: BatchGetItemRequest = serde_json::from_value(json!({
            "RequestItems": {table.as_ref(): {"Keys": [key], "ProjectionExpression": "#missing"}}
        })).unwrap();
        let expected = batch_get.validate_for_dynamodb().unwrap_err();
        assert_request_error(operations.batch_get_item(batch_get).await.unwrap_err(), expected);

        let batch_write: BatchWriteItemRequest = serde_json::from_value(json!({
            "RequestItems": {table.as_ref(): [{"PutRequest": {"Item": key}}]},
            "ReturnItemCollectionMetrics": "invalid"
        })).unwrap();
        let expected = batch_write.validate_for_dynamodb().unwrap_err();
        assert_request_error(operations.batch_write_item(batch_write).await.unwrap_err(), expected);

        let transact_get: TransactGetItemsRequest = serde_json::from_value(json!({
            "TransactItems": [{"Get": {"TableName": table, "Key": key, "ProjectionExpression": "#missing"}}]
        })).unwrap();
        let expected = transact_get.validate_for_dynamodb().unwrap_err();
        assert_request_error(operations.transact_get_items(transact_get).await.unwrap_err(), expected);

        let transact_write: TransactWriteItemsRequest = serde_json::from_value(json!({
            "TransactItems": [{"Update": {"TableName": table, "Key": key, "UpdateExpression": "SET note = :missing"}}]
        })).unwrap();
        let expected = transact_write.validate_for_dynamodb().unwrap_err();
        assert_request_error(operations.transact_write_items(transact_write).await.unwrap_err(), expected);
    }).catch_unwind().await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
