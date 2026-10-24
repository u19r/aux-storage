//! DeleteItem conditional atomicity, readback, and CDC effects.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{
    CreateTableRequest, DeleteItemRequest, GetItemRequest, PutItemRequest, QueryRequest, StreamName,
};
use stream_provider::{StreamRowKind, SystemStreamPageRequest};

use super::test_support::{TestStore, assert_dynamo_error_response_exact, unique_table_name};

/// Business rule from the legacy SQL no-op delete test: deleting an absent
/// item succeeds without publishing a change or advancing the CDC checkpoint.
#[tokio::test]
async fn given_missing_item_when_delete_item_succeeds_then_response_is_empty_and_cdc_does_not_advance()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        let private_key = "private_delete_key_marker_7b1c";
        let create = CreateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
            "StreamSpecification": {
                "StreamEnabled": true,
                "StreamViewType": "NEW_AND_OLD_IMAGES"
            }
        }))
        .unwrap();
        store.operations.create_table(create).await.unwrap();
        let scope = store.stream_operations.system_stream_scope().await.unwrap();
        let initial = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope.clone(), None, 1).unwrap(),
        ))
        .await;
        initial.assert_no_warning_or_error();
        let initial = initial.result.unwrap();
        assert!(initial.records().is_empty());
        let checkpoint = initial.issued_checkpoint().token().clone();

        let request = DeleteItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"S": private_key}},
            "ReturnValues": "ALL_OLD"
        }))
        .unwrap();
        let deleted = observe(store.operations.delete_item(request)).await;
        deleted.assert_no_warning_or_error();
        let span = deleted.one_span_with("dynamo_operation", &[("operation", "DeleteItem")]);
        assert_eq!(span.fields.len(), 1);
        assert!(!format!("{:?}{:?}", deleted.events, deleted.spans).contains(private_key));
        assert_eq!(
            serde_json::to_value(deleted.result.unwrap()).unwrap(),
            json!({})
        );

        let after = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope, Some(checkpoint.clone()), 1).unwrap(),
        ))
        .await;
        after.assert_no_warning_or_error();
        let after = after.result.unwrap();
        assert!(after.records().is_empty());
        assert_eq!(after.issued_checkpoint().token(), &checkpoint);

        let get = GetItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"S": private_key}}
        }))
        .unwrap();
        assert_eq!(
            serde_json::to_value(store.operations.get_item(get).await.unwrap()).unwrap(),
            json!({})
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

/// Business and negative rules from legacy SQL stream and Cucumber DeleteItem:
/// a failed condition preserves the item and CDC, while a matching delete
/// returns the old image and appends exactly one committed change.
#[tokio::test]
async fn given_versioned_item_when_conditional_deletes_run_then_only_matching_delete_commits() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        let create = CreateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
            "StreamSpecification": {
                "StreamEnabled": true,
                "StreamViewType": "NEW_AND_OLD_IMAGES"
            }
        }))
        .unwrap();
        store.operations.create_table(create).await.unwrap();

        let original = json!({
            "pk": {"S": "customer-1"},
            "version": {"N": "2"},
            "status": {"S": "active"}
        });
        let put = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Item": original
        }))
        .unwrap();
        store.operations.put_item(put).await.unwrap();

        let system_stream = StreamRowKind::System(StreamName::system_table_stream());
        let before = store.stream_operations.read_forward(system_stream.clone(), None, 10)
            .await.unwrap();
        assert_eq!(before.items.len(), 1);

        let delete_request = |expected_version: &str| {
            DeleteItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Key": {"pk": {"S": "customer-1"}},
                "ConditionExpression": "#version = :expected",
                "ExpressionAttributeNames": {"#version": "version"},
                "ExpressionAttributeValues": {":expected": {"N": expected_version}},
                "ReturnValues": "ALL_OLD"
            }))
            .unwrap()
        };

        let rejected = observe(store.operations.delete_item(delete_request("1"))).await;
        rejected.assert_no_warning_or_error();
        assert_dynamo_error_response_exact(
            rejected.result.unwrap_err(),
            axum::http::StatusCode::BAD_REQUEST,
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","message":"The conditional request failed"}"#,
        );
        let get_request = || GetItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"S": "customer-1"}}
        })).unwrap();
        let unchanged = store.operations.get_item(get_request()).await.unwrap();
        assert_eq!(serde_json::to_value(unchanged).unwrap(),
            json!({"Item": original}));
        let after_rejection = store.stream_operations.read_forward(system_stream.clone(), None, 10)
            .await.unwrap();
        assert_eq!(after_rejection.items.len(), 1);
        assert_eq!(after_rejection.items[0].id, before.items[0].id);

        let accepted = observe(store.operations.delete_item(delete_request("2"))).await;
        accepted.assert_no_warning_or_error();
        assert_eq!(serde_json::to_value(accepted.result.unwrap())
            .unwrap(), json!({"Attributes": original}));
        let absent = store.operations.get_item(get_request()).await.unwrap();
        assert_eq!(serde_json::to_value(absent).unwrap(), json!({}));
        let after_commit = store.stream_operations.read_forward(system_stream, None, 10)
            .await.unwrap();
        assert_eq!(after_commit.items.len(), 2);
        assert_eq!(after_commit.items[0].id, before.items[0].id);
        assert_ne!(after_commit.items[1].id, before.items[0].id);
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

/// Business rule from the DeleteItem Cucumber scenario: a committed delete
/// removes only its own live GSI membership.
#[tokio::test]
async fn given_two_indexed_items_when_one_is_deleted_then_only_its_gsi_membership_disappears() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        let create = CreateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "status", "AttributeType": "S"}
            ],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
            "GlobalSecondaryIndexes": [{
                "IndexName": "ByStatus",
                "KeySchema": [{"AttributeName": "status", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }]
        }))
        .unwrap();
        store.operations.create_table(create).await.unwrap();

        for pk in ["keep", "remove"] {
            let put = PutItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Item": {"pk": {"S": pk}, "status": {"S": "active"}}
            }))
            .unwrap();
            store.operations.put_item(put).await.unwrap();
        }
        let query = QueryRequest::try_from(json!({
            "TableName": table.as_ref(),
            "IndexName": "ByStatus",
            "KeyConditionExpression": "#status = :active",
            "ExpressionAttributeNames": {"#status": "status"},
            "ExpressionAttributeValues": {":active": {"S": "active"}}
        }))
        .unwrap();
        let before = store.operations.query(query.clone()).await.unwrap();
        assert_eq!(before.count, 2);

        let delete = DeleteItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"S": "remove"}},
            "ReturnValues": "ALL_OLD"
        }))
        .unwrap();
        let deleted = observe(store.operations.delete_item(delete)).await;
        deleted.assert_no_warning_or_error();
        let span = deleted.one_span_with("dynamo_operation", &[("operation", "DeleteItem")]);
        assert_eq!(span.fields.len(), 1);
        assert_eq!(
            serde_json::to_value(deleted.result.unwrap()).unwrap(),
            json!({
                "Attributes": {"pk": {"S": "remove"}, "status": {"S": "active"}}
            })
        );

        let after = observe(store.operations.query(query)).await;
        after.assert_no_warning_or_error();
        assert_eq!(
            serde_json::to_value(after.result.unwrap()).unwrap(),
            json!({
                "Items": [{"pk": {"S": "keep"}, "status": {"S": "active"}}],
                "Count": 1,
                "ScannedCount": 1
            })
        );
        let get = GetItemRequest::try_from(json!({
            "TableName": table.as_ref(), "Key": {"pk": {"S": "remove"}}
        }))
        .unwrap();
        assert_eq!(
            serde_json::to_value(store.operations.get_item(get).await.unwrap()).unwrap(),
            json!({})
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
