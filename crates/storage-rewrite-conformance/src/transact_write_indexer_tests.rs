//! Transactional indexer declarations and their public side effects.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{
    CreateTableRequest, GetItemRequest, QueryRequest, StreamName, TransactWriteItemsRequest,
};
use stream_provider::StreamRowKind;

use super::test_support::{TestStore, assert_dynamo_error_response_exact, unique_table_name};

// Business and negative rules from Cucumber and canary: one invalid indexer
// declaration rejects every transaction member and their CDC effects.
#[tokio::test]
async fn given_transactional_indexers_when_one_is_invalid_then_no_item_gsi_or_cdc_effect_persists()
{
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
            "MaxIndexers": 2,
            "GlobalSecondaryIndexes": [{
                "IndexName": "ByStatus",
                "KeySchema": [{"AttributeName": "status", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }],
            "StreamSpecification": {
                "StreamEnabled": true,
                "StreamViewType": "NEW_AND_OLD_IMAGES"
            }
        }))
        .unwrap();
        store.operations.create_table(create).await.unwrap();

        let rejected = TransactWriteItemsRequest::try_from(json!({
            "TransactItems": [
                {"Put": {
                    "TableName": table.as_ref(),
                    "Item": {"pk": {"S": "first"}, "status": {"S": "ready"}},
                    "Indexers": ["status"]
                }},
                {"Put": {
                    "TableName": table.as_ref(),
                    "Item": {"pk": {"S": "second"}, "status": {"S": "ready"}},
                    "Indexers": ["status", "status"]
                }}
            ]
        }))
        .unwrap();
        let rejected = observe(store.operations.transact_write_items(rejected)).await;
        rejected.assert_no_warning_or_error();
        assert_dynamo_error_response_exact(
            rejected.result.unwrap_err(),
            axum::http::StatusCode::BAD_REQUEST,
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#TransactionCanceledException","CancellationReasons":[{"Code":"None"},{"Code":"ValidationError","Message":"Indexers:duplicate_attribute"}],"Message":"Transaction cancelled, please refer cancellation reasons for specific reasons [None, ValidationError]"}"#,
        );
        for key in ["first", "second"] {
            let get = GetItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Key": {"pk": {"S": key}}
            }))
            .unwrap();
            let response = store.operations.get_item(get).await.unwrap();
            assert_eq!(serde_json::to_value(response).unwrap(), json!({}));
        }
        let stream = StreamRowKind::System(StreamName::system_table_stream());
        let before = store.stream_operations.read_forward(stream.clone(), None, 10)
            .await.unwrap();
        assert!(before.items.is_empty(), "rejected transaction must not append CDC");
        let query = QueryRequest::try_from(json!({
            "TableName": table.as_ref(),
            "IndexName": "ByStatus",
            "KeyConditionExpression": "#status = :status",
            "ExpressionAttributeNames": {"#status": "status"},
            "ExpressionAttributeValues": {":status": {"S": "ready"}}
        }))
        .unwrap();
        let before_index = store.operations.query(query.clone()).await
            .unwrap();
        assert_eq!(
            serde_json::to_value(before_index).unwrap(),
            json!({"Items": [], "Count": 0, "ScannedCount": 0})
        );

        let accepted = TransactWriteItemsRequest::try_from(json!({
            "TransactItems": [
                {"Put": {
                    "TableName": table.as_ref(),
                    "Item": {"pk": {"S": "first"}, "status": {"S": "ready"}},
                    "Indexers": ["status"]
                }},
                {"Put": {
                    "TableName": table.as_ref(),
                    "Item": {"pk": {"S": "second"}},
                    "Indexers": ["status", "optional"]
                }}
            ]
        }))
        .unwrap();
        let accepted = observe(store.operations.transact_write_items(accepted)).await;
        accepted.assert_no_warning_or_error();
        assert_eq!(
            serde_json::to_value(accepted.result.unwrap())
                .unwrap(),
            json!({})
        );
        for (key, expected) in [
            ("first", json!({"pk": {"S": "first"}, "status": {"S": "ready"}})),
            ("second", json!({"pk": {"S": "second"}})),
        ] {
            let get = GetItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Key": {"pk": {"S": key}}
            }))
            .unwrap();
            let response = store.operations.get_item(get).await.unwrap();
            assert_eq!(
                serde_json::to_value(response).unwrap(),
                json!({"Item": expected})
            );
        }
        let indexed = store.operations.query(query).await.unwrap();
        assert_eq!(
            serde_json::to_value(indexed).unwrap(),
            json!({
                "Items": [{"pk": {"S": "first"}, "status": {"S": "ready"}}],
                "Count": 1,
                "ScannedCount": 1
            })
        );
        let after = store.stream_operations.read_forward(stream, None, 10)
            .await.unwrap();
        assert_eq!(after.items.len(), 2);
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
