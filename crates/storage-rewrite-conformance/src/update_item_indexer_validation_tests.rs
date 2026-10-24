//! Rejected UpdateItem indexer changes preserve the item and its CDC history.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{GetItemRequest, PutItemRequest, StreamName, UpdateItemRequest};
use stream_provider::StreamRowKind;

use super::test_support::{TestStore, assert_dynamo_error_response_exact, unique_table_name};

/// Negative rules from the canary's Indexers checks, exercised at the shared
/// provider boundary.
#[tokio::test]
async fn given_indexed_item_when_invalid_update_indexers_are_submitted_then_error_and_state_are_exact()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        let mut create = super::test_support::hash_table_request(table.clone());
        create.max_indexers = storage_types::MaxIndexers::try_from(2).unwrap();
        create.stream_specification = Some(storage_types::StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(storage_types::StreamViewType::NewAndOldImages),
        });
        store.operations.create_table(create).await.unwrap();

        let original = json!({"pk": {"S": "stable"}, "indexed": {"S": "original"}});
        let seed = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(), "Item": original, "Indexers": ["indexed"]
        }))
        .unwrap();
        store.operations.put_item(seed).await.unwrap();

        let read = GetItemRequest::try_from(json!({
            "TableName": table.as_ref(), "Key": {"pk": {"S": "stable"}}
        }))
        .unwrap();
        let stream = StreamRowKind::System(StreamName::system_table_stream());
        let before = store
            .stream_operations
            .read_forward(stream.clone(), None, 10)
            .await
            .unwrap();
        assert_eq!(before.items.len(), 1);

        for line in include_str!("../fixtures/indexer_invalid_cases.jsonl").lines() {
            let case: serde_json::Value = serde_json::from_str(line).unwrap();
            let name = case["name"].as_str().unwrap();
            let value = if case["indexed_value"].is_null() {
                json!({"S": "changed"})
            } else {
                case["indexed_value"].clone()
            };
            let request = UpdateItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Key": {"pk": {"S": "stable"}},
                "UpdateExpression": "SET #indexed = :value",
                "ExpressionAttributeNames": {"#indexed": "indexed"},
                "ExpressionAttributeValues": {":value": value},
                "Indexers": case["indexers"],
                "ReturnValues": "ALL_NEW"
            }))
            .unwrap();
            let rejected = observe(store.operations.update_item(request)).await;
            rejected.assert_no_warning_or_error();
            let span = rejected.one_span_with("dynamo_operation", &[("operation", "UpdateItem")]);
            assert_eq!(
                span.fields.len(),
                1,
                "{name}: operation span contains only safe context"
            );
            let message = case["message"].as_str().unwrap();
            let expected = format!(
                "{{\"__type\":\"com.amazon.coral.validate#ValidationException\",\"message\":\"\
                 {message}\"}}"
            );
            assert_dynamo_error_response_exact(
                rejected.result.unwrap_err(),
                axum::http::StatusCode::BAD_REQUEST,
                &expected,
            );
            let after = store.operations.get_item(read.clone()).await.unwrap();
            assert_eq!(
                serde_json::to_value(after).unwrap(),
                json!({"Item": original}),
                "{name}"
            );
        }

        let after = store
            .stream_operations
            .read_forward(stream, None, 10)
            .await
            .unwrap();
        assert_eq!(after.items.len(), 1, "rejected updates must not append CDC");
        assert_eq!(after.items[0].id, before.items[0].id);
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
