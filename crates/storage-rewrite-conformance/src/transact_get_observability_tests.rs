//! Public TransactGetItems batching and safe operation context.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{PutItemRequest, TransactGetItemsRequest};

use super::test_support::TestStore;

/// Business and implementation rules: ordered cross-table reads share one
/// primary batch while traces name tables without exposing keys or items.
#[tokio::test]
async fn given_interleaved_tables_when_transact_get_runs_then_ordered_response_and_one_safe_batch_are_observed()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let first = store.create_standard_table().await;
        let second = store.create_standard_table().await;
        for (table, key, value) in [
            (&first, "private-first-key", "private-first-value"),
            (&second, "private-second-key", "private-second-value"),
            (&first, "private-third-key", "private-third-value"),
        ] {
            let request = PutItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Item": {"pk": {"S": key}, "value": {"S": value}}
            }))
            .unwrap();
            store.operations.put_item(request).await.unwrap();
        }

        let request = TransactGetItemsRequest::try_from(json!({
            "TransactItems": [
                {"Get": {
                    "TableName": second.as_ref(),
                    "Key": {"pk": {"S": "private-second-key"}}
                }},
                {"Get": {
                    "TableName": first.as_ref(),
                    "Key": {"pk": {"S": "private-third-key"}}
                }},
                {"Get": {
                    "TableName": first.as_ref(),
                    "Key": {"pk": {"S": "private-first-key"}},
                    "ProjectionExpression": "#value",
                    "ExpressionAttributeNames": {"#value": "value"}
                }},
                {"Get": {
                    "TableName": first.as_ref(),
                    "Key": {"pk": {"S": "private-missing-key"}}
                }}
            ]
        }))
        .unwrap();
        let observed = observe(store.operations.transact_get_items(request)).await;
        observed.assert_no_warning_or_error();
        assert_eq!(
            serde_json::to_value(observed.result.as_ref().unwrap()).unwrap(),
            json!({
                "Responses": [
                    {"Item": {
                        "pk": {"S": "private-second-key"},
                        "value": {"S": "private-second-value"}
                    }},
                    {"Item": {
                        "pk": {"S": "private-third-key"},
                        "value": {"S": "private-third-value"}
                    }},
                    {"Item": {"value": {"S": "private-first-value"}}},
                    {}
                ]
            })
        );

        let span = observed.one_span_with("dynamo_operation", &[("operation", "TransactGetItems")]);
        assert_eq!(span.fields.len(), 1);
        let trace = format!("{:?} {:?}", observed.events, observed.spans);
        assert!(!trace.contains(first.as_ref()));
        assert!(!trace.contains(second.as_ref()));
        assert!(!trace.contains("private-first-key"));
        assert!(!trace.contains("private-second-key"));
        assert!(!trace.contains("private-first-value"));
        assert!(!trace.contains("private-second-value"));
        assert!(!trace.contains("private-third-key"));
        assert!(!trace.contains("private-third-value"));
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
