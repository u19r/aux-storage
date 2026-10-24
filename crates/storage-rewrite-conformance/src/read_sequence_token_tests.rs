//! ReadSequence continuation integrity at the replacement operation boundary.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{KeyAttributeType, PutItemRequest, ReadSequenceRequest};

use super::test_support::{TestStore, assert_dynamo_error_response_exact, unique_table_name};

/// Adversarial rule from Cucumber: token integrity is checked before deferred
/// provider reads, with an exact customer-visible validation error.
#[tokio::test]
async fn given_tampered_continuation_when_read_sequence_resumes_then_no_provider_read_occurs() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        store.create_hash_range_table(table.clone(), KeyAttributeType::S).await;
        for sk in ["a", "b"] {
            let put = PutItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Item": {"pk": {"S": "acct"}, "sk": {"S": sk}}
            })).unwrap();
            store.operations.put_item(put).await.unwrap();
        }

        let request = json!({
            "Nodes": [{
                "Name": "page",
                "Operation": {"Query": {
                    "TableName": table.as_ref(),
                    "KeyConditionExpression": "pk = :pk",
                    "ExpressionAttributeValues": {":pk": {"S": "acct"}},
                    "Limit": 1
                }},
                "Inputs": {},
                "After": []
            }],
            "Outputs": ["page"]
        });
        let first = ReadSequenceRequest::try_from(request.clone()).unwrap();
        let first = observe(store.operations.read_sequence(first)).await;
        first.assert_no_warning_or_error();
        let first = first.result.unwrap();
        assert!(first.partial, "bounded first page must be partial");
        let token = first.next_sequence_token.unwrap();
        let mut bytes = token.into_bytes();
        let last = bytes.len().checked_sub(1).unwrap();
        bytes[last] = if bytes[last] == b'0' { b'1' } else { b'0' };

        let mut resumed = request;
        resumed["NextSequenceToken"] = json!(String::from_utf8(bytes).unwrap());
        let resumed = ReadSequenceRequest::try_from(resumed).unwrap();
        let rejected = observe(store.operations.read_sequence(resumed)).await;
        rejected.assert_no_warning_or_error();
        rejected.assert_database_calls_exact(&[]);
        let span = rejected.one_span_with(
            "dynamo_operation",
            &[("operation", "ReadSequence"), ],
        );
        assert_eq!(span.fields.len(), 1);
        assert_dynamo_error_response_exact(
            rejected.result.expect_err("tampered continuation must fail"),
            axum::http::StatusCode::BAD_REQUEST,
            r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"ReadSequence token is stale"}"#,
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
