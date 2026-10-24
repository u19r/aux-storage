//! Stream discovery metadata through the replacement Dynamo catalog operation.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{CreateTableRequest, DescribeTableRequest};

use super::test_support::{TestStore, unique_table_name};

// Business and format rules: only a stream-enabled table publishes a coherent
// stream identity.
#[tokio::test]
async fn given_enabled_and_disabled_tables_when_created_and_described_then_only_enabled_table_has_stream_identity()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let enabled = unique_table_name();
        let enabled_request = CreateTableRequest::try_from(json!({
            "TableName": enabled.as_ref(),
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
            "StreamSpecification": {
                "StreamEnabled": true,
                "StreamViewType": "NEW_AND_OLD_IMAGES"
            }
        }))
        .expect("valid stream-enabled CreateTable request");
        let created = observe(store.operations.create_table(enabled_request)).await;
        created.assert_no_warning_or_error();
        let created = created
            .result
            .expect("create enabled table")
            .table_description;
        let described = observe(store.operations.describe_table(DescribeTableRequest {
            table_name: enabled.clone(),
        }))
        .await;
        described.assert_no_warning_or_error();
        let described = described.result.expect("describe enabled table").table;
        assert_eq!(created.latest_stream_arn, described.latest_stream_arn);
        assert_eq!(created.latest_stream_label, described.latest_stream_label);
        assert_eq!(
            serde_json::to_value(described.stream_specification).expect("serialize stream view"),
            json!({"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}),
        );
        let label = created
            .latest_stream_label
            .expect("enabled table has a stream label");
        assert_eq!(
            created.latest_stream_arn,
            Some(format!(
                "arn:aws:dynamodb:us-east-1:123456789012:table/{enabled}/stream/{label}"
            )),
        );

        let disabled = unique_table_name();
        let disabled_request = CreateTableRequest::try_from(json!({
            "TableName": disabled.as_ref(),
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .expect("valid stream-disabled CreateTable request");
        let created = observe(store.operations.create_table(disabled_request)).await;
        created.assert_no_warning_or_error();
        let created = created
            .result
            .expect("create disabled table")
            .table_description;
        let described = observe(store.operations.describe_table(DescribeTableRequest {
            table_name: disabled,
        }))
        .await;
        described.assert_no_warning_or_error();
        let described = described.result.expect("describe disabled table").table;
        assert!(created.latest_stream_arn.is_none());
        assert!(created.latest_stream_label.is_none());
        assert!(described.latest_stream_arn.is_none());
        assert!(described.latest_stream_label.is_none());
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
