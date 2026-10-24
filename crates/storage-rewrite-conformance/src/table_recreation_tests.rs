//! Public table controls belong to a table incarnation, not its reusable name.

use std::{
    panic::{AssertUnwindSafe, resume_unwind},
    time::Duration,
};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{
    CreateTableRequest, DeleteTableRequest, DescribeTableRequest, DescribeTimeToLiveRequest,
    TimeToLiveStatus, UpdateTimeToLiveRequest,
};

use super::test_support::{TestStore, hash_table_request, unique_table_name};

// Business and boundary rules: deleting a table retires its GSI and TTL
// controls with its identity.
#[tokio::test]
async fn given_gsi_and_ttl_when_table_recreated_then_old_controls_are_absent() {
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
        created.one_span_with("dynamo_operation", &[("operation", "CreateTable")]);
        assert_eq!(
            created
                .result
                .unwrap()
                .table_description
                .global_secondary_indexes
                .unwrap()
                .len(),
            1,
        );

        let request: UpdateTimeToLiveRequest = serde_json::from_value(json!({
            "TableName": table.as_ref(),
            "TimeToLiveSpecification": {"AttributeName": "expires_at", "Enabled": true}
        }))
        .unwrap();
        let enabled = observe(store.operations.update_time_to_live(request)).await;
        enabled.assert_no_warning_or_error();
        enabled.result.unwrap();
        let old_ttl = store
            .operations
            .describe_time_to_live(DescribeTimeToLiveRequest {
                table_name: table.clone(),
            })
            .await
            .unwrap()
            .time_to_live_description
            .unwrap();
        assert_eq!(old_ttl.attribute_name.as_deref(), Some("expires_at"));
        assert!(matches!(
            old_ttl.time_to_live_status,
            TimeToLiveStatus::Enabling | TimeToLiveStatus::Enabled
        ));

        let deleted = observe(store.operations.delete_table(DeleteTableRequest {
            table_name: table.clone(),
        }))
        .await;
        deleted.assert_no_warning_or_error();
        deleted.one_span_with("dynamo_operation", &[("operation", "DeleteTable")]);
        deleted.result.unwrap();

        store.advance_time(Duration::from_secs(30));
        let recreated = observe(
            store
                .operations
                .create_table(hash_table_request(table.clone())),
        )
        .await;
        recreated.assert_no_warning_or_error();
        recreated.one_span_with("dynamo_operation", &[("operation", "CreateTable")]);
        recreated.result.unwrap();

        let described = store
            .operations
            .describe_table(DescribeTableRequest {
                table_name: table.clone(),
            })
            .await
            .unwrap()
            .table;
        assert!(described.global_secondary_indexes.is_none());
        assert!(!described.deletion_protection_enabled);

        let new_ttl = store
            .operations
            .describe_time_to_live(DescribeTimeToLiveRequest {
                table_name: table.clone(),
            })
            .await
            .unwrap()
            .time_to_live_description
            .unwrap();
        assert_eq!(new_ttl.attribute_name, None);
        assert_eq!(new_ttl.time_to_live_status, TimeToLiveStatus::Disabled);
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
