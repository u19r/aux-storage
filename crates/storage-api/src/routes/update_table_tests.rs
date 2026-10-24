use serde_json::json;
use storage_types::{StreamRetentionDuration, TableName, UpdateTableRequest};

use crate::routes::routes_tests::{
    for_each_conformance_backend, handle_create_table, handle_update_table,
};

#[tokio::test]
async fn handle_update_table_accepts_custom_stream_duration_fields() {
    for_each_conformance_backend(|_backend, db| async move {
        handle_create_table(
            db.clone(),
            json!({
                "TableName": "RouteUpdateDuration",
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"}
                ],
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"}
                ]
            })
            .try_into()
            .expect("valid create table request"),
        )
        .await
        .expect("create table");

        handle_update_table(
            db.clone(),
            json!({
                "TableName": "RouteUpdateDuration",
                "AuxStreamDurationHours": 24,
                "AuxDefaultItemStreamDurationHours": 48
            })
            .try_into()
            .expect("valid update table request"),
        )
        .await
        .expect("update table with custom stream duration");

        let table = db
            .get_table_info(&TableName::new("RouteUpdateDuration"))
            .await
            .expect("table metadata");
        assert_eq!(
            table.table_stream_duration,
            StreamRetentionDuration::FiniteHours(24)
        );
        assert_eq!(
            table.default_item_stream_duration,
            StreamRetentionDuration::FiniteHours(48)
        );
    })
    .await;
}

#[test]
fn update_table_request_rejects_unsupported_multi_region_consistency_field() {
    let request: Result<UpdateTableRequest, String> = json!({
        "TableName": "RouteGlobalTable",
        "MultiRegionConsistency": "STRONG"
    })
    .try_into();

    let error = request.expect_err("unsupported MultiRegionConsistency should fail");
    assert!(
        error.contains("unknown field `MultiRegionConsistency`"),
        "unexpected error: {error}"
    );
}

#[test]
fn update_table_request_rejects_unsupported_witness_updates_field() {
    let request: Result<UpdateTableRequest, String> = json!({
        "TableName": "RouteGlobalTable",
        "GlobalTableWitnessUpdates": [
            {
                "Create": {
                    "RegionName": "us-west-2"
                }
            }
        ]
    })
    .try_into();

    let error = request.expect_err("unsupported witness updates should fail");
    assert!(
        error.contains("unknown field `GlobalTableWitnessUpdates`"),
        "unexpected error: {error}"
    );
}
