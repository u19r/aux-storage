use serde_json::json;
use storage::DatabaseManager;
use storage_types::{
    MultiRegionConsistency, ReplicaStatus, StreamRetentionDuration, TableName, UpdateTableRequest,
};

use crate::{
    routes::routes_support_tests::{create_test_db, handle_create_table, handle_update_table},
    types::Response,
};

async fn create_test_db_manager() -> std::sync::Arc<DatabaseManager> {
    create_test_db().await
}

#[tokio::test]
async fn handle_update_table_accepts_replica_updates_and_returns_replica_state() {
    let db = create_test_db_manager().await;

    handle_create_table(
        db.clone(),
        json!({
            "TableName": "RouteGlobalTable",
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ]
        })
        .try_into()
        .expect("valid create table request"),
    )
    .await
    .expect("create table");

    let response = handle_update_table(
        db,
        json!({
            "TableName": "RouteGlobalTable",
            "ReplicaUpdates": [
                {
                    "Create": {
                        "RegionName": "us-east-1"
                    }
                }
            ]
        })
        .try_into()
        .expect("valid update table request"),
    )
    .await
    .expect("update table");

    let Response::UpdateTable(response) = response else {
        panic!("unexpected response variant");
    };
    assert_eq!(
        response.table_description.multi_region_consistency,
        Some(MultiRegionConsistency::Eventual)
    );
    assert_eq!(
        response.table_description.replicas,
        Some(vec![storage_types::ReplicaDescription {
            region_name: "us-east-1".to_string(),
            replica_status: ReplicaStatus::Creating,
            replica_status_description: Some("Replica creation requested".to_string()),
            replica_inaccessible_date_time: None,
        }])
    );
}

#[tokio::test]
async fn handle_update_table_accepts_custom_stream_duration_fields() {
    let db = create_test_db_manager().await;

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
