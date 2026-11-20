use std::sync::Arc;

use storage_types::{StorageEnum, UpdateTableRequest};

use super::replica_ops::single_replica_update;
use crate::database_manager::{
    DatabaseManager, DatabaseManagerRuntimeOptions, construction::single_connection_registry,
    test_database::immediate_sqlite_test_config,
};

fn request(extra: serde_json::Value, updates: serde_json::Value) -> UpdateTableRequest {
    let mut value = serde_json::json!({"TableName": "orders"});
    if !updates.is_null() {
        value["ReplicaUpdates"] = updates;
    }
    if let Some(extra) = extra.as_object() {
        value.as_object_mut().unwrap().extend(extra.clone());
    }
    serde_json::from_value(value).unwrap()
}

#[test]
fn given_replica_update_alone_then_accepted() {
    let create = request(
        serde_json::json!({}),
        serde_json::json!([{"Create": {"RegionName": "b"}}]),
    );
    assert!(single_replica_update(&create).unwrap().create.is_some());
}

#[test]
fn given_delete_update_alone_then_accepted() {
    let delete = request(
        serde_json::json!({}),
        serde_json::json!([{"Delete": {"RegionName": "b"}}]),
    );
    assert!(single_replica_update(&delete).unwrap().delete.is_some());
}

#[test]
fn given_replica_update_with_stream_change_then_validation() {
    let combined = request(
        serde_json::json!({"StreamSpecification": {"StreamEnabled": false}}),
        serde_json::json!([{"Create": {"RegionName": "b"}}]),
    );
    assert!(single_replica_update(&combined).is_err());
}

#[test]
fn given_two_replica_updates_or_none_then_validation() {
    let two = request(
        serde_json::json!({}),
        serde_json::json!([{"Create": {"RegionName": "b"}}, {"Create": {"RegionName": "c"}}]),
    );
    assert!(single_replica_update(&two).is_err());
    let empty = request(serde_json::json!({}), serde_json::json!([]));
    assert!(single_replica_update(&empty).is_err());
    let none = request(serde_json::json!({}), serde_json::Value::Null);
    assert!(single_replica_update(&none).is_err());
}

fn is_unavailable(error: &storage_types::StorageError) -> bool {
    matches!(error.base_error(), Some(StorageEnum::Unsupported { message }) if message.contains("not available"))
}

fn create_request() -> UpdateTableRequest {
    request(
        serde_json::json!({}),
        serde_json::json!([{"Create": {"RegionName": "b"}}]),
    )
}

#[tokio::test]
async fn given_no_regional_file_when_replica_create_then_unsupported() {
    let manager = DatabaseManager::new_for_test().await.unwrap();
    let error = manager
        .update_table_replicas(create_request())
        .await
        .unwrap_err();
    assert!(is_unavailable(&error), "{error:?}");
    manager.shutdown_background_services().await.unwrap();
}

#[tokio::test]
async fn given_default_connection_named_primary_with_regional_file_when_replica_create_then_service_found()
 {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("regional.json");
    let regions = serde_json::json!([
        {"name": "a", "rank": 1, "address": format!("127.0.0.1:{port}")},
        {"name": "b", "rank": 2, "address": "127.0.0.1:9"},
    ]);
    std::fs::write(
        &path,
        serde_json::json!({"local_region": "a", "listen": format!("127.0.0.1:{port}"), "regions": regions}).to_string(),
    )
    .unwrap();
    let config = Arc::new(regional_replication::RegionalConfig::load(&path).unwrap());

    let mut registry = single_connection_registry(immediate_sqlite_test_config());
    let connection = registry.connections.remove("default").unwrap();
    registry.default_connection_id = "primary".to_owned();
    registry
        .connections
        .insert("primary".to_owned(), connection);
    let mut options = DatabaseManagerRuntimeOptions::builder()
        .enable_database_jobs(true)
        .build();
    options
        .regional_connections
        .insert("primary".to_owned(), config);
    let manager =
        DatabaseManager::new_with_connection_registry_and_runtime_options(registry, options)
            .await
            .unwrap();
    // The table does not exist: any error but "not available" shows the service
    // was found.
    let result = manager.update_table_replicas(create_request()).await;
    if let Err(error) = &result {
        assert!(!is_unavailable(error), "{error:?}");
    }
    manager.shutdown_background_services().await.unwrap();
}
