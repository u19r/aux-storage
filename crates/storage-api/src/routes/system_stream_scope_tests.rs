use std::sync::Arc;

use axum::{
    body::{self, Bytes},
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
};
use serde_json::{Value, json};
use storage::DatabaseManager;
#[cfg(feature = "rocksdb")]
use storage::DatabaseManagerRuntimeOptions;
#[cfg(feature = "rocksdb")]
use storage_provider::RocksdbSettings;
#[cfg(feature = "sqlite")]
use storage_provider::SqliteSettings;
#[cfg(feature = "remote")]
use storage_provider::{RemoteCredentialStrategy, RemoteStorageSettings};
#[cfg(any(feature = "sqlite", feature = "rocksdb"))]
use storage_provider::{StorageBackend, StorageConfig};

use crate::{
    routes::{dynamodb::dynamodb_endpoint, routes_tests::for_each_conformance_backend},
    types::AppState,
};

#[tokio::test]
async fn public_system_stream_scope_action_returns_scope_for_followup_page() {
    for_each_conformance_backend(|backend, db| async move {
        let app_state = Arc::new(AppState::new(db));

        let scope = execute_json(
            app_state.clone(),
            "DynamoDB_20120810.DescribeSystemStream",
            json!({}),
        )
        .await
        .unwrap_or_else(|error| panic!("{backend} scope discovery should succeed: {error}"));
        let source_scope = scope
            .get("SourceScope")
            .cloned()
            .unwrap_or_else(|| panic!("{backend} scope response should contain SourceScope"));

        assert_eq!(source_scope["Version"], 1, "backend: {backend}");
        assert!(
            source_scope["ScopeId"]
                .as_str()
                .is_some_and(|id| !id.is_empty()),
            "backend: {backend}"
        );
        assert!(
            source_scope["Generation"]
                .as_u64()
                .is_some_and(|generation| generation > 0),
            "backend: {backend}"
        );

        let page = execute_json(
            app_state,
            "DynamoDB_20120810.GetStreamRecords",
            json!({
                "SystemStream": true,
                "SourceScope": source_scope,
                "Limit": 1,
            }),
        )
        .await
        .unwrap_or_else(|error| {
            panic!("{backend} discovered scope should authorize a SystemStream page: {error}")
        });

        assert_eq!(page["Records"], json!([]), "backend: {backend}");
        assert_eq!(page["SourceScope"]["Version"], 1, "backend: {backend}");
        assert!(page["Checkpoint"].is_string(), "backend: {backend}");
    })
    .await;
}

#[tokio::test]
async fn given_terminal_system_stream_page_when_resuming_then_checkpoint_prevents_replay() {
    for limit in [10, 8_192] {
        for_each_conformance_backend(|backend, db| async move {
            let state = Arc::new(AppState::new(db));
            let table = "terminal_checkpoint_records";
            execute_json(
                state.clone(),
                "DynamoDB_20120810.CreateTable",
                json!({
                    "TableName": table,
                    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                    "BillingMode": "PAY_PER_REQUEST",
                    "StreamSpecification": {
                        "StreamEnabled": true,
                        "StreamViewType": "NEW_AND_OLD_IMAGES"
                    }
                }),
            )
            .await
            .expect("create streamed table");
            for key in ["first", "second"] {
                put_terminal_poll_item(state.clone(), table, key).await;
            }
            let described = execute_json(
                state.clone(),
                "DynamoDB_20120810.DescribeSystemStream",
                json!({}),
            )
            .await
            .expect("describe stream");
            let mut request = json!({
                "SystemStream": true,
                "SourceScope": described["SourceScope"],
                "Limit": limit
            });
            let first = execute_json(
                state.clone(),
                "DynamoDB_20120810.GetStreamRecords",
                request.clone(),
            )
            .await
            .expect("nonempty terminal page");
            let records = first["Records"].as_array().expect("records");
            let keys: Vec<_> = records
                .iter()
                .filter(|record| record["SourceTableName"] == table)
                .map(|record| record["Keys"]["pk"]["S"].as_str().expect("key"))
                .collect();
            assert_eq!(keys, ["first", "second"], "{backend}, limit {limit}");
            assert!(records.len() < limit as usize);
            assert!(first.get("LastEvaluatedKey").is_none());
            let checkpoint = first["Checkpoint"]
                .as_str()
                .unwrap_or_else(|| panic!("{backend}, limit {limit}: terminal checkpoint missing"));
            request["Checkpoint"] = json!(checkpoint);
            let caught_up = execute_json(
                state.clone(),
                "DynamoDB_20120810.GetStreamRecords",
                request.clone(),
            )
            .await
            .expect("resume persisted terminal checkpoint");
            assert_eq!(caught_up["Records"], json!([]));
            assert_eq!(caught_up["Checkpoint"], checkpoint);
            assert_eq!(caught_up["SourceScope"], described["SourceScope"]);

            put_terminal_poll_item(state.clone(), table, "third").await;
            let appended =
                execute_json(state.clone(), "DynamoDB_20120810.GetStreamRecords", request)
                    .await
                    .expect("terminal checkpoint remains resumable after append");
            let records = appended["Records"].as_array().expect("appended records");
            assert_eq!(records.len(), 1, "prior records must not be replayed");
            assert_eq!(records[0]["SourceTableName"], table);
            assert_eq!(records[0]["Keys"]["pk"]["S"], "third");
            assert_ne!(appended["Checkpoint"], first["Checkpoint"]);
            assert!(appended["Checkpoint"].is_string());
        })
        .await;
    }
}

async fn put_terminal_poll_item(state: Arc<AppState>, table: &str, key: &str) {
    execute_json(
        state,
        "DynamoDB_20120810.PutItem",
        json!({"TableName": table, "Item": {"pk": {"S": key}}}),
    )
    .await
    .expect("append item to logical stream");
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn public_system_stream_scope_action_is_stable_after_sqlite_reopen() {
    let path = crate::storage_api_test_support_tests::unique_path("system-stream-scope-api");
    let config = StorageConfig {
        backend_type: StorageBackend::SQLite,
        connection_string: Some(path.to_string_lossy().into_owned()),
        file_path: None,
        sqlite: Some(SqliteSettings {
            force_file_backed_database: true,
            ..SqliteSettings::default()
        }),
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    };

    let first = DatabaseManager::new_for_test_with_config(config.clone())
        .await
        .expect("first SQLite manager");
    let first_scope = discover_scope(Arc::new(first)).await;

    let second = DatabaseManager::new_for_test_with_config(config)
        .await
        .expect("reopened SQLite manager");
    let second_scope = discover_scope(Arc::new(second)).await;

    assert_eq!(first_scope, second_scope);
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn public_system_stream_scope_action_returns_scope_for_rocksdb() {
    let path = crate::storage_api_test_support_tests::unique_path("system-stream-scope-rocks");
    let config = StorageConfig {
        backend_type: StorageBackend::RocksDB,
        connection_string: None,
        file_path: Some(path.to_string_lossy().into_owned()),
        sqlite: None,
        postgres: None,
        turso: None,
        rocksdb: Some(RocksdbSettings {
            immediate_gsi_consistency: true,
        }),
        foundationdb: None,
        remote: None,
    };
    let runtime_options = DatabaseManagerRuntimeOptions::builder()
        // The provider's legacy maintenance timers own provider clones and
        // are intentionally not part of this persistence/reopen assertion.
        .enable_database_jobs(false)
        .build();
    let first = DatabaseManager::new_with_config_and_runtime_options(
        config.clone(),
        runtime_options.clone(),
    )
    .await
    .expect("first RocksDB manager");
    let first_scope = discover_scope(Arc::new(first)).await;
    let second = DatabaseManager::new_with_config_and_runtime_options(config, runtime_options)
        .await
        .expect("reopened RocksDB manager");
    let db = Arc::new(second);
    let app_state = Arc::new(AppState::new(db));
    let scope = execute_json(
        app_state.clone(),
        "DynamoDB_20120810.DescribeSystemStream",
        json!({}),
    )
    .await
    .expect("RocksDB scope discovery should succeed");
    let source_scope = scope["SourceScope"].clone();
    assert_eq!(source_scope, first_scope);
    assert_eq!(source_scope["Version"], 1);

    let page = execute_json(
        app_state,
        "DynamoDB_20120810.GetStreamRecords",
        json!({
            "SystemStream": true,
            "SourceScope": source_scope,
            "Limit": 1,
        }),
    )
    .await
    .expect("RocksDB scope should authorize a SystemStream page");
    assert_eq!(page["Records"], json!([]));
}

#[tokio::test]
async fn public_system_stream_page_without_scope_remains_a_bad_request() {
    for_each_conformance_backend(|backend, db| async move {
        let error = execute_json(
            Arc::new(AppState::new(db)),
            "DynamoDB_20120810.GetStreamRecords",
            json!({"SystemStream": true, "Limit": 1}),
        )
        .await
        .expect_err("SystemStream must require discovered scope");
        assert!(
            error.contains("SourceScope is required"),
            "backend {backend}: unexpected error: {error}"
        );
    })
    .await;
}

#[cfg(feature = "remote")]
#[tokio::test]
async fn public_system_stream_scope_action_reports_typed_unsupported_backend_error() {
    let provider = storage::RemoteStorageProvider::new(RemoteStorageSettings {
        endpoint_urls: vec!["http://127.0.0.1:1".to_string()],
        region: None,
        tls: false,
        credentials: RemoteCredentialStrategy::DefaultChain,
        timeouts: None,
    })
    .await
    .expect("construct remote provider");
    let db = Arc::new(
        DatabaseManager::new_with_mocks(Arc::new(provider)).expect("construct remote manager"),
    );
    let error = execute_json(
        Arc::new(AppState::new(db)),
        "DynamoDB_20120810.DescribeSystemStream",
        json!({}),
    )
    .await
    .expect_err("remote SystemStream scope must fail closed");
    assert!(
        error == "500 Internal Server Error: stream operations are unavailable",
        "unexpected error: {error}"
    );
}

async fn discover_scope(db: Arc<DatabaseManager>) -> Value {
    execute_json(
        Arc::new(AppState::new(db)),
        "DynamoDB_20120810.DescribeSystemStream",
        json!({}),
    )
    .await
    .expect("scope discovery should succeed")
    .get("SourceScope")
    .cloned()
    .expect("scope response should contain SourceScope")
}

async fn execute_json(
    app_state: Arc<AppState>,
    target: &'static str,
    payload: Value,
) -> Result<Value, String> {
    let mut headers = HeaderMap::new();
    headers.insert("x-amz-target", HeaderValue::from_static(target));
    let body = Bytes::from(serde_json::to_vec(&payload).expect("payload should serialize"));
    let response = dynamodb_endpoint(State(app_state), headers, body)
        .await
        .map_err(|error| {
            let (status, _headers, body) = error.into_parts();
            format!("{status}: {}", body.0.message)
        })?;
    if response.status() != StatusCode::OK {
        return Err(format!("unexpected response status {}", response.status()));
    }
    let body = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .map_err(|error| error.to_string())?;
    serde_json::from_slice(&body).map_err(|error| error.to_string())
}
