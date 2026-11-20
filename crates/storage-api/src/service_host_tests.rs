//! A real customer commit whose injected WAL sync failure stops HTTP intake.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use serde_json::{Value, json};
use sql_backend::{SqlBackend as _, SqlFaultCall, SqlFaultProbe, SqlNativeEvent};
use storage::{DatabaseManager, DatabaseManagerRuntimeOptions, InitializedStore};
use storage_types::{NativeUnavailable, StorageEnum, StorageError};

#[derive(Default)]
struct FailedWalSync {
    is_armed: AtomicBool,
    commits: AtomicU64,
    frames: AtomicU64,
    failures: AtomicU64,
}

impl SqlFaultProbe for FailedWalSync {
    fn fail(&self, _call: SqlFaultCall<'_>) -> Option<StorageError> {
        None
    }

    fn before_wal_sync(&self) -> std::io::Result<()> {
        if self.is_armed.load(Ordering::SeqCst) {
            self.failures.fetch_add(1, Ordering::SeqCst);
            return Err(std::io::Error::other("injected WAL sync failure"));
        }
        Ok(())
    }

    fn observe(&self, event: SqlNativeEvent) {
        match event {
            SqlNativeEvent::Commit => {
                self.commits.fetch_add(1, Ordering::SeqCst);
            }
            SqlNativeEvent::WalFrames { frames } => {
                self.frames.fetch_add(frames, Ordering::SeqCst);
            }
            _ => {}
        }
    }
}

fn assert_durability_failed(error: &StorageError) {
    assert!(
        matches!(
            error.as_ref(),
            StorageEnum::Attempt(storage_types::AttemptFailure::Unavailable(
                NativeUnavailable::DurabilityFailed
            ))
        ),
        "{error}"
    );
}

async fn send_dynamo(
    client: &reqwest::Client,
    url: &str,
    operation: &str,
    body: Value,
) -> reqwest::Response {
    client
        .post(url)
        .header("x-amz-target", format!("DynamoDB_20120810.{operation}"))
        .json(&body)
        .send()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_serving_sqlite_when_native_commit_wal_sync_fails_then_health_and_http_host_stop() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("durability.db");
    let fault = Arc::new(FailedWalSync::default());
    let backend = sql_backend::sqlite::SqliteBackend::initialize(&path, Default::default())
        .await
        .unwrap()
        .with_fault_probe(fault.clone());
    let health = backend.durability_health().unwrap();
    let store = Arc::new(
        sql_backend::SqlStore::initialize_schema(
            backend,
            storage_types::InitialStreamLineages::ONE,
            true,
        )
        .await
        .unwrap(),
    );
    let config = storage_provider::StorageConfig {
        backend_type: storage_provider::StorageBackend::SQLite,
        initial_stream_lineages: None,
        connection_string: Some(path.to_string_lossy().into_owned()),
        file_path: None,
        sqlite: Some(storage_provider::SqliteSettings::default()),
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    };
    let database = Arc::new(
        DatabaseManager::from_initialized_store(
            config,
            InitializedStore::native(store.clone()),
            DatabaseManagerRuntimeOptions::builder()
                .enable_database_jobs(false)
                .build(),
        )
        .await
        .unwrap(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let state = Arc::new(
        super::service_host::prepare_app_state(
            database.clone(),
            &config::RootConfig::default(),
            &address.to_string(),
        )
        .unwrap(),
    );
    let router = crate::router::server_router(state.clone(), false);
    let host = tokio::spawn(super::service_host::serve_listener(
        listener,
        router,
        state.clone(),
        Duration::from_secs(5),
    ));
    tokio::time::timeout(Duration::from_secs(10), async {
        while !state.is_startup_ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let url = format!("http://{address}/");
    let response = send_dynamo(
        &client,
        &url,
        "CreateTable",
        json!({
            "TableName":"durability", "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
            "BillingMode":"PAY_PER_REQUEST"
        }),
    )
    .await;
    assert!(
        response.status().is_success(),
        "{}",
        response.text().await.unwrap()
    );
    let response = send_dynamo(
        &client,
        &url,
        "PutItem",
        json!({"TableName":"durability","Item":{"pk":{"S":"healthy"},"count":{"N":"0"}}}),
    )
    .await;
    assert!(
        response.status().is_success(),
        "{}",
        response.text().await.unwrap()
    );
    let response = send_dynamo(
        &client,
        &url,
        "GetItem",
        json!({"TableName":"durability","Key":{"pk":{"S":"healthy"}},"ConsistentRead":true}),
    )
    .await;
    assert!(response.status().is_success());
    assert_eq!(
        response.json::<Value>().await.unwrap()["Item"]["count"]["N"],
        "0"
    );
    assert!(!health.is_stopped());
    database.check_background_services().unwrap();
    let commits_before = fault.commits.load(Ordering::SeqCst);
    let frames_before = fault.frames.load(Ordering::SeqCst);
    fault.is_armed.store(true, Ordering::SeqCst);
    let response = send_dynamo(&client, &url, "UpdateItem", json!({"TableName":"durability","Key":{"pk":{"S":"healthy"}},"UpdateExpression":"ADD #counter :one","ExpressionAttributeNames":{"#counter":"count"},"ExpressionAttributeValues":{":one":{"N":"1"}}})).await;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        "a failed sync must not acknowledge success"
    );
    let body = response.text().await.unwrap();
    assert!(body.contains("InternalServerError"), "{body}");
    assert!(fault.commits.load(Ordering::SeqCst) > commits_before);
    assert!(fault.frames.load(Ordering::SeqCst) > frames_before);
    assert_eq!(fault.failures.load(Ordering::SeqCst), 1);
    assert!(health.is_stopped());
    assert_durability_failed(&database.check_background_services().unwrap_err());
    use storage_provider::DynamoOperations as _;
    let commits_after_failure = fault.commits.load(Ordering::SeqCst);
    let error = store
        .list_tables(storage_types::ListTablesRequest {
            exclusive_start_table_name: None,
            limit: None,
        })
        .await
        .unwrap_err();
    assert_durability_failed(&error);
    assert_eq!(fault.commits.load(Ordering::SeqCst), commits_after_failure);
    let error = tokio::time::timeout(Duration::from_secs(10), host)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_durability_failed(&error);
    let new_client = reqwest::Client::new();
    assert!(
        new_client.post(&url).send().await.is_err(),
        "the listener must stop accepting requests"
    );
    database.shutdown_background_services().await.unwrap();
    drop(state);
    drop(database);
    drop(store);
    // Reopening uses the normal startup barrier; the prior unknown outcome
    // does not mean the native commit rolled back.
    let backend = sql_backend::sqlite::SqliteBackend::initialize(&path, Default::default())
        .await
        .unwrap();
    let recovered_health = backend.durability_health().unwrap();
    let recovered = sql_backend::SqlStore::initialize_schema(
        backend,
        storage_types::InitialStreamLineages::ONE,
        true,
    )
    .await
    .unwrap();
    let item = recovered
        .get_item(
            serde_json::from_value(json!({
                "TableName":"durability", "Key":{"pk":{"S":"healthy"}}, "ConsistentRead":true
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    let item = serde_json::to_value(item).unwrap();
    assert_eq!(
        item["Item"]["count"]["N"], "1",
        "the unknown increment committed exactly once and was not replayed"
    );
    assert!(!recovered_health.is_stopped());
    drop(recovered);
}
