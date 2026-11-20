use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};

use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
};
use serde_json::json;
use storage::DatabaseManager;
use storage_types::{CreateTableRequest, PairState, TableName};

use crate::types::AppState;

const POLLS: usize = 150;

fn port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct Region {
    name: &'static str,
    rank: u16,
    port: u16,
    database: std::path::PathBuf,
    regional_file: std::path::PathBuf,
}

impl Region {
    fn new(directory: &Path, name: &'static str, rank: u16, own: u16, peer: u16) -> Self {
        let (other_name, other_rank) = if name == "a" { ("b", 2) } else { ("a", 1) };
        let regional_file = directory.join(format!("{name}-regional.json"));
        let regions = json!([
            {"name": name, "rank": rank, "address": format!("127.0.0.1:{own}")},
            {"name": other_name, "rank": other_rank, "address": format!("127.0.0.1:{peer}")},
        ]);
        std::fs::write(
            &regional_file,
            json!({
                "local_region": name, "listen": format!("127.0.0.1:{own}"), "regions": regions,
                "send_interval_ms": 20, "page_entry_limit": 3, "request_timeout_ms": 2000
            })
            .to_string(),
        )
        .unwrap();
        Self {
            name,
            rank,
            port: own,
            database: directory.join(format!("{name}.db")),
            regional_file,
        }
    }

    async fn open(&self) -> DatabaseManager {
        let features = config::Features {
            backends: config::Backends {
                initial_stream_lineages: None,
                sqlite: Some(config::SqliteBackendConfig {
                    db_path: self.database.display().to_string(),
                }),
                turso: None,
                postgres: None,
                rocksdb: None,
                foundationdb: None,
                remote: None,
            },
            regional_connections: HashMap::from([(
                "default".to_owned(),
                self.regional_file.clone(),
            )]),
            ..config::Features::default()
        };
        let root = config::RootConfig {
            features,
            ..config::RootConfig::default()
        };
        let runtime = storage::startup::RuntimeConfig::from_file(&root).unwrap_or_else(|error| {
            panic!(
                "region {} ({}, port {}): {error:?}",
                self.name, self.rank, self.port
            )
        });
        runtime.open().await.unwrap_or_else(|error| {
            panic!(
                "region {} ({}, port {}): {error:?}",
                self.name, self.rank, self.port
            )
        })
    }
}

fn table() -> TableName {
    TableName::new("regional_orders")
}

async fn create_source(manager: &DatabaseManager) {
    let request: CreateTableRequest = serde_json::from_value(json!({
        "TableName": table().as_ref(),
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "BillingMode": "PAY_PER_REQUEST",
        "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}
    }))
    .unwrap();
    manager.create_table(&request).await.unwrap();
}

async fn http_update_table(
    db: Arc<DatabaseManager>,
    payload: serde_json::Value,
) -> Result<axum::response::Response, crate::routes::dynamodb::DynamoError> {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-target",
        HeaderValue::from_static("DynamoDB_20120810.UpdateTable"),
    );
    let body = Bytes::from(serde_json::to_vec(&payload).expect("payload"));
    crate::routes::dynamodb::dynamodb_endpoint(State(Arc::new(AppState::new(db))), headers, body)
        .await
}

#[tokio::test]
async fn given_regional_manager_when_update_table_replica_create_arrives_over_http_then_pair_becomes_active()
 {
    let directory = tempfile::tempdir().unwrap();
    let (port_a, port_b) = (port(), port());
    let a_region = Region::new(directory.path(), "a", 1, port_a, port_b);
    let b_region = Region::new(directory.path(), "b", 2, port_b, port_a);
    let a = Arc::new(a_region.open().await);
    let b = Arc::new(b_region.open().await);
    create_source(&a).await;
    let response = http_update_table(
        Arc::clone(&a),
        json!({"TableName": "regional_orders", "ReplicaUpdates": [{"Create": {"RegionName": "b"}}]}),
    )
    .await
    .unwrap_or_else(|_| panic!("UpdateTable over HTTP failed"));
    assert_eq!(response.status(), StatusCode::OK);
    for side in [&a, &b] {
        let mut state = None;
        for _ in 0..POLLS {
            state = side
                .regional_binding_for_test(&table())
                .await
                .unwrap()
                .map(|binding| binding.state);
            if state == Some(PairState::Active) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(state, Some(PairState::Active));
    }
    a.shutdown_background_services().await.unwrap();
    b.shutdown_background_services().await.unwrap();
}

#[tokio::test]
async fn given_no_regional_file_when_update_table_replica_create_arrives_over_http_then_library_error()
 {
    let db = Arc::new(DatabaseManager::new_for_test().await.unwrap());
    let library = db
        .update_table(
            serde_json::from_value(json!({
                "TableName": "regional_orders", "ReplicaUpdates": [{"Create": {"RegionName": "b"}}]
            }))
            .unwrap(),
        )
        .await
        .expect_err("library rejects");
    assert!(format!("{library:?}").contains("multi-region replication is not available"));
    let Err(rejection) = http_update_table(
        Arc::clone(&db),
        json!({"TableName": "regional_orders", "ReplicaUpdates": [{"Create": {"RegionName": "b"}}]}),
    )
    .await
    else {
        panic!("HTTP rejects");
    };
    let (status, _headers, body) = rejection.into_parts();
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body.0.error_type, "InternalServerError");
    assert_eq!(body.0.message, "multi-region replication is not available");
    db.shutdown_background_services().await.unwrap();
}
