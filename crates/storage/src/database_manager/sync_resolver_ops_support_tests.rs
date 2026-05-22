use std::collections::HashMap;

use storage_provider::{SqliteSettings, StorageBackend, StorageConfig};
use storage_sync::{SyncCommitMetadata, SyncLogId};
use storage_types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateGlobalSecondaryIndex,
    CreateTableRequest, IndexName, KeyAttributeType, KeySchemaElement, KeyType, Projection,
    ProjectionType, StreamSpecification, StreamViewType, TableName,
};

use crate::{DatabaseManager, database_manager::DatabaseManagerRuntimeOptions};

pub(super) async fn create_hash_table(db: &DatabaseManager, table_name: &TableName) {
    db.create_table(&CreateTableRequest::new(
        table_name.clone(),
        vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::S,
        }],
        vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        BillingMode::PayPerRequest,
    ))
    .await
    .expect("create table");
}

pub(super) async fn create_hash_table_with_stream(db: &DatabaseManager, table_name: &TableName) {
    let request = CreateTableRequest::new(
        table_name.clone(),
        vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::S,
        }],
        vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        BillingMode::PayPerRequest,
    )
    .with_stream_specification(Some(StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(StreamViewType::NewAndOldImages),
    }));
    db.create_table(&request).await.expect("create table");
}

pub(super) async fn create_gsi_table(db: &DatabaseManager, table_name: &TableName) {
    let request = CreateTableRequest::new(
        table_name.clone(),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        BillingMode::PayPerRequest,
    )
    .with_global_secondary_indexes(Some(vec![CreateGlobalSecondaryIndex {
        index_name: IndexName::new("TestGSI"),
        key_schema: vec![KeySchemaElement {
            attribute_name: "gsi_pk".to_string(),
            key_type: KeyType::Hash,
        }],
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    }]));
    db.create_table(&request).await.expect("create table");
}

pub(super) async fn create_single_node_sync_db() -> DatabaseManager {
    DatabaseManager::new_for_test_with_runtime_options(
        DatabaseManagerRuntimeOptions::builder()
            .enable_single_node_sync_mode(true)
            .build(),
    )
    .await
    .expect("db")
}

pub(super) async fn create_single_node_sync_db_with_immediate_gsi() -> DatabaseManager {
    DatabaseManager::new_with_config_and_runtime_options(
        StorageConfig {
            backend_type: StorageBackend::SQLite,
            connection_string: Some(":memory:".to_string()),
            file_path: None,
            sqlite: Some(SqliteSettings {
                immediate_gsi_consistency: true,
                ..SqliteSettings::default()
            }),
            postgres: None,
            turso: None,
            rocksdb: None,
            foundationdb: None,
            remote: None,
        },
        DatabaseManagerRuntimeOptions::builder()
            .enable_single_node_sync_mode(true)
            .build(),
    )
    .await
    .expect("db")
}

pub(super) fn file_backed_sqlite_config(label: &str) -> StorageConfig {
    let path = std::env::temp_dir().join(format!(
        "aux-storage-{label}-{}.db",
        storage_types::TimestampMillis::now().timestamp_millis()
    ));
    StorageConfig {
        backend_type: StorageBackend::SQLite,
        connection_string: Some(path.to_string_lossy().to_string()),
        file_path: None,
        sqlite: None,
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    }
}

pub(super) fn item(pk: &str, value: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_string(), AttributeValue::S(pk.to_string())),
        ("value".to_string(), AttributeValue::S(value.to_string())),
    ])
}

pub(super) fn commit_metadata(index: u64) -> SyncCommitMetadata {
    SyncCommitMetadata {
        log_id: SyncLogId::new(1, index),
        committed_at: storage_types::TimestampMillis::from_timestamp(1_700_000_000_000),
        leader_node_id: "node-a".to_string(),
    }
}
