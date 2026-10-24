use storage::{
    DatabaseManager,
    provider::{FoundationDbSettings, StorageBackend, StorageConfig},
};

fn sqlite_config() -> StorageConfig {
    StorageConfig {
        backend_type: StorageBackend::SQLite,
        connection_string: Some(":memory:".to_owned()),
        file_path: None,
        sqlite: None,
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: Some(FoundationDbSettings::default()),
        remote: None,
    }
}

#[tokio::test]
async fn given_public_provider_config_when_constructing_manager_then_succeeds() {
    DatabaseManager::new_for_test_with_config(sqlite_config())
        .await
        .expect("public storage facade should construct a test manager");
}
