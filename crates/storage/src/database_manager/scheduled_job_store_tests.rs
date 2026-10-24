use std::{
    collections::HashMap,
    fs,
    num::{NonZeroU32, NonZeroUsize},
    path::Path,
};

use bg_job_store::ScheduledJobStoreSource as _;
use bytes::Bytes;
use scheduled_bg_jobs::store::{
    CommitOperationId, Mutation, StoreKey, StoreTransaction, StoreTransactionInput,
    TransactionLimits,
};
use storage_provider::{
    SqliteSettings, StorageBackend, StorageConfig, StorageConnectionConfig,
    StorageConnectionRegistry,
};

use super::{DatabaseManager, DatabaseManagerRuntimeOptions};

fn sqlite_config(path: &str) -> StorageConfig {
    StorageConfig {
        backend_type: StorageBackend::SQLite,
        connection_string: Some(path.to_string()),
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
    }
}

fn sqlite_connection(path: &Path) -> StorageConnectionConfig {
    let config = sqlite_config(path.to_str().expect("UTF-8 test path"));
    StorageConnectionConfig {
        backend_type: config.backend_type,
        connection_string: config.connection_string,
        file_path: config.file_path,
        sqlite: config.sqlite,
        postgres: config.postgres,
        turso: config.turso,
        rocksdb: config.rocksdb,
        foundationdb: config.foundationdb,
        remote: config.remote,
    }
}

fn runtime_options(enable_database_jobs: bool) -> DatabaseManagerRuntimeOptions {
    DatabaseManagerRuntimeOptions::builder()
        .enable_database_jobs(enable_database_jobs)
        .build()
}

fn key(bytes: &[u8]) -> StoreKey {
    StoreKey::try_from(bytes).expect("non-empty scheduled-job key")
}

fn put_transaction(key_bytes: &[u8], value: &[u8]) -> StoreTransaction {
    StoreTransaction::try_from(StoreTransactionInput {
        operation_id: CommitOperationId::generate().expect("generate transaction operation ID"),
        checks: Vec::new(),
        mutations: vec![Mutation::Put(key(key_bytes), Bytes::copy_from_slice(value))],
        limits: TransactionLimits {
            max_checks: NonZeroU32::new(32).expect("non-zero check limit"),
            max_mutations: NonZeroU32::new(32).expect("non-zero mutation limit"),
            max_bytes: NonZeroUsize::new(1024 * 1024).expect("non-zero byte limit"),
        },
    })
    .expect("valid scheduled-job transaction")
}

async fn seed_scheduled_store(path: &Path, value: &[u8]) {
    let driver = sql_backend::sqlite::SqliteBackend::initialize(path)
        .await
        .expect("open seed database");
    let provider = sql_backend::SqlStore::initialize_schema(driver)
        .await
        .expect("initialize seed store");
    let store = provider
        .scheduled_job_store(std::time::Duration::from_secs(30).into())
        .expect("scheduled-job capabilities");
    store.bootstrap().await.expect("bootstrap seed store");
    store
        .commit(&put_transaction(b"store-marker", value))
        .await
        .expect("seed scheduled row");
}

#[tokio::test]
async fn given_sqlite_manager_when_reading_missing_key_then_bootstrapped_store_returns_none() {
    for enable_database_jobs in [false, true] {
        let manager = DatabaseManager::new_with_config_and_runtime_options(
            sqlite_config(":memory:"),
            runtime_options(enable_database_jobs),
        )
        .await
        .expect("construct SQLite manager");
        let store = manager
            .scheduled_job_store()
            .expect("default scheduled store");
        let value = store
            .get(key(b"missing-store-key"))
            .await
            .expect("read missing scheduled key");
        assert!(value.is_none(), "missing key must be an empty read");
    }
}

#[tokio::test]
async fn given_sqlite_runtime_toggle_when_manager_starts_then_driver_lifetime_matches_toggle() {
    let disabled = DatabaseManager::new_with_config_and_runtime_options(
        sqlite_config(":memory:"),
        runtime_options(false),
    )
    .await
    .expect("construct disabled SQLite manager");
    assert!(disabled.maintenance_runtimes.is_empty());
    let super::service_shutdown::BackgroundServiceState::Running(services) =
        &*disabled.background_services.lock().await
    else {
        panic!("disabled manager state");
    };
    assert!(services.maintenance.is_empty());

    let enabled = DatabaseManager::new_with_config_and_runtime_options(
        sqlite_config(":memory:"),
        runtime_options(true),
    )
    .await
    .expect("construct enabled SQLite manager");
    assert!(enabled.maintenance_runtimes.is_empty());
    let super::service_shutdown::BackgroundServiceState::Running(services) =
        &*enabled.background_services.lock().await
    else {
        panic!("enabled manager state");
    };
    assert!(!services.maintenance.is_empty());
}

#[tokio::test]
async fn given_initialized_store_when_manager_is_constructed_then_missing_table_is_absent() {
    let manager = super::construction_tests::sqlite_manager_without_system_tables()
        .await
        .expect("construct manager");
    assert!(
        !manager
            .table_exists(&storage_types::TableName::new("ordinary-mock-table"))
            .await
            .expect("ordinary manager operation")
    );
}

#[tokio::test]
async fn given_two_sqlite_connections_when_manager_is_constructed_then_store_reads_default_connection()
 {
    let default_path = crate::storage_test_support_tests::unique_path("scheduled-store-default");
    let secondary_path =
        crate::storage_test_support_tests::unique_path("scheduled-store-secondary");
    seed_scheduled_store(&default_path, b"default-store").await;
    seed_scheduled_store(&secondary_path, b"secondary-store").await;
    let registry = StorageConnectionRegistry {
        default_connection_id: "default".to_string(),
        connections: HashMap::from([
            ("default".to_string(), sqlite_connection(&default_path)),
            ("secondary".to_string(), sqlite_connection(&secondary_path)),
        ]),
    };

    let manager = DatabaseManager::new_with_connection_registry_and_runtime_options(
        registry,
        runtime_options(false),
    )
    .await
    .expect("construct multi-connection manager");
    let store = manager
        .scheduled_job_store()
        .expect("one default scheduled store");
    let value = store
        .get(key(b"store-marker"))
        .await
        .expect("read default scheduled row");

    assert_eq!(value, Some(Bytes::from_static(b"default-store")));
    assert_eq!(manager.dynamo_registry.len(), 2);
    drop(manager);
    let _ = fs::remove_file(default_path);
    let _ = fs::remove_file(secondary_path);
}

#[tokio::test]
async fn given_committed_job_when_initialized_manager_reopens_then_receipt_and_effect_survive() {
    let path = crate::storage_test_support_tests::unique_path("scheduled-store-reopen");
    let transaction = put_transaction(b"reopen-key", b"durable-value");
    let config = sqlite_config(path.to_str().expect("UTF-8 fixture path"));
    let bundle = crate::provider_bundle::create_storage_provider_bundle(
        config.clone(),
        &Default::default(),
        "default",
    )
    .await
    .expect("construct initialized bundle");
    let manager = DatabaseManager::from_initialized_bundle(config, bundle, runtime_options(false))
        .await
        .expect("construct initial manager");
    let store = manager
        .scheduled_job_store()
        .expect("initialized owner's job store");
    assert_eq!(
        store.commit(&transaction).await.expect("initial commit"),
        scheduled_bg_jobs::store::CommitOutcome::Committed
    );
    drop(store);
    drop(manager);

    let manager = DatabaseManager::new_with_config_and_runtime_options(
        sqlite_config(path.to_str().expect("UTF-8 fixture path")),
        runtime_options(false),
    )
    .await
    .expect("reopen initialized manager");
    let store = manager
        .scheduled_job_store()
        .expect("reopened owner's job store");
    assert_eq!(
        store
            .get(key(b"reopen-key"))
            .await
            .expect("read before replay"),
        Some(Bytes::from_static(b"durable-value"))
    );
    assert_eq!(
        store
            .commit(&transaction)
            .await
            .expect("replay durable receipt"),
        scheduled_bg_jobs::store::CommitOutcome::Committed
    );
    assert_eq!(
        store
            .get(key(b"reopen-key"))
            .await
            .expect("read after replay"),
        Some(Bytes::from_static(b"durable-value"))
    );
    drop(store);
    drop(manager);
    fs::remove_file(path).expect("remove owned fixture");
}
