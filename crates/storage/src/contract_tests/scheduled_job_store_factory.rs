#[cfg(feature = "remote")]
use std::time::Duration;
#[cfg(feature = "sqlite")]
use std::{
    collections::HashMap,
    num::{NonZeroU32, NonZeroUsize},
    sync::Arc,
};

#[cfg(feature = "sqlite")]
use bytes::Bytes;
#[cfg(feature = "sqlite")]
use scheduled_bg_jobs::{
    Namespace, OwnerId,
    runtime::JobSystemBuilder,
    store::{
        CommitOperationId, CommitOutcome, JobStore, Mutation, RangeDirection, RangeRequest,
        RangeRequestInput, StoreKey, StoreLevel, StoreTransaction, StoreTransactionInput,
        TransactionLimits,
    },
    test_support::TestProfile,
};
#[cfg(feature = "remote")]
use storage_provider::{RemoteCredentialStrategy, RemoteStorageSettings};
#[cfg(feature = "sqlite")]
use storage_provider::{SqliteSettings, StorageConnectionConfig, StorageConnectionRegistry};
#[cfg(any(feature = "remote", feature = "sqlite"))]
use storage_provider::{StorageBackend, StorageConfig};
#[cfg(feature = "sqlite")]
use storage_types::StorageEnum;

#[cfg(any(feature = "remote", feature = "sqlite"))]
use crate::DatabaseManager;
#[cfg(feature = "remote")]
use crate::RemoteScheduledJobStoreConfig;
#[cfg(feature = "sqlite")]
use crate::database_manager::construction_tests::{
    database_manager_with_components, database_manager_with_registry_components,
};
#[cfg(feature = "sqlite")]
use crate::{DatabaseManagerRuntimeOptions, ScheduledJobStore};

#[cfg(feature = "sqlite")]
fn sqlite_config(path: &str) -> StorageConfig {
    StorageConfig {
        backend_type: StorageBackend::SQLite,
        connection_string: Some(path.to_string()),
        file_path: None,
        sqlite: None,
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    }
}

#[cfg(feature = "sqlite")]
fn sqlite_connection(path: &str) -> StorageConnectionConfig {
    let config = sqlite_config(path);
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

#[cfg(feature = "sqlite")]
fn key(value: &[u8]) -> StoreKey {
    StoreKey::try_from(value).expect("test key is non-empty")
}

#[cfg(feature = "sqlite")]
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
    .expect("test transaction is valid")
}

#[cfg(feature = "sqlite")]
fn assert_job_store<S: JobStore>(_: &S) {}

#[cfg(feature = "sqlite")]
fn assert_job_system_builder<S: JobStore>(store: S) {
    JobSystemBuilder::new(store)
        .namespace(Namespace::try_new(b"storage-test").expect("test namespace is valid"))
        .owner(OwnerId::from([0x5a; 16]))
        .scheduler(Arc::new(TestProfile::new()))
        .build()
        .expect("storage store satisfies builder contract");
}

#[cfg(feature = "sqlite")]
fn storage_error_debug(error: &storage_types::StorageError) -> String {
    match error.as_ref() {
        StorageEnum::InternalServerError { message } => {
            format!("{error:?}; internal_detail={message}")
        }
        _ => format!("{error:?}; variant={:?}", error.as_ref()),
    }
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn given_sqlite_manager_when_store_is_requested_then_default_is_bootstrapped_and_usable() {
    let (_manager, _, store) = database_manager_with_components(
        sqlite_config(":memory:"),
        DatabaseManagerRuntimeOptions::builder()
            .enable_database_jobs(true)
            .build(),
    )
    .await
    .expect("construct SQLite manager");

    let store: ScheduledJobStore = store.expect("SQLite has a scheduled job store");
    assert_job_store(&store);
    assert_job_system_builder(store.clone());
    assert_eq!(store.capabilities().level(), StoreLevel::Local);
    assert_eq!(
        store
            .get(key(b"factory-bootstrap-marker"))
            .await
            .expect("read bootstrapped store"),
        None
    );

    let clone = store.clone();
    assert_eq!(
        clone
            .commit(&put_transaction(b"factory-shared-marker", b"shared"))
            .await
            .expect("commit through cloned handle"),
        CommitOutcome::Committed
    );
    assert_eq!(
        store
            .get(key(b"factory-shared-marker"))
            .await
            .expect("read through original handle"),
        Some(Bytes::from_static(b"shared"))
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn given_enabled_sqlite_manager_when_starting_then_scheduled_jobs_are_seeded() {
    let (_manager, _, store) = database_manager_with_components(
        sqlite_config(":memory:"),
        DatabaseManagerRuntimeOptions::builder()
            .enable_database_jobs(true)
            .build(),
    )
    .await
    .expect("construct SQLite manager");
    let store = store.expect("SQLite has a scheduled job store");
    let request = RangeRequest::try_from(RangeRequestInput {
        start: None,
        end: None,
        direction: RangeDirection::Forward,
        item_limit: NonZeroU32::new(32).expect("non-zero item limit"),
        byte_limit: NonZeroUsize::new(1024 * 1024).expect("non-zero byte limit"),
        continuation: None,
    })
    .expect("valid scheduled-job range");

    let page = store
        .read_range(request)
        .await
        .expect("read scheduled-job seeds");
    assert!(
        !page.entries.is_empty(),
        "enabled startup must seed provider jobs"
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn given_two_live_file_backed_sqlite_managers_when_second_disables_jobs_then_both_stores_are_usable()
 {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_nanos();
    let database_path = std::env::temp_dir().join(format!(
        "aux-storage-two-manager-scheduled-store-{}-{nonce}.db",
        std::process::id()
    ));
    let mut config = sqlite_config(database_path.to_str().expect("UTF-8 SQLite path"));
    config.sqlite = Some(SqliteSettings {
        immediate_gsi_consistency: true,
        force_file_backed_database: true,
    });

    let (first, _, first_store) =
        database_manager_with_components(config.clone(), DatabaseManagerRuntimeOptions::default())
            .await
            .expect("first SQLite manager should bootstrap");
    let first_store = first_store.expect("first manager scheduled store");
    assert_eq!(
        first_store
            .commit(&put_transaction(b"first-manager", b"first"))
            .await
            .expect("first manager commit"),
        CommitOutcome::Committed
    );

    let (second, _, second_store) = database_manager_with_components(
        config,
        DatabaseManagerRuntimeOptions::builder()
            .enable_database_jobs(false)
            .enable_single_table_mode(false)
            .build(),
    )
    .await
    .expect("second SQLite manager should bootstrap with jobs disabled");
    let second_store = second_store.expect("second manager scheduled store");
    assert_eq!(
        second_store
            .get(key(b"first-manager"))
            .await
            .expect("second manager read"),
        Some(Bytes::from_static(b"first"))
    );
    assert_eq!(
        second_store
            .commit(&put_transaction(b"second-manager", b"second"))
            .await
            .expect("second manager commit"),
        CommitOutcome::Committed
    );
    assert_eq!(
        first_store
            .get(key(b"second-manager"))
            .await
            .expect("first manager read after second commit"),
        Some(Bytes::from_static(b"second"))
    );

    drop(second);
    drop(first);
    let _ = std::fs::remove_file(database_path);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn given_synchronized_sqlite_manager_construction_when_http_and_observer_attach_then_both_stores_are_usable()
 {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_nanos();
    let database_path = std::env::temp_dir().join(format!(
        "aux-storage-concurrent-manager-scheduled-store-{}-{nonce}.db",
        std::process::id()
    ));
    let mut config = sqlite_config(database_path.to_str().expect("UTF-8 SQLite path"));
    config.sqlite = Some(SqliteSettings {
        immediate_gsi_consistency: true,
        force_file_backed_database: true,
    });

    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(3));
    let http_barrier = std::sync::Arc::clone(&barrier);
    let http_config = config.clone();
    let http = tokio::spawn(async move {
        http_barrier.wait().await;
        database_manager_with_components(
            http_config,
            DatabaseManagerRuntimeOptions::builder()
                .enable_database_jobs(true)
                .enable_single_table_mode(true)
                .build(),
        )
        .await
    });
    let observer_barrier = std::sync::Arc::clone(&barrier);
    let observer = tokio::spawn(async move {
        observer_barrier.wait().await;
        database_manager_with_components(config, DatabaseManagerRuntimeOptions::default()).await
    });

    barrier.wait().await;
    let http_result = http.await.expect("HTTP manager task should join");
    let observer_result = observer.await.expect("observer manager task should join");
    let http_error = http_result.as_ref().err().map(storage_error_debug);
    let observer_error = observer_result.as_ref().err().map(storage_error_debug);
    let (http_manager, http_store, observer_manager, observer_store) =
        match (http_result, observer_result) {
            (Ok((http_manager, _, http_store)), Ok((observer_manager, _, observer_store))) => (
                http_manager,
                http_store.expect("HTTP manager scheduled store"),
                observer_manager,
                observer_store.expect("observer manager scheduled store"),
            ),
            (http_result, observer_result) => {
                let _ = std::fs::remove_file(&database_path);
                panic!(
                    "concurrent SQLite manager construction failed: http_ok={} \
                     http_error={http_error:?} observer_ok={} observer_error={observer_error:?}",
                    http_result.is_ok(),
                    observer_result.is_ok()
                );
            }
        };
    assert_eq!(
        http_store
            .commit(&put_transaction(b"concurrent-http", b"http"))
            .await
            .expect("HTTP manager commit"),
        CommitOutcome::Committed
    );
    assert_eq!(
        observer_store
            .get(key(b"concurrent-http"))
            .await
            .expect("observer manager read"),
        Some(Bytes::from_static(b"http"))
    );
    assert_eq!(
        observer_store
            .commit(&put_transaction(b"concurrent-observer", b"observer"))
            .await
            .expect("observer manager commit"),
        CommitOutcome::Committed
    );
    assert_eq!(
        http_store
            .get(key(b"concurrent-observer"))
            .await
            .expect("HTTP manager read"),
        Some(Bytes::from_static(b"observer"))
    );

    drop(observer_manager);
    drop(http_manager);
    let _ = std::fs::remove_file(database_path);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn given_multiple_connections_when_store_is_requested_then_only_default_is_exposed() {
    let registry = StorageConnectionRegistry {
        default_connection_id: "default".to_string(),
        connections: HashMap::from([
            ("default".to_string(), sqlite_connection(":memory:")),
            ("secondary".to_string(), sqlite_connection(":memory:")),
        ]),
    };

    let (_manager, _, store) = database_manager_with_registry_components(
        registry,
        DatabaseManagerRuntimeOptions::builder().build(),
    )
    .await
    .expect("construct multi-connection manager");

    let store = store.expect("default connection has a scheduled job store");
    assert_eq!(
        store
            .commit(&put_transaction(b"default-only-marker", b"default"))
            .await
            .expect("commit default store row"),
        CommitOutcome::Committed
    );
    assert_eq!(
        store
            .get(key(b"default-only-marker"))
            .await
            .expect("read default store row"),
        Some(Bytes::from_static(b"default"))
    );
}

#[cfg(all(not(feature = "remote"), feature = "sqlite"))]
#[tokio::test]
async fn given_remote_backend_when_store_is_requested_then_manager_reports_unsupported_backend() {
    // Disable database jobs: this test targets backend support, and the
    // jobs-enabled maintenance validation would reject before the
    // backend-disabled branch under test is reached.
    let result = DatabaseManager::new_with_config_and_runtime_options(
        StorageConfig {
            backend_type: StorageBackend::Remote,
            connection_string: None,
            file_path: None,
            sqlite: None,
            postgres: None,
            turso: None,
            rocksdb: None,
            foundationdb: None,
            remote: None,
        },
        crate::database_manager::DatabaseManagerRuntimeOptions::builder()
            .enable_database_jobs(false)
            .build(),
    )
    .await;

    let error = match result {
        Ok(_) => panic!("remote backend is unsupported in this build"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("not enabled"));
}

#[cfg(feature = "remote")]
#[test]
fn remote_scheduled_store_config_is_http_only_and_bounded() {
    let config = RemoteScheduledJobStoreConfig::new(
        vec!["http://private-storage", "http://private-storage-replica"],
        Duration::from_secs(5),
    )
    .expect("valid private-network scheduled-store config");
    let debug = format!("{config:?}");
    assert!(debug.contains("endpoint_count: 2"));
    assert!(debug.contains("request_timeout: 5s"));
    assert!(!debug.contains("private-storage"));

    assert!(
        RemoteScheduledJobStoreConfig::new(Vec::<String>::new(), Duration::from_secs(5)).is_err()
    );

    assert!(
        RemoteScheduledJobStoreConfig::new(
            vec!["https://private-storage"],
            Duration::from_secs(5),
        )
            .is_err()
    );
    assert!(
        RemoteScheduledJobStoreConfig::new(vec!["ftp://private-storage"], Duration::from_secs(5),)
            .is_err()
    );
    assert!(RemoteScheduledJobStoreConfig::new(vec!["http://"], Duration::from_secs(5)).is_err());
    assert!(
        RemoteScheduledJobStoreConfig::new(
            vec!["http://user:password@private-storage"],
            Duration::from_secs(5),
        )
        .is_err()
    );
    assert!(
        RemoteScheduledJobStoreConfig::new(
            vec!["http://private-storage/path#fragment"],
            Duration::from_secs(5),
        )
        .is_err()
    );
    assert!(
        RemoteScheduledJobStoreConfig::new(vec!["http://private-storage"], Duration::ZERO).is_err()
    );
    assert!(
        RemoteScheduledJobStoreConfig::new(
            vec!["http://private-storage"],
            Duration::from_secs(31),
        )
            .is_err()
    );
}

#[cfg(feature = "remote")]
#[tokio::test]
async fn given_remote_manager_with_endpoint_credentials_when_constructing_then_rejects_before_network()
 {
    let result = DatabaseManager::new_with_config_and_runtime_options(
        StorageConfig {
            backend_type: StorageBackend::Remote,
            connection_string: None,
            file_path: None,
            sqlite: None,
            postgres: None,
            turso: None,
            rocksdb: None,
            foundationdb: None,
            remote: Some(RemoteStorageSettings {
                endpoint_urls: vec!["http://user:password@127.0.0.1:1/".to_string()],
                region: None,
                tls: false,
                credentials: RemoteCredentialStrategy::DefaultChain,
                timeouts: None,
            }),
        },
        DatabaseManagerRuntimeOptions::builder()
            .enable_database_jobs(false)
            .build(),
    )
    .await;

    let error = match result {
        Ok(_) => panic!("manager must validate remote endpoints before connecting"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("without credentials"));
}
