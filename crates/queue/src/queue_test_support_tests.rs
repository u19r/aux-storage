#[cfg(any(feature = "sqlite", feature = "rocksdb"))]
use std::path::Path;
#[cfg(feature = "foundationdb")]
use std::path::PathBuf;
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use queue_provider::QueueOperations;

use crate::QueueManager;

pub(crate) const QUEUE_CORRECTNESS_BACKEND_ENV: &str = "STORAGE_TEST_BACKEND";

/// One native store of the queue tests. Every `open_test_store` call opens a
/// new store value over the same database, as another node would.
#[derive(Clone)]
pub(crate) enum TestStore {
    #[cfg(feature = "sqlite")]
    Sqlite { path: String },
    #[cfg(feature = "turso")]
    Turso { path: String },
    #[cfg(feature = "postgres")]
    Postgres { dsn: String, schema: String },
    #[cfg(feature = "rocksdb")]
    RocksDb { path: String },
    #[cfg(feature = "foundationdb")]
    FoundationDb {
        cluster_file: String,
        subspace_prefix: String,
    },
}

pub(crate) struct BackendCase {
    pub(crate) name: &'static str,
    pub(crate) store: TestStore,
}

pub(crate) fn unique_suffix(label: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("{label}-{nanos}-{}", std::process::id())
}

/// Open the driver and initialize the shared format the way the storage
/// provider bundle does, with default attempt deadlines and no admission.
/// Test fixture only: a process opens its store through
/// `storage::DatabaseManager`.
pub(crate) async fn open_test_store(store: TestStore) -> Arc<dyn QueueOperations> {
    match store {
        #[cfg(feature = "sqlite")]
        TestStore::Sqlite { path } => Arc::new(open_sqlite_store(path).await),
        #[cfg(feature = "turso")]
        TestStore::Turso { path } => Arc::new(open_turso_store(path).await),
        #[cfg(feature = "postgres")]
        TestStore::Postgres { dsn, schema } => Arc::new(open_postgres_store(dsn, schema).await),
        #[cfg(feature = "rocksdb")]
        TestStore::RocksDb { path } => Arc::new(open_rocksdb_store(path).await),
        #[cfg(feature = "foundationdb")]
        TestStore::FoundationDb {
            cluster_file,
            subspace_prefix,
        } => Arc::new(open_foundationdb_store(cluster_file, subspace_prefix).await),
    }
}

#[cfg(feature = "sqlite")]
pub(crate) async fn open_sqlite_store(
    path: String,
) -> sql_backend::SqlStore<sql_backend::sqlite::SqliteBackend> {
    let driver = sql_backend::sqlite::SqliteBackend::initialize(
        path,
        storage_types::AttemptDeadlines::default(),
    )
    .await
    .expect("open SQLite queue test driver");
    sql_backend::SqlStore::initialize_schema(
        driver,
        storage_provider::StorageBackend::SQLite.default_initial_stream_lineages(),
        false,
    )
    .await
    .expect("initialize SQLite queue test schema")
}

#[cfg(feature = "turso")]
pub(crate) async fn open_turso_store(
    path: String,
) -> sql_backend::SqlStore<sql_backend::turso::TursoBackend> {
    let driver = sql_backend::turso::TursoBackend::initialize(
        &path,
        storage_types::AttemptDeadlines::default(),
    )
    .await
    .expect("open Turso queue test driver");
    sql_backend::SqlStore::initialize_schema(
        driver,
        storage_provider::StorageBackend::Turso.default_initial_stream_lineages(),
        false,
    )
    .await
    .expect("initialize Turso queue test schema")
}

#[cfg(feature = "postgres")]
pub(crate) async fn open_postgres_store(
    dsn: String,
    schema: String,
) -> sql_backend::SqlStore<sql_backend::postgres::PostgresBackend> {
    sql_backend::postgres::test_schema::create_test_schema(&dsn, &schema)
        .await
        .expect("create Postgres queue test schema");
    let driver = sql_backend::postgres::PostgresBackend::initialize(
        &dsn,
        16,
        false,
        storage_types::AttemptDeadlines::default(),
    )
    .await
    .expect("open Postgres queue test driver");
    sql_backend::SqlStore::initialize_schema(
        driver,
        storage_provider::StorageBackend::Postgres.default_initial_stream_lineages(),
        false,
    )
    .await
    .expect("initialize Postgres queue test schema")
}

#[cfg(feature = "rocksdb")]
pub(crate) async fn open_rocksdb_store(
    path: String,
) -> kv_backend::KvStore<kv_backend::rocksdb::RocksDbBackend> {
    let driver = kv_backend::rocksdb::RocksDbBackend::initialize(
        std::path::PathBuf::from(path),
        storage_types::AttemptDeadlines::default(),
        storage_provider::ROCKSDB_DEFAULT_WRITER_QUEUE_CAPACITY,
    )
    .expect("open RocksDB queue test driver");
    kv_backend::KvStore::initialize_format(
        driver,
        storage_provider::StorageBackend::RocksDB.default_initial_stream_lineages(),
    )
    .await
    .expect("initialize RocksDB queue test format")
}

#[cfg(feature = "foundationdb")]
pub(crate) async fn open_foundationdb_store(
    cluster_file: String,
    subspace_prefix: String,
) -> kv_backend::KvStore<kv_backend::foundationdb::FoundationDbBackend> {
    let driver = kv_backend::foundationdb::FoundationDbBackend::initialize(
        kv_backend::foundationdb::FoundationDbOptions {
            cluster_file: Some(cluster_file.as_str()),
            subspace_prefix: Some(subspace_prefix.as_bytes()),
            ..kv_backend::foundationdb::FoundationDbOptions::default()
        },
    )
    .await
    .expect("open FoundationDB queue test driver");
    kv_backend::KvStore::initialize_format(
        driver,
        storage_provider::StorageBackend::FoundationDb.default_initial_stream_lineages(),
    )
    .await
    .expect("initialize FoundationDB queue test format")
}

#[cfg(feature = "sqlite")]
pub(crate) async fn create_sqlite_service(path: impl AsRef<Path>) -> QueueManager {
    std::fs::create_dir_all(path.as_ref().parent().expect("queue fixture directory"))
        .expect("create queue fixture directory");
    create_service(TestStore::Sqlite {
        path: path.as_ref().to_string_lossy().to_string(),
    })
    .await
}

#[cfg(feature = "sqlite")]
fn sqlite_case(_label: &str) -> BackendCase {
    BackendCase {
        name: "sqlite",
        store: TestStore::Sqlite {
            path: ":memory:".to_string(),
        },
    }
}

#[cfg(feature = "turso")]
pub(crate) fn turso_case(_label: &str) -> BackendCase {
    BackendCase {
        name: "turso",
        store: TestStore::Turso {
            path: ":memory:".to_string(),
        },
    }
}

#[cfg(feature = "rocksdb")]
pub(crate) fn rocksdb_case(label: &str) -> BackendCase {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .join("target")
        .join("queue-test-data")
        .join(unique_suffix(label))
        .to_string_lossy()
        .to_string();
    BackendCase {
        name: "rocksdb",
        store: TestStore::RocksDb { path },
    }
}

#[cfg(feature = "postgres")]
fn postgres_case(_label: &str) -> Option<BackendCase> {
    let dsn = std::env::var("TEST_POSTGRES_DSN").ok()?;
    if dsn.trim().is_empty() {
        return None;
    }
    Some(BackendCase {
        name: "postgres",
        store: {
            let (dsn, schema) =
                sql_backend::postgres::test_schema::isolated_test_dsn(&dsn, "queue_test");
            TestStore::Postgres { dsn, schema }
        },
    })
}

#[cfg(feature = "foundationdb")]
fn local_fdb_cluster_file_path() -> Option<PathBuf> {
    let path = PathBuf::from(std::env::var("TEST_FDB_CLUSTER_FILE").ok()?);
    path.is_file().then_some(path)
}

#[cfg(feature = "foundationdb")]
fn foundationdb_case(label: &str) -> Option<BackendCase> {
    let cluster_file = local_fdb_cluster_file_path()?;
    Some(BackendCase {
        name: "foundationdb",
        store: TestStore::FoundationDb {
            cluster_file: cluster_file.to_string_lossy().to_string(),
            subspace_prefix: format!("tests/queue-correctness/{}/", unique_suffix(label)),
        },
    })
}

fn compiled_native_backend_names() -> Vec<&'static str> {
    vec![
        #[cfg(feature = "sqlite")]
        "sqlite",
        #[cfg(feature = "turso")]
        "turso",
        #[cfg(feature = "postgres")]
        "postgres",
        #[cfg(feature = "rocksdb")]
        "rocksdb",
        #[cfg(feature = "foundationdb")]
        "foundationdb",
    ]
}

pub(crate) fn select_native_backend_name(
    requested: Option<&str>,
    compiled: &[&'static str],
) -> Result<&'static str, String> {
    if let Some(requested) = requested {
        let requested = requested.trim().to_ascii_lowercase();
        return compiled
            .iter()
            .copied()
            .find(|backend| *backend == requested)
            .ok_or_else(|| {
                format!(
                    "requested queue correctness backend {requested:?} is not enabled; enabled: {}",
                    compiled.join(", ")
                )
            });
    }
    if compiled.contains(&"sqlite") {
        return Ok("sqlite");
    }
    match compiled {
        [backend] => Ok(*backend),
        [] => Err("queue correctness tests require one native backend feature".to_string()),
        _ => Err(format!(
            "queue correctness backend is ambiguous; set {QUEUE_CORRECTNESS_BACKEND_ENV} to one \
             of: {}",
            compiled.join(", ")
        )),
    }
}

pub(crate) fn selected_native_backend_case(label: &str) -> BackendCase {
    let compiled = compiled_native_backend_names();
    let requested = std::env::var(QUEUE_CORRECTNESS_BACKEND_ENV).ok();
    let backend = select_native_backend_name(requested.as_deref(), &compiled)
        .unwrap_or_else(|error| panic!("{error}"));
    match backend {
        #[cfg(feature = "sqlite")]
        "sqlite" => sqlite_case(label),
        #[cfg(feature = "turso")]
        "turso" => turso_case(label),
        #[cfg(feature = "postgres")]
        "postgres" => postgres_case(label)
            .expect("Postgres queue correctness fixture requires TEST_POSTGRES_DSN"),
        #[cfg(feature = "rocksdb")]
        "rocksdb" => rocksdb_case(label),
        #[cfg(feature = "foundationdb")]
        "foundationdb" => foundationdb_case(label)
            .expect("FoundationDB queue correctness fixture requires TEST_FDB_CLUSTER_FILE"),
        _ => unreachable!("selected backend must come from the compiled backend inventory"),
    }
}

pub(crate) fn selected_distributed_backend_case(_label: &str) -> BackendCase {
    let compiled = compiled_native_backend_names()
        .into_iter()
        .filter(|backend| matches!(*backend, "postgres" | "foundationdb"))
        .collect::<Vec<_>>();
    let requested = std::env::var(QUEUE_CORRECTNESS_BACKEND_ENV).ok();
    let backend = select_native_backend_name(requested.as_deref(), &compiled)
        .unwrap_or_else(|error| panic!("{error}"));
    match backend {
        #[cfg(feature = "postgres")]
        "postgres" => postgres_case(_label)
            .expect("Postgres queue correctness fixture requires TEST_POSTGRES_DSN"),
        #[cfg(feature = "foundationdb")]
        "foundationdb" => foundationdb_case(_label)
            .expect("FoundationDB queue correctness fixture requires TEST_FDB_CLUSTER_FILE"),
        _ => unreachable!("selected backend must be a compiled distributed queue backend"),
    }
}

pub(crate) async fn create_service(store: TestStore) -> QueueManager {
    QueueManager::new(open_test_store(store).await)
}

pub(crate) async fn required_manager_for_case(case: &BackendCase) -> Arc<QueueManager> {
    Arc::new(QueueManager::new(open_test_store(case.store.clone()).await))
}

pub(crate) async fn create_selected_test_service(label: &str) -> QueueManager {
    let case = selected_native_backend_case(label);
    create_service(case.store).await
}
