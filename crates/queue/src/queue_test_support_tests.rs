#[cfg(any(feature = "sqlite", feature = "rocksdb"))]
use std::path::Path;
#[cfg(feature = "foundationdb")]
use std::path::PathBuf;
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(feature = "foundationdb")]
use queue_provider::FoundationDbSettings;
#[cfg(feature = "postgres")]
use queue_provider::PostgresSettings;
use queue_provider::{QueueBackend, QueueConfig};

use crate::{QueueManager, create_queue_provider};

pub(crate) const QUEUE_CORRECTNESS_BACKEND_ENV: &str = "AUX_QUEUE_CORRECTNESS_BACKEND";

pub(crate) struct BackendCase {
    pub(crate) name: &'static str,
    pub(crate) config: QueueConfig,
}

pub(crate) fn unique_suffix(label: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("{label}-{nanos}-{}", std::process::id())
}

fn empty_config(backend_type: QueueBackend) -> QueueConfig {
    QueueConfig {
        backend_type,
        connection_string: None,
        file_path: None,
        postgres: None,
        foundationdb: None,
        remote: None,
    }
}

#[cfg(feature = "sqlite")]
pub(crate) async fn create_sqlite_service(path: impl AsRef<Path>) -> QueueManager {
    std::fs::create_dir_all(path.as_ref().parent().expect("queue fixture directory"))
        .expect("create queue fixture directory");
    let mut config = empty_config(QueueBackend::SQLite);
    config.connection_string = Some(path.as_ref().to_string_lossy().to_string());
    create_service(config).await
}

#[cfg(feature = "sqlite")]
pub(crate) fn local_sqlite_test_path(label: &str) -> std::path::PathBuf {
    let crate_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root");
    workspace_root
        .join("target")
        .join("queue-test-data")
        .join(format!("{label}-{}.sqlite3", unique_suffix("sqlite")))
}

#[cfg(feature = "sqlite")]
fn sqlite_case(_label: &str) -> BackendCase {
    let mut config = empty_config(QueueBackend::SQLite);
    config.connection_string = Some(":memory:".to_string());
    BackendCase {
        name: "sqlite",
        config,
    }
}

#[cfg(feature = "turso")]
pub(crate) fn turso_case(_label: &str) -> BackendCase {
    let mut config = empty_config(QueueBackend::Turso);
    config.connection_string = Some(":memory:".to_string());
    BackendCase {
        name: "turso",
        config,
    }
}

#[cfg(feature = "rocksdb")]
pub(crate) fn rocksdb_case(label: &str) -> BackendCase {
    let mut config = empty_config(QueueBackend::RocksDB);
    config.connection_string = Some(
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .expect("workspace root")
            .join("target")
            .join("queue-test-data")
            .join(unique_suffix(label))
            .to_string_lossy()
            .to_string(),
    );
    BackendCase {
        name: "rocksdb",
        config,
    }
}

#[cfg(feature = "postgres")]
fn postgres_case(_label: &str) -> Option<BackendCase> {
    let dsn = std::env::var("AUX_QUEUE_POSTGRES_DSN")
        .or_else(|_| std::env::var("POSTGRES_DSN"))
        .unwrap_or_else(|_| {
            let user = std::env::var("USER").unwrap_or_else(|_| "postgres".to_string());
            format!("postgres://{user}@localhost:5432/postgres")
        });
    if dsn.trim().is_empty() {
        return None;
    }
    let mut config = empty_config(QueueBackend::Postgres);
    config.connection_string = Some(dsn.clone());
    config.postgres = Some(PostgresSettings {
        dsn,
        max_pool_size: 16,
        background_max_pool_size: 4,
        tls: false,
    });
    Some(BackendCase {
        name: "postgres",
        config,
    })
}

#[cfg(feature = "foundationdb")]
fn local_fdb_cluster_file_path() -> Option<PathBuf> {
    if let Ok(path) = std::env::var("FDB_CLUSTER_FILE") {
        let path = PathBuf::from(path);
        if path.is_file() {
            return Some(path);
        }
    }

    [
        "/usr/local/etc/foundationdb/fdb.cluster",
        "/opt/homebrew/etc/foundationdb/fdb.cluster",
        "/etc/foundationdb/fdb.cluster",
    ]
    .into_iter()
    .map(PathBuf::from)
    .find(|path| path.is_file())
}

#[cfg(feature = "foundationdb")]
fn foundationdb_case(label: &str) -> Option<BackendCase> {
    let cluster_file = local_fdb_cluster_file_path()?;
    let mut config = empty_config(QueueBackend::FoundationDb);
    config.foundationdb = Some(FoundationDbSettings {
        cluster_file: Some(cluster_file.to_string_lossy().to_string()),
        subspace_prefix: Some(format!("tests/queue-correctness/{}/", unique_suffix(label))),
        ..FoundationDbSettings::default()
    });
    Some(BackendCase {
        name: "foundationdb",
        config,
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
        "postgres" => postgres_case(label).expect("Postgres queue correctness fixture"),
        #[cfg(feature = "rocksdb")]
        "rocksdb" => rocksdb_case(label),
        #[cfg(feature = "foundationdb")]
        "foundationdb" => foundationdb_case(label).expect(
            "FoundationDB queue correctness fixture requires FDB_CLUSTER_FILE or a local cluster",
        ),
        _ => unreachable!("selected backend must come from the compiled backend inventory"),
    }
}

pub(crate) fn selected_distributed_backend_case(label: &str) -> BackendCase {
    let compiled = compiled_native_backend_names()
        .into_iter()
        .filter(|backend| matches!(*backend, "postgres" | "foundationdb"))
        .collect::<Vec<_>>();
    let requested = std::env::var(QUEUE_CORRECTNESS_BACKEND_ENV).ok();
    let backend = select_native_backend_name(requested.as_deref(), &compiled)
        .unwrap_or_else(|error| panic!("{error}"));
    match backend {
        #[cfg(feature = "postgres")]
        "postgres" => postgres_case(label).expect("Postgres queue correctness fixture"),
        #[cfg(feature = "foundationdb")]
        "foundationdb" => foundationdb_case(label).expect(
            "FoundationDB queue correctness fixture requires FDB_CLUSTER_FILE or a local cluster",
        ),
        _ => unreachable!("selected backend must be a compiled distributed queue backend"),
    }
}

pub(crate) async fn create_service(config: QueueConfig) -> QueueManager {
    let storage = create_queue_provider(config)
        .await
        .expect("create queue test provider");
    QueueManager::new(Arc::from(storage))
}

pub(crate) async fn required_manager_for_case(case: &BackendCase) -> Arc<QueueManager> {
    let storage = create_queue_provider(case.config.clone())
        .await
        .unwrap_or_else(|error| panic!("create {} queue provider: {error:?}", case.name));
    Arc::new(QueueManager::new(Arc::from(storage)))
}

pub(crate) async fn create_selected_test_service(label: &str) -> QueueManager {
    let case = selected_native_backend_case(label);
    create_service(case.config).await
}
