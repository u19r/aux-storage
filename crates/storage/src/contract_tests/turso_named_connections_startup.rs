#[cfg(feature = "turso")]
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(feature = "turso")]
use sql_backend::turso::TursoBackend;

#[cfg(feature = "turso")]
use crate::{
    DatabaseManager, DatabaseManagerRuntimeOptions,
    provider::{StorageBackend, StorageConnectionConfig, StorageConnectionRegistry, TursoSettings},
};
#[cfg(feature = "turso")]
fn unique_database_path(label: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "aux-storage-{label}-{}-{suffix}",
        std::process::id()
    ))
}

#[cfg(feature = "turso")]
fn turso_connection(path: &Path) -> StorageConnectionConfig {
    StorageConnectionConfig {
        backend_type: StorageBackend::Turso,
        connection_string: Some(path.to_string_lossy().into_owned()),
        file_path: None,
        sqlite: None,
        postgres: None,
        turso: Some(TursoSettings::default()),
        rocksdb: None,
        foundationdb: None,
        remote: None,
    }
}

#[cfg(feature = "turso")]
fn remove_database_artifacts(path: &Path) {
    for artifact in [
        path.to_path_buf(),
        path.with_extension("db-log"),
        path.with_extension("db-wal"),
        path.with_extension("db-shm"),
        PathBuf::from(format!("{}-wal", path.display())),
        PathBuf::from(format!("{}-shm", path.display())),
    ] {
        let _ = fs::remove_file(artifact);
    }
}

#[cfg(feature = "turso")]
fn named_registry(primary_path: &Path, secondary_path: &Path) -> StorageConnectionRegistry {
    StorageConnectionRegistry {
        default_connection_id: "primary".to_owned(),
        connections: HashMap::from([
            ("primary".to_owned(), turso_connection(primary_path)),
            ("secondary".to_owned(), turso_connection(secondary_path)),
        ]),
    }
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn given_two_fresh_named_turso_connections_when_jobs_are_disabled_then_manager_starts() {
    let primary_path = unique_database_path("named-primary");
    let secondary_path = unique_database_path("named-secondary");
    let registry = named_registry(&primary_path, &secondary_path);

    let result = DatabaseManager::new_with_connection_registry_and_runtime_options(
        registry,
        DatabaseManagerRuntimeOptions::builder()
            .enable_database_jobs(false)
            .build(),
    )
    .await;

    let manager = result.expect("fresh named Turso connections must start");
    drop(manager);

    for path in [primary_path, secondary_path] {
        remove_database_artifacts(&path);
    }
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn given_two_managers_open_named_turso_connections_concurrently_then_both_start() {
    let primary_path = unique_database_path("concurrent-primary");
    let secondary_path = unique_database_path("concurrent-secondary");
    let registry = named_registry(&primary_path, &secondary_path);
    let runtime_options = DatabaseManagerRuntimeOptions::builder()
        .enable_database_jobs(false)
        .build();

    let first = DatabaseManager::new_with_connection_registry_and_runtime_options(
        registry.clone(),
        runtime_options.clone(),
    );
    let second = DatabaseManager::new_with_connection_registry_and_runtime_options(
        registry,
        runtime_options,
    );
    let (first_result, second_result) = tokio::join!(first, second);

    first_result.expect("first manager must start");
    second_result.expect("second manager must start");

    for path in [primary_path, secondary_path] {
        remove_database_artifacts(&path);
    }
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn given_distinct_turso_paths_with_a_dot_prefix_and_database_extension_when_opened_then_both_start()
 {
    let stem = unique_database_path("sidecar-collision");
    let primary_path = PathBuf::from(format!("{}.feature_primary.db", stem.display()));
    let secondary_path = PathBuf::from(format!("{}.feature_secondary.db", stem.display()));

    let primary = TursoBackend::initialize(primary_path.to_string_lossy().as_ref())
        .await
        .expect("first Turso database must start");
    drop(primary);

    let secondary = TursoBackend::initialize(secondary_path.to_string_lossy().as_ref())
        .await
        .expect("second Turso database must start");
    drop(secondary);

    for path in [primary_path, secondary_path] {
        remove_database_artifacts(&path);
    }
    remove_database_artifacts(&stem);
}
