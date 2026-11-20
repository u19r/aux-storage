use storage_provider::{
    PostgresSettings, SqliteSettings, StorageBackend, StorageConfig, StorageConnectionRegistry,
};
use storage_types::StorageResult;

use super::construction::{ConnectionPlan, destination_namespaces, single_connection_registry};
use crate::{
    ScheduledJobStore,
    admission::AdmissionConfig,
    database_manager::{DatabaseManager, DatabaseManagerRuntimeOptions},
    provider_bundle::MaintenanceRepositories,
};

pub(crate) async fn sqlite_manager_without_system_tables() -> StorageResult<DatabaseManager> {
    let registry = single_connection_registry(super::test_database::immediate_sqlite_test_config());
    let options = DatabaseManagerRuntimeOptions::builder()
        .enable_database_jobs(false)
        .build();
    let plan = ConnectionPlan::new(registry, options)?;
    let stores = plan.open_stores().await?;
    plan.assemble(stores).await
}

pub(crate) async fn database_manager_from_store(
    store: crate::InitializedStore,
) -> StorageResult<DatabaseManager> {
    let registry = single_connection_registry(super::test_database::immediate_sqlite_test_config());
    let options = DatabaseManagerRuntimeOptions::builder()
        .enable_database_jobs(false)
        .build();
    let plan = ConnectionPlan::new(registry, options)?;
    let stores =
        std::collections::HashMap::from([(plan.default_connection_id().to_owned(), store)]);
    plan.assemble(stores).await
}

pub(crate) async fn database_manager_with_components(
    config: StorageConfig,
    runtime_options: DatabaseManagerRuntimeOptions,
) -> StorageResult<(
    DatabaseManager,
    Option<MaintenanceRepositories>,
    Option<ScheduledJobStore>,
)> {
    let registry = single_connection_registry(config);
    database_manager_with_registry_components(registry, runtime_options).await
}

pub(crate) async fn database_manager_with_registry_components(
    registry: StorageConnectionRegistry,
    runtime_options: DatabaseManagerRuntimeOptions,
) -> StorageResult<(
    DatabaseManager,
    Option<MaintenanceRepositories>,
    Option<ScheduledJobStore>,
)> {
    let plan = ConnectionPlan::new(registry, runtime_options)?;
    let stores = plan.open_stores().await?;
    let maintenance_repositories = stores
        .get(plan.default_connection_id())
        .and_then(crate::InitializedStore::native_store)
        .map(|store| store.maintenance(storage_common::native_admission::AttemptAdmission::Caller));
    let mut manager = plan.assemble(stores).await?;
    manager.finish_ordinary_initialization().await?;
    let scheduled_job_store = manager.scheduled_job_store();
    Ok((manager, maintenance_repositories, scheduled_job_store))
}

#[test]
fn named_connections_reject_aliases_to_one_physical_destination() {
    let connection = StorageConfig {
        initial_stream_lineages: None,
        backend_type: StorageBackend::SQLite,
        connection_string: Some("/tmp/aux-storage-alias-test.db".to_owned()),
        file_path: Some("/tmp/aux-storage-alias-test.db".to_owned()),
        sqlite: Some(SqliteSettings::default()),
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    };
    let registry = StorageConnectionRegistry {
        default_connection_id: "primary".to_owned(),
        connections: std::collections::HashMap::from([
            ("primary".to_owned(), connection.clone()),
            (
                "alias".to_owned(),
                StorageConfig {
                    connection_string: Some("/tmp/./aux-storage-alias-test.db".to_owned()),
                    file_path: Some("/tmp/./aux-storage-alias-test.db".to_owned()),
                    ..connection
                },
            ),
        ]),
    };

    let error = destination_namespaces(&registry).expect_err("aliases must be rejected");
    assert!(error.to_string().contains("alias one physical destination"));
}

#[test]
fn equivalent_postgres_dsns_share_one_destination_namespace() {
    let connection = |dsn: &str| StorageConfig {
        initial_stream_lineages: None,
        backend_type: StorageBackend::Postgres,
        connection_string: Some(dsn.to_owned()),
        file_path: None,
        sqlite: None,
        postgres: Some(PostgresSettings {
            dsn: dsn.to_owned(),
            ..PostgresSettings::default()
        }),
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    };
    let registry = StorageConnectionRegistry {
        default_connection_id: "primary".to_owned(),
        connections: std::collections::HashMap::from([
            (
                "primary".to_owned(),
                connection(
                    "postgres://first:secret@localhost/app?application_name=one&\
                     options=-csearch_path%3Dtenant_a",
                ),
            ),
            (
                "alias".to_owned(),
                connection(
                    "postgresql://second:rotated@127.0.0.1:5432/app?options=-csearch_path%\
                     3Dtenant_a&application_name=two",
                ),
            ),
        ]),
    };

    let error = destination_namespaces(&registry).expect_err("equivalent DSNs must alias");
    assert!(error.to_string().contains("alias one physical destination"));
}

#[test]
fn postgres_admission_limit_is_clamped_to_foreground_pool_size() {
    let connection = StorageConfig {
        initial_stream_lineages: None,
        backend_type: StorageBackend::Postgres,
        connection_string: None,
        file_path: None,
        sqlite: None,
        postgres: Some(PostgresSettings {
            max_pool_size: 8,
            ..PostgresSettings::default()
        }),
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    };
    let config = AdmissionConfig::default()
        .qualify_native_capacity(storage_types::NativeAttemptCapacity::SharedPool {
            connections: std::num::NonZeroUsize::new(connection.postgres.unwrap().max_pool_size)
                .unwrap(),
        })
        .expect("pool-sized admission config");
    assert_eq!(config.effective_maximum(), 4);
    assert_eq!(config.maximum_concurrency, 8);
}

#[test]
fn postgres_admission_rejects_pool_below_configured_minimum() {
    let connection = StorageConfig {
        initial_stream_lineages: None,
        backend_type: StorageBackend::Postgres,
        connection_string: None,
        file_path: None,
        sqlite: None,
        postgres: Some(PostgresSettings {
            max_pool_size: 2,
            ..PostgresSettings::default()
        }),
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    };
    let error = AdmissionConfig::default()
        .qualify_native_capacity(storage_types::NativeAttemptCapacity::SharedPool {
            connections: std::num::NonZeroUsize::new(connection.postgres.unwrap().max_pool_size)
                .unwrap(),
        })
        .expect_err("pool below admission minimum");
    assert!(error.to_string().contains("native"));
}

#[test]
fn local_path_prefers_connection_string_for_namespace() {
    let connection = |connection_string: &str, file_path: Option<&str>| StorageConfig {
        initial_stream_lineages: None,
        backend_type: StorageBackend::SQLite,
        connection_string: Some(connection_string.to_owned()),
        file_path: file_path.map(str::to_owned),
        sqlite: Some(SqliteSettings::default()),
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    };
    let registry = StorageConnectionRegistry {
        default_connection_id: "primary".to_owned(),
        connections: std::collections::HashMap::from([
            (
                "primary".to_owned(),
                connection(
                    "/tmp/aux-storage-arch3-a.db",
                    Some("/tmp/aux-storage-arch3-b.db"),
                ),
            ),
            (
                "other".to_owned(),
                connection("/tmp/aux-storage-arch3-b.db", None),
            ),
        ]),
    };

    destination_namespaces(&registry).expect("distinct destinations by connection string");
}

#[test]
fn postgres_namespace_requires_postgres_settings() {
    let registry = single_connection_registry(StorageConfig {
        initial_stream_lineages: None,
        backend_type: StorageBackend::Postgres,
        connection_string: Some("postgres://u@localhost/app".to_owned()),
        file_path: None,
        sqlite: None,
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    });

    let error = destination_namespaces(&registry).expect_err("postgres settings are required");
    assert!(
        error
            .to_string()
            .contains("Postgres destination DSN is missing")
    );
}
