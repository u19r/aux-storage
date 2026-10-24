use storage_provider::{
    PostgresSettings, SqliteSettings, StorageBackend, StorageConfig, StorageConnectionConfig,
    StorageConnectionRegistry,
};
use storage_types::StorageResult;

use super::construction::{
    PreparedStorageProviders, prepare_ordinary_storage_providers, single_connection_registry,
};
use crate::{
    ScheduledJobStore,
    admission::AdmissionConfig,
    database_manager::{DatabaseManager, DatabaseManagerRuntimeOptions},
    provider_bundle::MaintenanceRepositories,
};

pub(crate) async fn sqlite_manager_without_system_tables() -> StorageResult<DatabaseManager> {
    let registry = single_connection_registry(super::construction::immediate_sqlite_test_config());
    let options = DatabaseManagerRuntimeOptions::builder()
        .enable_database_jobs(false)
        .build();
    let (prepared, _) = prepare_ordinary_storage_providers(&registry, &options).await?;
    DatabaseManager::assemble_from_prepared(registry, options, prepared)
}

pub(crate) fn database_manager_from_bundle(
    bundle: crate::provider_bundle::StorageProviderBundle,
) -> StorageResult<DatabaseManager> {
    let registry = single_connection_registry(super::construction::immediate_sqlite_test_config());
    let options = DatabaseManagerRuntimeOptions::builder()
        .enable_database_jobs(false)
        .build();
    let capacity = bundle
        .native_attempt_capacity
        .expect("native fixture capacity");
    let admission = options.admission_config.qualify_native_capacity(capacity)?;
    let resources = super::native_admission::NativeConnectionResources::new(
        "default".into(),
        admission,
        bundle.scheduled_job_store.clone(),
        bundle.maintenance_source.clone(),
    );
    let prepared = PreparedStorageProviders {
        dynamo_providers: [("default".to_owned(), bundle.dynamo)]
            .into_iter()
            .collect(),
        stream_providers: bundle
            .stream
            .into_iter()
            .map(|value| ("default".to_owned(), value))
            .collect(),
        queue_providers: bundle
            .queue
            .into_iter()
            .map(|value| ("default".to_owned(), value))
            .collect(),
        pubsub_providers: bundle
            .pubsub
            .into_iter()
            .map(|value| ("default".to_owned(), value))
            .collect(),

        default_ttl_sweep_repository: bundle.ttl_sweep_repository,
        default_system_stream_trim_repository: bundle.system_stream_trim_repository,
        default_table_reclaim_repository: bundle.table_reclaim_repository,
        default_gsi_lifecycle_repository: bundle.gsi_lifecycle_repository,
        maintenance_runtimes: Vec::new(),
        default_scheduled_job_store: Some(bundle.scheduled_job_store),
        default_delivery_source: bundle.delivery_source,
        native_admission: vec![resources],
        #[cfg(feature = "regional")]
        regional_services: Vec::new(),
    };
    DatabaseManager::assemble_from_prepared(registry, options, prepared)
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
    let (prepared, maintenance_repositories) =
        prepare_ordinary_storage_providers(&registry, &runtime_options).await?;
    let mut manager = DatabaseManager::assemble_from_prepared(registry, runtime_options, prepared)?;
    manager.finish_ordinary_initialization().await?;
    let scheduled_job_store = manager.scheduled_job_store();
    Ok((manager, maintenance_repositories, scheduled_job_store))
}

#[test]
fn named_connections_reject_aliases_to_one_physical_destination() {
    let connection = StorageConnectionConfig {
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
                StorageConnectionConfig {
                    connection_string: Some("/tmp/./aux-storage-alias-test.db".to_owned()),
                    file_path: Some("/tmp/./aux-storage-alias-test.db".to_owned()),
                    ..connection
                },
            ),
        ]),
    };

    let error = PreparedStorageProviders::destination_namespaces(&registry)
        .expect_err("aliases must be rejected");
    assert!(error.to_string().contains("alias one physical destination"));
}

#[test]
fn equivalent_postgres_dsns_share_one_destination_namespace() {
    let connection = |dsn: &str| StorageConnectionConfig {
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

    let error = PreparedStorageProviders::destination_namespaces(&registry)
        .expect_err("equivalent DSNs must alias");
    assert!(error.to_string().contains("alias one physical destination"));
}

#[test]
fn postgres_admission_limit_is_clamped_to_foreground_pool_size() {
    let connection = StorageConnectionConfig {
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
    let connection = StorageConnectionConfig {
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
