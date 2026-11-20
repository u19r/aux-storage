#[cfg(any(feature = "sqlite", feature = "remote"))]
use std::sync::Arc;

use storage_provider::{StorageBackend, StorageConfig};
#[cfg(feature = "sqlite")]
use storage_types::Clock;
use storage_types::StorageResult;

use super::{DatabaseManager, DatabaseManagerRuntimeOptions};

impl DatabaseManager {
    pub async fn new_for_test() -> StorageResult<Self> {
        Self::new_for_test_with_runtime_options(DatabaseManagerRuntimeOptions::default()).await
    }

    pub async fn new_for_test_with_config(config: StorageConfig) -> StorageResult<Self> {
        Self::new_with_config(config).await
    }

    pub async fn new_for_test_with_runtime_options(
        runtime_options: DatabaseManagerRuntimeOptions,
    ) -> StorageResult<Self> {
        Self::new_with_config_and_runtime_options(immediate_sqlite_test_config(), runtime_options)
            .await
    }

    /// Construct native storage with the same time authority as its test
    /// runtime.
    #[cfg(feature = "sqlite")]
    pub async fn new_for_test_with_clock(clock: Arc<dyn Clock>) -> StorageResult<Self> {
        let driver = sql_backend::sqlite::SqliteBackend::initialize(
            ":memory:",
            storage_types::AttemptDeadlines::default(),
        )
        .await?;
        let store = Arc::new(
            sql_backend::SqlStore::initialize_schema(
                driver,
                storage_types::InitialStreamLineages::ONE,
                true,
            )
            .await?
            .with_clock(clock),
        );
        Self::from_initialized_store(
            immediate_sqlite_test_config(),
            crate::InitializedStore::native(store),
            DatabaseManagerRuntimeOptions::builder()
                .enable_database_jobs(false)
                .build(),
        )
        .await
    }

    /// A manager over one remote connection that neither bootstraps its job
    /// store nor creates system tables (no network at construction).
    #[cfg(all(feature = "remote", any(test, feature = "test-support")))]
    pub async fn new_for_test_with_remote(
        settings: storage_provider::RemoteStorageSettings,
    ) -> StorageResult<Self> {
        let jobs = storage_remote::build_scheduled_job_store(settings.clone()).await?;
        let provider = Arc::new(storage_remote::RemoteStorageProvider::new(settings).await?);
        let options = DatabaseManagerRuntimeOptions::builder()
            .enable_database_jobs(false)
            .build();
        let connection = crate::connection::Connection::remote(
            super::ROUTED_DEFAULT_CONNECTION_ID,
            provider,
            jobs,
            &options,
        )?;
        Ok(Self::from_connections(
            connection,
            storage_types::VecMap::new(),
            false,
        ))
    }
}

pub(crate) fn immediate_sqlite_test_config() -> StorageConfig {
    StorageConfig {
        initial_stream_lineages: None,
        backend_type: StorageBackend::SQLite,
        connection_string: Some(":memory:".to_string()),
        file_path: None,
        sqlite: Some(storage_provider::SqliteSettings::default()),
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    }
}
