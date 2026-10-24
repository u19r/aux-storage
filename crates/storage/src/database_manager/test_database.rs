#[cfg(feature = "sqlite")]
use std::sync::Arc;

use storage_provider::StorageConfig;
#[cfg(feature = "sqlite")]
use storage_types::Clock;
use storage_types::StorageResult;

use super::{
    DatabaseManager, DatabaseManagerRuntimeOptions, construction::immediate_sqlite_test_config,
};

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
        let driver = sql_backend::sqlite::SqliteBackend::initialize(":memory:").await?;
        let store = Arc::new(
            sql_backend::SqlStore::initialize_schema(driver)
                .await?
                .with_clock(clock),
        );
        let bundle =
            crate::StorageProviderBundle::try_from_initialized_store(store, Default::default())?;
        Self::from_initialized_bundle(
            immediate_sqlite_test_config(),
            bundle,
            DatabaseManagerRuntimeOptions::builder()
                .enable_database_jobs(false)
                .build(),
        )
        .await
    }
}
