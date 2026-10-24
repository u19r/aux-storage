use std::{future::Future, panic::AssertUnwindSafe};

use futures::FutureExt as _;
use kv_backend::foundationdb::FoundationDbBackend;
use storage_provider::{FoundationDbSettings, StorageBackend, StorageConfig};
use storage_types::{StorageError, StorageResult};

use crate::{DatabaseManager, DatabaseManagerRuntimeOptions};

/// Parent-owned native test namespace. Child processes receive its
/// configuration; they never own deletion. The parent awaits every child before
/// scope cleanup.
pub struct FoundationDbTestScope {
    backend: FoundationDbBackend,
    settings: FoundationDbSettings,
}

impl FoundationDbTestScope {
    pub async fn try_new(cluster_file: String) -> StorageResult<Self> {
        let prefix = format!("tests/auxfn/{}", uuid::Uuid::new_v4());
        let backend =
            FoundationDbBackend::initialize(kv_backend::foundationdb::FoundationDbOptions {
                cluster_file: Some(&cluster_file),
                subspace_prefix: Some(prefix.as_bytes()),
                ..Default::default()
            })
            .await?;
        Ok(Self {
            backend,
            settings: Self::native_settings(FoundationDbSettings {
                cluster_file: Some(cluster_file),
                subspace_prefix: Some(prefix),
                ..FoundationDbSettings::default()
            }),
        })
    }

    pub fn settings(&self) -> &FoundationDbSettings {
        &self.settings
    }

    /// Open a parent or child connection with the same writer-lifetime policy.
    /// Passing inherited settings grants no namespace deletion authority.
    pub async fn open_database(settings: FoundationDbSettings) -> StorageResult<DatabaseManager> {
        DatabaseManager::new_with_config_and_runtime_options(
            StorageConfig {
                backend_type: StorageBackend::FoundationDb,
                connection_string: None,
                file_path: None,
                sqlite: None,
                postgres: None,
                turso: None,
                rocksdb: None,
                foundationdb: Some(Self::native_settings(settings)),
                remote: None,
            },
            DatabaseManagerRuntimeOptions::builder()
                .enable_database_jobs(false)
                .build(),
        )
        .await
    }

    fn native_settings(mut settings: FoundationDbSettings) -> FoundationDbSettings {
        settings.cache_read_version_ms = 0;
        settings.immediate_gsi_consistency = true;
        settings
    }

    /// Run the complete test body, including database construction, then clear
    /// the namespace. Panic unwinding drops body-owned writers before deletion.
    /// Cancellation and process termination require a surviving parent owner.
    pub async fn run<F>(&self, body: F) -> StorageResult<()>
    where F: Future<Output = ()> {
        let result = AssertUnwindSafe(body).catch_unwind().await;
        let cleanup = self.cleanup().await;
        match result {
            Ok(()) => cleanup,
            Err(panic) => {
                if let Err(error) = cleanup {
                    eprintln!("FoundationDB test namespace cleanup failed: {error}");
                }
                std::panic::resume_unwind(panic)
            }
        }
    }

    /// Idempotent deletion remains callable after an ambiguous commit result.
    pub async fn cleanup(&self) -> StorageResult<()> {
        self.backend.delete_namespace_keys().await?;
        if self.backend.namespace_has_keys().await? {
            return Err(StorageError::internal(
                "FoundationDB test namespace retains keys",
            ));
        }
        Ok(())
    }
}
