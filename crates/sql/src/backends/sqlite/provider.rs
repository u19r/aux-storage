use std::{collections::HashMap, fs, path::Path, sync::Arc};

use ::storage_provider::SqliteSettings;
use bg_jobs::JobManager;
use rusqlite::OpenFlags;
use storage_common::{DatabaseJobIntervals, GsiPropagationGovernor};
use storage_types::{StorageError, StorageResult, StoredTableInfo, TableName};
use tokio_rusqlite::Connection;
use tracing::instrument;

use crate::sqlite_cache_config::sqlite_page_cache_size_kb;

#[derive(Clone)]
pub struct SQLiteStorageProvider {
    pub(crate) connection: Arc<Connection>,
    pub(crate) read_sequence_snapshot_path: Option<String>,
    pub(crate) job_manager: JobManager,
    pub(crate) table_info_cache: Arc<tokio::sync::RwLock<HashMap<TableName, Arc<StoredTableInfo>>>>,
    pub(crate) immediate_gsi_consistency: bool,
    pub(crate) database_jobs_enabled: bool,
    pub(crate) database_job_intervals: DatabaseJobIntervals,
    pub(crate) gsi_propagation_governor: Arc<GsiPropagationGovernor>,
}

impl SQLiteStorageProvider {
    #[instrument(level = "info", fields(feature = "storage", database_path = tracing::field::Empty, use_memory = tracing::field::Empty))]
    pub async fn new(database_path: &str) -> StorageResult<Self> {
        Self::new_with_settings(database_path, SqliteSettings::default()).await
    }

    #[instrument(level = "info", fields(feature = "storage", database_path = tracing::field::Empty, use_memory = tracing::field::Empty))]
    pub async fn new_with_settings(
        database_path: &str,
        settings: SqliteSettings,
    ) -> StorageResult<Self> {
        let use_memory_db = database_path == ":memory:"
            || (!settings.force_file_backed_database
                && (cfg!(test)
                    || std::env::var("RUST_TEST_THREADS").is_ok()
                    || database_path.contains("test")));

        let final_path = if use_memory_db {
            ":memory:".to_string()
        } else {
            // Remove the sqlite: prefix and query parameters if present
            database_path
                .strip_prefix("sqlite:")
                .unwrap_or(database_path)
                .split('?')
                .next()
                .unwrap_or(database_path)
                .to_string()
        };

        tracing::Span::current().record("database_path", &final_path);
        tracing::Span::current().record("use_memory", use_memory_db);

        ensure_db_directory(&final_path).map_err(|error| {
            StorageError::internal(&format!(
                "prepare sqlite database path failed: {final_path}: {error}"
            ))
        })?;

        let connection = Connection::open_with_flags(
            &final_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "connect to sqlite database failed");
            StorageError::internal(&format!(
                "connect to sqlite database failed: {final_path}: {e}; check file permissions and \
                 directory access"
            ))
        })?;

        connection
            .call(|conn| {
                let page_cache_size_kb = sqlite_page_cache_size_kb();
                conn.pragma_update(None, "journal_mode", "WAL")?;
                conn.pragma_update(None, "synchronous", "FULL")?;
                conn.pragma_update(None, "busy_timeout", 5_000)?;
                conn.pragma_update(None, "cache_size", page_cache_size_kb)?;
                Ok(())
            })
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "set sqlite pragmas failed");
                StorageError::internal(&format!("set sqlite pragmas failed: {final_path}: {e}"))
            })?;

        Ok(Self {
            connection: Arc::new(connection),
            read_sequence_snapshot_path: (!use_memory_db).then_some(final_path),
            job_manager: JobManager::new_for_test(),
            table_info_cache: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            immediate_gsi_consistency: settings.immediate_gsi_consistency,
            database_jobs_enabled: true,
            database_job_intervals: DatabaseJobIntervals::default(),
            gsi_propagation_governor: Arc::new(GsiPropagationGovernor::default()),
        })
    }

    #[must_use]
    pub fn with_job_manager(mut self, job_manager: JobManager) -> Self {
        self.job_manager = job_manager;
        self
    }

    #[must_use]
    pub fn with_database_jobs_enabled(mut self, enabled: bool) -> Self {
        self.database_jobs_enabled = enabled;
        self
    }

    #[must_use]
    pub fn with_database_job_intervals(mut self, intervals: DatabaseJobIntervals) -> Self {
        self.database_job_intervals = intervals;
        self
    }
}

pub(crate) fn ensure_db_directory(path_str: &str) -> std::io::Result<()> {
    if path_str.trim().is_empty() || path_str == ":memory:" {
        return Ok(());
    }

    let path = Path::new(path_str);
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }

    Ok(())
}
