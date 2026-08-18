use std::{
    collections::HashMap,
    fs,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use ::storage_provider::SqliteSettings;
use bg_jobs::JobManager;
use rusqlite::OpenFlags;
use storage_common::{DatabaseJobIntervals, GsiPropagationGovernor};
use storage_types::{StorageError, StorageResult, StoredTableInfo, TableName};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_rusqlite::Connection;
use tracing::instrument;

use crate::sqlite_cache_config::sqlite_page_cache_size_kb;

// Bound the extra SQLite worker threads independently of request concurrency.
pub(crate) const SQLITE_SNAPSHOT_POOL_SIZE: usize = 8;

pub(crate) struct SQLiteSnapshotConnectionPool {
    path: String,
    page_cache_size_kb: i32,
    idle: Mutex<Vec<Connection>>,
    permits: Arc<Semaphore>,
    active: AtomicUsize,
    created: AtomicUsize,
}

pub(crate) struct SQLiteSnapshotConnectionLease {
    connection: Option<Connection>,
    pool: Arc<SQLiteSnapshotConnectionPool>,
    permit: Option<OwnedSemaphorePermit>,
}

impl SQLiteSnapshotConnectionPool {
    fn new(path: String, page_cache_size_kb: i32) -> Self {
        Self {
            path,
            page_cache_size_kb,
            idle: Mutex::new(Vec::with_capacity(SQLITE_SNAPSHOT_POOL_SIZE)),
            permits: Arc::new(Semaphore::new(SQLITE_SNAPSHOT_POOL_SIZE)),
            active: AtomicUsize::new(0),
            created: AtomicUsize::new(0),
        }
    }

    pub(crate) async fn acquire(self: &Arc<Self>) -> StorageResult<SQLiteSnapshotConnectionLease> {
        let started = Instant::now();
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| StorageError::internal("sqlite snapshot connection pool is closed"))?;
        let waited = started.elapsed();
        let connection = self
            .idle
            .lock()
            .map_err(|_| StorageError::internal("sqlite snapshot connection pool is poisoned"))?
            .pop();
        let connection = match connection {
            Some(connection) => connection,
            None => self.open_connection().await?,
        };
        connection
            .call(|conn| {
                conn.execute_batch("BEGIN DEFERRED TRANSACTION")?;
                Ok(())
            })
            .await
            .map_err(|error| {
                StorageError::internal(&format!(
                    "begin sqlite read-sequence snapshot transaction failed: {}: {error}",
                    self.path
                ))
            })?;
        let active = self.active.fetch_add(1, Ordering::Relaxed) + 1;
        tracing::debug!(
            wait_micros = waited.as_micros() as u64,
            active,
            created = self.created.load(Ordering::Relaxed),
            capacity = SQLITE_SNAPSHOT_POOL_SIZE,
            "acquired sqlite read-sequence snapshot connection"
        );
        Ok(SQLiteSnapshotConnectionLease {
            connection: Some(connection),
            pool: Arc::clone(self),
            permit: Some(permit),
        })
    }

    async fn open_connection(&self) -> StorageResult<Connection> {
        let connection = Connection::open_with_flags(&self.path, OpenFlags::SQLITE_OPEN_READ_WRITE)
            .await
            .map_err(|error| {
                StorageError::internal(&format!(
                    "open sqlite read-sequence snapshot connection failed: {}: {error}",
                    self.path
                ))
            })?;
        let page_cache_size_kb = self.page_cache_size_kb;
        connection
            .call(move |conn| {
                conn.pragma_update(None, "busy_timeout", 5_000)?;
                conn.pragma_update(None, "cache_size", page_cache_size_kb)?;
                Ok(())
            })
            .await
            .map_err(|error| {
                StorageError::internal(&format!(
                    "configure sqlite read-sequence snapshot connection failed: {}: {error}",
                    self.path
                ))
            })?;
        self.created.fetch_add(1, Ordering::Relaxed);
        Ok(connection)
    }

    async fn release(self: Arc<Self>, connection: Connection, permit: OwnedSemaphorePermit) {
        let rollback = connection
            .call(|conn| {
                conn.execute_batch("ROLLBACK")?;
                Ok(())
            })
            .await;
        if rollback.is_ok()
            && let Ok(mut idle) = self.idle.lock()
        {
            idle.push(connection);
        }
        self.active.fetch_sub(1, Ordering::Relaxed);
        drop(permit);
    }

    #[cfg(test)]
    pub(crate) fn counts(&self) -> (usize, usize) {
        (
            self.active.load(Ordering::Relaxed),
            self.created.load(Ordering::Relaxed),
        )
    }
}

impl SQLiteSnapshotConnectionLease {
    pub(crate) fn connection(&self) -> StorageResult<&Connection> {
        self.connection
            .as_ref()
            .ok_or_else(|| StorageError::internal("sqlite snapshot connection lease is closed"))
    }
}

impl Drop for SQLiteSnapshotConnectionLease {
    fn drop(&mut self) {
        let (Some(connection), Some(permit)) = (self.connection.take(), self.permit.take()) else {
            return;
        };
        let pool = Arc::clone(&self.pool);
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(pool.release(connection, permit));
        } else {
            pool.active.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

#[derive(Clone)]
pub struct SQLiteStorageProvider {
    pub(crate) connection: Arc<Connection>,
    pub(crate) snapshot_connection_pool: Option<Arc<SQLiteSnapshotConnectionPool>>,
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
        let use_memory_db =
            database_path == ":memory:" || (!settings.force_file_backed_database && cfg!(test));

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

        let read_sequence_snapshot_path = (!use_memory_db).then_some(final_path);
        let snapshot_connection_pool = read_sequence_snapshot_path.as_ref().map(|path| {
            Arc::new(SQLiteSnapshotConnectionPool::new(
                path.clone(),
                sqlite_page_cache_size_kb(),
            ))
        });
        Ok(Self {
            connection: Arc::new(connection),
            snapshot_connection_pool,
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
