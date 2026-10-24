use std::{
    future::Future,
    num::NonZeroUsize,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use futures::{FutureExt as _, future::BoxFuture};
use storage_common::{durability::DurabilityBatch, retry::retry_native_conflicts};
use storage_types::{StorageResult, SystemClock};

use super::{
    attempt::SqliteAttempt,
    transaction::{Session, execute_attempt},
};
use crate::{AttemptMode, SqlAttempt, SqlBackend, SqlEngine};

/// One writer and file-backed WAL readers; each attempt owns one callback.
pub struct SqliteBackend {
    work_limits: std::sync::OnceLock<storage_types::NativeWorkLimits>,
    connections: Arc<[tokio_rusqlite::Connection]>,
    next_reader: AtomicUsize,
    durability: Option<Arc<DurabilityBatch>>,
    record_limits: super::record_limits::SqliteRecordLimits,
    #[cfg(feature = "fault-injection")]
    fault_probe: Option<Arc<dyn crate::SqlFaultProbe>>,
}

impl SqliteBackend {
    /// Open and configure the native connection. SQL catalog setup follows
    /// in the shared SQL store before this backend is published.
    pub async fn initialize(path: impl AsRef<Path>) -> StorageResult<Self> {
        let path = path.as_ref();
        let connection = Self::open_connection(path, AttemptMode::ReadWrite).await?;
        let durability = super::durability::initialize_batch(connection.clone()).await?;

        let reader_count = if path == Path::new(":memory:") || path.as_os_str().is_empty() {
            0
        } else {
            // TODO: benchmark reader counts at half and twice the CPU-based
            // default.
            std::thread::available_parallelism()
                .map_or(4, NonZeroUsize::get)
                .min(crate::constants::LOCAL_SQL_MAX_CONNECTIONS)
        };
        let readers = futures::future::try_join_all(
            (0..reader_count).map(|_| Self::open_connection(path, AttemptMode::ReadOnly)),
        )
        .await?;
        let mut connections = Vec::with_capacity(reader_count + 1);
        connections.push(connection);
        connections.extend(readers);
        let record_limits = super::record_limits::SqliteRecordLimits::load(&connections).await?;
        Ok(Self {
            work_limits: Default::default(),
            connections: connections.into(),
            next_reader: AtomicUsize::new(0),
            durability,
            record_limits,
            #[cfg(feature = "fault-injection")]
            fault_probe: None,
        })
    }

    async fn open_connection(
        path: &Path,
        mode: AttemptMode,
    ) -> StorageResult<tokio_rusqlite::Connection> {
        let connection = tokio_rusqlite::Connection::open(path)
            .await
            .map_err(super::error::map_worker_error)?;
        retry_native_conflicts(
            &SystemClock,
            storage_provider::DYNAMO_WRITE_RETRY,
            || async {
                connection
                    .call(move |connection| {
                        connection.pragma_update(None, "busy_timeout", 5_000)?;
                        connection.pragma_update(None, "synchronous", "NORMAL")?;
                        let journal_mode: String =
                            connection
                                .pragma_query_value(None, "journal_mode", |row| row.get(0))?;
                        if !journal_mode.eq_ignore_ascii_case("wal") {
                            connection.pragma_update(None, "journal_mode", "WAL")?;
                        }
                        if mode == AttemptMode::ReadOnly {
                            connection.pragma_update(None, "query_only", true)?;
                        }
                        Ok(())
                    })
                    .await
                    .map_err(super::error::map_worker_error)
            },
        )
        .await?;
        Ok(connection)
    }

    #[cfg(feature = "fault-injection")]
    pub fn with_fault_probe(mut self, probe: Arc<dyn crate::SqlFaultProbe>) -> Self {
        self.fault_probe = Some(probe);
        self
    }

    async fn run_attempt<R, F>(
        connection: tokio_rusqlite::Connection,
        execution: (AttemptMode, Option<storage_types::NativeWorkLimits>),
        operation: F,
        #[cfg(feature = "fault-injection")] fault_probe: Option<Arc<dyn crate::SqlFaultProbe>>,
    ) -> StorageResult<R>
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a mut SqliteAttempt) -> BoxFuture<'a, StorageResult<R>>
            + Send
            + 'static,
    {
        let mode = execution.0;
        let runtime = tokio::runtime::Handle::current();
        // Fault tests capture safe failure diagnostics across the worker
        // boundary.
        #[cfg(feature = "fault-injection")]
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        #[cfg(feature = "fault-injection")]
        let span = tracing::Span::current();
        #[cfg(feature = "fault-injection")]
        let commit_probe = fault_probe.clone();
        let (session, commands) = Session::open();
        let mut attempt = SqliteAttempt::new(
            session,
            execution,
            #[cfg(feature = "fault-injection")]
            fault_probe,
        );
        let operation = async move {
            let value = operation(&mut attempt)
                .await
                .map_err(|error| error.preserve_native_settlement(attempt.require_settlement()))?;
            attempt.require_settlement()?;
            #[cfg(feature = "fault-injection")]
            crate::SqlCommitBoundary::BeforeCommit
                .observe(commit_probe.as_deref())
                .await?;
            Ok(value)
        }
        .boxed();
        connection
            .call(move |native| {
                let _runtime = runtime.enter();
                #[cfg(feature = "fault-injection")]
                let _dispatch = tracing::dispatcher::set_default(&dispatch);
                #[cfg(feature = "fault-injection")]
                let _span = span.enter();
                Ok(execute_attempt(native, mode, operation, commands))
            })
            .await
            .map_err(super::error::map_worker_error)?
    }
}

impl SqlBackend for SqliteBackend {
    fn bind_work_limits(&self, limits: storage_types::NativeWorkLimits) -> StorageResult<()> {
        self.work_limits.set(limits).map_err(|_| {
            storage_types::StorageError::validation("native work budgets are already bound")
        })
    }

    type Attempt = SqliteAttempt;

    fn record_capacity(
        &self,
        limits: infrastructure_provider::limits::InfrastructureLimits,
    ) -> StorageResult<infrastructure_provider::physical_store::PhysicalRecordCapacity> {
        self.record_limits.record_capacity(limits)
    }

    fn attempt_capacity(&self) -> StorageResult<storage_types::NativeAttemptCapacity> {
        Ok(storage_types::NativeAttemptCapacity::SerializedWriter)
    }

    fn engine(&self) -> SqlEngine {
        SqlEngine::Sqlite
    }

    fn with_attempt<R, F>(
        &self,
        mode: AttemptMode,
        operation: F,
    ) -> impl Future<Output = StorageResult<R>> + Send
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a mut Self::Attempt) -> BoxFuture<'a, StorageResult<R>>
            + Send
            + 'static,
    {
        let index = match mode {
            AttemptMode::ReadOnly if self.connections.len() > 1 => {
                1 + self.next_reader.fetch_add(1, Ordering::Relaxed) % (self.connections.len() - 1)
            }
            _ => 0,
        };
        let work_limits = self.work_limits.get().copied();
        let connection = self.connections[index].clone();
        let durability = (mode != AttemptMode::ReadOnly)
            .then(|| self.durability.clone())
            .flatten();
        #[cfg(feature = "fault-injection")]
        let fault_probe = self.fault_probe.clone();
        #[cfg(feature = "fault-injection")]
        let commit_probe = fault_probe.clone();
        // One detached operation task owns settlement even if its caller is
        // cancelled. Native transaction execution stays on the SQLite worker.
        let attempt = async move {
            let value = Self::run_attempt(
                connection,
                (mode, work_limits),
                operation,
                #[cfg(feature = "fault-injection")]
                fault_probe,
            )
            .await?;
            if let Some(durability) = durability {
                durability.wait().await?;
            }
            #[cfg(feature = "fault-injection")]
            crate::SqlCommitBoundary::AfterDurableAcknowledgement
                .observe(commit_probe.as_deref())
                .await?;
            Ok(value)
        };
        crate::backend::supervise_attempt(attempt, "SQLite attempt task stopped")
    }
}
