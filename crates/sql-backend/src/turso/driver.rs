use std::{
    future::Future,
    num::NonZeroUsize,
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures::{FutureExt as _, future::BoxFuture};
use storage_types::{StorageError, StorageResult};
use tokio::sync::Mutex;

use super::attempt::TursoAttempt;
use crate::{AttemptMode, SqlAttempt, SqlBackend, SqlEngine};

/// Independent MVCC connections; each attempt exclusively leases one
/// connection.
pub struct TursoBackend {
    work_limits: std::sync::OnceLock<storage_types::NativeWorkLimits>,
    _database: Arc<turso::Database>,
    connections: Arc<[Arc<Mutex<turso::Connection>>]>,
    next_connection: AtomicUsize,
    #[cfg(feature = "fault-injection")]
    fault_probe: Option<Arc<dyn crate::SqlFaultProbe>>,
}

impl TursoBackend {
    /// Open and configure the native database at the provider-specific path.
    pub async fn initialize(database_path: &str) -> StorageResult<Self> {
        let path = database_path
            .strip_prefix("sqlite:")
            .unwrap_or(database_path)
            .split('?')
            .next()
            .unwrap_or(database_path);
        let database = turso::Builder::new_local(path)
            .build()
            .await
            .map_err(super::error::map_driver_error)?;
        let connection = Self::open_connection(&database).await?;
        connection
            .pragma_update("journal_mode", "'mvcc'")
            .await
            .map_err(super::error::map_driver_error)?;
        let mut health = connection
            .query("SELECT 1", ())
            .await
            .map_err(super::error::map_driver_error)?;
        health
            .next()
            .await
            .map_err(super::error::map_driver_error)?
            .ok_or_else(|| StorageError::internal("Turso startup check returned no row"))?;
        // TODO: benchmark connection counts against throughput and contention.
        let count = std::thread::available_parallelism()
            .map_or(4, NonZeroUsize::get)
            .min(crate::constants::LOCAL_SQL_MAX_CONNECTIONS);
        let others =
            futures::future::try_join_all((1..count).map(|_| Self::open_connection(&database)))
                .await?;
        let connections = std::iter::once(connection)
            .chain(others)
            .map(|connection| Arc::new(Mutex::new(connection)))
            .collect::<Vec<_>>();
        Ok(Self {
            work_limits: Default::default(),
            _database: Arc::new(database),
            connections: connections.into(),
            next_connection: AtomicUsize::new(0),
            #[cfg(feature = "fault-injection")]
            fault_probe: None,
        })
    }

    async fn open_connection(database: &turso::Database) -> StorageResult<turso::Connection> {
        let connection = database.connect().map_err(super::error::map_driver_error)?;
        connection
            .busy_timeout(Duration::from_millis(5_000))
            .map_err(super::error::map_driver_error)?;
        connection
            .pragma_update("synchronous", "'FULL'")
            .await
            .map_err(super::error::map_driver_error)?;
        Ok(connection)
    }

    #[cfg(feature = "fault-injection")]
    pub fn with_fault_probe(mut self, probe: Arc<dyn crate::SqlFaultProbe>) -> Self {
        self.fault_probe = Some(probe);
        self
    }

    async fn run_attempt<R, F>(
        connection: Arc<Mutex<turso::Connection>>,
        execution: (AttemptMode, Option<storage_types::NativeWorkLimits>),
        #[cfg(feature = "fault-injection")] fault_probe: Option<Arc<dyn crate::SqlFaultProbe>>,
        operation: F,
    ) -> StorageResult<R>
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a mut TursoAttempt) -> BoxFuture<'a, StorageResult<R>> + Send + 'static,
    {
        let guard = connection.lock_owned().await;
        let mut attempt = TursoAttempt::new(
            guard,
            execution,
            #[cfg(feature = "fault-injection")]
            fault_probe,
        );
        attempt.begin().await?;
        let outcome = AssertUnwindSafe(operation(&mut attempt))
            .catch_unwind()
            .await;
        match outcome {
            Ok(Ok(value)) => {
                attempt.commit().await?;
                Ok(value)
            }
            Ok(Err(error)) => {
                attempt.rollback().await?;
                Err(error.preserve_native_settlement(attempt.require_settlement()))
            }
            Err(panic) => {
                let _ = attempt.rollback().await;
                std::panic::resume_unwind(panic);
            }
        }
    }
}

impl SqlBackend for TursoBackend {
    fn bind_work_limits(&self, limits: storage_types::NativeWorkLimits) -> StorageResult<()> {
        self.work_limits.set(limits).map_err(|_| {
            storage_types::StorageError::validation("native work budgets are already bound")
        })
    }

    type Attempt = TursoAttempt;

    fn record_capacity(
        &self,
        limits: infrastructure_provider::limits::InfrastructureLimits,
    ) -> StorageResult<infrastructure_provider::physical_store::PhysicalRecordCapacity> {
        super::record_limits::qualify_record_format(limits)
    }

    fn attempt_capacity(&self) -> StorageResult<storage_types::NativeAttemptCapacity> {
        Ok(storage_types::NativeAttemptCapacity::SerializedWriter)
    }

    fn engine(&self) -> SqlEngine {
        SqlEngine::Turso
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
        // Immediate transactions admit one native writer. Queue on its lease
        // before BEGIN so waiting writers do not spin in the busy handler or
        // occupy read connections.
        let index = match mode {
            AttemptMode::ReadWrite | AttemptMode::ReadWriteItems => 0,
            AttemptMode::ReadOnly if self.connections.len() > 1 => {
                1 + self.next_connection.fetch_add(1, Ordering::Relaxed)
                    % (self.connections.len() - 1)
            }
            AttemptMode::ReadOnly => 0,
        };
        let work_limits = self.work_limits.get().copied();
        let connection = Arc::clone(&self.connections[index]);
        #[cfg(feature = "fault-injection")]
        let fault_probe = self.fault_probe.clone();
        async move {
            crate::backend::supervise_attempt(
                Self::run_attempt(
                    connection,
                    (mode, work_limits),
                    #[cfg(feature = "fault-injection")]
                    fault_probe,
                    operation,
                ),
                "Turso attempt task stopped",
            )
            .await
        }
    }
}
