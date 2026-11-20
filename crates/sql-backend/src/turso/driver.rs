use std::{
    future::Future,
    num::NonZeroUsize,
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use futures::{FutureExt as _, future::BoxFuture};
use storage_common::native_attempt_lifecycle::{AttemptLifecycle, LateSettle, await_settle};
use storage_types::{
    AttemptDeadline, AttemptPhase, AttemptRequest, Clock, StorageError, StorageResult, SystemClock,
    expired_in,
};
use tokio::sync::Mutex;

use super::attempt::TursoAttempt;
use crate::{AttemptMode, SqlAttempt, SqlBackend, SqlEngine};

/// Independent MVCC connections; each attempt exclusively leases one
/// connection.
pub struct TursoBackend {
    work_limits: storage_types::BoundWorkLimits,
    _database: Arc<turso::Database>,
    connections: Arc<[Arc<Mutex<turso::Connection>>]>,
    next_connection: AtomicUsize,
    deadlines: storage_types::AttemptDeadlines,
    /// Sleeps toward attempt deadlines; tests inject a manual clock.
    clock: Arc<dyn Clock>,
    fault_probe: Option<Arc<dyn crate::SqlFaultProbe>>,
    /// The lifecycle observer of every attempt; `None` in production.
    commit_observer: Option<Arc<dyn storage_common::native_observer::NativeObserver>>,
}

impl TursoBackend {
    /// Open and configure the native database at the provider-specific path.
    pub async fn initialize(
        database_path: &str,
        deadlines: storage_types::AttemptDeadlines,
    ) -> StorageResult<Self> {
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
        let connection = Self::open_connection(&database, false).await?;
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
            .clamp(2, crate::constants::LOCAL_SQL_MAX_CONNECTIONS);
        let others = futures::future::try_join_all(
            (1..count).map(|_| Self::open_connection(&database, true)),
        )
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
            deadlines,
            clock: Arc::new(SystemClock),
            fault_probe: None,
            commit_observer: None,
        })
    }

    /// Budgets given at construction.
    #[must_use]
    pub fn attempt_deadlines(&self) -> storage_types::AttemptDeadlines {
        self.deadlines
    }

    async fn open_connection(
        database: &turso::Database,
        query_only: bool,
    ) -> StorageResult<turso::Connection> {
        let connection = database.connect().map_err(super::error::map_driver_error)?;
        connection
            .busy_timeout(crate::constants::LOCAL_SQL_BUSY_TIMEOUT)
            .map_err(super::error::map_driver_error)?;
        connection
            .pragma_update("synchronous", "'FULL'")
            .await
            .map_err(super::error::map_driver_error)?;
        if query_only {
            // Every ReadOnly attempt runs here: Turso refuses a writing
            // statement at prepare, before it changes anything (FIN-a).
            connection
                .pragma_update("query_only", 1)
                .await
                .map_err(super::error::map_driver_error)?;
        }
        Ok(connection)
    }

    #[cfg(feature = "fault-injection")]
    pub fn with_fault_probe(mut self, probe: Arc<dyn crate::SqlFaultProbe>) -> Self {
        self.commit_observer = Some(crate::fault_injection::commit_observer(&probe));
        self.fault_probe = Some(probe);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// The native database, for tests that hold a lock beside the store.
    #[cfg(test)]
    pub(super) fn database(&self) -> &turso::Database {
        &self._database
    }

    async fn run_attempt<R, F>(
        connection: Arc<Mutex<turso::Connection>>,
        lifecycle: Arc<AttemptLifecycle>,
        execution: (AttemptMode, Option<storage_types::NativeWorkLimits>),
        (deadline, clock): (AttemptDeadline, Arc<dyn Clock>),
        fault_probe: Option<Arc<dyn crate::SqlFaultProbe>>,
        operation: F,
    ) -> StorageResult<R>
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a mut TursoAttempt) -> BoxFuture<'a, StorageResult<R>> + Send + 'static,
    {
        deadline.require_remaining(AttemptPhase::Acquire)?;
        let lease = connection.lock_owned();
        let guard = match futures::future::select(
            std::pin::pin!(lease),
            std::pin::pin!(deadline.expired(clock.as_ref())),
        )
        .await
        {
            futures::future::Either::Left((guard, _)) => guard,
            futures::future::Either::Right(_) => return Err(expired_in(AttemptPhase::Acquire)),
        };
        // Readiness wins the race above; the deadline may have passed.
        deadline.require_remaining(AttemptPhase::Acquire)?;
        if !lifecycle.open() {
            return Err(expired_in(AttemptPhase::Acquire));
        }
        let wrote = execution.0 != AttemptMode::ReadOnly;
        let mut attempt = TursoAttempt::new(guard, execution, fault_probe);
        attempt.bound_busy_wait(&deadline)?;
        if let Err(error) = attempt.begin().await {
            return Err(crate::backend::local_begin_failure(error, &deadline));
        }
        let outcome = AssertUnwindSafe(operation(&mut attempt))
            .catch_unwind()
            .await;
        match outcome {
            Ok(Ok(value)) => {
                // A missing acknowledgement leaves the lease with the
                // detached commit until it ends.
                let commit = async move { attempt.commit(deadline, &lifecycle).await };
                await_settle(
                    clock.as_ref(),
                    deadline,
                    commit,
                    // A read submits no ticket and, since FIN-a, cannot write.
                    LateSettle::of_write(
                        wrote,
                        "Turso commit was not acknowledged before its deadline",
                    ),
                )
                .await?;
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
    fn attempt_deadlines(&self) -> storage_types::AttemptDeadlines {
        self.deadlines
    }

    fn bind_work_limits(&self, limits: storage_types::NativeWorkLimits) -> StorageResult<()> {
        self.work_limits.bind(limits)
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
        request: AttemptRequest,
        operation: F,
    ) -> impl Future<Output = StorageResult<R>> + Send
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a mut Self::Attempt) -> BoxFuture<'a, StorageResult<R>>
            + Send
            + 'static,
    {
        let mode = request.mode;
        // `request.durability` needs no handling here. Each connection runs
        // with `synchronous = FULL`, so COMMIT returns after its log sync, and
        // the MVCC engine makes a commit final only after that sync: a reader
        // that saw it while it was preparing takes a commit dependency and
        // cannot commit before it (turso_core 0.8.3, `mvcc/database/mod.rs`,
        // `CommitState` order and Hekaton section 2.7). A read never returns
        // data that a crash can remove, and Relaxed would gain nothing.
        // Immediate transactions admit one native writer. Queue on its lease
        // before BEGIN so waiting writers do not spin in the busy handler or
        // occupy read connections.
        // `initialize` opens at least two connections; 1.. are `query_only`.
        let index = match mode {
            AttemptMode::ReadWrite | AttemptMode::ReadWriteItems => 0,
            AttemptMode::ReadOnly => {
                1 + self.next_connection.fetch_add(1, Ordering::Relaxed)
                    % (self.connections.len() - 1)
            }
        };
        let work_limits = self.work_limits.get();
        let connection = Arc::clone(&self.connections[index]);
        let deadline = (request.deadline, Arc::clone(&self.clock));
        let deadline_class = deadline.0.class;
        let fault_probe = self.fault_probe.clone();
        let commit_observer = self.commit_observer.clone();
        async move {
            storage_common::native_supervision::supervise_attempt(
                storage_common::native_supervision::AttemptLabel {
                    provider: "turso",
                    class: deadline_class,
                },
                commit_observer,
                move |lifecycle| {
                    Self::run_attempt(
                        connection,
                        lifecycle,
                        (mode, work_limits),
                        deadline,
                        fault_probe,
                        operation,
                    )
                },
                "Turso attempt task stopped",
            )
            .await
        }
    }
}
