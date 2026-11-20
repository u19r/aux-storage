use std::{future::Future, panic::AssertUnwindSafe, sync::Arc};

use futures::{
    FutureExt as _,
    future::{BoxFuture, Either, select},
};
use storage_common::native_attempt_lifecycle::{AttemptLifecycle, settle_or_drop};
use storage_types::{
    AttemptPhase, AttemptRequest, AttemptScope, Clock, StorageError, StorageResult, SystemClock,
    expired_in,
};
use tokio_postgres::{Config, NoTls};

use super::{
    attempt::PostgresAttempt,
    network::build_tls_connector,
    pool::{Manager, Pool},
};
use crate::{AttemptMode, SqlBackend, SqlEngine};

pub(super) enum PostgresTransaction {
    Snapshot,
    /// Item operations whose every write targets a row that an earlier
    /// protected read of the same attempt holds (decision D2 of the W2
    /// design); no table lock.
    ReadCommitted,
    Serializable,
    SerializableData,
    /// Item and stream data writers: READ COMMITTED behind the shared
    /// topology fence.
    DataWrite,
    /// A stream topology change: drains `DataWrite` attempts before sealing.
    Topology,
}

impl PostgresTransaction {
    /// The one mapping of a request to its Postgres transaction.
    pub(super) fn of(mode: AttemptMode, scope: AttemptScope) -> Self {
        match (mode, scope) {
            (AttemptMode::ReadOnly, _) => Self::Snapshot,
            (AttemptMode::ReadWrite, AttemptScope::Default | AttemptScope::Schema) => {
                Self::Serializable
            }
            (AttemptMode::ReadWriteItems, AttemptScope::Default | AttemptScope::Schema) => {
                Self::ReadCommitted
            }
            (AttemptMode::ReadWrite, AttemptScope::Data | AttemptScope::ItemWrite) => {
                Self::SerializableData
            }
            (AttemptMode::ReadWriteItems, AttemptScope::Data | AttemptScope::ItemWrite) => {
                Self::DataWrite
            }
            (AttemptMode::ReadWrite | AttemptMode::ReadWriteItems, AttemptScope::Topology) => {
                Self::Topology
            }
        }
    }

    pub(super) fn capability(&self) -> AttemptMode {
        match self {
            Self::Snapshot => AttemptMode::ReadOnly,
            Self::ReadCommitted
            | Self::Serializable
            | Self::SerializableData
            | Self::DataWrite
            | Self::Topology => AttemptMode::ReadWrite,
        }
    }

    pub(super) fn begin_sql(&self) -> &'static str {
        match self {
            Self::Snapshot => "START TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",
            Self::ReadCommitted => "START TRANSACTION ISOLATION LEVEL READ COMMITTED",
            Self::Serializable => "START TRANSACTION ISOLATION LEVEL SERIALIZABLE",
            Self::SerializableData => {
                "START TRANSACTION ISOLATION LEVEL SERIALIZABLE; LOCK TABLE \
                 sys_system_stream_scope IN ROW EXCLUSIVE MODE"
            }
            Self::DataWrite => {
                // Compatible writers hold the topology fence before observing
                // routing. Exclusive topology attempts drain them before
                // sealing.
                "START TRANSACTION ISOLATION LEVEL READ COMMITTED; LOCK TABLE \
                 sys_system_stream_scope IN ROW EXCLUSIVE MODE"
            }
            Self::Topology => {
                "START TRANSACTION ISOLATION LEVEL READ COMMITTED; LOCK TABLE \
                 sys_system_stream_scope IN SHARE ROW EXCLUSIVE MODE"
            }
        }
    }
}

/// Pool-backed driver. Every attempt keeps one exclusive client until native
/// settlement; a client with an unacknowledged settlement is discarded.
pub struct PostgresBackend {
    work_limits: storage_types::BoundWorkLimits,
    pub(super) pool: Pool,
    pub(super) deadlines: storage_types::AttemptDeadlines,
    wake_connect: super::queue_wake::WakeConnect,
    queue_wake: std::sync::OnceLock<Option<queue_provider::QueueWakeTasks>>,
    /// The time source of attempt deadline timers; tests inject a manual clock.
    clock: Arc<dyn Clock>,
    fault_probe: Option<Arc<dyn crate::SqlFaultProbe>>,
    /// The lifecycle observer of every attempt; `None` in production.
    commit_observer: Option<Arc<dyn storage_common::native_observer::NativeObserver>>,
}

impl PostgresBackend {
    /// Open and check a pool using this provider's connection settings.
    /// Shared SQL catalog setup follows before the store is published.
    pub async fn initialize(
        dsn: &str,
        max_pool_size: usize,
        tls: bool,
        deadlines: storage_types::AttemptDeadlines,
    ) -> StorageResult<Self> {
        let config: Config = dsn
            .parse()
            .map_err(|_| StorageError::validation("invalid Postgres DSN"))?;
        if max_pool_size == 0 {
            return Err(StorageError::validation(
                "Postgres pool size must be positive",
            ));
        }
        let wake_config = config.clone();
        let (pool, wake_connect) = if tls {
            let connector = build_tls_connector()?;
            let wake_connect =
                super::queue_wake::wake_connect(wake_config, connector.clone(), deadlines);
            let pool = Pool::builder(Manager::try_new(config, connector, deadlines)?)
                .max_size(max_pool_size)
                .build();
            (pool, wake_connect)
        } else {
            let wake_connect = super::queue_wake::wake_connect(wake_config, NoTls, deadlines);
            let pool = Pool::builder(Manager::try_new(config, NoTls, deadlines)?)
                .max_size(max_pool_size)
                .build();
            (pool, wake_connect)
        };
        let pool = pool.map_err(|_| StorageError::internal("failed to build Postgres pool"))?;
        let client = pool
            .get()
            .await
            .map_err(super::error::map_acquisition_error)?;
        drop(client);
        Ok(Self {
            work_limits: Default::default(),
            pool,
            deadlines,
            wake_connect,
            queue_wake: Default::default(),
            clock: Arc::new(SystemClock),
            fault_probe: None,
            commit_observer: None,
        })
    }

    #[cfg(all(test, feature = "fault-injection"))]
    pub(crate) fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Budgets given at construction.
    #[must_use]
    pub fn attempt_deadlines(&self) -> storage_types::AttemptDeadlines {
        self.deadlines
    }

    #[cfg(feature = "fault-injection")]
    pub fn with_fault_probe(mut self, probe: Arc<dyn crate::SqlFaultProbe>) -> Self {
        self.pool.manager().set_cleanup_probe(Arc::clone(&probe));
        self.commit_observer = Some(crate::fault_injection::commit_observer(&probe));
        self.fault_probe = Some(probe);
        self
    }

    /// One attempt: pool wait, callback, COMMIT or ROLLBACK, all raced by one
    /// timer on the backend clock. Before COMMIT is sent an expiry drops the
    /// connection and is a known abort; after it the answer is awaited until
    /// the commit grace ends, and a missing answer is unknown.
    pub(super) async fn run_attempt<R, F>(
        pool: Pool,
        lifecycle: Arc<AttemptLifecycle>,
        execution: (PostgresTransaction, Option<storage_types::NativeWorkLimits>),
        durability: storage_types::AttemptDurability,
        (deadline, clock): (storage_types::AttemptDeadline, Arc<dyn Clock>),
        fault_probe: Option<Arc<dyn crate::SqlFaultProbe>>,
        operation: F,
    ) -> StorageResult<R>
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a mut PostgresAttempt) -> BoxFuture<'a, StorageResult<R>>
            + Send
            + 'static,
    {
        deadline.require_remaining(AttemptPhase::Acquire)?;
        let mut timer = std::pin::pin!(deadline.expired(clock.as_ref()));
        let client = match select(std::pin::pin!(pool.get()), timer.as_mut()).await {
            Either::Left((client, _)) => client.map_err(super::error::map_acquisition_error)?,
            Either::Right(_) => return Err(expired_in(AttemptPhase::Acquire)),
        };
        // Readiness wins the race above; the deadline may have passed.
        deadline.require_remaining(AttemptPhase::Acquire)?;
        if !lifecycle.open() {
            return Err(expired_in(AttemptPhase::Acquire));
        }
        let mut attempt = PostgresAttempt::new(
            client,
            execution,
            durability,
            deadline,
            Arc::clone(&lifecycle),
            fault_probe,
        );
        let finished = {
            let run = std::pin::pin!(AssertUnwindSafe(attempt.run(operation)).catch_unwind());
            match select(run, timer).await {
                Either::Left((result, _)) => Some(result),
                // COMMIT was sent: wait for its answer until the commit grace
                // ends.
                Either::Right(((), run)) if lifecycle.awaits_answer() => {
                    settle_or_drop(clock.as_ref(), deadline, run).await
                }
                Either::Right(_) => None,
            }
        };
        match finished {
            Some(Ok(result)) => {
                let result = normalize_committed_result(result, attempt.has_committed_write());
                attempt.release_client().await;
                result
            }
            Some(Err(panic)) => {
                attempt.discard_client().await;
                std::panic::resume_unwind(panic)
            }
            None => {
                let error = attempt.expired_error();
                attempt.discard_client().await;
                normalize_committed_result::<R>(Err(error), attempt.has_committed_write())
            }
        }
    }

    /// The one body of every attempt entry point.
    fn supervised_attempt<R, F>(
        &self,
        request: AttemptRequest,
        operation: F,
    ) -> impl Future<Output = StorageResult<R>> + Send + 'static
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a mut PostgresAttempt) -> BoxFuture<'a, StorageResult<R>>
            + Send
            + 'static,
    {
        let transaction = PostgresTransaction::of(request.mode, request.scope);
        let deadline = request.deadline;
        let pool = self.pool.clone();
        let limits = self.work_limits.get();
        let clock = Arc::clone(&self.clock);
        let fault_probe = self.fault_probe.clone();
        storage_common::native_supervision::supervise_attempt(
            storage_common::native_supervision::AttemptLabel {
                provider: "postgres",
                class: deadline.class,
            },
            self.commit_observer.clone(),
            // Boxed: the attempt state is 100 KB or more in a debug build, and
            // every layer above (settle, publish, retry) would hold it by value
            // on the caller's stack.
            move |lifecycle| {
                Box::pin(Self::run_attempt(
                    pool,
                    lifecycle,
                    (transaction, limits),
                    request.durability,
                    (deadline, clock),
                    fault_probe,
                    operation,
                ))
            },
            "Postgres attempt task stopped",
        )
    }
}

impl SqlBackend for PostgresBackend {
    fn queue_wake_board(&self) -> Option<Arc<queue_provider::QueueWakeBoard>> {
        self.queue_wake
            .get_or_init(|| super::queue_wake::start(Arc::clone(&self.wake_connect)))
            .as_ref()
            .map(queue_provider::QueueWakeTasks::board)
    }

    fn attempt_deadlines(&self) -> storage_types::AttemptDeadlines {
        self.deadlines
    }

    async fn execute_autocommit_ddl(&self, ddl: crate::SqlDdl) -> StorageResult<()> {
        let pool = self.pool.clone();
        let fault_probe = self.fault_probe.clone();
        storage_common::native_supervision::supervise_attempt(
            storage_common::native_supervision::AttemptLabel {
                provider: "postgres",
                class: storage_types::DeadlineClass::SchemaChange,
            },
            None,
            move |_lifecycle| async move {
                let crate::SqlDdl {
                    object,
                    guard,
                    statement,
                    lock_timeout,
                } = ddl;
                // Taking the pooled connection makes cancellation close the
                // native session. A session lock can never leak into a later
                // borrower.
                let client =
                    super::pool::Client::take(pool.get().await.map_err(|_| {
                        StorageError::internal("Postgres DDL pool acquisition failed")
                    })?);
                let locked: bool = client
                    .query_one(
                        "SELECT pg_try_advisory_lock(hashtextextended($1, 0))",
                        &[&object],
                    )
                    .await
                    .map_err(|error| super::error::map_driver_error(&error))?
                    .get(0);
                if !locked {
                    return Ok(());
                }
                let (guard_sql, guard_binds) = guard.into_parts();
                let binds = guard_binds
                    .iter()
                    .map(|value| value as &(dyn tokio_postgres::types::ToSql + Sync))
                    .collect::<Vec<_>>();
                let allowed: bool = client
                    .query_one(&*guard_sql, &binds)
                    .await
                    .map_err(|error| super::error::map_driver_error(&error))?
                    .get(0);
                if !allowed {
                    return Ok(());
                }
                // The pooled session deadlines bound ordinary attempts; a
                // concurrent index build can take longer and a cancelled one
                // leaves an INVALID index. This taken session is closed after
                // the DDL, so the settings never reach a later borrower.
                let lock_timeout = lock_timeout.map_or_else(
                    || "0".to_owned(),
                    |timeout| format!("{}ms", timeout.as_millis()),
                );
                client
                    .query_one(
                        "SELECT set_config('statement_timeout', '0', false), \
                         set_config('transaction_timeout', '0', false), \
                         set_config('lock_timeout', $1, false)",
                        &[&lock_timeout],
                    )
                    .await
                    .map_err(|error| super::error::map_driver_error(&error))?;
                let (sql, binds) = statement.into_parts();
                if !binds.is_empty() {
                    return Err(StorageError::internal(
                        "Autocommit DDL has unexpected binds",
                    ));
                }
                let wave =
                    crate::fault_injection::SqlWave::start_plain(fault_probe.as_deref(), &[&sql])
                        .await?;
                let result = client.batch_execute(&sql).await;
                crate::fault_injection::end_wave(wave);
                client
                    .query_one(
                        "SELECT pg_advisory_unlock(hashtextextended($1, 0))",
                        &[&object],
                    )
                    .await
                    .map_err(|error| super::error::map_driver_error(&error))?;
                result.map_err(|error| super::error::map_driver_error(&error))
            },
            "Postgres DDL task stopped",
        )
        .await
    }
    fn bind_work_limits(&self, limits: storage_types::NativeWorkLimits) -> StorageResult<()> {
        self.work_limits.bind(limits)
    }

    type Attempt = PostgresAttempt;

    fn record_capacity(
        &self,
        limits: infrastructure_provider::limits::InfrastructureLimits,
    ) -> StorageResult<infrastructure_provider::physical_store::PhysicalRecordCapacity> {
        super::record_limits::qualify_record_protocol(limits)
    }

    fn attempt_capacity(&self) -> StorageResult<storage_types::NativeAttemptCapacity> {
        let connections =
            std::num::NonZeroUsize::new(self.pool.status().max_size).ok_or_else(|| {
                storage_types::StorageError::internal("initialized Postgres pool has no capacity")
            })?;
        Ok(storage_types::NativeAttemptCapacity::SharedPool { connections })
    }

    fn engine(&self) -> SqlEngine {
        SqlEngine::Postgres
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
        self.supervised_attempt(request, operation)
    }
}

// A confirmed write cannot grant replay through either result shape. Native
// unknown and permanent errors keep their existing priority and concrete cause.
pub(super) fn normalize_committed_result<R>(
    result: StorageResult<R>,
    committed: bool,
) -> StorageResult<R> {
    if !committed {
        return result;
    }
    let Err(error) = result else {
        return result;
    };
    if error
        .attempt_kind()
        .is_some_and(|kind| kind != storage_types::AttemptKind::CommitUnknown)
        || error.class().is_retryable()
    {
        Err(
            StorageError::internal("Postgres callback failed after confirmed write commit")
                .with_context(error.to_string()),
        )
    } else {
        Err(error)
    }
}
