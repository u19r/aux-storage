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
use storage_common::{
    durability::{AttemptCompletion, DurabilityBatch, NativeVersionMemo},
    native_attempt_lifecycle::{AttemptLifecycle, LateSettle, LifecycleState, await_settle},
    native_observer::{CommitBoundary, observe_commit_boundary},
    retry::retry_native_conflicts,
    writer_occupancy::{ForeignAwaitPolicy, QueuedWriter, SerializedWriter, WriterOccupancy},
};
use storage_types::{
    AttemptDeadline, AttemptPhase, AttemptPlan, AttemptRequest, Clock, DeadlineClass,
    StorageResult, SystemClock, expired_in,
};

use super::{
    attempt::{DurableWriteSlot, SqliteAttempt},
    transaction::{Session, execute_attempt},
};
use crate::{AttemptMode, SqlAttempt, SqlBackend, SqlEngine};

/// One writer and file-backed WAL readers; each attempt owns one callback.
pub struct SqliteBackend {
    work_limits: storage_types::BoundWorkLimits,
    pub(super) connections: Arc<[tokio_rusqlite::Connection]>,
    /// One memo per connection of a native commit version known durable.
    versions: Arc<[NativeVersionMemo]>,
    next_reader: AtomicUsize,
    pub(super) durability: Option<Arc<DurabilityBatch>>,
    record_limits: super::record_limits::SqliteRecordLimits,
    deadlines: storage_types::AttemptDeadlines,
    /// Sleeps toward attempt deadlines; tests inject a manual clock.
    clock: Arc<dyn Clock>,
    /// The writer connection's queue, hold, and foreign-await guard.
    occupancy: Arc<WriterOccupancy>,
    fault_probe: Option<Arc<dyn crate::SqlFaultProbe>>,
    /// The lifecycle observer of every attempt; `None` in production.
    commit_observer: Option<Arc<dyn storage_common::native_observer::NativeObserver>>,
    /// Receives a `DurabilityBarrier` event when each barrier starts.
    #[cfg(feature = "fault-injection")]
    barrier_observer: super::durability::BarrierObserver,
}

impl SqliteBackend {
    /// Open and configure the native connection. SQL catalog setup follows
    /// in the shared SQL store before this backend is published.
    pub async fn initialize(
        path: impl AsRef<Path>,
        deadlines: storage_types::AttemptDeadlines,
    ) -> StorageResult<Self> {
        let path = path.as_ref();
        let connection = Self::open_connection(path, AttemptMode::ReadWrite, deadlines).await?;
        let barrier_observer = super::durability::BarrierObserver::default();
        #[cfg(feature = "fault-injection")]
        super::durability::observe_wal_frames(&connection, Arc::clone(&barrier_observer)).await?;
        let durability =
            super::durability::initialize_batch(connection.clone(), Arc::clone(&barrier_observer))
                .await?;

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
            (0..reader_count)
                .map(|_| Self::open_connection(path, AttemptMode::ReadOnly, deadlines)),
        )
        .await?;
        let mut connections = Vec::with_capacity(reader_count + 1);
        connections.push(connection);
        connections.extend(readers);
        let record_limits = super::record_limits::SqliteRecordLimits::load(&connections).await?;
        let versions = connections
            .iter()
            .map(|_| NativeVersionMemo::default())
            .collect();
        Ok(Self {
            work_limits: Default::default(),
            connections: connections.into(),
            versions,
            next_reader: AtomicUsize::new(0),
            durability,
            record_limits,
            deadlines,
            clock: Arc::new(SystemClock),
            occupancy: Arc::new(WriterOccupancy::new(
                SerializedWriter::Sqlite,
                ForeignAwaitPolicy::Count,
            )),
            fault_probe: None,
            commit_observer: None,
            #[cfg(feature = "fault-injection")]
            barrier_observer,
        })
    }

    /// Budgets given at construction.
    #[must_use]
    pub fn attempt_deadlines(&self) -> storage_types::AttemptDeadlines {
        self.deadlines
    }

    async fn open_connection(
        path: &Path,
        mode: AttemptMode,
        deadlines: storage_types::AttemptDeadlines,
    ) -> StorageResult<tokio_rusqlite::Connection> {
        let connection = tokio_rusqlite::Connection::open(path)
            .await
            .map_err(super::error::map_worker_error)?;
        retry_native_conflicts(
            &SystemClock,
            storage_provider::DYNAMO_WRITE_RETRY,
            deadlines,
            AttemptPlan::new(mode, DeadlineClass::SchemaChange),
            |_attempt_request| async {
                connection
                    .call(move |connection| {
                        connection.pragma_update(None, "busy_timeout", 5_000)?;
                        connection.pragma_update(None, "synchronous", "NORMAL")?;
                        // A checkpoint that runs between a barrier's capture
                        // and its sync must reach the disk like the barrier
                        // itself, which uses `F_FULLFSYNC` on macOS
                        // (`File::sync_all`).
                        connection.pragma_update(None, "checkpoint_fullfsync", true)?;
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
        let _ = self.barrier_observer.set(Arc::clone(&probe));
        self.commit_observer = Some(crate::fault_injection::commit_observer(&probe));
        self.fault_probe = Some(probe);
        self
    }

    #[cfg(test)]
    pub(super) fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// What a write operation that awaits anything but its own statements
    /// does while it holds the writer connection. Production counts it;
    /// tests choose `Fail`. Call before the first attempt.
    #[must_use]
    pub fn with_foreign_await_policy(mut self, policy: ForeignAwaitPolicy) -> Self {
        self.occupancy = Arc::new(WriterOccupancy::new(SerializedWriter::Sqlite, policy));
        self
    }

    #[cfg(test)]
    pub(super) fn writer_counts(&self) -> storage_common::writer_occupancy::WriterOccupancyCounts {
        self.occupancy.counts()
    }

    /// For other crates' tests that pause Tokio time: auto-advanced time
    /// would otherwise expire attempt and settle bounds around real blocking
    /// SQLite work.
    #[cfg(feature = "test-support")]
    #[must_use]
    pub fn without_deadline_timers(mut self) -> Self {
        self.clock = Arc::new(UnexpiringClock);
        self
    }

    async fn run_attempt<R, F>(
        (connection, lifecycle): (tokio_rusqlite::Connection, Arc<AttemptLifecycle>),
        execution: (AttemptMode, Option<storage_types::NativeWorkLimits>),
        (deadline, clock): (AttemptDeadline, &dyn Clock),
        durable_write: DurableWriteSlot,
        queued: Option<QueuedWriter>,
        operation: F,
        fault_probe: Option<Arc<dyn crate::SqlFaultProbe>>,
    ) -> (StorageResult<R>, Option<u64>)
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
        let settlement_probe = fault_probe.clone();
        let commit_observer = lifecycle.observer_handle();
        let (session, commands) = Session::open();
        let native_write = durable_write.clone();
        let mut attempt = SqliteAttempt::new(session, execution, durable_write, fault_probe);
        let operation = async move {
            let value = operation(&mut attempt)
                .await
                .map_err(|error| error.preserve_native_settlement(attempt.require_settlement()))?;
            attempt.require_settlement()?;
            if let Some(observer) = commit_observer.as_deref() {
                observe_commit_boundary(observer, CommitBoundary::BeforeCommit, mode).await?;
            }
            Ok(value)
        }
        .boxed();
        // The queue gate: the closure begins it when it starts, the timer
        // abandons it while the closure still waits behind the writer.
        let worker_lifecycle = Arc::clone(&lifecycle);
        let call = connection.call(move |native| {
            let writer = queued.map(QueuedWriter::start);
            let _runtime = runtime.enter();
            // An attempt abandoned or expired while queued opens no
            // transaction.
            if deadline.require_remaining(AttemptPhase::Queued).is_err() || !worker_lifecycle.open()
            {
                return Ok(None);
            }
            #[cfg(feature = "fault-injection")]
            let _dispatch = tracing::dispatcher::set_default(&dispatch);
            #[cfg(feature = "fault-injection")]
            let _span = span.enter();
            Ok(Some(execute_attempt(
                native,
                (mode, deadline),
                &worker_lifecycle,
                operation,
                commands,
                &native_write,
                writer.as_ref(),
            )))
        });
        let called = match futures::future::select(
            std::pin::pin!(call),
            std::pin::pin!(deadline.expired(clock)),
        )
        .await
        {
            futures::future::Either::Left((called, _)) => called,
            // A queued closure that starts later sees the abandoned lifecycle
            // and returns before BEGIN. A started one can no longer submit its
            // commit: it rolls back at its Settle check and answers.
            futures::future::Either::Right((_, call)) => match lifecycle.abandon() {
                LifecycleState::Queued => Ok(None),
                _ => call.await,
            },
        };
        let settled = match called {
            Ok(Some(settled)) => settled,
            Ok(None) => return (Err(expired_in(AttemptPhase::Queued)), None),
            Err(error) => (Err(super::error::map_worker_error(error)), None),
        };
        // The native transaction runs inside the one attempt hop, so its
        // settlement events are not waves.
        if let Some(probe) = settlement_probe {
            probe.observe(crate::SqlNativeEvent::Begin);
            probe.observe(if settled.0.is_ok() {
                crate::SqlNativeEvent::Commit
            } else {
                crate::SqlNativeEvent::Rollback
            });
        }
        settled
    }
}

impl SqlBackend for SqliteBackend {
    fn attempt_deadlines(&self) -> storage_types::AttemptDeadlines {
        self.deadlines
    }

    fn bind_work_limits(&self, limits: storage_types::NativeWorkLimits) -> StorageResult<()> {
        self.work_limits.bind(limits)
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

    fn durability_health(&self) -> Option<storage_common::durability::DurabilityHealth> {
        self.durability.as_deref().map(DurabilityBatch::health)
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
        let index = match mode {
            AttemptMode::ReadOnly if self.connections.len() > 1 => {
                1 + self.next_reader.fetch_add(1, Ordering::Relaxed) % (self.connections.len() - 1)
            }
            _ => 0,
        };
        let work_limits = self.work_limits.get();
        let connection = self.connections[index].clone();
        // Every mode settles: a read must not return data that a crash can
        // remove (`storage_common::durability`).
        let durability = self.durability.clone();
        let version_memo = self.versions[index].clone();
        let class = request.durability;
        let fault_probe = self.fault_probe.clone();
        // One detached operation task owns settlement even if its caller is
        // cancelled. Native transaction execution stays on the SQLite worker.
        let durable_write = DurableWriteSlot::new(durability.clone());
        let deadline = request.deadline;
        let deadline_class = deadline.class;
        let clock = Arc::clone(&self.clock);
        let occupancy = (mode != AttemptMode::ReadOnly).then(|| Arc::clone(&self.occupancy));
        storage_common::native_supervision::supervise_attempt(
            storage_common::native_supervision::AttemptLabel {
                provider: "sqlite",
                class: deadline_class,
            },
            self.commit_observer.clone(),
            move |lifecycle| async move {
                deadline.require_remaining(AttemptPhase::Acquire)?;
                if let Some(durability) = &durability {
                    durability.check_available()?;
                }
                let queued = occupancy.map(|occupancy| occupancy.enqueue(Arc::clone(&clock)));
                let after_observer = lifecycle.observer_handle();
                let (result, observed) = Self::run_attempt(
                    (connection, lifecycle),
                    (mode, work_limits),
                    (deadline, clock.as_ref()),
                    durable_write.clone(),
                    queued,
                    operation,
                    fault_probe,
                )
                .await;
                let value = match durability {
                    Some(durability) => {
                        let write = durable_write.take();
                        let wrote = write.is_some();
                        let completion = AttemptCompletion::new(class, write)
                            .with_observed(observed.map(|version| version_memo.observe(version)));
                        // A missing acknowledgement leaves the registration
                        // with the detached settlement until its barrier ends.
                        await_settle(
                            clock.as_ref(),
                            deadline,
                            async move { durability.settle(result, completion).await },
                            LateSettle::of_write(
                                wrote,
                                "SQLite durable acknowledgement was not observed before its \
                                 deadline",
                            ),
                        )
                        .await?
                    }
                    None => result?,
                };
                if let Some(observer) = after_observer.as_deref() {
                    observe_commit_boundary(
                        observer,
                        CommitBoundary::AfterDurableAcknowledgement,
                        mode,
                    )
                    .await?;
                }
                Ok(value)
            },
            "SQLite attempt task stopped",
        )
    }
}

/// Reads system time; its deadline timers never fire.
#[cfg(feature = "test-support")]
struct UnexpiringClock;

#[cfg(feature = "test-support")]
impl Clock for UnexpiringClock {
    fn now(&self) -> storage_types::TimestampMillis {
        SystemClock.now()
    }

    fn sleep(
        &self,
        _duration: std::time::Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = StorageResult<()>> + Send + '_>> {
        Box::pin(std::future::pending())
    }
}
