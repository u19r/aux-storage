use std::{fs, future::Future, path::PathBuf, sync::Arc};

use futures::future::BoxFuture;
use rocksdb::{OptimisticTransactionDB, Options};
use storage_common::{
    durability::{AttemptCompletion, BarrierFailure, DurabilityBatch},
    native_attempt_lifecycle::{AttemptLifecycle, LateSettle, LifecycleState, await_settle},
    native_observer::{CommitBoundary, observe_commit_boundary},
    writer_occupancy::ForeignAwaitPolicy,
};
use storage_types::{AttemptPhase, AttemptRequest, Clock, StorageError, StorageResult, expired_in};
use tokio::sync::{mpsc, oneshot};

use super::{
    attempt::RocksDbAttempt,
    error::map_operation_error,
    transaction::{NativeStart, drive_attempt},
    writer::{RocksDbWriter, WriteAttempt},
};
use crate::{
    AttemptMode, KvAttempt, KvBackend, KvTopology, constants::BACKEND_HEALTH_KEY,
    native_deadline::NativeDeadline,
};

/// One DB handle. Native writers share a queue because RocksDB's
/// optimistic transactions do not protect range phantoms.
pub struct RocksDbBackend {
    work_limits: storage_types::BoundWorkLimits,
    database: Arc<OptimisticTransactionDB>,
    writer: Arc<RocksDbWriter>,
    pub(super) durability: Arc<DurabilityBatch>,
    deadlines: storage_types::AttemptDeadlines,
    /// Sleeps toward attempt deadlines; tests inject a manual clock.
    clock: Arc<dyn Clock>,
    fault_probe: Option<Arc<dyn crate::KvFaultProbe>>,
    /// The lifecycle observer of every attempt; `None` in production.
    commit_observer: Option<Arc<dyn storage_common::native_observer::NativeObserver>>,
    /// Receives a `DurabilityBarrier` event when each barrier starts.
    #[cfg(feature = "fault-injection")]
    barrier_observer: Arc<std::sync::OnceLock<Arc<dyn crate::KvFaultProbe>>>,
}

enum NativeWorker {
    Read(tokio::task::JoinHandle<StorageResult<()>>),
    Write(oneshot::Receiver<StorageResult<()>>),
}

impl NativeWorker {
    async fn finish(self) -> StorageResult<()> {
        match self {
            Self::Read(worker) => worker
                .await
                .map_err(|_| StorageError::internal("RocksDB worker stopped"))?,
            Self::Write(completion) => completion
                .await
                .map_err(|_| StorageError::internal("RocksDB worker stopped"))?,
        }
    }
}

impl RocksDbBackend {
    pub fn initialize(
        path: PathBuf,
        deadlines: storage_types::AttemptDeadlines,
        writer_queue_capacity: usize,
    ) -> StorageResult<Self> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent)
                .map_err(|_| StorageError::internal("create RocksDB parent directory failed"))?;
        }
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_enable_pipelined_write(true);
        options.set_log_level(rocksdb::LogLevel::Warn);
        options.set_use_fsync(true);
        let database = OptimisticTransactionDB::open(&options, path)
            .map_err(|_| StorageError::internal("open RocksDB failed"))?;
        database
            .get(BACKEND_HEALTH_KEY)
            .map_err(map_operation_error)?;
        // Recovery may keep replayed WAL data in memtables; write it to synced
        // tables before serving. The batch's startup barrier then syncs the
        // WAL.
        database.flush().map_err(map_operation_error)?;
        let database = Arc::new(database);
        let sync_database = Arc::downgrade(&database);
        let barrier_observer = Arc::<std::sync::OnceLock<Arc<dyn crate::KvFaultProbe>>>::default();
        let observer = Arc::clone(&barrier_observer);
        let durability = Arc::new(DurabilityBatch::new(move |start| {
            if let Some(probe) = observer.get() {
                probe.observe(crate::KvNativeEvent::DurabilityBarrier);
            }
            let database = sync_database.clone();
            async move {
                // A closed store has nothing left to sync; the batch stops.
                let database = database.upgrade().ok_or(BarrierFailure::SyncFailed)?;
                // One process owns the RocksDB lock: no native version probe.
                let capture = start.begin(None);
                tokio::task::spawn_blocking(move || database.flush_wal(true))
                    .await
                    .map_err(|_| BarrierFailure::Interrupted)?
                    .map_err(|_| BarrierFailure::SyncFailed)?;
                Ok(capture)
            }
        }));
        Ok(Self {
            work_limits: Default::default(),
            database,
            durability,
            writer: RocksDbWriter::try_new(writer_queue_capacity, ForeignAwaitPolicy::Count)?,
            deadlines,
            clock: Arc::new(storage_types::SystemClock),
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

    #[cfg(feature = "fault-injection")]
    pub fn with_fault_probe(mut self, probe: Arc<dyn crate::KvFaultProbe>) -> Self {
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

    /// What a write operation that awaits anything but its own native
    /// requests does while it holds the writer. Production counts it; tests
    /// choose `Fail`. Call before the first attempt.
    pub fn with_foreign_await_policy(mut self, policy: ForeignAwaitPolicy) -> StorageResult<Self> {
        self.writer = RocksDbWriter::try_new(self.writer.capacity, policy)?;
        Ok(self)
    }

    #[cfg(test)]
    pub(super) fn with_writer_capacity(mut self, capacity: usize) -> StorageResult<Self> {
        self.writer = RocksDbWriter::try_new(capacity, ForeignAwaitPolicy::Fail)?;
        Ok(self)
    }

    #[cfg(test)]
    pub(super) fn writer_counts(&self) -> storage_common::writer_occupancy::WriterOccupancyCounts {
        self.writer.occupancy.counts()
    }

    async fn run_attempt<R, F>(
        (database, writer): (Arc<OptimisticTransactionDB>, Arc<RocksDbWriter>),
        lifecycle: Arc<AttemptLifecycle>,
        execution: (AttemptMode, Option<storage_types::NativeWorkLimits>),
        deadline: &NativeDeadline,
        durability: Option<Arc<DurabilityBatch>>,
        fault_probe: Option<Arc<dyn crate::KvFaultProbe>>,
        operation: F,
    ) -> (
        StorageResult<R>,
        Option<storage_common::durability::PendingDurableWrite>,
    )
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a mut RocksDbAttempt) -> BoxFuture<'a, StorageResult<R>>
            + Send
            + 'static,
    {
        let (mode, limits) = execution;
        let (sender, receiver) = mpsc::unbounded_channel();
        let settlement = Arc::new(storage_types::NativeAttemptSettlement::new(limits));
        let native_settlement = Arc::clone(&settlement);
        let start = NativeStart {
            lifecycle: Arc::clone(&lifecycle),
            deadline: deadline.deadline,
        };
        let worker = match mode {
            AttemptMode::ReadOnly => NativeWorker::Read(tokio::task::spawn_blocking(move || {
                drive_attempt(database, mode, receiver, native_settlement, start, None)
            })),
            AttemptMode::ReadWrite | AttemptMode::ReadWriteItems => {
                let (slot, queued) = match writer.reserve(deadline).await {
                    Ok(reserved) => reserved,
                    Err(error) => return (Err(error), None),
                };
                let (settled, completion) = oneshot::channel();
                if let Err(error) = writer.submit_attempt(WriteAttempt {
                    database,
                    mode,
                    commands: receiver,
                    settlement: native_settlement,
                    start,
                    settled,
                    slot,
                    queued,
                }) {
                    return (Err(error), None);
                }
                NativeWorker::Write(completion)
            }
        };
        let mut attempt = RocksDbAttempt::new(sender, mode, settlement, durability, fault_probe);
        // At expiry a worker that has not begun sees the abandoned lifecycle
        // and opens no transaction; `true` answers the attempt now. A begun
        // one can no longer submit its commit and rolls back.
        let abandon = || lifecycle.abandon() == LifecycleState::Queued;
        let guard = (mode != AttemptMode::ReadOnly)
            .then(|| (Arc::clone(&writer.occupancy), attempt.native_requests()));
        let result = {
            let mut operation = operation(&mut attempt);
            // Each pending poll of a write operation waits for one of its
            // native requests; anything else stalls every queued writer.
            let callback = std::future::poll_fn(move |context| match &guard {
                Some((occupancy, requests)) => {
                    occupancy.poll_write_operation(operation.as_mut(), context, || {
                        requests.load(std::sync::atomic::Ordering::Relaxed) > 0
                    })
                }
                None => operation.as_mut().poll(context),
            });
            match futures::future::select(
                callback,
                std::pin::pin!(deadline.deadline.expired(deadline.clock.as_ref())),
            )
            .await
            {
                // A callback that outran the timer cannot commit past expiry.
                futures::future::Either::Left((result, _)) => {
                    let expired = deadline.deadline.remaining().is_zero();
                    if result.is_ok() && expired && abandon() {
                        None
                    } else {
                        Some(result)
                    }
                }
                futures::future::Either::Right((_, callback)) => {
                    if abandon() {
                        None
                    } else {
                        Some(callback.await)
                    }
                }
            }
        };
        let Some(result) = result else {
            return (
                Err(expired_in(AttemptPhase::Queued)),
                attempt.take_durable_write(),
            );
        };
        let result = result
            .map_err(|error| error.preserve_native_settlement(attempt.settlement().require_open()));
        let settled = attempt.finish(result.is_ok()).await;
        let joined = worker.finish().await;
        // A join error after a recorded commit becomes a commit-unknown
        // outcome in `DurabilityBatch::settle`, which waits for the ticket.
        let result = settled.and(joined).and(result);
        (result, attempt.take_durable_write())
    }
}

impl KvBackend for RocksDbBackend {
    fn attempt_deadlines(&self) -> storage_types::AttemptDeadlines {
        self.deadlines
    }

    fn bind_work_limits(&self, limits: storage_types::NativeWorkLimits) -> StorageResult<()> {
        self.work_limits.bind(limits)
    }

    type Attempt = RocksDbAttempt;

    fn attempt_capacity(&self) -> StorageResult<storage_types::NativeAttemptCapacity> {
        Ok(storage_types::NativeAttemptCapacity::SerializedWriter)
    }

    fn topology(&self) -> KvTopology {
        KvTopology::Local
    }

    fn durability_health(&self) -> Option<storage_common::durability::DurabilityHealth> {
        Some(self.durability.health())
    }

    fn native_layout(&self) -> crate::KvNativeLayout {
        crate::KvNativeLayout {
            key_bytes: None,
            value_bytes: None,
            transaction_bytes:
                infrastructure_provider::physical_store::NativeTransactionByteLimit::NoFixedCeiling,
            prefix_bytes: 0,
            batch_metadata_bytes: crate::constants::ROCKSDB_BATCH_METADATA_BYTES,
            mutation_metadata_bytes: crate::constants::ROCKSDB_MUTATION_METADATA_BYTES,
            conflict_ranges: crate::NativeConflictRangeLayout::NotCharged,
        }
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
        let class = request.durability;
        let deadline_class = request.class();
        let work_limits = self.work_limits.get();
        let database = Arc::clone(&self.database);
        let writer = self.writer.clone();
        let durability = Arc::clone(&self.durability);
        let deadline = NativeDeadline {
            deadline: request.deadline,
            clock: Arc::clone(&self.clock),
        };
        let fault_probe = self.fault_probe.clone();
        let commit_observer = self.commit_observer.clone();
        async move {
            // The supervisor survives cancellation of the public request and
            // waits for native settlement and durable acknowledgement.
            storage_common::native_supervision::supervise_attempt(
                storage_common::native_supervision::AttemptLabel {
                    provider: "rocksdb",
                    class: deadline_class,
                },
                commit_observer,
                move |lifecycle| async move {
                    // Keep the native handle alive through acknowledgement,
                    // without an idle durability worker retaining the RocksDB
                    // file lock after store drop.
                    let durable_database = Arc::clone(&database);
                    deadline.deadline.require_remaining(AttemptPhase::Acquire)?;
                    durability.check_available()?;
                    let after_observer = lifecycle.observer_handle();
                    let (result, durable_write) = Self::run_attempt(
                        (database, writer),
                        lifecycle,
                        (mode, work_limits),
                        &deadline,
                        (mode != AttemptMode::ReadOnly).then(|| Arc::clone(&durability)),
                        fault_probe,
                        operation,
                    )
                    .await;
                    let wrote = durable_write.is_some();
                    // Every mode settles: a read must not return data that a
                    // crash can remove (`storage_common::durability`). A
                    // missing acknowledgement leaves the registration and the
                    // native handle with the detached settlement until its
                    // barrier ends.
                    let completion = AttemptCompletion::new(class, durable_write);
                    let settle = async move {
                        let settled = durability.settle(result, completion).await;
                        drop(durable_database);
                        settled
                    };
                    let value = await_settle(
                        deadline.clock.as_ref(),
                        deadline.deadline,
                        settle,
                        LateSettle::of_write(
                            wrote,
                            "RocksDB durable acknowledgement was not observed before its deadline",
                        ),
                    )
                    .await?;
                    if wrote && let Some(observer) = after_observer.as_deref() {
                        observe_commit_boundary(
                            observer,
                            CommitBoundary::AfterDurableAcknowledgement,
                            mode,
                        )
                        .await?;
                    }
                    Ok(value)
                },
                "RocksDB attempt supervisor stopped",
            )
            .await
        }
    }

    async fn with_versionstamped_attempt<R, F>(
        &self,
        _request: AttemptRequest,
        _operation: F,
    ) -> StorageResult<(R, Option<[u8; 10]>)>
    where
        R: Send + 'static,
        F: for<'a> FnOnce(&'a mut Self::Attempt) -> BoxFuture<'a, StorageResult<R>>
            + Send
            + 'static,
    {
        Err(StorageError::unsupported(
            "RocksDB has no native commit versionstamp",
        ))
    }
}
