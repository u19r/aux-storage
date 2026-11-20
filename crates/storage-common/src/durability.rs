//! Durable acknowledgements for stores whose native commit is not yet on disk
//! (SQLite `synchronous=NORMAL`, RocksDB WAL without a sync for each write).
//!
//! This type owns every acknowledgement rule. Drivers only register writers,
//! record commits, supply the sync barrier and settle each attempt here. The
//! rules follow `quint/durable_ack.qnt` (modules `durable_ack_impl` and
//! `durable_ack_impl_local`):
//!
//! - A Durable answer depends only on commits that a completed barrier covers.
//!   This includes attempts that wrote nothing, read-only attempts, and error
//!   answers that can reveal observed data (a failed condition with its item,
//!   transaction cancel reasons): a read never returns data that a crash can
//!   then remove.
//! - A Relaxed answer does not wait: a Relaxed writer leaves a hold for its
//!   ticket, and a Relaxed attempt that wrote nothing returns at once. Its
//!   caller accepts that a crash can remove what it observed (W2 D11). After a
//!   failed sync, a Relaxed observer fails as a Durable observer does.
//! - The queue uses Relaxed for receive claims, receipt operations, and the
//!   receive probe. Two consequences are accepted: a Relaxed answer can show a
//!   catalog change that is not on disk yet (for example `QueueDoesNotExist`
//!   after a `DeleteQueue` whose sync is in progress), and a Relaxed claim can
//!   deliver a send whose sync is in progress (a crash then gives one more
//!   delivery; at-least-once holds). `GetQueueAttributes` is Durable, so its
//!   catalog attributes never show an unsynced change; its message counts stay
//!   approximate.
//! - A writer registers before its commit can become visible and gets a commit
//!   ticket on the writer thread right after its native commit returns. A
//!   barrier captures the highest ticket immediately before its sync starts
//!   and, when it completes, releases every ticket at or below that capture.
//! - An attempt that wrote nothing checks, after its last read, whether a
//!   registration or hold exists, or whether another process committed since
//!   the last completed barrier. If every registration has a ticket and no
//!   barrier hold or external commit exists, it waits until a completed barrier
//!   covers the highest ticket; otherwise it waits for a barrier that starts
//!   after the check.
//! - A registration ends when the attempt's guard and the writer thread's
//!   recorder are both gone. A ticket that no completed barrier covers then
//!   becomes a hold (a Relaxed answer, an aborted task); a registration with no
//!   ticket and no known outcome becomes a hold that the next barrier to start
//!   releases. A new batch starts with such a hold, because a process restart
//!   keeps native commits that were never synced. A hold starts a barrier.
//! - An error after the native commit returned is a commit-unknown outcome.
//!   Every commit-unknown outcome waits before its registration ends.
//! - A failed sync stops the batch. A later successful sync does not prove that
//!   earlier data reached the disk, so nothing is acknowledged again, new
//!   attempts are refused, and `DurabilityHealth` reports the stop so that the
//!   process restarts. A barrier that did not run its sync is retried.

use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
    time::Duration,
};

use storage_types::{
    AttemptDurability, NativeUnavailable, StorageEnum, StorageError, StorageResult,
};
use tokio::sync::{Notify, watch};

use crate::constants::{DURABILITY_BARRIER_RETRY_MS, DURABILITY_BATCH_WINDOW_MS};

/// A boxed barrier step.
pub type BarrierStep = Pin<Box<dyn Future<Output = Result<BarrierCapture, BarrierFailure>> + Send>>;
/// A boxed native version read.
pub type VersionStep = Pin<Box<dyn Future<Output = StorageResult<u64>> + Send>>;

type Barrier = Arc<dyn Fn(BarrierStart) -> BarrierStep + Send + Sync>;
type VersionProbe = Arc<dyn Fn() -> VersionStep + Send + Sync>;

/// Why a barrier returned without a capture.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BarrierFailure {
    /// The native sync ran and failed. The batch stops.
    SyncFailed,
    /// No sync result is known (a worker stopped, a read before the sync
    /// failed). Nothing was acknowledged; the batch runs another barrier.
    Interrupted,
}

/// Shared acknowledgement state for one native store in one process.
pub struct DurabilityBatch {
    shared: Arc<Shared>,
}

struct Shared {
    barrier: Barrier,
    probe: Option<VersionProbe>,
    state: Mutex<State>,
    /// Wakes the sync task: new need, a recorded commit, or a writer ended.
    wake: Notify,
    progress: watch::Sender<Progress>,
    task_started: AtomicBool,
    /// The runtime that runs the sync task, so that a writer thread outside it
    /// can restart the task after a close.
    runtime: std::sync::OnceLock<tokio::runtime::Handle>,
}

#[derive(Clone, Copy, Debug, Default)]
struct Progress {
    durable: u64,
    finished: u64,
    stopped: bool,
}

#[derive(Debug, Default)]
struct State {
    /// Highest commit ticket recorded. Tickets follow native commit order.
    committed: u64,
    /// Highest ticket that a completed barrier captured.
    durable: u64,
    /// Barriers begun, and the index of the last one that completed.
    started: u64,
    finished: u64,
    /// Live registrations, those without a ticket, and those of them whose
    /// commit is still in progress (the bounded window waits for these).
    registered: usize,
    unticketed: usize,
    in_progress: usize,
    /// Tickets that no completed barrier covers and whose registration ended.
    ticket_holds: BTreeMap<u64, usize>,
    /// Holds released by a completed barrier with at least this index.
    barrier_holds: BTreeMap<u64, usize>,
    /// Highest ticket and barrier index that a waiter needs.
    ticket_need: u64,
    barrier_need: u64,
    /// Native commit version captured by the last completed barrier.
    native_capture: Option<u64>,
    stopped: bool,
    closed: bool,
    waits: u64,
}

impl State {
    fn holds(&self) -> usize {
        self.ticket_holds.values().sum::<usize>() + self.barrier_holds.values().sum::<usize>()
    }

    fn has_pending_commits(&self) -> bool {
        self.registered > 0 || self.holds() > 0
    }

    fn needs_barrier(&self) -> bool {
        !self.ticket_holds.is_empty()
            || !self.barrier_holds.is_empty()
            || self.ticket_need > self.durable
            || self.barrier_need > self.finished
    }

    fn progress(&self) -> Progress {
        Progress {
            durable: self.durable,
            finished: self.finished,
            stopped: self.stopped,
        }
    }

    fn add_barrier_hold(&mut self) {
        *self.barrier_holds.entry(self.started + 1).or_default() += 1;
    }
}

/// Counters for tests and diagnostics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DurabilityStats {
    /// Barriers whose sync began (`BarrierStart::begin`).
    pub barriers_started: u64,
    /// The index of the last barrier that completed without error.
    pub barriers_completed: u64,
    /// Settlements that waited for a barrier.
    pub waits: u64,
    /// Registrations and holds that keep attempts that wrote nothing waiting.
    pub pending: usize,
    pub stopped: bool,
}

/// Whether the batch stopped after a failed sync; cheap to clone and poll.
#[derive(Clone, Debug)]
pub struct DurabilityHealth(watch::Receiver<Progress>);

impl DurabilityHealth {
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.0.borrow().stopped
    }

    /// Completes when the batch stops. Never completes for a healthy batch.
    pub async fn stopped(mut self) {
        if self.0.wait_for(|progress| progress.stopped).await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

const REGISTRATION_OPEN: u8 = 0;
/// The outcome is settled: no hold is needed for an end without a ticket.
const REGISTRATION_SETTLED: u8 = 1;
/// A failed sync: the registration stays counted.
const REGISTRATION_RETAINED: u8 = 2;

/// One writer's registration. It ends when the attempt's guard and every
/// recorder are gone, so a writer thread that commits after its attempt
/// was dropped still records under a live registration.
struct Registration {
    shared: Arc<Shared>,
    ticket: AtomicU64,
    /// Counted out of `unticketed` and `in_progress` (once each).
    left_unticketed: AtomicBool,
    left_in_progress: AtomicBool,
    end: AtomicU8,
}

impl Registration {
    fn ticket(&self) -> Option<u64> {
        let ticket = self.ticket.load(Ordering::SeqCst);
        (ticket > 0).then_some(ticket)
    }

    fn leave_in_progress(&self, state: &mut State) {
        if !self.left_in_progress.swap(true, Ordering::SeqCst) {
            debug_assert!(state.in_progress > 0);
            state.in_progress = state.in_progress.saturating_sub(1);
        }
    }

    fn leave_unticketed(&self, state: &mut State) {
        self.leave_in_progress(state);
        if !self.left_unticketed.swap(true, Ordering::SeqCst) {
            debug_assert!(state.unticketed > 0);
            state.unticketed = state.unticketed.saturating_sub(1);
        }
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        let end = self.end.load(Ordering::SeqCst);
        if end == REGISTRATION_RETAINED {
            return;
        }
        let mut state = self.shared.lock();
        debug_assert!(state.registered > 0);
        state.registered = state.registered.saturating_sub(1);
        self.leave_unticketed(&mut state);
        match self.ticket() {
            Some(ticket) if ticket > state.durable => {
                *state.ticket_holds.entry(ticket).or_default() += 1;
            }
            Some(_) => {}
            None if end == REGISTRATION_SETTLED => {}
            // An attempt dropped with no known outcome: its commit may be
            // visible, so the next barrier to start must cover it.
            None => state.add_barrier_hold(),
        }
        drop(state);
        self.shared.ensure_task();
        self.shared.wake.notify_one();
    }
}

/// The attempt's side of one registration. Hand it to `settle`; dropping it
/// in any other way leaves a hold for whatever it may have committed.
pub struct PendingDurableWrite {
    registration: Arc<Registration>,
}

/// The writer thread's side of one registration.
#[derive(Clone)]
pub struct CommitRecorder {
    registration: Arc<Registration>,
}

/// Given to the barrier. The barrier calls `begin` immediately before its
/// native sync starts; the capture covers every commit recorded before it.
pub struct BarrierStart {
    shared: Arc<Shared>,
}

/// What a barrier captured when its sync began.
#[derive(Debug)]
pub struct BarrierCapture {
    index: u64,
    covered: u64,
    native_version: Option<u64>,
}

/// The native commit version a connection observed at the end of an
/// attempt, with that connection's memo of the last version known to be
/// covered by a completed barrier.
pub struct ObservedVersion {
    memo: NativeVersionMemo,
    version: u64,
}

/// Per-connection memo of a native commit version whose commits a completed
/// barrier covers. Versions are compared for equality on one connection only.
#[derive(Clone, Debug)]
pub struct NativeVersionMemo(Arc<AtomicU64>);

impl Default for NativeVersionMemo {
    fn default() -> Self {
        Self(Arc::new(AtomicU64::new(u64::MAX)))
    }
}

impl NativeVersionMemo {
    #[must_use]
    pub fn observe(&self, version: u64) -> ObservedVersion {
        ObservedVersion {
            memo: self.clone(),
            version,
        }
    }
}

impl ObservedVersion {
    fn is_covered(&self) -> bool {
        self.memo.0.load(Ordering::SeqCst) == self.version
    }

    fn mark_covered(&self) {
        self.memo.0.store(self.version, Ordering::SeqCst);
    }
}

/// How one attempt ends, given to `DurabilityBatch::settle`.
pub struct AttemptCompletion {
    pub durability: AttemptDurability,
    /// The registration, if the attempt wrote.
    pub write: Option<PendingDurableWrite>,
    /// The native commit version at the end of an attempt that committed
    /// nothing (an answer or a data-dependent error).
    pub observed: Option<ObservedVersion>,
}

impl AttemptCompletion {
    #[must_use]
    pub fn new(durability: AttemptDurability, write: Option<PendingDurableWrite>) -> Self {
        Self {
            durability,
            write,
            observed: None,
        }
    }

    #[must_use]
    pub fn with_observed(mut self, observed: Option<ObservedVersion>) -> Self {
        self.observed = observed;
        self
    }
}

/// Whether an error answer can carry data that the attempt observed (a
/// failed condition with its item, cancel reasons, validation against stored
/// state). Native, infrastructure, capacity and conflict errors and unknown
/// commits carry none. Unknown kinds count as revealing.
#[must_use]
pub fn error_reveals_observation(error: &StorageError) -> bool {
    !matches!(
        error.as_ref(),
        StorageEnum::Database(_)
            | StorageEnum::InfrastructureResult(_)
            | StorageEnum::Attempt(_)
            | StorageEnum::InternalServerError { .. }
            | StorageEnum::TransactionInProgress { .. }
            | StorageEnum::ProvisionedThroughputExceeded { .. }
            | StorageEnum::Throttled { .. }
            | StorageEnum::ServiceUnavailable { .. }
            | StorageEnum::LimitExceeded { .. }
            | StorageEnum::RequestLimitExceeded
    )
}

/// How an attempt that committed nothing waits.
enum ObserverWait {
    /// Until a completed barrier covers this ticket.
    Ticket(u64),
    /// Until a barrier with this index (started after the check) completes.
    Barrier(u64),
}

impl DurabilityBatch {
    /// `barrier` must call `BarrierStart::begin` immediately before its sync
    /// starts, and its sync must make every commit before that call durable.
    pub fn new<F, Fut>(barrier: F) -> Self
    where
        F: Fn(BarrierStart) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<BarrierCapture, BarrierFailure>> + Send + 'static,
    {
        Self::build(Arc::new(move |start| Box::pin(barrier(start))), None)
    }

    /// As `new`, for a store that other processes can commit to. `probe`
    /// reads the current native commit version; the barrier passes the
    /// version it read before `begin` to `begin`.
    pub fn with_version_probe<F, Fut, P, PFut>(barrier: F, probe: P) -> Self
    where
        F: Fn(BarrierStart) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<BarrierCapture, BarrierFailure>> + Send + 'static,
        P: Fn() -> PFut + Send + Sync + 'static,
        PFut: Future<Output = StorageResult<u64>> + Send + 'static,
    {
        Self::build(
            Arc::new(move |start| Box::pin(barrier(start))),
            Some(Arc::new(move || Box::pin(probe()))),
        )
    }

    fn build(barrier: Barrier, probe: Option<VersionProbe>) -> Self {
        let mut state = State::default();
        // Commits of an earlier process may not be on disk yet.
        state.add_barrier_hold();
        let shared = Arc::new(Shared {
            barrier,
            probe,
            state: Mutex::new(state),
            wake: Notify::new(),
            progress: watch::Sender::new(Progress::default()),
            task_started: AtomicBool::new(false),
            runtime: std::sync::OnceLock::new(),
        });
        shared.ensure_task();
        Self { shared }
    }

    /// Refuse a new attempt after a failed barrier stopped the store.
    pub fn check_available(&self) -> StorageResult<()> {
        if self.shared.lock().stopped {
            return Err(durability_stopped());
        }
        Ok(())
    }

    #[must_use]
    pub fn health(&self) -> DurabilityHealth {
        DurabilityHealth(self.shared.progress.subscribe())
    }

    /// Register a writer before its first write can commit. Give the guard to
    /// `settle`, and record its native commit with a `CommitRecorder`.
    pub fn begin_write(&self) -> PendingDurableWrite {
        let mut state = self.shared.lock();
        state.registered += 1;
        state.unticketed += 1;
        state.in_progress += 1;
        drop(state);
        PendingDurableWrite {
            registration: Arc::new(Registration {
                shared: Arc::clone(&self.shared),
                ticket: AtomicU64::new(0),
                left_unticketed: AtomicBool::new(false),
                left_in_progress: AtomicBool::new(false),
                end: AtomicU8::new(REGISTRATION_OPEN),
            }),
        }
    }

    #[must_use]
    pub fn stats(&self) -> DurabilityStats {
        let state = self.shared.lock();
        DurabilityStats {
            barriers_started: state.started,
            barriers_completed: state.finished,
            waits: state.waits,
            pending: state.registered + state.holds(),
            stopped: state.stopped,
        }
    }

    /// Stop accepting attempts; the sync task ends once nothing needs a
    /// barrier. Dropping the batch does the same.
    pub fn close(&self) {
        self.shared.lock().closed = true;
        self.shared.wake.notify_one();
    }

    /// Finish one attempt after its native commit, rollback, or failure.
    ///
    /// - A Durable writer waits until a completed barrier covers its ticket.
    /// - A Relaxed writer answers now; its commit keeps a hold.
    /// - An attempt that committed nothing and answers, or fails with an error
    ///   that can reveal observed data, waits as an observer.
    /// - A writer with an error after its commit returned, or with an unknown
    ///   outcome, waits before its registration ends and reports an unknown
    ///   outcome.
    pub async fn settle<R>(
        &self,
        result: StorageResult<R>,
        completion: AttemptCompletion,
    ) -> StorageResult<R> {
        self.shared.ensure_task();
        let AttemptCompletion {
            durability,
            write,
            observed,
        } = completion;
        match (result, write) {
            (result, Some(write)) => {
                self.settle_writer(result, write, durability, observed)
                    .await
            }
            // A Relaxed answer may depend on commits that no barrier covers
            // yet; that is what Relaxed means. An attempt that wrote nothing
            // has nothing of its own to cover, but a stopped batch answers
            // nothing that it read, as on the other paths.
            (result, None) if durability == AttemptDurability::Relaxed => {
                self.settle_relaxed_observer(result)
            }
            (Ok(value), None) => self.settle_observer(observed).await.map(|()| value),
            (Err(error), None) => self.settle_error(error, observed).await,
        }
    }

    fn settle_relaxed_observer<R>(&self, result: StorageResult<R>) -> StorageResult<R> {
        let reveals = result
            .as_ref()
            .map_or_else(error_reveals_observation, |_| true);
        if reveals && self.shared.lock().stopped {
            return Err(durability_stopped());
        }
        result
    }

    async fn settle_error<R>(
        &self,
        error: StorageError,
        observed: Option<ObservedVersion>,
    ) -> StorageResult<R> {
        if error_reveals_observation(&error) {
            self.settle_observer(observed).await?;
        }
        Err(error)
    }

    async fn settle_writer<R>(
        &self,
        result: StorageResult<R>,
        write: PendingDurableWrite,
        durability: AttemptDurability,
        observed: Option<ObservedVersion>,
    ) -> StorageResult<R> {
        let registration = &write.registration;
        match (result, registration.ticket()) {
            (Ok(value), Some(_)) if durability == AttemptDurability::Relaxed => {
                if self.shared.lock().stopped {
                    write.retain();
                    return Err(unknown_durability());
                }
                // The registration ends as a hold for the uncovered ticket.
                drop(write);
                Ok(value)
            }
            (Ok(value), Some(ticket)) => {
                self.finish_wait(write, self.shared.wait_ticket(ticket).await)?;
                Ok(value)
            }
            // A registered writer whose commit was not recorded: its commit
            // may be visible, so it waits like an unknown outcome.
            (Ok(value), None) => {
                let target = self.shared.leave_in_progress(registration);
                self.finish_wait(write, self.shared.wait_barrier(target).await)?;
                Ok(value)
            }
            (Err(error), Some(ticket)) => {
                let error = if error.is_commit_unknown() {
                    error
                } else {
                    StorageError::commit_unknown(
                        "The native commit applied, but the attempt failed afterwards",
                    )
                };
                let _ = self.finish_wait(write, self.shared.wait_ticket(ticket).await);
                Err(error)
            }
            (Err(error), None) if error.is_commit_unknown() => {
                let target = self.shared.leave_in_progress(registration);
                let _ = self.finish_wait(write, self.shared.wait_barrier(target).await);
                Err(error)
            }
            // A known rollback. Its answer may still reveal what it read.
            (Err(error), None) => {
                write.settle();
                self.settle_error(error, observed).await
            }
        }
    }

    /// Ends the registration after a wait. After a failed barrier the writer
    /// keeps its registration: its commit may not be on disk.
    fn finish_wait(
        &self,
        write: PendingDurableWrite,
        waited: Result<(), Stopped>,
    ) -> StorageResult<()> {
        match waited {
            Ok(()) => {
                write.settle();
                Ok(())
            }
            Err(Stopped) => {
                write.retain();
                Err(unknown_durability())
            }
        }
    }

    async fn settle_observer(&self, observed: Option<ObservedVersion>) -> StorageResult<()> {
        let check_version = {
            let state = self.shared.lock();
            if state.stopped {
                return Err(durability_stopped());
            }
            self.shared.probe.is_some()
                && !observed.as_ref().is_some_and(ObservedVersion::is_covered)
                && !(state.has_pending_commits() && !state.fast_wait_allowed())
        };
        let external = match (&self.shared.probe, check_version) {
            (Some(probe), true) => Some(probe().await),
            _ => None,
        };
        let wait = {
            let state = self.shared.lock();
            let external = match external {
                None => false,
                Some(Ok(version)) => state.native_capture != Some(version),
                Some(Err(_)) => true,
            };
            if external || (state.has_pending_commits() && !state.fast_wait_allowed()) {
                Some(ObserverWait::Barrier(state.started + 1))
            } else if state.has_pending_commits() {
                Some(ObserverWait::Ticket(state.committed))
            } else {
                None
            }
        };
        let waited = match wait {
            None => Ok(()),
            Some(ObserverWait::Ticket(ticket)) => self.shared.wait_ticket(ticket).await,
            Some(ObserverWait::Barrier(target)) => self.shared.wait_barrier(target).await,
        };
        waited.map_err(|Stopped| durability_stopped())?;
        if let Some(observed) = observed {
            observed.mark_covered();
        }
        Ok(())
    }
}

impl State {
    /// Every commit this process made visible has a ticket, and nothing
    /// needs a barrier that starts later: the highest ticket covers it all.
    fn fast_wait_allowed(&self) -> bool {
        self.unticketed == 0 && self.barrier_holds.is_empty()
    }
}

impl Drop for DurabilityBatch {
    fn drop(&mut self) {
        self.close();
    }
}

impl PendingDurableWrite {
    /// The handle the writer thread uses to record the native commit.
    #[must_use]
    pub fn recorder(&self) -> CommitRecorder {
        CommitRecorder {
            registration: Arc::clone(&self.registration),
        }
    }

    /// Record the native commit right after it returned success.
    pub fn record_commit(&self) {
        self.recorder().record();
    }

    fn settle(self) {
        let _ = self.registration.end.compare_exchange(
            REGISTRATION_OPEN,
            REGISTRATION_SETTLED,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }

    fn retain(self) {
        self.registration
            .end
            .store(REGISTRATION_RETAINED, Ordering::SeqCst);
    }
}

impl CommitRecorder {
    /// Assign the next ticket in commit order. Call on the writer thread
    /// right after the native commit returned success, never before.
    pub fn record(&self) {
        let registration = &self.registration;
        let mut state = registration.shared.lock();
        if registration.ticket.load(Ordering::SeqCst) == 0 {
            state.committed += 1;
            registration.ticket.store(state.committed, Ordering::SeqCst);
            registration.leave_unticketed(&mut state);
        }
        drop(state);
        registration.shared.wake.notify_one();
    }
}

impl BarrierStart {
    /// Call immediately before the native sync starts. `native_version` is a
    /// native commit version read before this call, for a store that other
    /// processes can commit to.
    #[must_use]
    pub fn begin(self, native_version: Option<u64>) -> BarrierCapture {
        let mut state = self.shared.lock();
        state.started += 1;
        BarrierCapture {
            index: state.started,
            covered: state.committed,
            native_version,
        }
    }
}

#[derive(Debug)]
struct Stopped;

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn ensure_task(self: &Arc<Self>) {
        if self.task_started.load(Ordering::Acquire) {
            return;
        }
        let runtime = match tokio::runtime::Handle::try_current() {
            Ok(current) => self.runtime.get_or_init(|| current).clone(),
            Err(_) => match self.runtime.get() {
                Some(runtime) => runtime.clone(),
                None => return,
            },
        };
        if self
            .task_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            runtime.spawn(Self::synchronize(Arc::clone(self)));
        }
    }

    /// An unknown outcome is no longer in progress: the bounded window does
    /// not wait for it. Returns the first barrier that starts after this call.
    fn leave_in_progress(&self, registration: &Registration) -> u64 {
        let mut state = self.lock();
        registration.leave_in_progress(&mut state);
        let target = state.started + 1;
        drop(state);
        self.wake.notify_one();
        target
    }

    async fn wait_ticket(self: &Arc<Self>, ticket: u64) -> Result<(), Stopped> {
        {
            let mut state = self.lock();
            if state.durable >= ticket {
                return Ok(());
            }
            if state.stopped {
                return Err(Stopped);
            }
            state.ticket_need = state.ticket_need.max(ticket);
            state.waits += 1;
        }
        self.ensure_task();
        self.wake.notify_one();
        self.wait_progress(|progress| progress.durable >= ticket)
            .await
    }

    async fn wait_barrier(self: &Arc<Self>, target: u64) -> Result<(), Stopped> {
        {
            let mut state = self.lock();
            if state.finished >= target {
                return Ok(());
            }
            if state.stopped {
                return Err(Stopped);
            }
            state.barrier_need = state.barrier_need.max(target);
            state.waits += 1;
        }
        self.ensure_task();
        self.wake.notify_one();
        self.wait_progress(|progress| progress.finished >= target)
            .await
    }

    async fn wait_progress(&self, done: impl Fn(&Progress) -> bool) -> Result<(), Stopped> {
        let mut progress = self.progress.subscribe();
        let reached = progress
            .wait_for(|progress| done(progress) || progress.stopped)
            .await
            .map(|progress| done(&progress))
            .unwrap_or(false);
        if reached { Ok(()) } else { Err(Stopped) }
    }

    /// One sync task per batch. Barriers run one at a time; a barrier in
    /// progress is the collection window for commits that arrive during it.
    async fn synchronize(shared: Arc<Self>) {
        let window = Duration::from_millis(DURABILITY_BATCH_WINDOW_MS);
        loop {
            loop {
                {
                    let state = shared.lock();
                    if state.stopped {
                        return;
                    }
                    if state.needs_barrier() {
                        break;
                    }
                    if state.closed {
                        // A later need (an attempt still in flight, a hold)
                        // starts the task again through `ensure_task`.
                        shared.task_started.store(false, Ordering::Release);
                        return;
                    }
                }
                shared.wake.notified().await;
            }
            // Other write attempts are between their first write and their
            // commit: wait for them or for the window, whichever is first.
            let deadline = tokio::time::Instant::now() + window;
            while shared.lock().in_progress > 0 {
                if tokio::time::timeout_at(deadline, shared.wake.notified())
                    .await
                    .is_err()
                {
                    break;
                }
            }
            let start = BarrierStart {
                shared: Arc::clone(&shared),
            };
            match (shared.barrier)(start).await {
                Ok(capture) => shared.complete(capture),
                Err(BarrierFailure::Interrupted) => {
                    tokio::time::sleep(Duration::from_millis(DURABILITY_BARRIER_RETRY_MS)).await;
                }
                Err(BarrierFailure::SyncFailed) => {
                    shared.fail();
                    return;
                }
            }
        }
    }

    fn complete(&self, capture: BarrierCapture) {
        let mut state = self.lock();
        state.durable = state.durable.max(capture.covered);
        state.finished = state.finished.max(capture.index);
        if capture.native_version.is_some() {
            state.native_capture = capture.native_version;
        }
        state.ticket_holds = state.ticket_holds.split_off(&(capture.covered + 1));
        state.barrier_holds = state.barrier_holds.split_off(&(capture.index + 1));
        let progress = state.progress();
        self.progress.send_replace(progress);
    }

    fn fail(&self) {
        let mut state = self.lock();
        state.stopped = true;
        let progress = state.progress();
        self.progress.send_replace(progress);
    }
}

fn unknown_durability() -> StorageError {
    StorageError::commit_unknown("Native durability acknowledgement is unknown")
}

/// The store stopped after a failed sync; nothing was committed by this
/// attempt that a client can rely on.
fn durability_stopped() -> StorageError {
    storage_types::AttemptFailure::Unavailable(NativeUnavailable::DurabilityFailed).into()
}
