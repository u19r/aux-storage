//! The typed lifecycle of one native attempt: when it can still end without
//! a commit, and when its commit outcome becomes unknown. Also the shared
//! waits toward the settle bound of a commit.

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
};

use futures::future::{Either, select};
use storage_types::{
    AttemptDeadline, AttemptPhase, Clock, StorageError, StorageResult, expired_in,
};

use crate::{
    native_observer::NativeObserver,
    native_outcome::{NativeCommitError, classify_commit},
    native_supervision::finish_or_detach,
};

/// Where one native attempt stands. It only moves forward:
/// `Queued → Open → CommitSubmitted → Committed | Aborted`, or from `Queued`
/// or `Open` to `Abandoned`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum LifecycleState {
    /// Created by the supervisor; no native transaction exists yet.
    Queued = 0,
    /// The driver began native work; it can still end without a commit.
    Open = 1,
    /// The native call that carries COMMIT was issued and has not answered.
    CommitSubmitted = 2,
    /// The native COMMIT answered success.
    Committed = 3,
    /// The native COMMIT answered with a known failure.
    Aborted = 4,
    /// A timer or a stopped task ended the attempt before its commit; no
    /// commit can follow.
    Abandoned = 5,
}

impl LifecycleState {
    const fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Queued,
            1 => Self::Open,
            2 => Self::CommitSubmitted,
            3 => Self::Committed,
            4 => Self::Aborted,
            _ => Self::Abandoned,
        }
    }
}

/// One per native attempt. The supervisor creates it and lends it to the
/// driver; worker threads and deadline timers share it.
pub struct AttemptLifecycle {
    state: AtomicU8,
    /// `None` in production. Set once at creation, never changed.
    observer: Option<Arc<dyn NativeObserver>>,
}

impl std::fmt::Debug for AttemptLifecycle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AttemptLifecycle")
            .field("state", &self.state())
            .field("observed", &self.observer.is_some())
            .finish()
    }
}

impl Default for AttemptLifecycle {
    fn default() -> Self {
        Self::new()
    }
}

impl AttemptLifecycle {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            state: AtomicU8::new(LifecycleState::Queued as u8),
            observer: None,
        }
    }

    /// A lifecycle whose commit boundaries `observer` sees.
    #[must_use]
    pub fn observed(observer: Option<Arc<dyn NativeObserver>>) -> Self {
        Self {
            state: AtomicU8::new(LifecycleState::Queued as u8),
            observer,
        }
    }

    /// The observer of this attempt; `None` in production.
    #[inline]
    #[must_use]
    pub fn observer(&self) -> Option<&dyn NativeObserver> {
        self.observer.as_deref()
    }

    /// An owned handle to the observer, for a boundary that runs in a
    /// `'static` future (the SQLite and RocksDB workers). `None` in
    /// production: one test, no atomic.
    #[must_use]
    pub fn observer_handle(&self) -> Option<Arc<dyn NativeObserver>> {
        self.observer.clone()
    }

    #[must_use]
    pub fn state(&self) -> LifecycleState {
        LifecycleState::from_u8(self.state.load(Ordering::Acquire))
    }

    /// `Queued → Open`, before the driver begins native work. False when the
    /// attempt was abandoned first: the driver then opens no transaction.
    #[must_use]
    pub fn open(&self) -> bool {
        self.state
            .compare_exchange(
                LifecycleState::Queued as u8,
                LifecycleState::Open as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// `Queued | Open → Abandoned`. Returns the state it found: `Queued` or
    /// `Open` when this call abandoned the attempt; any other state is left
    /// unchanged and returned.
    pub fn abandon(&self) -> LifecycleState {
        let mut current = self.state.load(Ordering::Acquire);
        loop {
            let state = LifecycleState::from_u8(current);
            if !matches!(state, LifecycleState::Queued | LifecycleState::Open) {
                return state;
            }
            match self.state.compare_exchange_weak(
                current,
                LifecycleState::Abandoned as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return state,
                Err(actual) => current = actual,
            }
        }
    }

    /// The one commit-submission point of every driver. A write attempt calls
    /// it on the thread that issues the native COMMIT, directly before that
    /// call. Past the deadline, or once abandoned, it refuses with
    /// `Deadline(Settle)`: a known abort, and the driver must roll back.
    pub fn submit_commit(&self, deadline: &AttemptDeadline) -> StorageResult<CommitTicket<'_>> {
        deadline.require_remaining(AttemptPhase::Settle)?;
        match self.state.compare_exchange(
            LifecycleState::Open as u8,
            LifecycleState::CommitSubmitted as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Ok(CommitTicket { lifecycle: self }),
            Err(state) if state == LifecycleState::Abandoned as u8 => {
                Err(expired_in(AttemptPhase::Settle))
            }
            Err(_) => Err(StorageError::internal(
                "native commit submitted outside an open attempt",
            )),
        }
    }

    /// COMMIT was issued and its answer has not been recorded.
    #[must_use]
    pub fn awaits_answer(&self) -> bool {
        self.state() == LifecycleState::CommitSubmitted
    }

    /// The result of an attempt whose task stopped before it returned. It
    /// abandons the attempt first, so a worker that still holds the native
    /// transaction cannot commit afterwards. ARCH-1-c types this result.
    #[must_use]
    pub fn stopped_error(&self, message: &'static str) -> StorageError {
        match self.abandon() {
            LifecycleState::Queued
            | LifecycleState::Open
            | LifecycleState::Abandoned
            | LifecycleState::Aborted => StorageError::internal(message),
            // A commit that answered success but whose value was lost is
            // reported unknown: never known, never a failure.
            LifecycleState::CommitSubmitted | LifecycleState::Committed => {
                StorageError::commit_unknown(message)
            }
        }
    }
}

/// Proof that this attempt's COMMIT was issued. Dropping it without `settle`
/// leaves the attempt `CommitSubmitted`: its outcome is unknown.
#[must_use = "an unsettled commit ticket leaves the outcome unknown"]
pub struct CommitTicket<'lifecycle> {
    lifecycle: &'lifecycle AttemptLifecycle,
}

impl CommitTicket<'_> {
    /// The native call that carried COMMIT answered `result`. A failure is
    /// classified by `classify_commit(true, ..)`: an unknown outcome leaves
    /// the attempt `CommitSubmitted`; `Ok` records `Committed`; any other
    /// error records `Aborted`.
    pub fn settle<T>(self, result: Result<T, NativeCommitError>) -> StorageResult<T> {
        let result = result.map_err(|error| classify_commit(true, error));
        let state = match &result {
            Ok(_) => LifecycleState::Committed,
            Err(error) if error.is_commit_unknown() => return result,
            Err(_) => LifecycleState::Aborted,
        };
        // Only the ticket moves the lifecycle out of `CommitSubmitted`.
        self.lifecycle.state.store(state as u8, Ordering::Release);
        result
    }
}

/// What a settle wait that reached `settle_by` reports.
#[derive(Clone, Copy, Debug)]
pub enum LateSettle {
    /// The work may have committed: the outcome is unknown.
    Unknown(&'static str),
    /// Nothing was written: a known `Deadline(Settle)`.
    Expired,
}

impl LateSettle {
    #[must_use]
    pub const fn of_write(wrote: bool, message: &'static str) -> Self {
        if wrote {
            Self::Unknown(message)
        } else {
            Self::Expired
        }
    }

    /// ARCH-1-c types this result.
    #[must_use]
    pub fn error(self) -> StorageError {
        match self {
            Self::Unknown(message) => StorageError::commit_unknown(message),
            Self::Expired => expired_in(AttemptPhase::Settle),
        }
    }
}

/// Waits for `work` until the deadline's `settle_by`. Work that has
/// completed wins over the timer, also at the bound. Past the bound `work`
/// runs on detached, keeping what it owns until it ends, and the caller gets
/// `late`'s error.
pub async fn await_settle<R: Send + 'static>(
    clock: &dyn Clock,
    deadline: AttemptDeadline,
    work: impl Future<Output = StorageResult<R>> + Send + 'static,
    late: LateSettle,
) -> StorageResult<R> {
    match finish_or_detach(deadline.settle().expired(clock), work).await {
        Some(result) => result,
        None => Err(late.error()),
    }
}

/// Waits for `work` until the deadline's `settle_by` and drops it past the
/// bound. Work that has completed wins over the timer, also at the bound.
/// For work that needs no owner once submitted (a FoundationDB commit) or
/// that borrows the attempt (Postgres).
pub async fn settle_or_drop<F: Future>(
    clock: &dyn Clock,
    deadline: AttemptDeadline,
    work: F,
) -> Option<F::Output> {
    match select(
        std::pin::pin!(work),
        std::pin::pin!(deadline.settle().expired(clock)),
    )
    .await
    {
        Either::Left((value, _)) => Some(value),
        Either::Right(_) => None,
    }
}
