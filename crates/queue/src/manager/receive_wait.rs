//! Waiting receives share one observation, one probe schedule, and one claim
//! gate per queue in this process.
//!
//! What a process knows about a queue is a hint: another node can send,
//! purge, delete, or change attributes at any time. A queue with waiters is
//! therefore probed again one interval after the latest observation completed,
//! whatever that observation said, and shared due evidence older than one
//! interval opens a probe, not a write claim. A request that ran a probe or
//! claim itself uses its result whatever its age. The rules follow
//! `quint/queue_wake.qnt` (configuration `probeFixed`, with the claim's next
//! due time). A local commit advances the queue's generation through a `watch`
//! channel. A waiter subscribes before it reads the observation, so a commit
//! after that read always marks its receiver changed and cannot be missed.

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use queue_provider::{QueueWakeBoard, QueueWakeInterest, QueueWakeMark, QueueWakeSlot};
use tokio::{sync::watch, time::Instant};

use crate::constants::{RECEIVE_WAIT_RETENTION, RECEIVE_WAIT_SWEEP_FLOOR, RECEIVE_WAKE_SPACING};

/// What this process knows about the due messages of one queue.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct QueueObservation {
    /// Advanced by each local commit that can make a message due.
    pub(super) generation: u64,
    /// Advanced by each local change of the queue's identity or defaults.
    /// Probes and claims that started in an older epoch are not recorded.
    pub(super) epoch: u64,
    /// Start sequence of the latest recorded probe or claim. Records are
    /// ordered by this number, not by their start instants, which two
    /// observations can share (decision D17).
    sequence: u64,
    /// Start of the latest recorded probe or claim; anchors its due times.
    started_at: Option<Instant>,
    /// Completion of the latest recorded probe or claim. The observation is
    /// fresh for one probe interval after it.
    completed_at: Option<Instant>,
    /// Earliest known due time; `None` when no message is known.
    next_due: Option<Instant>,
    /// When the evidence for `next_due` was taken (a probe or claim start, or
    /// a local commit). Shared evidence older than one probe interval starts
    /// a probe, not a claim (decision D17); the request that took the
    /// evidence uses it whatever its age.
    evidence_at: Option<Instant>,
    /// The latest claim took messages and left more due, so claims may run
    /// in parallel (decision D15).
    deep: bool,
    /// The latest observation found no queue.
    gone: bool,
    /// The queue's `ReceiveMessageWaitTimeSeconds` from the latest observation.
    wait_time_seconds: Option<u32>,
    /// The slot mark when the recorded probe or claim started; `None`
    /// without a board.
    wake: Option<QueueWakeMark>,
}

/// What one probe or claim observed.
pub(super) enum Observed {
    Probe {
        next_due: Option<Instant>,
        wait_time_seconds: u32,
    },
    Claim {
        /// The claim returned at least one message.
        took: bool,
        /// No due message that the claim could see remains.
        drained: bool,
        /// The earliest due time that the claim left, when drained.
        next_due: Option<Instant>,
        /// Pause before the next claim when the claim was empty but not
        /// drained (a lost conflict, a skipped lock).
        backoff: Duration,
        wait_time_seconds: u32,
    },
    Gone,
}

/// The generation, epoch, and sequence a probe or claim started in.
#[derive(Clone, Copy)]
pub(super) struct ObservationStart {
    generation: u64,
    epoch: u64,
    sequence: u64,
    started: Instant,
    mark: Option<QueueWakeMark>,
}

impl QueueObservation {
    /// Valid without a time limit: covered when it started and the slot has
    /// not changed or lost coverage since.
    fn is_covered(&self, mark: Option<QueueWakeMark>) -> bool {
        self.completed_at.is_some()
            && matches!((self.wake, mark), (Some(seen), Some(mark)) if seen == mark && mark.covered)
    }

    pub(super) fn is_fresh(
        &self,
        now: Instant,
        interval: Duration,
        mark: Option<QueueWakeMark>,
    ) -> bool {
        self.is_covered(mark)
            || self
                .completed_at
                .is_some_and(|completed| now < completed + self.fresh_for(interval, mark))
    }

    /// After the slot of an observation that is covered now, or was when it
    /// started, changed: re-probe no sooner than `RECEIVE_WAKE_SPACING` after
    /// it (hot-slot bound), but not later either, whatever the probe interval.
    /// An observation taken before coverage began is therefore replaced by a
    /// covered one as soon as the slot is covered.
    fn fresh_for(&self, interval: Duration, mark: Option<QueueWakeMark>) -> Duration {
        match (self.wake, mark) {
            (Some(seen), Some(now)) if seen.covered || now.covered => {
                interval.min(RECEIVE_WAKE_SPACING)
            }
            _ => interval,
        }
    }

    fn has_fresh_evidence(&self, now: Instant, interval: Duration) -> bool {
        self.evidence_at
            .is_some_and(|evidence| now < evidence + interval)
    }

    fn is_due_known(&self, now: Instant) -> bool {
        !self.gone && self.next_due.is_some_and(|due| due <= now)
    }

    /// The evidence is at least as new as the latest probe or claim that the
    /// asking request ran itself (`own` is that work's start). A request uses
    /// the result of its own work whatever its age: the age limit applies only
    /// to evidence that other requests of this process shared as a hint, so a
    /// slow store cannot make every receive empty.
    fn has_own_evidence(&self, own: Option<Instant>) -> bool {
        own.is_some_and(|own| self.evidence_at.is_some_and(|evidence| evidence >= own))
    }

    /// A message is due by evidence younger than one probe interval, or by
    /// the evidence of the asking request's own probe or claim.
    pub(super) fn is_due(&self, now: Instant, interval: Duration, own: Option<Instant>) -> bool {
        self.is_due_known(now)
            && (self.has_fresh_evidence(now, interval) || self.has_own_evidence(own))
    }

    /// The observation is old, or it says that a message is due on shared
    /// evidence that is too old to open a write claim: probe again, never
    /// answer empty on it.
    pub(super) fn needs_probe(
        &self,
        now: Instant,
        interval: Duration,
        mark: Option<QueueWakeMark>,
    ) -> bool {
        !self.is_fresh(now, interval, mark)
            || (self.is_due_known(now) && !self.has_fresh_evidence(now, interval))
    }

    /// Due, and the latest claim left more due messages; claims may run in
    /// parallel.
    pub(super) fn is_deep(&self, now: Instant, interval: Duration, own: Option<Instant>) -> bool {
        self.deep && self.is_due(now, interval, own)
    }

    pub(super) fn is_gone(
        &self,
        now: Instant,
        interval: Duration,
        mark: Option<QueueWakeMark>,
    ) -> bool {
        self.gone && self.is_fresh(now, interval, mark)
    }

    /// The queue's wait default when a fresh observation knows it.
    pub(super) fn wait_time_seconds(
        &self,
        now: Instant,
        interval: Duration,
        mark: Option<QueueWakeMark>,
    ) -> Option<u32> {
        self.is_fresh(now, interval, mark)
            .then_some(self.wait_time_seconds)
            .flatten()
    }

    /// The earliest of the deadline, the next probe, and the next due time.
    pub(super) fn wake_at(
        &self,
        deadline: Instant,
        interval: Duration,
        mark: Option<QueueWakeMark>,
    ) -> Instant {
        let mut wake = deadline;
        if !self.is_covered(mark)
            && let Some(completed) = self.completed_at
        {
            wake = wake.min(completed + self.fresh_for(interval, mark));
        }
        if let Some(due) = self.next_due {
            wake = wake.min(due);
        }
        wake
    }
}

pub(super) struct QueueWaitState {
    observation: watch::Sender<QueueObservation>,
    /// Start sequence numbers of the probes and claims of this queue.
    sequence: AtomicU64,
    /// One waiter probes; the others reuse its observation.
    probe: tokio::sync::Mutex<()>,
    /// Outside a deep queue, one claim at a time; the others reuse its
    /// observation instead of opening empty write attempts.
    claim: tokio::sync::Mutex<()>,
    wake: Option<(Arc<QueueWakeBoard>, QueueWakeSlot)>,
}

impl QueueWaitState {
    #[cfg(test)]
    pub(super) fn new_for_test() -> Self {
        Self::new(None)
    }

    fn new(wake: Option<(Arc<QueueWakeBoard>, QueueWakeSlot)>) -> Self {
        Self {
            wake,
            observation: watch::Sender::new(QueueObservation::default()),
            sequence: AtomicU64::new(0),
            probe: tokio::sync::Mutex::new(()),
            claim: tokio::sync::Mutex::new(()),
        }
    }

    pub(super) fn wake_mark(&self) -> Option<QueueWakeMark> {
        self.wake.as_ref().map(|(board, slot)| board.mark(*slot))
    }

    /// Subscribe before the first read of the mark, like `subscribe`.
    pub(super) fn subscribe_wake(&self) -> Option<watch::Receiver<QueueWakeMark>> {
        self.wake
            .as_ref()
            .map(|(board, slot)| board.subscribe(*slot))
    }

    /// Only a receive that waits registers (no arming for short polls).
    pub(super) fn register_wake(&self) -> Option<QueueWakeInterest> {
        self.wake
            .as_ref()
            .map(|(board, slot)| board.register(*slot))
    }

    /// Subscribe before reading the observation that a wait decision uses.
    pub(super) fn subscribe(&self) -> watch::Receiver<QueueObservation> {
        self.observation.subscribe()
    }

    pub(super) fn current(&self) -> QueueObservation {
        *self.observation.borrow()
    }

    pub(super) fn probe_lock(&self) -> &tokio::sync::Mutex<()> {
        &self.probe
    }

    pub(super) fn claim_lock(&self) -> &tokio::sync::Mutex<()> {
        &self.claim
    }

    /// Capture the generation, epoch, and sequence before a probe or claim
    /// starts.
    pub(super) fn start(&self) -> ObservationStart {
        let mark = self.wake_mark();
        let current = self.current();
        ObservationStart {
            generation: current.generation,
            epoch: current.epoch,
            sequence: self.sequence.fetch_add(1, Ordering::AcqRel) + 1,
            started: instant_now(),
            mark,
        }
    }

    pub(super) fn started_at(start: ObservationStart) -> Instant {
        start.started
    }

    /// Record a probe or claim. A newer record replaces the observation; an
    /// older one can only lower the next due time. A local commit during the
    /// work keeps the due time that it signalled.
    pub(super) fn record(&self, start: ObservationStart, observed: Observed) {
        let completed = instant_now();
        self.observation.send_if_modified(|observation| {
            if observation.epoch != start.epoch {
                return false;
            }
            if observation.sequence > start.sequence {
                let due = match observed {
                    Observed::Probe { next_due, .. }
                    | Observed::Claim {
                        drained: true,
                        next_due,
                        ..
                    } => next_due,
                    Observed::Claim { took: true, .. } => Some(start.started),
                    Observed::Claim { backoff, .. } => Some(completed + backoff),
                    Observed::Gone => None,
                };
                let Some(due) =
                    due.filter(|due| observation.next_due.is_none_or(|known| *due < known))
                else {
                    return false;
                };
                observation.next_due = Some(due);
                observation.evidence_at = observation.evidence_at.max(Some(start.started));
                return true;
            }
            let commits_during = observation.generation != start.generation;
            // Due times learned after this work started are still pending.
            let later_known = observation
                .next_due
                .filter(|due| commits_during && *due > start.started);
            let (next_due, deep, gone, wait_time_seconds) = match observed {
                Observed::Probe {
                    next_due,
                    wait_time_seconds,
                } => (
                    earliest(next_due, later_known),
                    false,
                    false,
                    Some(wait_time_seconds),
                ),
                Observed::Claim {
                    took,
                    drained,
                    next_due,
                    backoff,
                    wait_time_seconds,
                } => {
                    let next_due = match (drained, took) {
                        (true, _) => earliest(next_due, later_known),
                        (false, true) => Some(start.started),
                        (false, false) => Some(completed + backoff),
                    };
                    (next_due, !drained && took, false, Some(wait_time_seconds))
                }
                Observed::Gone => (None, false, true, observation.wait_time_seconds),
            };
            let became_due = next_due.is_some() && next_due != observation.next_due;
            let became_gone = gone && !observation.gone;
            let evidence_at = if commits_during {
                observation.evidence_at.max(Some(start.started))
            } else {
                Some(start.started)
            };
            observation.sequence = start.sequence;
            observation.wake = start.mark;
            observation.started_at = Some(start.started);
            observation.completed_at = Some(completed);
            observation.next_due = next_due;
            observation.evidence_at = evidence_at;
            observation.deep = deep;
            observation.gone = gone;
            observation.wait_time_seconds = wait_time_seconds;
            became_due || became_gone
        });
    }

    fn signal(&self, due: Instant) {
        let now = instant_now();
        self.observation.send_modify(|observation| {
            observation.generation = observation.generation.wrapping_add(1);
            observation.gone = false;
            observation.next_due = earliest(observation.next_due, Some(due));
            observation.evidence_at = Some(now);
        });
    }

    fn forget(&self) {
        self.observation.send_modify(|observation| {
            *observation = QueueObservation {
                generation: observation.generation.wrapping_add(1),
                epoch: observation.epoch.wrapping_add(1),
                sequence: observation.sequence,
                ..QueueObservation::default()
            };
        });
    }

    fn is_retained(&self, now: Instant) -> bool {
        self.current()
            .completed_at
            .is_some_and(|completed| now < completed + RECEIVE_WAIT_RETENTION)
    }
}

/// Per-process wait states and the bound on waiting receives.
pub(crate) struct ReceiveWaiters {
    queues: Mutex<WaitStates>,
    waiting: AtomicUsize,
    max_waiting: usize,
    probe_interval: Duration,
    wake: Option<Arc<QueueWakeBoard>>,
}

/// Keyed by queue name, so every URL form of one queue shares one state.
struct WaitStates {
    by_name: HashMap<String, Arc<QueueWaitState>>,
    sweep_at: usize,
}

impl ReceiveWaiters {
    pub(crate) fn new(
        max_waiting: usize,
        probe_interval: Duration,
        wake: Option<Arc<QueueWakeBoard>>,
    ) -> Self {
        Self {
            queues: Mutex::new(WaitStates {
                by_name: HashMap::new(),
                sweep_at: RECEIVE_WAIT_SWEEP_FLOOR,
            }),
            waiting: AtomicUsize::new(0),
            max_waiting,
            probe_interval,
            wake,
        }
    }

    pub(super) fn wake_board(&self) -> Option<Arc<QueueWakeBoard>> {
        self.wake.clone()
    }

    pub(super) const fn probe_interval(&self) -> Duration {
        self.probe_interval
    }

    pub(super) fn max_waiting(&self) -> usize {
        self.max_waiting
    }

    pub(super) fn waiting(&self) -> usize {
        self.waiting.load(Ordering::Acquire)
    }

    pub(super) fn queue(&self, queue_url: &str) -> Arc<QueueWaitState> {
        let name = queue_provider::queue_name_of(queue_url);
        let mut queues = self.lock();
        if let Some(state) = queues.by_name.get(name) {
            return Arc::clone(state);
        }
        if queues.by_name.len() >= queues.sweep_at {
            let now = instant_now();
            queues
                .by_name
                .retain(|_, state| Arc::strong_count(state) > 1 || state.is_retained(now));
            queues.sweep_at = RECEIVE_WAIT_SWEEP_FLOOR.max(queues.by_name.len() * 2);
        }
        let state = Arc::new(QueueWaitState::new(
            self.wake
                .as_ref()
                .map(|board| (Arc::clone(board), QueueWakeSlot::of_queue(name))),
        ));
        queues.by_name.insert(name.to_owned(), Arc::clone(&state));
        state
    }

    /// A local commit can make a message of this queue due after `delay`.
    pub(super) fn signal(&self, queue_url: &str, delay: Duration) {
        let state = self
            .lock()
            .by_name
            .get(queue_provider::queue_name_of(queue_url))
            .cloned();
        if let Some(state) = state {
            state.signal(instant_now() + delay);
        }
    }

    /// The queue's identity or defaults changed; observe it again.
    pub(super) fn forget(&self, queue_url: &str) {
        let state = self
            .lock()
            .by_name
            .get(queue_provider::queue_name_of(queue_url))
            .cloned();
        if let Some(state) = state {
            state.forget();
        }
    }

    /// Reserve one waiting slot, or `None` when the bound is reached.
    pub(super) fn begin_waiting(self: &Arc<Self>) -> Option<WaitingReceive> {
        let previous = self.waiting.fetch_add(1, Ordering::AcqRel);
        if previous >= self.max_waiting {
            self.waiting.fetch_sub(1, Ordering::AcqRel);
            return None;
        }
        Some(WaitingReceive(Arc::clone(self)))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, WaitStates> {
        self.queues
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Releases one waiting slot on drop, including when the receive is dropped.
pub(super) struct WaitingReceive(Arc<ReceiveWaiters>);

impl Drop for WaitingReceive {
    fn drop(&mut self) {
        self.0.waiting.fetch_sub(1, Ordering::AcqRel);
    }
}

fn earliest(left: Option<Instant>, right: Option<Instant>) -> Option<Instant> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (left, right) => left.or(right),
    }
}

/// Tokio time, or the simulated time of an installed ambient source, so that
/// receive gating replays under FoundationDB simulation.
pub(super) fn instant_now() -> Instant {
    storage_types::ambient::source_instant().map_or_else(Instant::now, Instant::from_std)
}
