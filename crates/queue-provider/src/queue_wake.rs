//! Process-wide wake slots for waiting receives (N1). A board says that a
//! queue of a slot may have changed; it never carries or reserves a message.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU32, Ordering},
};

use tokio::sync::{Notify, watch};

use crate::constants::QUEUE_WAKE_SLOTS;

/// One of `QUEUE_WAKE_SLOTS` slots; every URL form of a queue maps to the
/// slot of its name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueueWakeSlot(u16);

impl QueueWakeSlot {
    /// FNV-1a 64 of the queue name modulo `QUEUE_WAKE_SLOTS`: the same on
    /// every node and build (no std hasher).
    #[must_use]
    pub fn of_queue(queue_url_or_name: &str) -> Self {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in crate::queue_name_of(queue_url_or_name).as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        Self((hash % u64::from(QUEUE_WAKE_SLOTS)) as u16)
    }

    #[must_use]
    pub fn from_index(index: u16) -> Option<Self> {
        (index < QUEUE_WAKE_SLOTS).then_some(Self(index))
    }

    #[must_use]
    pub const fn index(self) -> u16 {
        self.0
    }

    pub fn all() -> impl Iterator<Item = Self> {
        (0..QUEUE_WAKE_SLOTS).map(Self)
    }
}

/// The slots that one native attempt staged, each once.
#[derive(Debug, Default)]
pub struct QueueWakeSlots(Vec<QueueWakeSlot>);

impl QueueWakeSlots {
    /// False when this attempt already staged `slot`.
    pub fn insert(&mut self, slot: QueueWakeSlot) -> bool {
        if self.0.contains(&slot) {
            return false;
        }
        self.0.push(slot);
        true
    }

    #[must_use]
    pub fn as_slice(&self) -> &[QueueWakeSlot] {
        &self.0
    }

    pub fn take(&mut self) -> Vec<QueueWakeSlot> {
        std::mem::take(&mut self.0)
    }
}

/// What this process knows about one slot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QueueWakeMark {
    /// Advanced by every reported change, by covering, and by uncovering.
    pub generation: u64,
    /// While true, any commit in the slot that a later read can see advances
    /// `generation` within the provider's bound (local board: at once;
    /// FoundationDB: re-arm or 1 s fallback; Postgres: NOTIFY or 10 s).
    pub covered: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BoardKind {
    /// One process owns the store; every commit reaches `committed`.
    Local,
    /// A native feed covers slots; `forwards` queues committed slots for a
    /// notifier (Postgres).
    Feed { forwards: bool },
}

pub struct QueueWakeBoard {
    kind: BoardKind,
    marks: Box<[watch::Sender<QueueWakeMark>]>,
    interest: Box<[AtomicU32]>,
    interest_added: Notify,
    pending: Box<[AtomicBool]>,
    pending_added: Notify,
    closed: AtomicBool,
    closing: Notify,
}

impl QueueWakeBoard {
    /// True when this board itself must be told of every commit: a local
    /// board, or a feed board whose notifier forwards committed slots.
    /// A store with such a board publishes after the commit even when the
    /// caller is gone or the commit outcome is unknown.
    pub fn records_commits(&self) -> bool {
        !matches!(self.kind, BoardKind::Feed { forwards: false })
    }

    /// SQLite, Turso, RocksDB: every slot covered for the board's life.
    #[must_use]
    pub fn local() -> Arc<Self> {
        Self::new(BoardKind::Local, true)
    }

    /// FoundationDB (`forwards_commits` false), Postgres (true): every slot
    /// uncovered until the feed arms it.
    #[must_use]
    pub fn feed(forwards_commits: bool) -> Arc<Self> {
        Self::new(
            BoardKind::Feed {
                forwards: forwards_commits,
            },
            false,
        )
    }

    fn new(kind: BoardKind, covered: bool) -> Arc<Self> {
        let slots = usize::from(QUEUE_WAKE_SLOTS);
        Arc::new(Self {
            kind,
            marks: (0..slots)
                .map(|_| {
                    watch::Sender::new(QueueWakeMark {
                        generation: 0,
                        covered,
                    })
                })
                .collect(),
            interest: (0..slots).map(|_| AtomicU32::new(0)).collect(),
            interest_added: Notify::new(),
            pending: (0..slots).map(|_| AtomicBool::new(false)).collect(),
            pending_added: Notify::new(),
            closed: AtomicBool::new(false),
            closing: Notify::new(),
        })
    }

    #[must_use]
    pub fn mark(&self, slot: QueueWakeSlot) -> QueueWakeMark {
        *self.marks[usize::from(slot.0)].borrow()
    }

    #[must_use]
    pub fn subscribe(&self, slot: QueueWakeSlot) -> watch::Receiver<QueueWakeMark> {
        self.marks[usize::from(slot.0)].subscribe()
    }

    /// A waiting receive of this process; dropping the guard ends it.
    #[must_use]
    pub fn register(self: &Arc<Self>, slot: QueueWakeSlot) -> QueueWakeInterest {
        if self.interest[usize::from(slot.0)].fetch_add(1, Ordering::AcqRel) == 0 {
            self.interest_added.notify_one();
        }
        QueueWakeInterest {
            board: Arc::clone(self),
            slot,
        }
    }

    #[must_use]
    pub fn has_interest(&self, slot: QueueWakeSlot) -> bool {
        self.interest[usize::from(slot.0)].load(Ordering::Acquire) > 0
    }

    #[must_use]
    pub fn interested_slots(&self) -> Vec<QueueWakeSlot> {
        QueueWakeSlot::all()
            .filter(|slot| self.has_interest(*slot))
            .collect()
    }

    /// Resolves after a slot went from no waiter to one waiter (one permit
    /// is stored when nobody awaits).
    pub async fn interest_added(&self) {
        self.interest_added.notified().await;
    }

    /// The store committed writes that staged `slots`. Called by the store
    /// after its native commit returned Ok, never before.
    pub fn committed(&self, slots: &[QueueWakeSlot]) {
        match self.kind {
            BoardKind::Local => slots.iter().for_each(|slot| self.advance(*slot)),
            BoardKind::Feed { forwards: true } => {
                for slot in slots {
                    self.pending[usize::from(slot.0)].store(true, Ordering::Release);
                }
                if !slots.is_empty() {
                    self.pending_added.notify_one();
                }
            }
            BoardKind::Feed { forwards: false } => {}
        }
    }

    /// Committed slots not yet forwarded, each once, in slot order.
    #[must_use]
    pub fn take_committed(&self) -> Vec<QueueWakeSlot> {
        QueueWakeSlot::all()
            .filter(|slot| self.pending[usize::from(slot.0)].swap(false, Ordering::AcqRel))
            .collect()
    }

    pub async fn commits_pending(&self) {
        self.pending_added.notified().await;
    }

    pub fn advance(&self, slot: QueueWakeSlot) {
        self.marks[usize::from(slot.0)]
            .send_modify(|mark| mark.generation = mark.generation.wrapping_add(1));
    }

    pub fn advance_all(&self) {
        QueueWakeSlot::all().for_each(|slot| self.advance(slot));
    }

    /// Covering always advances: changes before the cover were not watched.
    pub fn cover(&self, slot: QueueWakeSlot) {
        self.set_covered(slot, true);
    }

    pub fn uncover(&self, slot: QueueWakeSlot) {
        self.set_covered(slot, false);
    }

    pub fn cover_all(&self) {
        QueueWakeSlot::all().for_each(|slot| self.cover(slot));
    }

    pub fn uncover_all(&self) {
        QueueWakeSlot::all().for_each(|slot| self.uncover(slot));
    }

    fn set_covered(&self, slot: QueueWakeSlot, covered: bool) {
        self.marks[usize::from(slot.0)].send_if_modified(|mark| {
            if mark.covered == covered {
                return false;
            }
            mark.covered = covered;
            mark.generation = mark.generation.wrapping_add(1);
            true
        });
    }

    /// Stop the feed tasks; every slot becomes uncovered.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.closing.notify_waiters();
        self.uncover_all();
    }

    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub async fn closed(&self) {
        let notified = self.closing.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.is_closed() {
            return;
        }
        notified.await;
    }
}

/// Ends one registration on drop, also when the receive is cancelled.
pub struct QueueWakeInterest {
    board: Arc<QueueWakeBoard>,
    slot: QueueWakeSlot,
}

impl Drop for QueueWakeInterest {
    fn drop(&mut self) {
        self.board.interest[usize::from(self.slot.0)].fetch_sub(1, Ordering::AcqRel);
    }
}

/// The feed tasks of one backend: dropping it closes the board and aborts
/// the tasks (N1b, N1c own one).
pub struct QueueWakeTasks {
    board: Arc<QueueWakeBoard>,
    tasks: Vec<tokio::task::AbortHandle>,
}

impl QueueWakeTasks {
    #[must_use]
    pub fn new(board: Arc<QueueWakeBoard>, tasks: Vec<tokio::task::AbortHandle>) -> Self {
        Self { board, tasks }
    }

    #[must_use]
    pub fn board(&self) -> Arc<QueueWakeBoard> {
        Arc::clone(&self.board)
    }
}

impl Drop for QueueWakeTasks {
    fn drop(&mut self) {
        self.board.close();
        self.tasks.iter().for_each(tokio::task::AbortHandle::abort);
    }
}
