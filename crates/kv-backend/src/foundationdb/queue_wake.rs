//! FoundationDB queue wake feed: one task per backend arms watches on the
//! slot keys that have local waiters and re-reads all slot keys periodically.

use std::{
    collections::BTreeSet,
    sync::{Arc, Weak},
    time::Duration,
};

use foundationdb::{Database, RangeOption};
use futures::{
    FutureExt as _, StreamExt as _, TryStreamExt as _,
    future::{AbortHandle, BoxFuture},
    stream::FuturesUnordered,
};
use queue_provider::{QUEUE_WAKE_SLOTS, QueueWakeBoard, QueueWakeSlot, QueueWakeTasks};
use storage_types::{StorageError, StorageResult};
use tokio::time::Instant;

use super::{
    driver::NativeTransactionOptions,
    error::{map_operation_error, map_unticketed_commit_error},
};

/// Bound on the delay of a lost or late watch.
const QUEUE_WAKE_FALLBACK: Duration = Duration::from_secs(1);
/// At most one arm transaction per this interval (40/s) under a hot slot.
const QUEUE_WAKE_MIN_REARM: Duration = Duration::from_millis(25);
const QUEUE_WAKE_MAX_BACKOFF: Duration = Duration::from_secs(1);
const QUEUE_WAKE_ARM_TIMEOUT: Duration = Duration::from_secs(5);

/// What the feed last read for one slot.
#[derive(Clone, PartialEq, Eq)]
enum SlotSeen {
    Unread,
    Absent,
    Value(Vec<u8>),
}

type ArmedWatch = (
    QueueWakeSlot,
    BoxFuture<'static, QueueWakeSlot>,
    AbortHandle,
);
type ArmOutcome = (Vec<Option<Vec<u8>>>, Vec<ArmedWatch>);

pub(crate) struct Feed {
    board: Arc<QueueWakeBoard>,
    database: Weak<Database>,
    prefix: Arc<[u8]>,
    options: NativeTransactionOptions,
    seen: Vec<SlotSeen>,
    pub(crate) watched: Vec<Option<AbortHandle>>,
    dirty: BTreeSet<QueueWakeSlot>,
}

fn i(slot: QueueWakeSlot) -> usize {
    usize::from(slot.index())
}

pub(super) fn start(
    database: &Arc<Database>,
    prefix: Arc<[u8]>,
    options: NativeTransactionOptions,
) -> Option<QueueWakeTasks> {
    let handle = tokio::runtime::Handle::try_current().ok()?;
    let board = QueueWakeBoard::feed(false);
    let feed = Feed {
        board: Arc::clone(&board),
        database: Arc::downgrade(database),
        prefix,
        options,
        seen: vec![SlotSeen::Unread; usize::from(QUEUE_WAKE_SLOTS)],
        watched: vec![None; usize::from(QUEUE_WAKE_SLOTS)],
        dirty: BTreeSet::new(),
    };
    let task = handle.spawn(feed.run());
    Some(QueueWakeTasks::new(board, vec![task.abort_handle()]))
}

impl Feed {
    async fn run(mut self) {
        let mut watches: FuturesUnordered<BoxFuture<'static, QueueWakeSlot>> =
            FuturesUnordered::new();
        let mut full_at = Instant::now();
        let mut last_arm = Instant::now() - QUEUE_WAKE_MIN_REARM;
        let mut backoff = QUEUE_WAKE_MIN_REARM;
        loop {
            if self.board.is_closed() {
                return;
            }
            // Slots with a waiter and no live watch must be read and watched.
            for slot in self.board.interested_slots() {
                if self.watched[i(slot)].is_none() {
                    self.dirty.insert(slot);
                }
            }
            let full = Instant::now() >= full_at;
            if self.dirty.is_empty() && !full {
                tokio::select! {
                    fired = watches.next(), if !watches.is_empty() => {
                        if let Some(slot) = fired {
                            self.fire(slot);
                        }
                    }
                    () = self.board.interest_added() => {}
                    () = tokio::time::sleep_until(full_at) => {}
                    () = self.board.closed() => return,
                }
                continue;
            }
            tokio::time::sleep_until(last_arm + QUEUE_WAKE_MIN_REARM).await;
            last_arm = Instant::now();
            // Collect fired watches that resolved meanwhile (non-blocking).
            while let Some(Some(slot)) = watches.next().now_or_never() {
                self.fire(slot);
            }
            let reading: Vec<QueueWakeSlot> = if full {
                QueueWakeSlot::all().collect()
            } else {
                self.dirty.iter().copied().collect()
            };
            match tokio::time::timeout(QUEUE_WAKE_ARM_TIMEOUT, self.arm(&reading, full)).await {
                Ok(Ok((values, new_watches))) => {
                    backoff = QUEUE_WAKE_MIN_REARM;
                    for (slot, watch, abort) in new_watches {
                        self.watched[i(slot)] = Some(abort);
                        watches.push(watch);
                    }
                    self.apply(&reading, values);
                    self.dirty.clear();
                    if full {
                        full_at = Instant::now() + QUEUE_WAKE_FALLBACK;
                        // A covered slot with no waiter at the fallback is
                        // dropped.
                        for slot in QueueWakeSlot::all() {
                            if !self.board.has_interest(slot) && self.board.mark(slot).covered {
                                if let Some(abort) = self.watched[i(slot)].take() {
                                    abort.abort();
                                }
                                self.board.uncover(slot);
                            }
                        }
                    }
                }
                Ok(Err(_)) | Err(_) => {
                    // Includes too_many_watches (1032) and a commit error.
                    // Every slot of this pass loses coverage (its observations
                    // fall back to the interval rule) and is read again after
                    // the backoff.
                    for slot in &reading {
                        if self.watched[i(*slot)].is_none() {
                            self.board.uncover(*slot);
                        }
                    }
                    tracing::warn!("queue.wake.arm.failed");
                    last_arm = Instant::now() + backoff - QUEUE_WAKE_MIN_REARM;
                    backoff = (backoff * 2).min(QUEUE_WAKE_MAX_BACKOFF);
                }
            }
        }
    }

    /// A watch future resolved: read the slot again. An aborted watch of a
    /// dropped slot is ignored.
    fn fire(&mut self, slot: QueueWakeSlot) {
        if self.watched[i(slot)].is_none() && !self.board.has_interest(slot) {
            return;
        }
        if let Some(abort) = self.watched[i(slot)].take() {
            abort.abort();
        }
        self.dirty.insert(slot);
    }

    /// `values[n]` is the value read for `reading[n]` (`None`: key absent).
    pub(crate) fn apply(&mut self, reading: &[QueueWakeSlot], values: Vec<Option<Vec<u8>>>) {
        for (slot, value) in reading.iter().zip(values) {
            let seen = value.map_or(SlotSeen::Absent, SlotSeen::Value);
            let index = i(*slot);
            if !self.board.mark(*slot).covered {
                // Only cover a slot whose watch is live or that has a waiter
                // (a fallback pass may read slots nobody watches: leave those
                // uncovered).
                if self.watched[index].is_some() || self.board.has_interest(*slot) {
                    self.board.cover(*slot);
                }
            } else if self.seen[index] != seen {
                self.board.advance(*slot);
            }
            self.seen[index] = seen;
        }
    }

    /// One read-only transaction: one wave for the reads, then watches.
    async fn arm(&self, reading: &[QueueWakeSlot], full: bool) -> StorageResult<ArmOutcome> {
        let database = self
            .database
            .upgrade()
            .ok_or_else(|| StorageError::internal("FoundationDB closed"))?;
        let transaction = database.create_trx().map_err(map_operation_error)?;
        self.options.configure_transaction(&transaction)?;
        let key = |slot| {
            [
                &*self.prefix,
                &crate::queue_wake_layout::queue_wake_key(slot),
            ]
            .concat()
        };
        let values = if full {
            // One range read of the whole slot family (at most 1,024 keys,
            // snapshot).
            let begin = [&*self.prefix, crate::constants::QUEUE_WAKE_PREFIX].concat();
            let end = crate::range::prefix_upper_bound(&begin)
                .ok_or_else(|| StorageError::internal("queue wake range has no bound"))?;
            let mut by_slot: Vec<Option<Vec<u8>>> = vec![None; usize::from(QUEUE_WAKE_SLOTS)];
            let mut stream = transaction
                .get_ranges_keyvalues(RangeOption::from((begin.as_slice(), end.as_slice())), true);
            while let Some(pair) = stream.try_next().await.map_err(map_operation_error)? {
                let logical = pair.key().strip_prefix(&*self.prefix).unwrap_or_default();
                if let Some(slot) = crate::queue_wake_layout::decode_queue_wake_key(logical) {
                    by_slot[i(slot)] = Some(pair.value().to_vec());
                }
            }
            reading
                .iter()
                .map(|slot| by_slot[i(*slot)].take())
                .collect()
        } else {
            futures::future::try_join_all(
                reading
                    .iter()
                    .map(|slot| transaction.get(&key(*slot), true)),
            )
            .await
            .map_err(map_operation_error)?
            .into_iter()
            .map(|value| value.map(|value| value.to_vec()))
            .collect()
        };
        let mut watches = Vec::new();
        for slot in reading
            .iter()
            .copied()
            .filter(|slot| self.board.has_interest(*slot) && self.watched[i(*slot)].is_none())
        {
            let (watch, abort) = futures::future::abortable(transaction.watch(&key(slot)));
            // Ok, a native error (commit failure, transaction_cancelled) and an
            // abort all report "read this slot again".
            watches.push((
                slot,
                async move {
                    let _ = watch.await;
                    slot
                }
                .boxed(),
                abort,
            ));
        }
        // Read-only commit: no conflict check; it registers the watches.
        transaction
            .commit()
            .await
            .map_err(map_unticketed_commit_error)?;
        Ok((values, watches))
    }

    #[cfg(test)]
    pub(crate) fn for_test(board: Arc<QueueWakeBoard>) -> Self {
        Self {
            board,
            database: Weak::new(),
            prefix: Arc::from(Vec::new()),
            options: NativeTransactionOptions::default(),
            seen: vec![SlotSeen::Unread; usize::from(QUEUE_WAKE_SLOTS)],
            watched: vec![None; usize::from(QUEUE_WAKE_SLOTS)],
            dirty: BTreeSet::new(),
        }
    }
}
