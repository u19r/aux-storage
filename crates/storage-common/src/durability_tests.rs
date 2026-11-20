//! Acknowledgement rules of `DurabilityBatch` with a gated fake barrier and
//! paused Tokio time. Each test asserts barrier counts and order; the test
//! names give the rule or the failing model trace (`quint/durable_ack.qnt`).

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use storage_types::{
    AttemptDurability, CancellationReason, NativeUnavailable, StorageEnum, StorageError,
    StorageResult,
};
use tokio::{sync::watch, task::JoinHandle, time::Instant};

use crate::durability::{AttemptCompletion, BarrierFailure, DurabilityBatch, NativeVersionMemo};

/// A barrier that starts and finishes only when the test allows it. Its
/// version source is `version`, a stand-in for a native commit counter.
pub(crate) struct FakeBarrier {
    invoked: AtomicU64,
    begun: AtomicU64,
    probes: AtomicU64,
    start_allowed: watch::Sender<u64>,
    finish_allowed: watch::Sender<u64>,
    failure: Mutex<Option<(BarrierFailure, bool)>>,
    pub(crate) probe_fails: AtomicBool,
    pub(crate) version: Arc<AtomicU64>,
    invoked_at: Mutex<Vec<Instant>>,
}

impl FakeBarrier {
    pub(crate) fn new(version: Arc<AtomicU64>) -> Arc<Self> {
        Arc::new(Self {
            invoked: AtomicU64::new(0),
            begun: AtomicU64::new(0),
            probes: AtomicU64::new(0),
            start_allowed: watch::Sender::new(0),
            finish_allowed: watch::Sender::new(0),
            failure: Mutex::new(None),
            probe_fails: AtomicBool::new(false),
            version,
            invoked_at: Mutex::new(Vec::new()),
        })
    }

    /// A batch without a version probe (one process, as RocksDB).
    pub(crate) fn batch(self: &Arc<Self>) -> DurabilityBatch {
        let fake = Arc::clone(self);
        DurabilityBatch::new(move |start| {
            let fake = Arc::clone(&fake);
            async move { fake.run(start).await }
        })
    }

    /// A batch whose store other processes can commit to (as SQLite).
    pub(crate) fn batch_with_probe(self: &Arc<Self>) -> DurabilityBatch {
        let fake = Arc::clone(self);
        let probe = Arc::clone(self);
        DurabilityBatch::with_version_probe(
            move |start| {
                let fake = Arc::clone(&fake);
                async move { fake.run(start).await }
            },
            move || {
                probe.probes.fetch_add(1, Ordering::SeqCst);
                let version = if probe.probe_fails.load(Ordering::SeqCst) {
                    Err(StorageError::internal("fake version read failed"))
                } else {
                    Ok(probe.version.load(Ordering::SeqCst))
                };
                async move { version }
            },
        )
    }

    async fn run(
        &self,
        start: crate::durability::BarrierStart,
    ) -> Result<crate::durability::BarrierCapture, BarrierFailure> {
        let index = self.invoked.fetch_add(1, Ordering::SeqCst) + 1;
        self.invoked_at.lock().unwrap().push(Instant::now());
        let closed = || BarrierFailure::SyncFailed;
        self.start_allowed
            .subscribe()
            .wait_for(|allowed| *allowed >= index)
            .await
            .map_err(|_| closed())?;
        // The barrier's own version read fails like the reader's.
        let version =
            (!self.probe_fails.load(Ordering::SeqCst)).then(|| self.version.load(Ordering::SeqCst));
        let capture = start.begin(version);
        self.begun.fetch_add(1, Ordering::SeqCst);
        self.finish_allowed
            .subscribe()
            .wait_for(|allowed| *allowed >= index)
            .await
            .map_err(|_| closed())?;
        let mut failure = self.failure.lock().unwrap();
        if let Some((kind, persistent)) = *failure {
            if !persistent {
                *failure = None;
            }
            return Err(kind);
        }
        Ok(capture)
    }

    pub(crate) fn invoked(&self) -> u64 {
        self.invoked.load(Ordering::SeqCst)
    }

    pub(crate) fn begun(&self) -> u64 {
        self.begun.load(Ordering::SeqCst)
    }

    fn invoked_at(&self, index: usize) -> Instant {
        self.invoked_at.lock().unwrap()[index]
    }

    pub(crate) fn allow_start(&self, count: u64) {
        self.start_allowed.send_modify(|allowed| *allowed += count);
    }

    pub(crate) fn allow_finish(&self, count: u64) {
        self.finish_allowed.send_modify(|allowed| *allowed += count);
    }

    pub(crate) fn fail_from_now(&self) {
        *self.failure.lock().unwrap() = Some((BarrierFailure::SyncFailed, true));
    }

    fn interrupt_once(&self) {
        *self.failure.lock().unwrap() = Some((BarrierFailure::Interrupted, false));
    }

    fn open(&self) {
        self.start_allowed.send_replace(u64::MAX);
        self.finish_allowed.send_replace(u64::MAX);
    }
}

/// Lets every spawned task run until it waits.
pub(crate) async fn quiesce() {
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
}

fn fake() -> Arc<FakeBarrier> {
    FakeBarrier::new(Arc::new(AtomicU64::new(0)))
}

/// A batch whose startup barrier completed; the gate is closed again.
async fn ready(fake: &Arc<FakeBarrier>, probe: bool) -> Arc<DurabilityBatch> {
    let batch = if probe {
        fake.batch_with_probe()
    } else {
        fake.batch()
    };
    fake.allow_start(1);
    fake.allow_finish(1);
    quiesce().await;
    assert_eq!(batch.stats().barriers_completed, 1, "startup barrier");
    Arc::new(batch)
}

fn completion(
    durability: AttemptDurability,
    write: bool,
    batch: &DurabilityBatch,
) -> AttemptCompletion {
    let write = write.then(|| {
        let write = batch.begin_write();
        write.record_commit();
        write
    });
    AttemptCompletion::new(durability, write)
}

fn spawn_settle(
    batch: &Arc<DurabilityBatch>,
    result: StorageResult<()>,
    completion: AttemptCompletion,
) -> JoinHandle<StorageResult<()>> {
    let batch = Arc::clone(batch);
    tokio::spawn(async move { batch.settle(result, completion).await })
}

fn spawn_write(batch: &Arc<DurabilityBatch>) -> JoinHandle<StorageResult<()>> {
    spawn_settle(
        batch,
        Ok(()),
        completion(AttemptDurability::Durable, true, batch),
    )
}

fn spawn_read(batch: &Arc<DurabilityBatch>) -> JoinHandle<StorageResult<()>> {
    spawn_settle(
        batch,
        Ok(()),
        AttemptCompletion::new(AttemptDurability::Durable, None),
    )
}

async fn read(batch: &DurabilityBatch) -> StorageResult<()> {
    batch
        .settle(
            Ok(()),
            AttemptCompletion::new(AttemptDurability::Durable, None),
        )
        .await
}

fn is_stopped(error: &StorageError) -> bool {
    matches!(
        error.as_ref(),
        StorageEnum::Attempt(storage_types::AttemptFailure::Unavailable(
            NativeUnavailable::DurabilityFailed
        ))
    )
}

// Rule 8: no fixed sleep for a lone writer.
#[tokio::test(start_paused = true)]
async fn given_lone_writer_when_it_waits_then_one_barrier_releases_it_without_time_advancing() {
    let fake = fake();
    let batch = ready(&fake, false).await;
    fake.open();
    let started = Instant::now();
    batch
        .settle(Ok(()), completion(AttemptDurability::Durable, true, &batch))
        .await
        .unwrap();
    assert_eq!(fake.invoked(), 2, "startup barrier and one barrier");
    assert_eq!(fake.invoked_at(1), started);
    assert_eq!(Instant::now(), started);
}

// Rule 8: a barrier in progress is the collection window.
#[tokio::test(start_paused = true)]
async fn given_barrier_blocked_when_fifty_writers_wait_then_exactly_two_barrier_calls_release_them()
{
    let fake = fake();
    let batch = ready(&fake, false).await;
    let first = spawn_write(&batch);
    quiesce().await;
    fake.allow_start(1);
    quiesce().await;
    assert_eq!(fake.begun(), 2, "the first writer's barrier is in progress");
    let others = (0..50).map(|_| spawn_write(&batch)).collect::<Vec<_>>();
    quiesce().await;
    assert!(!first.is_finished());
    fake.open();
    first.await.unwrap().unwrap();
    for other in others {
        other.await.unwrap().unwrap();
    }
    assert_eq!(
        fake.invoked(),
        3,
        "startup barrier and exactly two barriers"
    );
}

// Rule 8: a completed barrier releases every commit it covers.
#[tokio::test(start_paused = true)]
async fn given_commit_recorded_before_barrier_start_when_wait_is_called_late_then_that_barrier_releases_it()
 {
    let fake = fake();
    let batch = ready(&fake, false).await;
    let first = spawn_write(&batch);
    quiesce().await;
    let late = batch.begin_write();
    late.record_commit();
    fake.allow_start(1);
    fake.allow_finish(1);
    first.await.unwrap().unwrap();
    batch
        .settle(
            Ok(()),
            AttemptCompletion::new(AttemptDurability::Durable, Some(late)),
        )
        .await
        .unwrap();
    assert_eq!(
        fake.invoked(),
        2,
        "no barrier after the one that covered it"
    );
}

// Rule 8: a bounded extra wait only while other write attempts are in
// progress.
#[tokio::test(start_paused = true)]
async fn given_three_writes_in_progress_when_sync_is_due_then_it_waits_for_them_or_two_ms_not_longer()
 {
    let fake = fake();
    let batch = ready(&fake, false).await;
    fake.open();
    let in_progress = (0..3).map(|_| batch.begin_write()).collect::<Vec<_>>();

    let window_start = Instant::now();
    let waiter = spawn_write(&batch);
    tokio::time::sleep(Duration::from_millis(10)).await;
    waiter.await.unwrap().unwrap();
    assert_eq!(fake.invoked_at(1) - window_start, Duration::from_millis(2));

    let window_start = Instant::now();
    let waiter = spawn_write(&batch);
    tokio::time::sleep(Duration::from_millis(1)).await;
    for write in &in_progress {
        write.record_commit();
    }
    waiter.await.unwrap().unwrap();
    assert_eq!(fake.invoked_at(2) - window_start, Duration::from_millis(1));
    for write in in_progress {
        batch
            .settle(
                Ok(()),
                AttemptCompletion::new(AttemptDurability::Durable, Some(write)),
            )
            .await
            .unwrap();
    }
    assert_eq!(
        fake.invoked(),
        3,
        "the last barrier covered the three commits"
    );
}

// DUR-F9: every registration has a ticket, so the barrier in progress, which
// captured the highest ticket, covers what the reader saw.
#[tokio::test(start_paused = true)]
async fn given_steady_writer_when_reader_completes_then_it_waits_at_most_for_the_barrier_in_progress()
 {
    let fake = fake();
    let batch = ready(&fake, false).await;
    let writer = spawn_write(&batch);
    quiesce().await;
    fake.allow_start(1);
    quiesce().await;
    let reader = spawn_read(&batch);
    quiesce().await;
    assert!(!reader.is_finished());
    fake.allow_finish(1);
    writer.await.unwrap().unwrap();
    reader.await.unwrap().unwrap();
    assert_eq!(fake.invoked(), 2, "no barrier after the one in progress");
}

// Rules 6 and 8, traces `p3_reader_seq` and `impl_fast_unsafe`: with a
// registered writer that has no ticket yet, the highest ticket does not
// cover what the reader may have seen; a later barrier must.
#[tokio::test(start_paused = true)]
async fn given_unticketed_writer_when_reader_completes_during_a_barrier_then_only_a_later_barrier_releases_it()
 {
    let fake = fake();
    let batch = ready(&fake, false).await;
    let writer = spawn_write(&batch);
    quiesce().await;
    fake.allow_start(1);
    quiesce().await;
    let unticketed = batch.begin_write();
    let reader = spawn_read(&batch);
    quiesce().await;
    fake.allow_finish(1);
    writer.await.unwrap().unwrap();
    quiesce().await;
    assert!(
        !reader.is_finished(),
        "the covering barrier began before the read ended"
    );
    unticketed.record_commit();
    fake.open();
    reader.await.unwrap().unwrap();
    batch
        .settle(
            Ok(()),
            AttemptCompletion::new(AttemptDurability::Durable, Some(unticketed)),
        )
        .await
        .unwrap();
    assert_eq!(fake.invoked(), 3);
}

// Rule 5, traces `p2_register_at_wait` and `p2_check_at_start`: a writer
// registered before its commit holds a reader that completes after it.
#[tokio::test(start_paused = true)]
async fn given_registered_writer_before_commit_when_reader_completes_then_reader_waits_for_a_barrier()
 {
    let fake = fake();
    let batch = ready(&fake, false).await;
    let write = batch.begin_write();
    let reader = spawn_read(&batch);
    quiesce().await;
    assert!(!reader.is_finished());
    write.record_commit();
    fake.open();
    reader.await.unwrap().unwrap();
    batch
        .settle(
            Ok(()),
            AttemptCompletion::new(AttemptDurability::Durable, Some(write)),
        )
        .await
        .unwrap();
    assert_eq!(
        fake.invoked(),
        2,
        "one barrier covered the reader and the writer"
    );
}

// Rule 6: an idle store adds no wait and no barrier.
#[tokio::test(start_paused = true)]
async fn given_no_pending_writer_when_readers_complete_then_zero_barrier_calls_follow() {
    for probe in [false, true] {
        let fake = fake();
        let batch = ready(&fake, probe).await;
        for _ in 0..10 {
            read(&batch).await.unwrap();
        }
        let stats = batch.stats();
        assert_eq!((fake.invoked(), stats.waits), (1, 0));
    }
}

// Rule 1, trace `p2_restart`: a new batch holds readers until its first
// barrier, which starts without any attempt.
#[tokio::test(start_paused = true)]
async fn given_restarted_store_when_reader_completes_then_it_waits_for_the_startup_barrier() {
    let fake = fake();
    let batch = Arc::new(fake.batch());
    quiesce().await;
    assert_eq!((fake.invoked(), batch.stats().pending), (1, 1));
    let reader = spawn_read(&batch);
    quiesce().await;
    assert!(!reader.is_finished());
    fake.open();
    reader.await.unwrap().unwrap();
    read(&batch).await.unwrap();
    assert_eq!((fake.invoked(), batch.stats().pending), (1, 0));
}

// Rule 2, trace `p2_relaxed`: a relaxed writer answers at once; its hold
// starts a barrier and holds readers until a barrier covers it.
#[tokio::test(start_paused = true)]
async fn given_relaxed_commit_when_it_settles_then_it_answers_at_once_and_holds_readers_until_covered()
 {
    let fake = fake();
    let batch = ready(&fake, false).await;
    batch
        .settle(Ok(()), completion(AttemptDurability::Relaxed, true, &batch))
        .await
        .unwrap();
    quiesce().await;
    assert_eq!(fake.invoked(), 2, "the hold started a barrier");
    assert_eq!(batch.stats().pending, 1);
    let reader = spawn_read(&batch);
    quiesce().await;
    assert!(!reader.is_finished());
    fake.open();
    reader.await.unwrap().unwrap();
    assert_eq!(batch.stats().pending, 0);
    read(&batch).await.unwrap();
    assert_eq!(fake.invoked(), 2);
}

// W2 D11: a Relaxed attempt that wrote nothing (a receive probe, an empty
// claim, approximate counts) answers while a Relaxed hold waits for its
// barrier; a Durable read still waits.
#[tokio::test(start_paused = true)]
async fn given_relaxed_hold_when_a_relaxed_observer_settles_then_it_answers_without_a_barrier() {
    let fake = fake();
    let batch = ready(&fake, false).await;
    batch
        .settle(Ok(()), completion(AttemptDurability::Relaxed, true, &batch))
        .await
        .unwrap();
    quiesce().await;
    batch
        .settle(
            Ok(()),
            AttemptCompletion::new(AttemptDurability::Relaxed, None),
        )
        .await
        .unwrap();
    assert_eq!(batch.stats().pending, 1, "the Relaxed hold is still open");
    let durable = spawn_read(&batch);
    quiesce().await;
    assert!(!durable.is_finished());
    fake.open();
    durable.await.unwrap().unwrap();
}

// Rule 3, traces `p2_barrier_fail` and `p2_fail_holds_without_stop`.
#[tokio::test(start_paused = true)]
async fn given_failed_barrier_when_attempts_settle_then_store_stops_and_writer_keeps_its_hold() {
    let fake = fake();
    let batch = ready(&fake, false).await;
    let writer = spawn_write(&batch);
    let reader = spawn_read(&batch);
    quiesce().await;
    fake.fail_from_now();
    fake.open();
    assert!(writer.await.unwrap().unwrap_err().is_commit_unknown());
    assert!(is_stopped(&reader.await.unwrap().unwrap_err()));
    assert!(is_stopped(&batch.check_available().unwrap_err()));
    assert!(is_stopped(&read(&batch).await.unwrap_err()));
    let late = batch
        .settle(Ok(()), completion(AttemptDurability::Durable, true, &batch))
        .await;
    assert!(late.unwrap_err().is_commit_unknown());
    let stats = batch.stats();
    assert!(stats.stopped);
    assert_eq!((stats.pending, fake.invoked()), (2, 2));
}

// Rule 3: a stopped batch also refuses a Relaxed observer (a receive probe,
// an empty claim), because what it read may not be on disk.
#[tokio::test(start_paused = true)]
async fn given_stopped_batch_when_a_relaxed_observer_settles_then_it_reports_the_stop() {
    let fake = fake();
    let batch = ready(&fake, false).await;
    let writer = spawn_write(&batch);
    quiesce().await;
    fake.fail_from_now();
    fake.open();
    assert!(writer.await.unwrap().unwrap_err().is_commit_unknown());
    let relaxed = |result: StorageResult<()>| {
        batch.settle(
            result,
            AttemptCompletion::new(AttemptDurability::Relaxed, None),
        )
    };
    assert!(is_stopped(&relaxed(Ok(())).await.unwrap_err()));
    let rollback = relaxed(Err(StorageError::internal("rolled back"))).await;
    assert!(
        !is_stopped(&rollback.unwrap_err()),
        "an error that reveals no observation keeps its own kind"
    );
}

// Rule 4, trace `p2_post_commit_error`: an error after the native commit
// returned is an unknown outcome and waits; so does an unknown commit.
#[tokio::test(start_paused = true)]
async fn given_error_after_commit_when_writer_settles_then_it_waits_and_reports_unknown_outcome() {
    let fake = fake();
    let batch = ready(&fake, false).await;
    let after_commit = spawn_settle(
        &batch,
        Err(StorageError::internal("worker stopped")),
        completion(AttemptDurability::Relaxed, true, &batch),
    );
    let unknown = spawn_settle(
        &batch,
        Err(StorageError::commit_unknown("commit unknown")),
        AttemptCompletion::new(AttemptDurability::Durable, Some(batch.begin_write())),
    );
    quiesce().await;
    assert!(!after_commit.is_finished() && !unknown.is_finished());
    fake.open();
    assert!(after_commit.await.unwrap().unwrap_err().is_commit_unknown());
    assert!(unknown.await.unwrap().unwrap_err().is_commit_unknown());
    assert_eq!(batch.stats().pending, 0);
}

// Rule 7, trace `p2_external`: another process committed since the last
// completed barrier's capture.
#[tokio::test(start_paused = true)]
async fn given_external_commit_when_reader_completes_then_it_waits_unless_its_connection_saw_nothing_new()
 {
    let fake = fake();
    let batch = ready(&fake, true).await;
    fake.open();
    read(&batch).await.unwrap();
    assert_eq!(fake.invoked(), 1);

    fake.version.store(1, Ordering::SeqCst);
    read(&batch).await.unwrap();
    assert_eq!(fake.invoked(), 2, "the external commit needed a barrier");

    let memo = NativeVersionMemo::default();
    let observed = |version| {
        AttemptCompletion::new(AttemptDurability::Durable, None)
            .with_observed(Some(memo.observe(version)))
    };
    batch.settle(Ok(()), observed(7)).await.unwrap();
    fake.version.store(2, Ordering::SeqCst);
    let probes = fake.probes.load(Ordering::SeqCst);
    batch.settle(Ok(()), observed(7)).await.unwrap();
    assert_eq!(
        fake.probes.load(Ordering::SeqCst),
        probes,
        "same connection version"
    );
    assert_eq!(fake.invoked(), 2);
    batch.settle(Ok(()), observed(8)).await.unwrap();
    assert_eq!(
        fake.invoked(),
        3,
        "a new connection version found the external commit"
    );
}

// DUR-F1: an error that carries observed data (a failed condition with its
// item) waits like an answer; an infrastructure error does not.
#[tokio::test(start_paused = true)]
async fn given_condition_failure_on_unsynced_item_when_attempt_settles_then_it_waits_for_the_barrier()
 {
    let fake = fake();
    let batch = ready(&fake, false).await;
    let writer = spawn_write(&batch);
    let reader = spawn_settle(
        &batch,
        Err(StorageEnum::ConditionalCheckFailedWithItem {
            item: Default::default(),
        }
        .into()),
        AttemptCompletion::new(AttemptDurability::Durable, None),
    );
    let rolled_back = spawn_settle(
        &batch,
        Err(StorageEnum::TransactionCanceled {
            reasons: vec![CancellationReason::conditional_check_failed(None)],
        }
        .into()),
        AttemptCompletion::new(AttemptDurability::Durable, Some(batch.begin_write())),
    );
    let native = batch
        .settle::<()>(
            Err(StorageError::internal("native failure")),
            AttemptCompletion::new(AttemptDurability::Durable, None),
        )
        .await;
    assert!(native.is_err(), "an infrastructure error answers at once");
    quiesce().await;
    assert!(!reader.is_finished() && !rolled_back.is_finished());
    fake.open();
    writer.await.unwrap().unwrap();
    assert!(matches!(
        reader.await.unwrap().unwrap_err().as_ref(),
        StorageEnum::ConditionalCheckFailedWithItem { .. }
    ));
    assert!(rolled_back.await.unwrap().is_err());
    assert_eq!(batch.stats().pending, 0);
}

// DUR-F2: a guard dropped outside `settle` leaves a hold: by ticket while a
// barrier has not covered it, else for the next barrier to start.
#[tokio::test(start_paused = true)]
async fn given_guard_dropped_without_settle_when_reader_completes_then_it_waits_for_the_barrier() {
    let fake = fake();
    let batch = ready(&fake, false).await;
    let ticketed = batch.begin_write();
    ticketed.record_commit();
    drop(ticketed);
    drop(batch.begin_write());
    assert_eq!(batch.stats().pending, 2, "a ticket hold and a barrier hold");
    let reader = spawn_read(&batch);
    quiesce().await;
    assert!(!reader.is_finished());
    fake.open();
    reader.await.unwrap().unwrap();
    assert_eq!((batch.stats().pending, fake.invoked()), (0, 2));
}

// DUR-F3: the writer thread records after the attempt dropped its guard.
#[tokio::test(start_paused = true)]
async fn given_record_after_guard_dropped_when_recorder_finishes_then_counts_hold_and_do_not_underflow()
 {
    let fake = fake();
    let batch = ready(&fake, false).await;
    let write = batch.begin_write();
    let recorder = write.recorder();
    drop(write);
    assert_eq!(batch.stats().pending, 1, "the writer thread keeps it");
    recorder.record();
    drop(recorder);
    assert_eq!(batch.stats().pending, 1, "the uncovered ticket holds");
    fake.open();
    read(&batch).await.unwrap();
    assert_eq!(batch.stats().pending, 0);
}

// DUR-F5: a barrier with no sync result and a failed version read do not
// stop the store.
#[tokio::test(start_paused = true)]
async fn given_interrupted_barrier_or_failed_probe_when_attempts_settle_then_store_continues() {
    let fake = fake();
    let batch = ready(&fake, true).await;
    fake.interrupt_once();
    fake.open();
    batch
        .settle(Ok(()), completion(AttemptDurability::Durable, true, &batch))
        .await
        .unwrap();
    assert_eq!(fake.invoked(), 3, "the interrupted barrier was retried");
    fake.probe_fails.store(true, Ordering::SeqCst);
    read(&batch).await.unwrap();
    assert_eq!(
        fake.invoked(),
        4,
        "a failed version read waits for a barrier"
    );
    assert!(!batch.stats().stopped && batch.check_available().is_ok());
}

// DUR-F8: an unknown outcome is not in progress: the barrier for it does not
// wait for the window.
#[tokio::test(start_paused = true)]
async fn given_unknown_outcome_without_ticket_when_it_waits_then_its_barrier_starts_without_the_window()
 {
    let fake = fake();
    let batch = ready(&fake, false).await;
    fake.open();
    let started = Instant::now();
    let unknown = batch
        .settle::<()>(
            Err(StorageError::commit_unknown("unknown")),
            AttemptCompletion::new(AttemptDurability::Durable, Some(batch.begin_write())),
        )
        .await;
    assert!(unknown.unwrap_err().is_commit_unknown());
    assert_eq!(fake.invoked_at(1), started);
}

// DUR-F4: the stop is visible to the process through `DurabilityHealth`.
#[tokio::test(start_paused = true)]
async fn given_failed_sync_when_health_is_watched_then_it_reports_the_stop() {
    let fake = fake();
    let batch = ready(&fake, false).await;
    let health = batch.health();
    assert!(!health.is_stopped());
    let watcher = tokio::spawn(health.clone().stopped());
    fake.fail_from_now();
    fake.open();
    let _ = batch
        .settle(Ok(()), completion(AttemptDurability::Durable, true, &batch))
        .await;
    watcher.await.unwrap();
    assert!(health.is_stopped());
}
