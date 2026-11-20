//! Durable acknowledgement on real RocksDB: reads, attempts that wrote
//! nothing, relaxed writes, and a reopen.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use storage_common::{durability::DurabilityBatch, writer_occupancy::ForeignAwaitPolicy};
use storage_types::{AttemptDurability, AttemptPlan, AttemptRequest, DeadlineClass};
use tokio::sync::Semaphore;

use super::RocksDbBackend;
use crate::{AttemptMode, KvAttempt as _, KvBackend as _, KvMutation, ReadProtection};

fn open(path: &std::path::Path) -> RocksDbBackend {
    RocksDbBackend::initialize(
        path.to_path_buf(),
        storage_types::AttemptDeadlines::default(),
        256,
    )
    .unwrap()
    .with_foreign_await_policy(ForeignAwaitPolicy::Fail)
    .unwrap()
}

async fn read(backend: &RocksDbBackend, mode: AttemptMode) -> Option<Vec<u8>> {
    backend
        .with_attempt(
            AttemptPlan::new(
                mode,
                if mode == AttemptMode::ReadOnly {
                    DeadlineClass::PointRead
                } else {
                    DeadlineClass::Write
                },
            )
            .start(backend.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    let protection = if mode == AttemptMode::ReadOnly {
                        ReadProtection::Snapshot
                    } else {
                        ReadProtection::Conflict
                    };
                    let values = attempt
                        .get_batch(vec![b"proof".to_vec()], protection)
                        .await?;
                    Ok(values.into_iter().next().flatten())
                })
            },
        )
        .await
        .unwrap()
}

async fn write(backend: &RocksDbBackend, request: AttemptRequest, value: &'static [u8]) {
    backend
        .with_attempt(request, move |attempt| {
            Box::pin(async move {
                attempt.stage(vec![KvMutation::Put {
                    key: b"proof".to_vec(),
                    value: value.to_vec(),
                }])
            })
        })
        .await
        .unwrap();
}

/// A barrier that syncs nothing and begins only when the gate allows it.
/// The first permit is the startup barrier's.
struct Gate {
    permits: Semaphore,
    synced: AtomicU64,
}

fn gated(backend: &mut RocksDbBackend) -> Arc<Gate> {
    let gate = Arc::new(Gate {
        permits: Semaphore::new(1),
        synced: AtomicU64::new(0),
    });
    let barrier_gate = Arc::clone(&gate);
    backend.durability = Arc::new(DurabilityBatch::new(move |start| {
        let gate = Arc::clone(&barrier_gate);
        async move {
            gate.permits.acquire().await.unwrap().forget();
            let capture = start.begin(None);
            gate.synced.fetch_add(1, Ordering::SeqCst);
            Ok(capture)
        }
    }));
    gate
}

async fn until(mut done: impl FnMut() -> bool) {
    while !done() {
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn given_idle_store_when_attempts_write_nothing_then_no_barrier_or_wait_follows() {
    let root = tempfile::tempdir().unwrap();
    let backend = open(&root.path().join("store"));
    write(
        &backend,
        AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
            .start(backend.attempt_deadlines()),
        b"1",
    )
    .await;
    let before = backend.durability.stats();
    for _ in 0..5 {
        assert!(read(&backend, AttemptMode::ReadOnly).await.is_some());
        assert!(read(&backend, AttemptMode::ReadWrite).await.is_some());
    }
    let after = backend.durability.stats();
    assert_eq!(
        (after.barriers_started, after.waits),
        (before.barriers_started, before.waits)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_unsynced_write_when_read_only_attempt_reads_it_then_it_returns_after_the_barrier() {
    let root = tempfile::tempdir().unwrap();
    let mut backend = open(&root.path().join("store"));
    let gate = gated(&mut backend);
    let backend = Arc::new(backend);
    until(|| gate.synced.load(Ordering::SeqCst) == 1).await;

    let writer = tokio::spawn({
        let backend = Arc::clone(&backend);
        async move {
            write(
                &backend,
                AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                    .start(backend.attempt_deadlines()),
                b"1",
            )
            .await
        }
    });
    until(|| backend.durability.stats().waits == 1).await;
    let mut reader = tokio::spawn({
        let backend = Arc::clone(&backend);
        let gate = Arc::clone(&gate);
        async move {
            let value = read(&backend, AttemptMode::ReadOnly).await;
            (value, gate.synced.load(Ordering::SeqCst))
        }
    });
    loop {
        tokio::select! {
            finished = &mut reader => panic!("reader finished before the barrier: {finished:?}"),
            () = tokio::task::yield_now() => {
                if backend.durability.stats().waits >= 2 {
                    break;
                }
            }
        }
    }
    gate.permits.add_permits(Semaphore::MAX_PERMITS);
    let (value, barriers_before_reply) = reader.await.unwrap();
    writer.await.unwrap();
    assert_eq!(value.as_deref(), Some(b"1".as_slice()));
    assert!(
        barriers_before_reply >= 2,
        "a barrier after the read covered it"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_relaxed_write_when_it_commits_then_it_returns_without_a_barrier_and_a_later_one_covers_it()
 {
    let root = tempfile::tempdir().unwrap();
    let mut backend = open(&root.path().join("store"));
    let gate = gated(&mut backend);
    until(|| gate.synced.load(Ordering::SeqCst) == 1).await;
    let relaxed = AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
        .with_durability(AttemptDurability::Relaxed)
        .start(backend.attempt_deadlines());
    write(&backend, relaxed, b"1").await;
    assert_eq!(
        gate.synced.load(Ordering::SeqCst),
        1,
        "no barrier before the answer"
    );
    assert_eq!(
        backend.durability.stats().pending,
        1,
        "the relaxed commit holds"
    );
    gate.permits.add_permits(1);
    until(|| backend.durability.stats().pending == 0).await;
    assert_eq!(gate.synced.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn given_acknowledged_writes_when_store_reopens_then_all_are_readable() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let backend = open(&path);
    write(
        &backend,
        AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
            .start(backend.attempt_deadlines()),
        b"1",
    )
    .await;
    drop(backend);
    let backend = open(&path);
    assert_eq!(
        read(&backend, AttemptMode::ReadOnly).await.as_deref(),
        Some(b"1".as_slice())
    );
    write(
        &backend,
        AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
            .start(backend.attempt_deadlines()),
        b"2",
    )
    .await;
    drop(backend);
    let backend = open(&path);
    assert_eq!(
        read(&backend, AttemptMode::ReadOnly).await.as_deref(),
        Some(b"2".as_slice())
    );
}

// DUR-F1: an attempt that read an unsynced commit and fails with an error
// that carries what it read answers only after the barrier.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_unsynced_write_when_reader_fails_with_its_item_then_the_error_waits_for_the_barrier()
{
    let root = tempfile::tempdir().unwrap();
    let mut backend = open(&root.path().join("store"));
    let gate = gated(&mut backend);
    let backend = Arc::new(backend);
    until(|| gate.synced.load(Ordering::SeqCst) == 1).await;
    let writer = tokio::spawn({
        let backend = Arc::clone(&backend);
        async move {
            write(
                &backend,
                AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                    .start(backend.attempt_deadlines()),
                b"1",
            )
            .await
        }
    });
    until(|| backend.durability.stats().waits == 1).await;
    let mut reader = tokio::spawn({
        let backend = Arc::clone(&backend);
        let gate = Arc::clone(&gate);
        async move {
            let failed = backend
                .with_attempt(
                    AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                        .start(backend.attempt_deadlines()),
                    |attempt| {
                        Box::pin(async move {
                            let values = attempt
                                .get_batch(vec![b"proof".to_vec()], ReadProtection::Snapshot)
                                .await?;
                            assert!(values[0].is_some());
                            Err::<(), _>(
                                storage_types::StorageEnum::ConditionalCheckFailedWithItem {
                                    item: Default::default(),
                                }
                                .into(),
                            )
                        })
                    },
                )
                .await;
            (failed.is_err(), gate.synced.load(Ordering::SeqCst))
        }
    });
    loop {
        tokio::select! {
            finished = &mut reader => panic!("error answered before the barrier: {finished:?}"),
            () = tokio::task::yield_now() => {
                if backend.durability.stats().waits >= 2 {
                    break;
                }
            }
        }
    }
    gate.permits.add_permits(Semaphore::MAX_PERMITS);
    let (failed, barriers_before_reply) = reader.await.unwrap();
    writer.await.unwrap();
    assert!(failed);
    assert!(barriers_before_reply >= 2);
}

/// W2 D11: on a standard queue only the send waits for the disk; receive and
/// delete commit Relaxed and answer without a barrier wait.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_queue_operations_when_they_settle_then_send_waits_and_receive_and_delete_do_not() {
    use queue_provider::{Queue, QueueOperations as _, SendMessageRequest, queue_message_for_send};
    let directory = tempfile::tempdir().unwrap();
    let backend = open(directory.path());
    let batch = Arc::clone(&backend.durability);
    let store =
        crate::KvStore::initialize_format(backend, storage_types::InitialStreamLineages::ONE)
            .await
            .unwrap();
    let url = store
        .create_queue(Queue {
            queue_name: "durability".into(),
            queue_url: "http://localhost/queue/durability".into(),
            attributes: Default::default(),
            created_at: storage_types::TimestampMillis::now(),
            ..Default::default()
        })
        .await
        .unwrap()
        .queue_url;
    let waits = || batch.stats().waits;
    let before = waits();
    store
        .send_message(queue_message_for_send(
            SendMessageRequest {
                queue_url: url.clone(),
                message_body: "durable".into(),
                delay_seconds: None,
                message_attributes: None,
                ..Default::default()
            },
            storage_types::TimestampMillis::now(),
        ))
        .await
        .unwrap();
    let after_send = waits();
    let received = store
        .receive_messages(&url, 1, None, None)
        .await
        .unwrap()
        .messages;
    let after_receive = waits();
    store
        .delete_message(&url, received[0].receipt_handle.as_str().into())
        .await
        .unwrap();
    let after_delete = waits();
    assert_eq!(
        (
            after_send - before,
            after_receive - after_send,
            after_delete - after_receive
        ),
        (1, 0, 0)
    );
}

#[tokio::test]
async fn given_queued_writer_when_predecessor_commits_then_snapshot_includes_predecessor() {
    let root = tempfile::tempdir().unwrap();
    // The predecessor holds the writer on a channel on purpose.
    let backend = Arc::new(
        open(root.path())
            .with_foreign_await_policy(ForeignAwaitPolicy::Count)
            .unwrap(),
    );
    let (entered, entered_wait) = tokio::sync::oneshot::channel();
    let (release, release_wait) = tokio::sync::oneshot::channel();
    let first_backend = Arc::clone(&backend);
    let first = tokio::spawn(async move {
        first_backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                    .start(first_backend.attempt_deadlines()),
                move |attempt| {
                    Box::pin(async move {
                        let sequence = attempt.native_snapshot_sequence().await?.unwrap();
                        attempt.stage(vec![KvMutation::Put {
                            key: b"predecessor".to_vec(),
                            value: vec![1],
                        }])?;
                        entered.send(sequence).unwrap();
                        release_wait.await.unwrap();
                        Ok(sequence)
                    })
                },
            )
            .await
            .unwrap()
    });
    let old_sequence = entered_wait.await.unwrap();
    let (queued, queued_wait) = tokio::sync::oneshot::channel();
    let next_backend = Arc::clone(&backend);
    let next = tokio::spawn(async move {
        next_backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                    .start(next_backend.attempt_deadlines()),
                move |attempt| {
                    Box::pin(async move {
                        queued.send(()).unwrap();
                        let sequence = attempt.native_snapshot_sequence().await?.unwrap();
                        let value = attempt
                            .get(b"predecessor", ReadProtection::Conflict)
                            .await?;
                        assert_eq!(value, Some(vec![1]));
                        Ok(sequence)
                    })
                },
            )
            .await
            .unwrap()
    });
    // The async operation has started while its native worker remains queued.
    queued_wait.await.unwrap();
    assert!(!next.is_finished());
    release.send(()).unwrap();
    assert_eq!(first.await.unwrap(), old_sequence);
    assert!(next.await.unwrap() > old_sequence);
}

// Attempt deadlines. Tokio time is paused and moved by the test; the backend
// sleeps on a manual clock, so no result depends on elapsed real time.

/// A deadline clock that sleeps until the test moves Tokio's paused time past
/// the sleep's end. It starts no Tokio timer, so paused time never jumps.
#[derive(Default)]
struct ManualClock {
    tick: tokio::sync::Notify,
    sleeps: std::sync::atomic::AtomicUsize,
}

impl ManualClock {
    fn sleeps(&self) -> usize {
        self.sleeps.load(Ordering::SeqCst)
    }

    /// Moves paused time just past `until` and wakes every sleeper.
    async fn pass(&self, until: tokio::time::Instant) {
        let target = until + std::time::Duration::from_millis(1);
        let now = tokio::time::Instant::now();
        if target > now {
            tokio::time::advance(target - now).await;
        }
        self.tick.notify_waiters();
    }
}

impl storage_types::Clock for ManualClock {
    fn now(&self) -> storage_types::TimestampMillis {
        storage_types::TimestampMillis::now()
    }

    fn sleep(
        &self,
        duration: std::time::Duration,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = storage_types::StorageResult<()>> + Send + '_>,
    > {
        let end = tokio::time::Instant::now() + duration;
        self.sleeps.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            loop {
                let tick = self.tick.notified();
                let mut tick = std::pin::pin!(tick);
                tick.as_mut().enable();
                if tokio::time::Instant::now() >= end {
                    return Ok(());
                }
                tick.await;
            }
        })
    }
}

fn unavailable(result: &storage_types::StorageResult<()>) -> bool {
    result
        .as_ref()
        .err()
        .and_then(storage_types::StorageError::attempt_kind)
        .is_some_and(storage_types::AttemptKind::is_unavailable)
}

fn long_deadline() -> storage_types::AttemptDeadline {
    storage_types::AttemptDeadlines::try_new(
        [std::time::Duration::from_secs(120); DeadlineClass::COUNT],
        std::time::Duration::from_secs(2),
    )
    .unwrap()
    .start(DeadlineClass::Write)
}

type Hold = (
    tokio::sync::oneshot::Sender<()>,
    tokio::sync::oneshot::Receiver<()>,
);

/// Reads, then stages a put of the proof key, counting callback calls. With
/// `hold`, signals `entered` after the writer answered the read and waits for
/// `release` before it returns.
async fn put(
    backend: &RocksDbBackend,
    deadline: storage_types::AttemptDeadline,
    calls: Arc<AtomicU64>,
    hold: Option<Hold>,
) -> storage_types::StorageResult<()> {
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, deadline.class).started(deadline),
            move |attempt| {
                calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    attempt
                        .get_batch(vec![b"proof".to_vec()], ReadProtection::Conflict)
                        .await?;
                    attempt.stage(vec![KvMutation::Put {
                        key: b"proof".to_vec(),
                        value: b"1".to_vec(),
                    }])?;
                    if let Some((entered, release)) = hold {
                        entered.send(()).unwrap();
                        release.await.unwrap();
                    }
                    Ok(())
                })
            },
        )
        .await
}

#[tokio::test]
async fn given_expired_deadline_when_attempt_starts_then_callback_never_runs_and_nothing_is_written()
 {
    let root = tempfile::tempdir().unwrap();
    let clock = Arc::new(ManualClock::default());
    let backend = open(&root.path().join("store")).with_clock(clock.clone());
    tokio::time::pause();
    let deadline = storage_types::AttemptDeadlines::default().start(DeadlineClass::Write);
    clock.pass(deadline.expires_at).await;
    let calls = Arc::new(AtomicU64::new(0));
    let result = put(&backend, deadline, calls.clone(), None).await;
    assert!(unavailable(&result), "{result:?}");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(backend.durability.stats().pending, 0);
    assert_eq!(read(&backend, AttemptMode::ReadOnly).await, None);
}

#[tokio::test]
async fn given_held_writer_when_slot_wait_deadline_passes_then_it_is_rejected_and_holder_commits() {
    let root = tempfile::tempdir().unwrap();
    let clock = Arc::new(ManualClock::default());
    let backend = Arc::new(
        open(&root.path().join("store"))
            .with_writer_capacity(1)
            .unwrap()
            // The holder waits on a channel on purpose.
            .with_foreign_await_policy(ForeignAwaitPolicy::Count)
            .unwrap()
            .with_clock(clock.clone()),
    );
    tokio::time::pause();
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    let holder = tokio::spawn({
        let backend = Arc::clone(&backend);
        async move {
            put(
                &backend,
                long_deadline(),
                Arc::default(),
                Some((entered, release_rx)),
            )
            .await
        }
    });
    entered_rx.await.unwrap();
    let deadline = storage_types::AttemptDeadlines::default().start(DeadlineClass::Write);
    let calls = Arc::new(AtomicU64::new(0));
    let waiter = tokio::spawn({
        let (backend, calls) = (Arc::clone(&backend), calls.clone());
        async move { put(&backend, deadline, calls, None).await }
    });
    until(|| clock.sleeps() >= 2).await;
    clock.pass(deadline.expires_at).await;
    let result = waiter.await.unwrap();
    assert!(unavailable(&result), "{result:?}");
    assert!(!holder.is_finished(), "the holder is not disturbed");
    release.send(()).unwrap();
    holder.await.unwrap().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        read(&backend, AttemptMode::ReadOnly).await.as_deref(),
        Some(b"1".as_slice())
    );
}

#[tokio::test]
async fn given_deadline_passes_before_commit_when_attempt_finishes_then_batch_is_not_written() {
    let root = tempfile::tempdir().unwrap();
    let clock = Arc::new(ManualClock::default());
    // The attempt waits on a channel on purpose while its deadline passes.
    let backend = Arc::new(
        open(&root.path().join("store"))
            .with_foreign_await_policy(ForeignAwaitPolicy::Count)
            .unwrap()
            .with_clock(clock.clone()),
    );
    tokio::time::pause();
    let deadline = storage_types::AttemptDeadlines::default().start(DeadlineClass::Write);
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    let calls = Arc::new(AtomicU64::new(0));
    let attempt = tokio::spawn({
        let (backend, calls) = (Arc::clone(&backend), calls.clone());
        async move { put(&backend, deadline, calls, Some((entered, release_rx))).await }
    });
    entered_rx.await.unwrap();
    clock.pass(deadline.expires_at).await;
    release.send(()).unwrap();
    let result = attempt.await.unwrap();
    assert!(unavailable(&result), "{result:?}");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(backend.durability.stats().pending, 0);
    assert_eq!(read(&backend, AttemptMode::ReadOnly).await, None);
}

#[tokio::test]
async fn given_missing_durable_acknowledgement_when_settle_by_passes_then_outcome_is_unknown_and_key_survives_reopen()
 {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let mut backend = open(&path);
    let gate = gated(&mut backend);
    let clock = Arc::new(ManualClock::default());
    let backend = Arc::new(backend.with_clock(clock.clone()));
    until(|| gate.synced.load(Ordering::SeqCst) == 1).await;
    tokio::time::pause();
    let deadline = storage_types::AttemptDeadlines::default().start(DeadlineClass::Write);
    let calls = Arc::new(AtomicU64::new(0));
    let attempt = tokio::spawn({
        let (backend, calls) = (Arc::clone(&backend), calls.clone());
        async move { put(&backend, deadline, calls, None).await }
    });
    until(|| backend.durability.stats().waits == 1).await;
    clock.pass(deadline.settle_by()).await;
    let result = attempt.await.unwrap();
    assert!(
        result
            .as_ref()
            .is_err_and(storage_types::StorageError::is_commit_unknown),
        "{result:?}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1, "no replay");
    assert_eq!(
        backend.durability.stats().pending,
        1,
        "the detached settlement keeps the registration"
    );
    gate.permits.add_permits(Semaphore::MAX_PERMITS);
    until(|| backend.durability.stats().pending == 0).await;
    drop(backend);
    tokio::time::resume();
    assert_eq!(
        read(&open(&path), AttemptMode::ReadOnly).await.as_deref(),
        Some(b"1".as_slice())
    );
}

// Writer occupancy (W4 section 7). Counts only; the stepped clock moves one
// millisecond on each read, so no result depends on elapsed real time.

/// Each read of the time is one millisecond later than the one before.
#[derive(Default)]
struct SteppedClock {
    reads: AtomicU64,
}

impl storage_types::Clock for SteppedClock {
    fn now(&self) -> storage_types::TimestampMillis {
        let step = self.reads.fetch_add(1, Ordering::SeqCst);
        storage_types::TimestampMillis::from_timestamp(1_000_000 + i64::try_from(step).unwrap())
    }
}

fn is_foreign_await(result: &storage_types::StorageResult<()>) -> bool {
    matches!(
        result.as_ref().map_err(storage_types::context::WrappedError::to_enum),
        Err(storage_types::StorageEnum::InternalServerError { message })
            if message == "foreign await in a serialized write attempt"
    )
}

async fn write_after_timer(backend: &RocksDbBackend) -> storage_types::StorageResult<()> {
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt.stage(vec![KvMutation::Put {
                        key: b"proof".to_vec(),
                        value: b"timer".to_vec(),
                    }])?;
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    Ok(())
                })
            },
        )
        .await
}

#[tokio::test]
async fn given_fail_policy_when_write_attempt_awaits_a_timer_then_it_fails_without_effect_and_a_put_passes()
 {
    let directory = tempfile::tempdir().unwrap();
    let backend = open(directory.path());

    let result = write_after_timer(&backend).await;

    assert!(is_foreign_await(&result), "{result:?}");
    assert_eq!(backend.writer_counts().foreign_waits, 1);
    assert_eq!(read(&backend, AttemptMode::ReadOnly).await, None);
    write(
        &backend,
        AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
            .start(backend.attempt_deadlines()),
        b"put",
    )
    .await;
    assert_eq!(
        read(&backend, AttemptMode::ReadOnly).await.as_deref(),
        Some(&b"put"[..])
    );
    assert_eq!(backend.writer_counts().foreign_waits, 1);
}

#[tokio::test]
async fn given_count_policy_when_write_attempt_awaits_a_timer_then_it_commits_and_counts_one_foreign_wait()
 {
    let directory = tempfile::tempdir().unwrap();
    let backend = open(directory.path())
        .with_foreign_await_policy(ForeignAwaitPolicy::Count)
        .unwrap();

    write_after_timer(&backend).await.unwrap();

    // Each pending poll of the timer is one hit.
    assert!(backend.writer_counts().foreign_waits >= 1);
    assert_eq!(
        read(&backend, AttemptMode::ReadOnly).await.as_deref(),
        Some(&b"timer"[..])
    );
}

#[tokio::test]
async fn given_three_sequential_puts_when_they_commit_then_writer_counts_busy_and_native_time_and_an_empty_queue()
 {
    let directory = tempfile::tempdir().unwrap();
    let backend = open(directory.path()).with_clock(Arc::new(SteppedClock::default()));

    for value in [&b"one"[..], b"two", b"three"] {
        write(
            &backend,
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            value,
        )
        .await;
    }
    read(&backend, AttemptMode::ReadOnly).await;

    let counts = backend.writer_counts();
    assert!(counts.busy_micros > 0, "{counts:?}");
    assert!(counts.native_micros > 0, "{counts:?}");
    assert_eq!(counts.queue_depth, 0);
    assert_eq!(counts.foreign_waits, 0);
}

/// Bound of every wait of a stopped-attempt test.
#[cfg(feature = "fault-injection")]
const STOP_WAIT: std::time::Duration = std::time::Duration::from_secs(10);

/// Holds an attempt at one commit boundary until released, and tells the test
/// that the boundary was reached.
#[cfg(feature = "fault-injection")]
struct HoldAtBoundary {
    boundary: crate::KvCommitBoundary,
    reached: std::sync::Mutex<std::sync::mpsc::Sender<()>>,
    release: tokio::sync::Notify,
}

#[cfg(feature = "fault-injection")]
impl crate::KvFaultProbe for HoldAtBoundary {
    fn fail(&self, _call: crate::KvFaultCall) -> Option<storage_types::StorageError> {
        None
    }

    fn at_commit_boundary(
        &self,
        boundary: crate::KvCommitBoundary,
        _mode: storage_types::AttemptMode,
    ) -> futures::future::BoxFuture<'_, storage_types::StorageResult<()>> {
        Box::pin(async move {
            if boundary == self.boundary {
                let _ = self.reached.lock().unwrap().send(());
                self.release.notified().await;
            }
            Ok(())
        })
    }
}

/// Runs one write attempt on a runtime that is shut down when the attempt
/// reaches `boundary`, then reads the key through a fresh store on the same
/// directory.
#[cfg(feature = "fault-injection")]
fn stopped_rocksdb_write(
    boundary: crate::KvCommitBoundary,
) -> (storage_types::StorageError, Option<Vec<u8>>) {
    let dir = tempfile::tempdir().unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let handle = runtime.handle().clone();
    let (reached, wait) = std::sync::mpsc::channel();
    let hold = Arc::new(HoldAtBoundary {
        boundary,
        reached: std::sync::Mutex::new(reached),
        release: tokio::sync::Notify::new(),
    });
    let backend = {
        let _context = handle.enter();
        open(dir.path()).with_fault_probe(hold.clone())
    };
    // Shutting the runtime down cancels the attempt task while this thread
    // waits for it outside the runtime.
    let stopper = std::thread::spawn(move || {
        let reached = wait.recv_timeout(STOP_WAIT).is_ok();
        runtime.shutdown_background();
        reached
    });
    let result = {
        let _context = handle.enter();
        futures::executor::block_on(
            backend.with_attempt(
                AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                    .start(backend.attempt_deadlines()),
                |attempt| {
                    Box::pin(async move {
                        attempt.stage(vec![KvMutation::Put {
                            key: b"stopped".to_vec(),
                            value: b"value".to_vec(),
                        }])?;
                        Ok(())
                    })
                },
            ),
        )
    };
    assert!(
        stopper.join().unwrap(),
        "the attempt did not reach {boundary:?} within {STOP_WAIT:?}"
    );
    hold.release.notify_one();
    // Dropping the store joins the writer thread.
    drop(backend);
    let error = result.expect_err("an attempt on a stopped runtime cannot return its value");
    let verifier = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let value = verifier.block_on(async {
        // The stopped runtime drops its tasks (and their handles on the
        // database) asynchronously: the file lock may need a moment.
        let started = std::time::Instant::now();
        let backend = loop {
            match RocksDbBackend::initialize(
                dir.path().to_path_buf(),
                storage_types::AttemptDeadlines::default(),
                256,
            ) {
                Ok(backend) => break backend,
                Err(error) if started.elapsed() < STOP_WAIT => {
                    let _ = error;
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                Err(error) => panic!("reopen did not succeed within {STOP_WAIT:?}: {error}"),
            }
        };
        backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                    .start(backend.attempt_deadlines()),
                |attempt| {
                    Box::pin(async move {
                        let values = attempt
                            .get_batch(vec![b"stopped".to_vec()], ReadProtection::Snapshot)
                            .await?;
                        Ok(values.into_iter().next().flatten())
                    })
                },
            )
            .await
            .unwrap()
    });
    (error, value)
}

#[cfg(feature = "fault-injection")]
#[test]
fn given_write_attempt_stopped_before_commit_submission_when_supervised_then_known_and_not_committed()
 {
    let (error, value) = stopped_rocksdb_write(crate::KvCommitBoundary::BeforeCommit);
    assert!(
        !error.is_commit_unknown(),
        "a stop before commit submission is a known failure: {error:?}"
    );
    assert_eq!(value, None, "the writer must not commit after the stop");
}

#[cfg(feature = "fault-injection")]
#[test]
fn given_write_attempt_stopped_after_commit_submission_when_supervised_then_unknown_and_committed()
{
    let (error, value) =
        stopped_rocksdb_write(crate::KvCommitBoundary::AfterDurableAcknowledgement);
    assert!(
        error.is_commit_unknown(),
        "a stop after commit submission is unknown: {error:?}"
    );
    assert_eq!(value.as_deref(), Some(b"value".as_slice()));
}

/// The optimistic conflict surfaces at COMMIT. The serialized writer queue
/// prevents two attempts of this backend from conflicting, so the native
/// transaction is driven directly with the options the driver uses
/// (`set_snapshot(true)`); the driver's own mapping then classifies the error.
#[test]
fn given_two_writers_same_key_when_second_commits_then_native_busy_conflict_at_commit() {
    use rocksdb::{OptimisticTransactionDB, OptimisticTransactionOptions, WriteOptions};
    use storage_types::{
        AttemptFailure, ConflictCause, NativeCallPhase, NativeCause, NativeCode, NativeEngine,
    };

    use super::error::commit_error;

    let directory = tempfile::tempdir().unwrap();
    let database: OptimisticTransactionDB =
        OptimisticTransactionDB::open_default(directory.path()).unwrap();
    let writes = WriteOptions::default();
    let mut options = OptimisticTransactionOptions::default();
    options.set_snapshot(true);
    let second = database.transaction_opt(&writes, &options);
    let first = database.transaction();
    first.put(b"key", b"first").unwrap();
    first.commit().unwrap();
    second.put(b"key", b"second").unwrap();
    let error = second
        .commit()
        .expect_err("the key changed after the snapshot");
    let classified = storage_common::native_outcome::classify_commit(true, commit_error(error));
    assert_eq!(
        classified.attempt_failure(),
        Some(&AttemptFailure::Conflict(ConflictCause::Native(
            NativeCause {
                engine: NativeEngine::RocksDb,
                code: NativeCode::Named("Busy"),
                phase: NativeCallPhase::Commit,
            }
        )))
    );
}

#[tokio::test]
async fn given_read_of_unsynced_write_when_settle_by_passes_then_known_settle_expiry_not_unknown() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let mut backend = open(&path);
    let gate = gated(&mut backend);
    let clock = Arc::new(ManualClock::default());
    let backend = Arc::new(backend.with_clock(clock.clone()));
    until(|| gate.synced.load(Ordering::SeqCst) == 1).await;
    tokio::time::pause();
    let writer = tokio::spawn({
        let backend = Arc::clone(&backend);
        let deadline = long_deadline();
        async move { put(&backend, deadline, Arc::default(), None).await }
    });
    until(|| backend.durability.stats().waits == 1).await;
    let read_deadline = storage_types::AttemptDeadlines::default().start(DeadlineClass::PointRead);
    let reader = tokio::spawn({
        let backend = Arc::clone(&backend);
        async move {
            backend
                .with_attempt(
                    AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                        .started(read_deadline),
                    |attempt| {
                        Box::pin(async move {
                            attempt
                                .get_batch(vec![b"proof".to_vec()], ReadProtection::Snapshot)
                                .await
                                .map(drop)
                        })
                    },
                )
                .await
        }
    });
    until(|| backend.durability.stats().waits >= 2).await;
    clock.pass(read_deadline.settle_by()).await;
    let error = reader
        .await
        .unwrap()
        .expect_err("the reader's settle bound passed");
    assert!(
        matches!(
            error.attempt_failure(),
            Some(storage_types::AttemptFailure::Unavailable(
                storage_types::NativeUnavailable::Deadline(storage_types::AttemptPhase::Settle)
            ))
        ),
        "{error:?}"
    );
    assert!(!error.is_commit_unknown(), "{error:?}");
    gate.permits.add_permits(Semaphore::MAX_PERMITS);
    writer.await.unwrap().unwrap();
    until(|| backend.durability.stats().pending == 0).await;
    drop(backend);
    tokio::time::resume();
}
