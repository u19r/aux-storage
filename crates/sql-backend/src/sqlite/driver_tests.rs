//! Durable acknowledgement on real SQLite: reads, attempts that wrote
//! nothing, relaxed writes, and a second store on the same file.

use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use storage_common::{
    durability::{DurabilityBatch, DurabilityStats},
    writer_occupancy::ForeignAwaitPolicy,
};
use storage_types::{AttemptDurability, AttemptPlan, AttemptRequest, DeadlineClass};
use tokio::sync::Semaphore;

use super::driver::SqliteBackend;
use crate::{AttemptMode, ReadProtection, SqlAttempt as _, SqlBackend as _, SqlStatement};

fn stats(backend: &SqliteBackend) -> DurabilityStats {
    backend
        .durability
        .as_deref()
        .expect("file database has a durability batch")
        .stats()
}

async fn file_backend(path: &std::path::Path) -> SqliteBackend {
    SqliteBackend::initialize(path, storage_types::AttemptDeadlines::default())
        .await
        .unwrap()
        .with_foreign_await_policy(ForeignAwaitPolicy::Fail)
}

async fn new_store(label: &str) -> (SqliteBackend, std::path::PathBuf) {
    let path = std::env::temp_dir().join(format!("{label}-{}.db", uuid::Uuid::now_v7()));
    let backend = file_backend(&path).await;
    execute(
        &backend,
        "CREATE TABLE proof (id INTEGER PRIMARY KEY, n INTEGER)",
    )
    .await;
    (backend, path)
}

fn remove(path: &std::path::Path) {
    for suffix in ["", "-wal", "-shm"] {
        let mut file = path.as_os_str().to_owned();
        file.push(suffix);
        let _ = std::fs::remove_file(file);
    }
}

/// A barrier that syncs nothing and begins only when the gate allows it.
/// The first permit is the startup barrier's.
struct Gate {
    permits: Semaphore,
    synced: AtomicU64,
}

fn gated_batch(gate: &Arc<Gate>) -> Arc<DurabilityBatch> {
    let gate = Arc::clone(gate);
    Arc::new(DurabilityBatch::new(move |start| {
        let gate = Arc::clone(&gate);
        async move {
            gate.permits.acquire().await.unwrap().forget();
            let capture = start.begin(None);
            gate.synced.fetch_add(1, Ordering::SeqCst);
            Ok(capture)
        }
    }))
}

fn gate() -> Arc<Gate> {
    Arc::new(Gate {
        permits: Semaphore::new(1),
        synced: AtomicU64::new(0),
    })
}

async fn execute_with(backend: &SqliteBackend, request: AttemptRequest, sql: &'static str) {
    backend
        .with_attempt(request, move |attempt| {
            Box::pin(async move { attempt.execute(SqlStatement::new(sql, vec![])).await })
        })
        .await
        .unwrap();
}

async fn execute(backend: &SqliteBackend, sql: &'static str) {
    execute_with(
        backend,
        AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
            .start(backend.attempt_deadlines()),
        sql,
    )
    .await;
}

async fn count(backend: &SqliteBackend, mode: AttemptMode) -> usize {
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
                    attempt
                        .query(
                            SqlStatement::new("SELECT id FROM proof", vec![]),
                            if mode == AttemptMode::ReadOnly {
                                ReadProtection::Snapshot
                            } else {
                                ReadProtection::Conflict
                            },
                            NonZeroUsize::new(1024).unwrap(),
                        )
                        .await
                })
            },
        )
        .await
        .unwrap()
        .rows
        .len()
}

async fn until(mut done: impl FnMut() -> bool) {
    while !done() {
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn given_idle_store_when_attempts_write_nothing_then_no_barrier_or_wait_follows() {
    let (backend, path) = new_store("sqlite-durable-idle").await;
    execute(&backend, "INSERT INTO proof VALUES (1, 0)").await;
    let before = stats(&backend);
    for _ in 0..5 {
        assert_eq!(count(&backend, AttemptMode::ReadOnly).await, 1);
        assert_eq!(count(&backend, AttemptMode::ReadWrite).await, 1);
    }
    let after = stats(&backend);
    assert_eq!(
        (after.barriers_started, after.waits),
        (before.barriers_started, before.waits)
    );
    drop(backend);
    remove(&path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_unsynced_write_when_read_only_attempt_reads_it_then_it_returns_after_the_barrier() {
    let (mut backend, path) = new_store("sqlite-durable-reader").await;
    let gate = gate();
    backend.durability = Some(gated_batch(&gate));
    let backend = Arc::new(backend);
    until(|| gate.synced.load(Ordering::SeqCst) == 1).await;

    let writer = tokio::spawn({
        let backend = Arc::clone(&backend);
        async move { execute(&backend, "INSERT INTO proof VALUES (1, 0)").await }
    });
    until(|| stats(&backend).waits == 1).await;
    let mut reader = tokio::spawn({
        let backend = Arc::clone(&backend);
        let gate = Arc::clone(&gate);
        async move {
            let rows = count(&backend, AttemptMode::ReadOnly).await;
            (rows, gate.synced.load(Ordering::SeqCst))
        }
    });
    loop {
        tokio::select! {
            finished = &mut reader => panic!("reader finished before the barrier: {finished:?}"),
            () = tokio::task::yield_now() => {
                if stats(&backend).waits >= 2 {
                    break;
                }
            }
        }
    }
    gate.permits.add_permits(Semaphore::MAX_PERMITS);
    let (rows, barriers_before_reply) = reader.await.unwrap();
    writer.await.unwrap();
    assert_eq!(rows, 1);
    assert!(
        barriers_before_reply >= 2,
        "a barrier after the read covered it"
    );

    drop(backend);
    remove(&path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_relaxed_write_when_it_commits_then_it_returns_without_a_barrier_and_a_later_one_covers_it()
 {
    let (mut backend, path) = new_store("sqlite-durable-relaxed").await;
    let gate = gate();
    backend.durability = Some(gated_batch(&gate));
    until(|| gate.synced.load(Ordering::SeqCst) == 1).await;
    let relaxed = AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
        .with_durability(AttemptDurability::Relaxed)
        .start(backend.attempt_deadlines());
    execute_with(&backend, relaxed, "INSERT INTO proof VALUES (1, 0)").await;
    assert_eq!(
        gate.synced.load(Ordering::SeqCst),
        1,
        "no barrier before the answer"
    );
    assert_eq!(stats(&backend).pending, 1, "the relaxed commit holds");
    gate.permits.add_permits(1);
    until(|| stats(&backend).pending == 0).await;
    assert_eq!(gate.synced.load(Ordering::SeqCst), 2);
    drop(backend);
    remove(&path);
}

// Rule 7: another store (standing in for another process) committed on the
// same file, and its commit is not synced yet.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_unsynced_commit_of_another_store_when_reader_completes_then_it_runs_a_barrier_first()
{
    let (mut writer, path) = new_store("sqlite-durable-external").await;
    let gate = gate();
    writer.durability = Some(gated_batch(&gate));
    let reader = file_backend(&path).await;
    until(|| gate.synced.load(Ordering::SeqCst) == 1 && stats(&reader).barriers_completed == 1)
        .await;
    assert_eq!(count(&reader, AttemptMode::ReadOnly).await, 0);
    let idle = stats(&reader);

    let writer = Arc::new(writer);
    let write = tokio::spawn({
        let writer = Arc::clone(&writer);
        async move { execute(&writer, "INSERT INTO proof VALUES (1, 0)").await }
    });
    until(|| stats(&writer).waits == 1).await;
    assert_eq!(count(&reader, AttemptMode::ReadOnly).await, 1);
    let after = stats(&reader);
    assert_eq!(
        (after.barriers_started, after.waits),
        (idle.barriers_started + 1, idle.waits + 1)
    );
    assert_eq!(count(&reader, AttemptMode::ReadOnly).await, 1);
    assert_eq!(stats(&reader).barriers_started, after.barriers_started);

    gate.permits.add_permits(Semaphore::MAX_PERMITS);
    write.await.unwrap();
    drop((writer, reader));
    remove(&path);
}

// DUR-F1: an attempt that read an unsynced commit and fails with an error
// that carries what it read answers only after the barrier.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_unsynced_write_when_reader_fails_with_its_item_then_the_error_waits_for_the_barrier()
{
    let (mut backend, path) = new_store("sqlite-durable-data-error").await;
    let gate = gate();
    backend.durability = Some(gated_batch(&gate));
    let backend = Arc::new(backend);
    until(|| gate.synced.load(Ordering::SeqCst) == 1).await;
    let writer = tokio::spawn({
        let backend = Arc::clone(&backend);
        async move { execute(&backend, "INSERT INTO proof VALUES (1, 0)").await }
    });
    until(|| stats(&backend).waits == 1).await;
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
                            let page = attempt
                                .query(
                                    SqlStatement::new("SELECT id FROM proof", vec![]),
                                    ReadProtection::Snapshot,
                                    NonZeroUsize::new(1024).unwrap(),
                                )
                                .await?;
                            assert_eq!(page.rows.len(), 1);
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
                if stats(&backend).waits >= 2 {
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
    drop(backend);
    remove(&path);
}

// Attempt deadlines. Tokio time is paused and moved by the test; the backend
// sleeps on a manual clock, so no result depends on elapsed real time.

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

/// Inserts `id`, counting callback calls; with `hold`, signals `entered` and
/// waits for `release` before it returns.
async fn insert(
    backend: &SqliteBackend,
    deadline: storage_types::AttemptDeadline,
    id: i64,
    calls: Arc<AtomicU64>,
    hold: Option<(
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    )>,
) -> storage_types::StorageResult<()> {
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, deadline.class).started(deadline),
            move |attempt| {
                calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    attempt
                        .execute(SqlStatement::new(
                            "INSERT INTO proof VALUES (?1, 0)",
                            vec![crate::SqlValue::Integer(id)],
                        ))
                        .await?;
                    if let Some((entered, release)) = hold {
                        entered.send(()).unwrap();
                        release.await.unwrap();
                    }
                    Ok(())
                })
            },
        )
        .await
        .map(drop)
}

#[tokio::test]
async fn given_expired_deadline_when_attempt_starts_then_callback_never_runs_and_no_row_exists() {
    let (backend, path) = new_store("sqlite-deadline-expired").await;
    let clock = Arc::new(crate::test_support::ManualClock::default());
    let backend = backend.with_clock(clock.clone());
    tokio::time::pause();
    let deadline = storage_types::AttemptDeadlines::default().start(DeadlineClass::Write);
    clock.pass(deadline.expires_at).await;
    let calls = Arc::new(AtomicU64::new(0));
    let result = insert(&backend, deadline, 1, calls.clone(), None).await;
    assert!(unavailable(&result), "{result:?}");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(count(&backend, AttemptMode::ReadWrite).await, 0);
    drop(backend);
    remove(&path);
}

#[tokio::test]
async fn given_held_writer_when_queued_deadline_passes_then_it_is_rejected_without_effects() {
    let (backend, path) = new_store("sqlite-deadline-queued").await;
    let clock = Arc::new(crate::test_support::ManualClock::default());
    // The attempt waits on a channel on purpose while it holds the writer.
    let backend = Arc::new(
        backend
            .with_foreign_await_policy(ForeignAwaitPolicy::Count)
            .with_clock(clock.clone()),
    );
    tokio::time::pause();
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    let holder = tokio::spawn({
        let backend = Arc::clone(&backend);
        async move {
            insert(
                &backend,
                long_deadline(),
                1,
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
        async move { insert(&backend, deadline, 2, calls, None).await }
    });
    until(|| clock.sleeps() >= 2).await;
    clock.pass(deadline.expires_at).await;
    let result = waiter.await.unwrap();
    assert!(unavailable(&result), "{result:?}");
    assert!(!holder.is_finished(), "the holder is not disturbed");
    release.send(()).unwrap();
    holder.await.unwrap().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    // The abandoned closure ran before this writer and began nothing.
    assert_eq!(count(&backend, AttemptMode::ReadWrite).await, 1);
    drop(backend);
    remove(&path);
}

#[tokio::test]
async fn given_deadline_passes_before_commit_when_attempt_settles_then_it_rolls_back() {
    let (backend, path) = new_store("sqlite-deadline-commit").await;
    let clock = Arc::new(crate::test_support::ManualClock::default());
    // The attempt waits on a channel on purpose while it holds the writer.
    let backend = Arc::new(
        backend
            .with_foreign_await_policy(ForeignAwaitPolicy::Count)
            .with_clock(clock.clone()),
    );
    tokio::time::pause();
    let deadline = storage_types::AttemptDeadlines::default().start(DeadlineClass::Write);
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    let attempt = tokio::spawn({
        let backend = Arc::clone(&backend);
        async move {
            insert(
                &backend,
                deadline,
                1,
                Arc::default(),
                Some((entered, release_rx)),
            )
            .await
        }
    });
    entered_rx.await.unwrap();
    clock.pass(deadline.expires_at).await;
    release.send(()).unwrap();
    let result = attempt.await.unwrap();
    assert!(unavailable(&result), "{result:?}");
    assert_eq!(count(&backend, AttemptMode::ReadWrite).await, 0);
    drop(backend);
    remove(&path);
}

#[tokio::test]
async fn given_missing_durable_acknowledgement_when_settle_by_passes_then_outcome_is_unknown_and_row_survives()
 {
    let (mut backend, path) = new_store("sqlite-deadline-durable").await;
    let gate = gate();
    backend.durability = Some(gated_batch(&gate));
    let clock = Arc::new(crate::test_support::ManualClock::default());
    let backend = Arc::new(backend.with_clock(clock.clone()));
    until(|| gate.synced.load(Ordering::SeqCst) == 1).await;
    tokio::time::pause();
    let deadline = storage_types::AttemptDeadlines::default().start(DeadlineClass::Write);
    let calls = Arc::new(AtomicU64::new(0));
    let attempt = tokio::spawn({
        let (backend, calls) = (Arc::clone(&backend), calls.clone());
        async move { insert(&backend, deadline, 1, calls, None).await }
    });
    until(|| stats(&backend).waits == 1).await;
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
        stats(&backend).pending,
        1,
        "the detached settlement keeps the registration"
    );
    gate.permits.add_permits(Semaphore::MAX_PERMITS);
    until(|| stats(&backend).pending == 0).await;
    drop(backend);
    tokio::time::resume();
    assert_eq!(
        count(&file_backend(&path).await, AttemptMode::ReadOnly).await,
        1
    );
    remove(&path);
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

async fn insert_after_timer(backend: &SqliteBackend) -> storage_types::StorageResult<()> {
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .execute(SqlStatement::new(
                            "INSERT INTO proof (n) VALUES (1)",
                            vec![],
                        ))
                        .await?;
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    Ok(())
                })
            },
        )
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn given_fail_policy_when_write_attempt_awaits_a_timer_then_it_fails_without_effect_and_a_put_passes()
 {
    let (backend, path) = new_store("foreign-await-fail").await;

    let result = insert_after_timer(&backend).await;

    assert!(is_foreign_await(&result), "{result:?}");
    assert_eq!(backend.writer_counts().foreign_waits, 1);
    assert_eq!(count(&backend, AttemptMode::ReadOnly).await, 0);
    execute(&backend, "INSERT INTO proof (n) VALUES (2)").await;
    assert_eq!(count(&backend, AttemptMode::ReadOnly).await, 1);
    assert_eq!(backend.writer_counts().foreign_waits, 1);
    remove(&path);
}

#[tokio::test(flavor = "multi_thread")]
async fn given_count_policy_when_write_attempt_awaits_a_timer_then_it_commits_and_counts_one_foreign_wait()
 {
    let (backend, path) = new_store("foreign-await-count").await;
    let backend = backend.with_foreign_await_policy(ForeignAwaitPolicy::Count);

    insert_after_timer(&backend).await.unwrap();

    // Each pending poll of the timer is one hit.
    assert!(backend.writer_counts().foreign_waits >= 1);
    assert_eq!(count(&backend, AttemptMode::ReadOnly).await, 1);
    remove(&path);
}

#[tokio::test(flavor = "multi_thread")]
async fn given_three_sequential_puts_when_they_commit_then_writer_counts_busy_and_native_time_and_an_empty_queue()
 {
    let (backend, path) = new_store("writer-occupancy").await;
    let backend = backend.with_clock(Arc::new(SteppedClock::default()));

    for _ in 0..3 {
        execute(&backend, "INSERT INTO proof (n) VALUES (1)").await;
    }
    assert_eq!(count(&backend, AttemptMode::ReadOnly).await, 3);

    let counts = backend.writer_counts();
    assert!(counts.busy_micros > 0, "{counts:?}");
    assert!(counts.native_micros > 0, "{counts:?}");
    assert_eq!(counts.queue_depth, 0);
    assert_eq!(counts.foreign_waits, 0);
    remove(&path);
}

/// Stops a write attempt at `boundary` on its own runtime, then reads the row
/// through a fresh store on the same file.
#[cfg(feature = "fault-injection")]
fn stopped_sqlite_write(
    boundary: crate::SqlCommitBoundary,
) -> (storage_types::StorageError, usize) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stopped.db");
    let opened = path.clone();
    let error = crate::fault_injection_tests::stopped_write(boundary, |hold| async move {
        let backend =
            SqliteBackend::initialize(&opened, storage_types::AttemptDeadlines::default())
                .await
                .unwrap();
        crate::fault_injection_tests::initialize_probe_table(&backend).await;
        backend.with_fault_probe(hold)
    });
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let rows = runtime.block_on(async {
        let backend = SqliteBackend::initialize(&path, storage_types::AttemptDeadlines::default())
            .await
            .unwrap();
        crate::fault_injection_tests::value_one_rows(&backend).await
    });
    (error, rows)
}

#[cfg(feature = "fault-injection")]
#[test]
fn given_write_attempt_stopped_before_commit_submission_when_supervised_then_known_and_not_committed()
 {
    let (error, rows) = stopped_sqlite_write(crate::SqlCommitBoundary::BeforeCommit);
    assert!(
        !error.is_commit_unknown(),
        "a stop before commit submission is a known failure: {error:?}"
    );
    assert_eq!(rows, 0, "the worker must not commit after the stop");
}

#[cfg(feature = "fault-injection")]
#[test]
fn given_write_attempt_stopped_after_commit_submission_when_supervised_then_unknown_and_committed()
{
    let (error, rows) = stopped_sqlite_write(crate::SqlCommitBoundary::AfterDurableAcknowledgement);
    assert!(
        error.is_commit_unknown(),
        "a stop after commit submission is unknown: {error:?}"
    );
    assert_eq!(rows, 1, "the commit reached the file");
}

fn internal_message(error: &storage_types::StorageError) -> Option<String> {
    match storage_types::context::WrappedError::to_enum(error) {
        storage_types::StorageEnum::InternalServerError { message } => Some(message.clone()),
        _ => None,
    }
}

fn deadline_phase(error: &storage_types::StorageError) -> Option<storage_types::AttemptPhase> {
    match error.attempt_failure() {
        Some(storage_types::AttemptFailure::Unavailable(
            storage_types::NativeUnavailable::Deadline(phase),
        )) => Some(*phase),
        _ => None,
    }
}

#[tokio::test]
async fn given_read_only_attempt_when_query_writes_then_it_is_refused_before_it_runs_on_writer_and_reader_connections()
 {
    let memory = SqliteBackend::initialize(":memory:", storage_types::AttemptDeadlines::default())
        .await
        .unwrap();
    execute(
        &memory,
        "CREATE TABLE proof (id INTEGER PRIMARY KEY, n INTEGER)",
    )
    .await;
    let (file, path) = new_store("sqlite-ro-guard").await;
    for (backend, is_file) in [(&memory, false), (&file, true)] {
        let refused = backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                    .start(backend.attempt_deadlines()),
                |attempt| {
                    Box::pin(async move {
                        attempt
                            .query(
                                SqlStatement::new(
                                    "INSERT INTO proof VALUES (7, 0) RETURNING id",
                                    vec![],
                                ),
                                ReadProtection::Snapshot,
                                NonZeroUsize::new(1024).unwrap(),
                            )
                            .await
                    })
                },
            )
            .await;
        let error = refused.err().expect("a ReadOnly write is refused");
        assert_eq!(
            internal_message(&error).as_deref(),
            Some("SQLite ReadOnly attempt refused a writing statement")
        );
        assert_eq!(count(backend, AttemptMode::ReadOnly).await, 0);
        if is_file {
            assert_eq!(stats(backend).pending, 0);
        }
        let written = backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                    .start(backend.attempt_deadlines()),
                |attempt| {
                    Box::pin(async move {
                        attempt
                            .query(
                                SqlStatement::new(
                                    "INSERT INTO proof VALUES (7, 0) RETURNING id",
                                    vec![],
                                ),
                                ReadProtection::Conflict,
                                NonZeroUsize::new(1024).unwrap(),
                            )
                            .await
                    })
                },
            )
            .await
            .unwrap();
        assert_eq!(written.rows.len(), 1);
        assert_eq!(count(backend, AttemptMode::ReadOnly).await, 1);
    }
    remove(&path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_another_connection_holds_the_write_lock_when_write_attempt_begins_then_it_ends_at_its_deadline()
 {
    let (first, path) = new_store("sqlite-busy-begin").await;
    drop(first);
    let backend = SqliteBackend::initialize(
        &path,
        storage_types::AttemptDeadlines::try_new(
            [std::time::Duration::from_millis(300); DeadlineClass::COUNT],
            std::time::Duration::from_millis(100),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let holder = rusqlite::Connection::open(&path).unwrap();
    holder.execute_batch("BEGIN IMMEDIATE").unwrap();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let started = std::time::Instant::now();
    let result = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            {
                let calls = Arc::clone(&calls);
                move |attempt| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Box::pin(async move {
                        attempt
                            .execute(SqlStatement::new("INSERT INTO proof VALUES (1, 0)", vec![]))
                            .await
                    })
                }
            },
        )
        .await;
    let elapsed = started.elapsed();
    let error = result.expect_err("the busy BEGIN fails");
    assert_eq!(
        deadline_phase(&error),
        Some(storage_types::AttemptPhase::Begin)
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(
        elapsed < std::time::Duration::from_secs(2),
        "the attempt waited {elapsed:?}"
    );
    holder.execute_batch("ROLLBACK").unwrap();
    assert_eq!(count(&backend, AttemptMode::ReadOnly).await, 0);
    remove(&path);
}

#[tokio::test]
async fn given_read_of_unsynced_write_when_settle_by_passes_then_known_settle_expiry_not_unknown() {
    let (mut backend, path) = new_store("sqlite-read-settle").await;
    let gate = gate();
    backend.durability = Some(gated_batch(&gate));
    let clock = Arc::new(crate::test_support::ManualClock::default());
    let backend = Arc::new(backend.with_clock(clock.clone()));
    until(|| gate.synced.load(Ordering::SeqCst) == 1).await;
    tokio::time::pause();
    let writer_deadline = long_deadline();
    let writer = tokio::spawn({
        let backend = Arc::clone(&backend);
        async move { insert(&backend, writer_deadline, 1, Arc::default(), None).await }
    });
    until(|| stats(&backend).waits == 1).await;
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
                                .query(
                                    SqlStatement::new("SELECT id FROM proof", vec![]),
                                    ReadProtection::Snapshot,
                                    NonZeroUsize::new(1024).unwrap(),
                                )
                                .await
                                .map(drop)
                        })
                    },
                )
                .await
        }
    });
    until(|| stats(&backend).waits >= 2).await;
    clock.pass(read_deadline.settle_by()).await;
    let result = reader.await.unwrap();
    let error = result.expect_err("the reader's settle bound passed");
    assert_eq!(
        deadline_phase(&error),
        Some(storage_types::AttemptPhase::Settle)
    );
    assert!(!error.is_commit_unknown(), "{error:?}");
    gate.permits.add_permits(Semaphore::MAX_PERMITS);
    writer.await.unwrap().unwrap();
    until(|| stats(&backend).pending == 0).await;
    drop(backend);
    tokio::time::resume();
    remove(&path);
}
