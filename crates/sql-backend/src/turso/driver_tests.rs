use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use storage_types::{AttemptDeadlines, AttemptPlan, DeadlineClass};

use super::driver::TursoBackend;
use crate::{
    AttemptMode, SqlAttempt as _, SqlBackend as _, SqlStatement, test_support::ManualClock,
};

#[tokio::test]
async fn given_held_writer_lease_when_queued_deadline_passes_then_it_is_rejected_without_callback()
{
    let clock = Arc::new(ManualClock::default());
    let backend = Arc::new(
        TursoBackend::initialize(":memory:", AttemptDeadlines::default())
            .await
            .unwrap()
            .with_clock(clock.clone()),
    );
    tokio::time::pause();
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel::<()>();
    let holder = tokio::spawn({
        let backend = Arc::clone(&backend);
        let long = AttemptDeadlines::try_new(
            [std::time::Duration::from_secs(120); DeadlineClass::COUNT],
            std::time::Duration::from_secs(2),
        )
        .unwrap()
        .start(DeadlineClass::Write);
        async move {
            backend
                .with_attempt(
                    AttemptPlan::new(AttemptMode::ReadWrite, long.class).started(long),
                    |attempt| {
                        Box::pin(async move {
                            attempt
                                .execute(SqlStatement::new(
                                    "CREATE TABLE proof (id INTEGER PRIMARY KEY)",
                                    vec![],
                                ))
                                .await?;
                            entered.send(()).unwrap();
                            release_rx.await.unwrap();
                            Ok(())
                        })
                    },
                )
                .await
        }
    });
    entered_rx.await.unwrap();
    let deadline = AttemptDeadlines::default().start(DeadlineClass::Write);
    let calls = Arc::new(AtomicUsize::new(0));
    let waiter = tokio::spawn({
        let (backend, calls) = (Arc::clone(&backend), calls.clone());
        async move {
            backend
                .with_attempt(
                    AttemptPlan::new(AttemptMode::ReadWrite, deadline.class).started(deadline),
                    move |_| {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Box::pin(async { Ok(()) })
                    },
                )
                .await
        }
    });
    while clock.sleeps() == 0 {
        tokio::task::yield_now().await;
    }
    clock.pass(deadline.expires_at).await;
    let result: storage_types::StorageResult<()> = waiter.await.unwrap();
    assert!(
        result
            .unwrap_err()
            .attempt_kind()
            .is_some_and(storage_types::AttemptKind::is_unavailable)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(!holder.is_finished(), "the holder is not disturbed");
    release.send(()).unwrap();
    holder.await.unwrap().unwrap();
}

/// Stops a write attempt at `boundary` on its own runtime, then reads the row
/// through a fresh store on the same file.
#[cfg(feature = "fault-injection")]
fn stopped_turso_write(boundary: crate::SqlCommitBoundary) -> (storage_types::StorageError, usize) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stopped.db");
    let path_text = path.to_str().unwrap().to_owned();
    let opened = path_text.clone();
    let error = crate::fault_injection_tests::stopped_write(boundary, |hold| async move {
        let backend = TursoBackend::initialize(&opened, AttemptDeadlines::default())
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
        let backend = TursoBackend::initialize(&path_text, AttemptDeadlines::default())
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
    let (error, rows) = stopped_turso_write(crate::SqlCommitBoundary::BeforeCommit);
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
    let (error, rows) = stopped_turso_write(crate::SqlCommitBoundary::AfterDurableAcknowledgement);
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

async fn probe_count(backend: &TursoBackend) -> usize {
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .query(
                            SqlStatement::new("SELECT value FROM native_commit_probe", vec![]),
                            crate::ReadProtection::Snapshot,
                            std::num::NonZeroUsize::new(1024).unwrap(),
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

async fn probe_insert(
    backend: &TursoBackend,
    mode: AttemptMode,
    protection: crate::ReadProtection,
) -> storage_types::StorageResult<usize> {
    let class = if mode == AttemptMode::ReadOnly {
        DeadlineClass::PointRead
    } else {
        DeadlineClass::Write
    };
    backend
        .with_attempt(
            AttemptPlan::new(mode, class).start(backend.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    attempt
                        .query(
                            SqlStatement::new(
                                "INSERT INTO native_commit_probe VALUES (1) RETURNING value",
                                vec![],
                            ),
                            protection,
                            std::num::NonZeroUsize::new(1024).unwrap(),
                        )
                        .await
                        .map(|rows| rows.rows.len())
                })
            },
        )
        .await
}

#[tokio::test]
async fn given_read_only_attempt_when_query_writes_then_turso_refuses_it_on_a_query_only_reader() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ro-guard.db");
    let backend = TursoBackend::initialize(path.to_str().unwrap(), AttemptDeadlines::default())
        .await
        .unwrap();
    crate::fault_injection_tests::initialize_probe_table(&backend).await;
    for _ in 0..3 {
        let error = probe_insert(
            &backend,
            AttemptMode::ReadOnly,
            crate::ReadProtection::Snapshot,
        )
        .await
        .expect_err("a ReadOnly write is refused");
        assert_eq!(
            internal_message(&error).as_deref(),
            Some("Turso native operation failed (Error)")
        );
        assert_eq!(probe_count(&backend).await, 0);
    }
    let written = probe_insert(
        &backend,
        AttemptMode::ReadWrite,
        crate::ReadProtection::Conflict,
    )
    .await
    .unwrap();
    assert_eq!(written, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_another_connection_holds_begin_immediate_when_write_attempt_begins_then_it_ends_at_its_deadline()
 {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("busy-begin.db");
    let path_text = path.to_str().unwrap();
    let first = TursoBackend::initialize(path_text, AttemptDeadlines::default())
        .await
        .unwrap();
    crate::fault_injection_tests::initialize_probe_table(&first).await;
    drop(first);
    let backend = TursoBackend::initialize(
        path_text,
        AttemptDeadlines::try_new(
            [std::time::Duration::from_millis(300); DeadlineClass::COUNT],
            std::time::Duration::from_millis(100),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let holder = backend.database().connect().unwrap();
    holder.execute("BEGIN IMMEDIATE", ()).await.unwrap();
    let probe = backend.database().connect().unwrap();
    probe.busy_timeout(std::time::Duration::ZERO).unwrap();
    assert!(
        probe.execute("BEGIN IMMEDIATE", ()).await.is_err(),
        "precondition: Turso BEGIN IMMEDIATE is not exclusive in MVCC; FIN-b Turso bound is \
         unreachable in-process"
    );
    let calls = Arc::new(AtomicUsize::new(0));
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
                            .execute(SqlStatement::new(
                                "INSERT INTO native_commit_probe VALUES (1)",
                                vec![],
                            ))
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
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(
        elapsed < std::time::Duration::from_secs(2),
        "the attempt waited {elapsed:?}"
    );
    holder.execute("ROLLBACK", ()).await.unwrap();
    assert_eq!(probe_count(&backend).await, 0);
}

#[cfg(feature = "fault-injection")]
struct HeldCommit {
    armed: std::sync::atomic::AtomicBool,
    held: tokio::sync::Semaphore, // 0 permits: a held COMMIT waits for one
    reached: AtomicUsize,
    committed: AtomicUsize,
}

#[cfg(feature = "fault-injection")]
impl Default for HeldCommit {
    fn default() -> Self {
        Self {
            armed: std::sync::atomic::AtomicBool::new(false),
            held: tokio::sync::Semaphore::new(0),
            reached: AtomicUsize::new(0),
            committed: AtomicUsize::new(0),
        }
    }
}

#[cfg(feature = "fault-injection")]
impl crate::SqlFaultProbe for HeldCommit {
    fn fail(&self, _: crate::SqlFaultCall<'_>) -> Option<storage_types::StorageError> {
        None
    }

    fn before_wave<'a>(
        &'a self,
        statements: &'a [crate::SqlWaveStatement<'a>],
    ) -> futures::future::BoxFuture<'a, storage_types::StorageResult<()>> {
        Box::pin(async move {
            if statements.iter().any(|s| s.sql == "COMMIT")
                && self.armed.swap(false, Ordering::SeqCst)
            {
                self.reached.fetch_add(1, Ordering::SeqCst);
                self.held.acquire().await.unwrap().forget();
            }
            Ok(())
        })
    }

    fn observe(&self, event: crate::SqlNativeEvent) {
        if matches!(event, crate::SqlNativeEvent::Commit) {
            self.committed.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[cfg(feature = "fault-injection")]
async fn held_commit_store(
    dir: &tempfile::TempDir,
) -> (Arc<TursoBackend>, Arc<ManualClock>, Arc<HeldCommit>) {
    let path = dir.path().join("held-commit.db");
    let backend = TursoBackend::initialize(path.to_str().unwrap(), AttemptDeadlines::default())
        .await
        .unwrap();
    crate::fault_injection_tests::initialize_probe_table(&backend).await;
    let clock = Arc::new(ManualClock::default());
    let probe = Arc::new(HeldCommit::default());
    let backend = Arc::new(
        backend
            .with_clock(clock.clone())
            .with_fault_probe(probe.clone()),
    );
    (backend, clock, probe)
}

#[cfg(feature = "fault-injection")]
const HELD_COMMIT_WAIT: std::time::Duration = std::time::Duration::from_secs(10);

#[cfg(feature = "fault-injection")]
async fn until_reached(probe: &HeldCommit) {
    let waited = tokio::time::timeout(HELD_COMMIT_WAIT, async {
        while probe.reached.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        waited.is_ok(),
        "waited for the held COMMIT to be reached (reached = {})",
        probe.reached.load(Ordering::SeqCst)
    );
}

#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn given_write_commit_held_past_settle_by_when_settled_then_unknown_and_detached_commit_applies()
 {
    let dir = tempfile::tempdir().unwrap();
    let (backend, clock, probe) = held_commit_store(&dir).await;
    tokio::time::pause();
    probe.armed.store(true, Ordering::SeqCst);
    let deadline = AttemptDeadlines::default().start(DeadlineClass::Write);
    let calls = Arc::new(AtomicUsize::new(0));
    let attempt = tokio::spawn({
        let (backend, calls) = (Arc::clone(&backend), calls.clone());
        async move {
            backend
                .with_attempt(
                    AttemptPlan::new(AttemptMode::ReadWrite, deadline.class).started(deadline),
                    move |attempt| {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Box::pin(async move {
                            attempt
                                .execute(SqlStatement::new(
                                    "INSERT INTO native_commit_probe VALUES (1)",
                                    vec![],
                                ))
                                .await
                        })
                    },
                )
                .await
                .map(drop)
        }
    });
    until_reached(&probe).await;
    clock.pass(deadline.settle_by()).await;
    let error = attempt
        .await
        .unwrap()
        .expect_err("the held COMMIT passed settle_by");
    assert!(error.is_commit_unknown(), "{error:?}");
    assert_eq!(calls.load(Ordering::SeqCst), 1, "no replay");
    probe.held.add_permits(1);
    let waited = tokio::time::timeout(HELD_COMMIT_WAIT, async {
        while probe.committed.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(waited.is_ok(), "waited for the detached COMMIT to apply");
    tokio::time::resume();
    assert_eq!(probe_count(&backend).await, 1);
}

#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn given_read_commit_held_past_settle_by_when_settled_then_known_settle_expiry() {
    let dir = tempfile::tempdir().unwrap();
    let (backend, clock, probe) = held_commit_store(&dir).await;
    tokio::time::pause();
    probe.armed.store(true, Ordering::SeqCst);
    let deadline = AttemptDeadlines::default().start(DeadlineClass::PointRead);
    let attempt = tokio::spawn({
        let backend = Arc::clone(&backend);
        async move {
            backend
                .with_attempt(
                    AttemptPlan::new(AttemptMode::ReadOnly, deadline.class).started(deadline),
                    |attempt| {
                        Box::pin(async move {
                            attempt
                                .query(
                                    SqlStatement::new(
                                        "SELECT value FROM native_commit_probe",
                                        vec![],
                                    ),
                                    crate::ReadProtection::Snapshot,
                                    std::num::NonZeroUsize::new(1024).unwrap(),
                                )
                                .await
                                .map(drop)
                        })
                    },
                )
                .await
        }
    });
    until_reached(&probe).await;
    clock.pass(deadline.settle_by()).await;
    let error = attempt
        .await
        .unwrap()
        .expect_err("the held COMMIT passed settle_by");
    assert_eq!(
        deadline_phase(&error),
        Some(storage_types::AttemptPhase::Settle)
    );
    assert!(!error.is_commit_unknown(), "{error:?}");
    probe.held.add_permits(1);
    tokio::time::resume();
}
