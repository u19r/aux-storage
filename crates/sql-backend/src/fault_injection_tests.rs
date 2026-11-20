use std::{
    future::Future,
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use futures::future::BoxFuture;
use storage_types::{AttemptPlan, DeadlineClass, StorageEnum, StorageError, StorageResult};

use crate::{
    AttemptMode, ReadProtection, SqlAttempt, SqlBackend, SqlCommitBoundary, SqlFaultCall,
    SqlFaultProbe, SqlStatement,
};

struct CommitFailure {
    boundary: SqlCommitBoundary,
    pending: AtomicBool,
}

impl CommitFailure {
    fn new(boundary: SqlCommitBoundary) -> Arc<Self> {
        Arc::new(Self {
            boundary,
            pending: AtomicBool::new(true),
        })
    }
}

impl SqlFaultProbe for CommitFailure {
    fn fail(&self, _call: SqlFaultCall<'_>) -> Option<StorageError> {
        None
    }

    fn at_commit_boundary(
        &self,
        boundary: SqlCommitBoundary,
        _mode: storage_types::AttemptMode,
    ) -> BoxFuture<'_, StorageResult<()>> {
        Box::pin(async move {
            if boundary == self.boundary && self.pending.swap(false, Ordering::SeqCst) {
                return Err(StorageError::commit_unknown("deliberate boundary failure"));
            }
            Ok(())
        })
    }
}

#[cfg(any(feature = "sqlite", feature = "turso"))]
pub(crate) async fn initialize_probe_table(backend: &impl SqlBackend) {
    backend
        .with_schema_attempt(|attempt| {
            Box::pin(async move {
                attempt
                    .execute(SqlStatement::new(
                        "CREATE TABLE native_commit_probe (value BIGINT NOT NULL)",
                        vec![],
                    ))
                    .await
            })
        })
        .await
        .unwrap();
}

async fn assert_native_boundary(backend: &impl SqlBackend, boundary: SqlCommitBoundary) {
    let result = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            |attempt| {
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
        .await;
    let error = result.expect_err("the boundary suppresses the native result");
    let expected_rows = match boundary {
        SqlCommitBoundary::BeforeCommit => {
            assert!(matches!(
                error.as_ref(),
                StorageEnum::Attempt(storage_types::AttemptFailure::Conflict(_))
            ));
            0
        }
        SqlCommitBoundary::AfterDurableAcknowledgement => {
            assert!(matches!(
                error.as_ref(),
                StorageEnum::Attempt(storage_types::AttemptFailure::CommitUnknown(_))
            ));
            1
        }
    };
    let page = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .query(
                            SqlStatement::new("SELECT value FROM native_commit_probe", vec![]),
                            ReadProtection::Snapshot,
                            NonZeroUsize::new(4096).unwrap(),
                        )
                        .await
                })
            },
        )
        .await
        .unwrap();
    assert_eq!(page.rows.len(), expected_rows);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn given_sqlite_commit_boundary_failure_when_attempt_settles_then_outcome_matches_durable_rows()
 {
    for boundary in [
        SqlCommitBoundary::BeforeCommit,
        SqlCommitBoundary::AfterDurableAcknowledgement,
    ] {
        let backend = crate::sqlite::SqliteBackend::initialize(
            ":memory:",
            storage_types::AttemptDeadlines::default(),
        )
        .await
        .unwrap();
        initialize_probe_table(&backend).await;
        let backend = backend.with_fault_probe(CommitFailure::new(boundary));
        assert_native_boundary(&backend, boundary).await;
    }
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn given_turso_commit_boundary_failure_when_attempt_settles_then_outcome_matches_durable_rows()
 {
    for boundary in [
        SqlCommitBoundary::BeforeCommit,
        SqlCommitBoundary::AfterDurableAcknowledgement,
    ] {
        let backend = crate::turso::TursoBackend::initialize(
            ":memory:",
            storage_types::AttemptDeadlines::default(),
        )
        .await
        .unwrap();
        initialize_probe_table(&backend).await;
        let backend = backend.with_fault_probe(CommitFailure::new(boundary));
        assert_native_boundary(&backend, boundary).await;
    }
}

#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_postgres_commit_boundary_failure_when_attempt_settles_then_outcome_matches_durable_rows()
 {
    for boundary in [
        SqlCommitBoundary::BeforeCommit,
        SqlCommitBoundary::AfterDurableAcknowledgement,
    ] {
        let fixture = crate::postgres::test_support::NativeFixture::open().await;
        let backend = fixture
            .open_independent_backend()
            .await
            .with_fault_probe(CommitFailure::new(boundary));
        assert_native_boundary(&backend, boundary).await;
        drop(backend);
        fixture.close().await;
    }
}

#[cfg(any(feature = "sqlite", feature = "turso"))]
async fn assert_unsupported_terminal_rolls_back(backend: &impl SqlBackend) {
    initialize_probe_table(backend).await;
    let result = backend.with_attempt(AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()), |attempt| {
        Box::pin(async move {
            attempt.execute(SqlStatement::new("INSERT INTO native_commit_probe VALUES (1)", vec![])).await?;
            let error = attempt.finish_write_batch(vec![], NonZeroUsize::new(4096).unwrap()).await.err().expect("unsupported terminal capability");
            assert!(matches!(error.as_ref(), StorageEnum::InternalServerError { message } if message == "terminal counted writes require Postgres"));
            Ok(())
        })
    }).await;
    let page = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .query(
                            SqlStatement::new("SELECT value FROM native_commit_probe", vec![]),
                            ReadProtection::Snapshot,
                            NonZeroUsize::new(4096).unwrap(),
                        )
                        .await
                })
            },
        )
        .await
        .unwrap();
    assert_eq!(
        page.rows.len(),
        0,
        "caught unsupported error must roll back the prior write"
    );
    let error = result.expect_err("unsupported capability permanently rejects its attempt");
    assert!(
        matches!(error.as_ref(), StorageEnum::InternalServerError { message } if message == "terminal counted writes require Postgres")
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn given_sqlite_prior_write_when_unsupported_terminal_error_is_caught_then_no_write_commits()
{
    let backend = crate::sqlite::SqliteBackend::initialize(
        ":memory:",
        storage_types::AttemptDeadlines::default(),
    )
    .await
    .unwrap();
    assert_unsupported_terminal_rolls_back(&backend).await;
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn given_turso_prior_write_when_unsupported_terminal_error_is_caught_then_no_write_commits() {
    let backend = crate::turso::TursoBackend::initialize(
        ":memory:",
        storage_types::AttemptDeadlines::default(),
    )
    .await
    .unwrap();
    assert_unsupported_terminal_rolls_back(&backend).await;
}

/// Bound of every wait of a stopped-attempt test.
pub(crate) const STOP_WAIT: Duration = Duration::from_secs(10);

/// Holds an attempt at one commit boundary until released, and tells the test
/// that the boundary was reached.
pub(crate) struct HoldAtBoundary {
    boundary: SqlCommitBoundary,
    reached: Mutex<std::sync::mpsc::Sender<()>>,
    release: tokio::sync::Notify,
}

impl SqlFaultProbe for HoldAtBoundary {
    fn fail(&self, _call: SqlFaultCall<'_>) -> Option<StorageError> {
        None
    }

    fn at_commit_boundary(
        &self,
        boundary: SqlCommitBoundary,
        _mode: storage_types::AttemptMode,
    ) -> BoxFuture<'_, StorageResult<()>> {
        Box::pin(async move {
            if boundary == self.boundary {
                let _ = self.reached.lock().unwrap().send(());
                self.release.notified().await;
            }
            Ok(())
        })
    }
}

/// Runs one write attempt (one row, value 1, into `native_commit_probe`) on a
/// runtime that is shut down when the attempt reaches `boundary`. `open` runs
/// inside that runtime and returns the backend with the given probe attached
/// and `native_commit_probe` created. Returns the attempt's error after the
/// hold was released and the backend dropped; the caller then reads the row
/// with a fresh backend in its own runtime (`value_one_rows`).
pub(crate) fn stopped_write<B, Fut>(
    boundary: SqlCommitBoundary,
    open: impl FnOnce(Arc<HoldAtBoundary>) -> Fut,
) -> StorageError
where
    B: SqlBackend,
    Fut: Future<Output = B>,
{
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let handle = runtime.handle().clone();
    let (reached, wait) = std::sync::mpsc::channel();
    let hold = Arc::new(HoldAtBoundary {
        boundary,
        reached: Mutex::new(reached),
        release: tokio::sync::Notify::new(),
    });
    let backend = handle.block_on(open(Arc::clone(&hold)));
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
                        attempt
                            .execute(SqlStatement::new(
                                "INSERT INTO native_commit_probe VALUES (1)",
                                vec![],
                            ))
                            .await
                            .map(|_| ())
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
    drop(backend);
    result.expect_err("an attempt on a stopped runtime cannot return its value")
}

/// Rows of value 1 in `native_commit_probe`, read through a fresh backend.
/// A marker write first proves that the stopped attempt's native writer ended
/// (a worker that still holds the write lock refuses or delays it).
pub(crate) async fn value_one_rows(backend: &impl SqlBackend) -> usize {
    let started = Instant::now();
    loop {
        let marker = backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                    .start(backend.attempt_deadlines()),
                |attempt| {
                    Box::pin(async move {
                        attempt
                            .execute(SqlStatement::new(
                                "INSERT INTO native_commit_probe VALUES (2)",
                                vec![],
                            ))
                            .await
                            .map(|_| ())
                    })
                },
            )
            .await;
        match marker {
            Ok(()) => break,
            Err(error) if started.elapsed() < STOP_WAIT => {
                let _ = error;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => panic!(
                "the marker write did not commit within {STOP_WAIT:?} after the stop: {error}"
            ),
        }
    }
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .query(
                            SqlStatement::new(
                                "SELECT value FROM native_commit_probe WHERE value = 1",
                                vec![],
                            ),
                            ReadProtection::Snapshot,
                            NonZeroUsize::new(4096).unwrap(),
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
