use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures::future::BoxFuture;
use storage_types::{
    AttemptDeadline, AttemptMode, AttemptPlan, DeadlineClass, StorageError, StorageResult,
};

use super::test_support::NativeFixture;
use crate::{
    ReadProtection, SqlAttempt, SqlBackend, SqlCommitBoundary, SqlFaultCall, SqlFaultProbe,
    SqlNativeEvent, SqlStatement, SqlWaveStatement,
};

fn deadline() -> AttemptDeadline {
    AttemptDeadline {
        class: DeadlineClass::Write,
        expires_at: tokio::time::Instant::now() + Duration::from_secs(2),
        commit_grace: Duration::from_secs(2),
    }
}

fn statement(sql: &'static str) -> SqlStatement {
    SqlStatement::new(sql, vec![])
}
fn bytes() -> NonZeroUsize {
    NonZeroUsize::new(4096).unwrap()
}

struct DeadlineProbe {
    cutoff: tokio::time::Instant,
    hold_before_commit: bool,
    hold_after_commit: bool,
    hold_before_preparation: bool,
    fail_after_commit: bool,
    commits: AtomicUsize,
    aborts: AtomicUsize,
}

impl DeadlineProbe {
    fn new(deadline: AttemptDeadline) -> Self {
        Self {
            cutoff: deadline.expires_at,
            hold_before_commit: false,
            hold_after_commit: false,
            hold_before_preparation: false,
            fail_after_commit: false,
            commits: AtomicUsize::new(0),
            aborts: AtomicUsize::new(0),
        }
    }
}

impl SqlFaultProbe for DeadlineProbe {
    fn fail(&self, _: SqlFaultCall<'_>) -> Option<StorageError> {
        None
    }
    fn at_commit_boundary(
        &self,
        boundary: SqlCommitBoundary,
        _mode: storage_types::AttemptMode,
    ) -> BoxFuture<'_, StorageResult<()>> {
        Box::pin(async move {
            if boundary == SqlCommitBoundary::BeforeCommit && self.hold_before_commit {
                // Both the callback and timer become ready at this exact
                // cutoff. The callback-first poll must still refuse final
                // dispatch.
                tokio::time::sleep_until(self.cutoff).await;
            }
            if boundary == SqlCommitBoundary::AfterDurableAcknowledgement && self.fail_after_commit
            {
                // The existing fault boundary deliberately maps any post-ACK
                // hook failure to native unknown (suppressed confirmation).
                return Err(storage_types::StorageEnum::Attempt(
                    storage_types::AttemptFailure::Conflict(storage_types::ConflictCause::Logical(
                        "post-commit hook conflict",
                    )),
                )
                .into());
            }
            if boundary == SqlCommitBoundary::AfterDurableAcknowledgement && self.hold_after_commit
            {
                std::future::pending::<()>().await;
            }
            Ok(())
        })
    }
    fn before_wave<'a>(
        &'a self,
        statements: &'a [SqlWaveStatement<'a>],
    ) -> BoxFuture<'a, StorageResult<()>> {
        Box::pin(async move {
            if self.hold_before_preparation
                && !statements
                    .iter()
                    .any(|statement| statement.sql == "ROLLBACK")
            {
                std::future::pending::<()>().await;
            }
            Ok(())
        })
    }
    fn observe(&self, event: SqlNativeEvent) {
        match event {
            SqlNativeEvent::Commit => {
                self.commits.fetch_add(1, Ordering::Relaxed);
            }
            SqlNativeEvent::Rollback => {
                self.aborts.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }
}

async fn check_expired_gate(route: u8) {
    let fixture = NativeFixture::open().await;
    fixture
        .admin
        .batch_execute(&format!(
            "CREATE TABLE {0}.counted_deadline (value bigint PRIMARY KEY, payload bigint NOT \
             NULL); CREATE TABLE {0}.sys_system_stream_scope (slot bigint NOT NULL)",
            fixture.schema
        ))
        .await
        .unwrap();
    let backend = fixture.open_independent_backend().await;
    let cutoff = deadline();
    let mut probe = DeadlineProbe::new(cutoff);
    probe.hold_before_commit = true;
    let probe = Arc::new(probe);
    let backend = backend.with_fault_probe(probe.clone());
    let mode = if route == 1 {
        AttemptMode::ReadOnly
    } else {
        AttemptMode::ReadWrite
    };
    let result = backend
        .with_attempt(
            AttemptPlan::new(mode, cutoff.class).started(cutoff),
            move |attempt| {
                Box::pin(async move {
                    match route {
                        0 => {
                            attempt
                                .execute(statement("INSERT INTO native_commit_probe VALUES (1)"))
                                .await?;
                        }
                        1 => {
                            attempt
                                .finish_read_batch(
                                    statement("SELECT value FROM native_commit_probe"),
                                    vec![],
                                    bytes(),
                                )
                                .await?;
                        }
                        2 => {
                            let counted = crate::write_batch::UpsertShape::try_new(
                                vec!["value".into(), "payload".into()],
                                vec![false, false],
                                vec!["value".into()],
                            )?
                            .render_counted(
                                crate::SqlEngine::Postgres,
                                "counted_deadline",
                                vec![vec![
                                    crate::SqlValue::Integer(1),
                                    crate::SqlValue::Integer(7),
                                ]],
                            )?;
                            attempt
                                .finish_write_batch(vec![counted], bytes())
                                .await?
                                .map_err(|_| {
                                    StorageError::internal("unexpected counted failure")
                                })?;
                        }
                        _ => unreachable!(),
                    }
                    Ok(())
                })
            },
        )
        .await;
    let error = result.unwrap_err();
    // Before COMMIT is sent an expiry is a known abort: the connection is
    // dropped without ROLLBACK and never returns to the pool.
    assert!(!error.is_commit_unknown(), "{error:?}");
    assert!(
        error
            .attempt_kind()
            .is_some_and(storage_types::AttemptKind::is_unavailable),
        "{error:?}"
    );
    assert_eq!(probe.commits.load(Ordering::Relaxed), 0);
    assert_eq!(probe.aborts.load(Ordering::Relaxed), 0);
    assert_eq!(backend.pool.status().size, 0);
    assert_eq!(fixture.count_records().await, 0);
    assert_eq!(
        fixture
            .admin
            .query_one(
                &format!("SELECT count(*) FROM {}.counted_deadline", fixture.schema),
                &[]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    drop(backend);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_ready_expired_ordinary_commit_hook_when_finalizing_then_known_abort_drops_connection()
 {
    check_expired_gate(0).await;
}
#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_ready_expired_read_commit_hook_when_finalizing_then_known_abort_drops_connection() {
    check_expired_gate(1).await;
}
#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_ready_expired_counted_commit_hook_when_finalizing_then_known_abort_drops_connection()
{
    check_expired_gate(2).await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_no_database_work_when_callback_expires_then_no_abort_or_commit_is_sent() {
    let fixture = NativeFixture::open().await;
    let backend = fixture.open_independent_backend().await;
    let cutoff = deadline();
    let probe = Arc::new(DeadlineProbe::new(cutoff));
    let backend = backend.with_fault_probe(probe.clone());
    let result = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, cutoff.class).started(cutoff),
            |_| Box::pin(std::future::pending::<StorageResult<()>>()),
        )
        .await;
    assert!(
        result
            .unwrap_err()
            .attempt_kind()
            .is_some_and(storage_types::AttemptKind::is_unavailable)
    );
    assert_eq!(probe.commits.load(Ordering::Relaxed), 0);
    assert_eq!(probe.aborts.load(Ordering::Relaxed), 0);
    drop(backend);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_held_native_query_when_deadline_expires_then_connection_drop_preserves_no_writes() {
    let fixture = NativeFixture::open().await;
    let backend = fixture.open_independent_backend().await;
    let cutoff = deadline();
    let probe = Arc::new(DeadlineProbe::new(cutoff));
    let backend = backend.with_fault_probe(probe.clone());
    let (sent, received) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let result: StorageResult<()> = backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadWrite, cutoff.class).started(cutoff),
                |attempt| {
                    Box::pin(async move {
                        attempt
                            .execute(statement("INSERT INTO native_commit_probe VALUES (1)"))
                            .await?;
                        let page = attempt
                            .query(
                                statement("SELECT pg_backend_pid()::bigint"),
                                ReadProtection::Snapshot,
                                bytes(),
                            )
                            .await?;
                        let crate::SqlValue::Integer(pid) =
                            page.rows[0].value(crate::ColumnIndex::new(0))?
                        else {
                            panic!("backend pid")
                        };
                        sent.send(*pid as i32).unwrap();
                        attempt
                            .query(
                                statement("SELECT 1::bigint FROM pg_sleep(30)"),
                                ReadProtection::Snapshot,
                                bytes(),
                            )
                            .await?;
                        panic!("expired callback must not resume");
                    })
                },
            )
            .await;
        (backend, result)
    });
    let pid = received.await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let blocked: bool = fixture
                .admin
                .query_one(
                    "SELECT wait_event = 'PgSleep' FROM pg_stat_activity WHERE pid = $1",
                    &[&pid],
                )
                .await
                .unwrap()
                .get::<_, Option<bool>>(0)
                .unwrap_or(false);
            if blocked {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("query reached actual server wait");
    let (backend, result) = task.await.unwrap();
    assert!(
        result
            .unwrap_err()
            .attempt_kind()
            .is_some_and(storage_types::AttemptKind::is_unavailable)
    );
    assert_eq!(
        probe.aborts.load(Ordering::Relaxed),
        0,
        "no ROLLBACK and no wait for it"
    );
    assert_eq!(probe.commits.load(Ordering::Relaxed), 0);
    assert_eq!(backend.pool.status().size, 0);
    assert_eq!(fixture.count_records().await, 0);
    drop(backend);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_confirmed_commit_when_post_commit_hook_blocks_then_failure_is_nonretryable() {
    let fixture = NativeFixture::open().await;
    let backend = fixture.open_independent_backend().await;
    let cutoff = deadline();
    let mut probe = DeadlineProbe::new(cutoff);
    probe.hold_after_commit = true;
    let probe = Arc::new(probe);
    let backend = backend.with_fault_probe(probe.clone());
    let result = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, cutoff.class).started(cutoff),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .execute(statement("INSERT INTO native_commit_probe VALUES (1)"))
                        .await?;
                    Ok(())
                })
            },
        )
        .await;
    let error = result.unwrap_err();
    assert!(!error.is_commit_unknown(), "{error:?}");
    assert!(
        !error
            .attempt_kind()
            .is_some_and(|kind| kind != storage_types::AttemptKind::CommitUnknown),
        "{error:?}"
    );
    assert!(!error.class().is_retryable());
    assert_eq!(probe.commits.load(Ordering::Relaxed), 1);
    assert_eq!(probe.aborts.load(Ordering::Relaxed), 0);
    assert_eq!(fixture.count_records().await, 1);
    drop(backend);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_confirmed_counted_write_when_callback_rejects_or_catches_retry_control_then_replay_is_forbidden()
 {
    let fixture = NativeFixture::open().await;
    fixture
        .admin
        .batch_execute(&format!(
            "CREATE TABLE {0}.counted_deadline (value bigint PRIMARY KEY, payload bigint NOT \
             NULL); CREATE TABLE {0}.sys_system_stream_scope (slot bigint NOT NULL)",
            fixture.schema
        ))
        .await
        .unwrap();
    for caught in [false, true] {
        let backend = fixture.open_independent_backend().await;
        let result = backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                    .scoped(storage_types::AttemptScope::Data)
                    .start(backend.attempt_deadlines()),
                move |attempt| {
                    Box::pin(async move {
                        let counted = crate::write_batch::UpsertShape::try_new(
                            vec!["value".into(), "payload".into()],
                            vec![false, false],
                            vec!["value".into()],
                        )?
                        .render_counted(
                            crate::SqlEngine::Postgres,
                            "counted_deadline",
                            vec![vec![
                                crate::SqlValue::Integer(i64::from(caught)),
                                crate::SqlValue::Integer(7),
                            ]],
                        )?;
                        attempt
                            .finish_write_batch(vec![counted], bytes())
                            .await?
                            .map_err(|_| StorageError::internal("unexpected counted failure"))?;
                        let error: StorageError = storage_types::StorageEnum::Attempt(
                            storage_types::AttemptFailure::Conflict(
                                storage_types::ConflictCause::Logical(
                                    "callback conflict after committed write",
                                ),
                            ),
                        )
                        .into();
                        attempt.reject_settlement_error(&error);
                        if caught { Ok(()) } else { Err(error) }
                    })
                },
            )
            .await;
        let error = result.expect_err(
            "confirmed writes cannot expose an Aborted result or success after callback rejection",
        );
        assert!(!error.is_commit_unknown());
        assert!(
            !error
                .attempt_kind()
                .is_some_and(|kind| kind != storage_types::AttemptKind::CommitUnknown),
            "{error:?}"
        );
        assert!(!error.class().is_retryable());
        drop(backend);
    }
    assert_eq!(
        fixture
            .admin
            .query_one(
                &format!("SELECT count(*) FROM {}.counted_deadline", fixture.schema),
                &[]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        2
    );
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_confirmed_terminal_write_when_callback_never_returns_then_nonretryable_failure_preserves_write()
 {
    let fixture = NativeFixture::open().await;
    fixture
        .admin
        .batch_execute(&format!(
            "CREATE TABLE {0}.counted_deadline (value bigint PRIMARY KEY, payload bigint NOT \
             NULL); CREATE TABLE {0}.sys_system_stream_scope (slot bigint NOT NULL)",
            fixture.schema
        ))
        .await
        .unwrap();
    let backend = fixture.open_independent_backend().await;
    let cutoff = deadline();
    let result: StorageResult<()> = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, cutoff.class).started(cutoff),
            move |attempt| {
                Box::pin(async move {
                    let counted = crate::write_batch::UpsertShape::try_new(
                        vec!["value".into(), "payload".into()],
                        vec![false, false],
                        vec!["value".into()],
                    )?
                    .render_counted(
                        crate::SqlEngine::Postgres,
                        "counted_deadline",
                        vec![vec![
                            crate::SqlValue::Integer(1),
                            crate::SqlValue::Integer(7),
                        ]],
                    )?;
                    attempt
                        .finish_write_batch(vec![counted], bytes())
                        .await?
                        .map_err(|_| StorageError::internal("unexpected counted failure"))?;
                    std::future::pending().await
                })
            },
        )
        .await;
    let error = result.unwrap_err();
    assert!(!error.is_commit_unknown());
    assert!(
        !error
            .attempt_kind()
            .is_some_and(|kind| kind != storage_types::AttemptKind::CommitUnknown)
    );
    assert!(!error.class().is_retryable());
    assert_eq!(
        fixture
            .admin
            .query_one(
                &format!("SELECT count(*) FROM {}.counted_deadline", fixture.schema),
                &[]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    drop(backend);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_counted_confirmation_suppressed_by_native_boundary_when_callback_catches_or_propagates_then_unknown_is_retained()
 {
    let fixture = NativeFixture::open().await;
    fixture
        .admin
        .batch_execute(&format!(
            "CREATE TABLE {0}.counted_deadline (value bigint PRIMARY KEY, payload bigint NOT \
             NULL); CREATE TABLE {0}.sys_system_stream_scope (slot bigint NOT NULL)",
            fixture.schema
        ))
        .await
        .unwrap();
    for caught in [false, true] {
        let backend = fixture.open_independent_backend().await;
        let mut probe = DeadlineProbe::new(deadline());
        probe.fail_after_commit = true;
        let backend = backend.with_fault_probe(Arc::new(probe));
        let result = backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                    .scoped(storage_types::AttemptScope::Data)
                    .start(backend.attempt_deadlines()),
                move |attempt| {
                    Box::pin(async move {
                        let counted = crate::write_batch::UpsertShape::try_new(
                            vec!["value".into(), "payload".into()],
                            vec![false, false],
                            vec!["value".into()],
                        )?
                        .render_counted(
                            crate::SqlEngine::Postgres,
                            "counted_deadline",
                            vec![vec![
                                crate::SqlValue::Integer(i64::from(caught)),
                                crate::SqlValue::Integer(7),
                            ]],
                        )?;
                        let error = attempt
                            .finish_write_batch(vec![counted], bytes())
                            .await
                            .err()
                            .expect("post-commit hook failed");
                        if caught { Ok(()) } else { Err(error) }
                    })
                },
            )
            .await;
        assert!(
            result
                .expect_err("no Aborted result after confirmed commit")
                .is_commit_unknown()
        );
        drop(backend);
    }
    assert_eq!(
        fixture
            .admin
            .query_one(
                &format!("SELECT count(*) FROM {}.counted_deadline", fixture.schema),
                &[]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        2
    );
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_blocked_nonfinal_counted_window_when_deadline_expires_then_dropped_connection_prevents_final_commit()
 {
    let fixture = NativeFixture::open().await;
    let lock = i64::from_le_bytes(uuid::Uuid::new_v4().as_bytes()[..8].try_into().unwrap());
    fixture
        .admin
        .query_one("SELECT pg_advisory_lock($1)", &[&lock])
        .await
        .unwrap();
    fixture
        .admin
        .batch_execute(&format!(
            "CREATE TABLE {0}.counted_deadline (value bigint PRIMARY KEY, payload bigint NOT \
             NULL); CREATE FUNCTION {0}.hold_counted() RETURNS trigger LANGUAGE plpgsql AS $$ \
             BEGIN PERFORM pg_advisory_xact_lock({lock}); RETURN NEW; END $$; CREATE TRIGGER \
             hold_counted BEFORE INSERT ON {0}.counted_deadline FOR EACH ROW EXECUTE FUNCTION \
             {0}.hold_counted()",
            fixture.schema,
        ))
        .await
        .unwrap();
    let backend = fixture.open_independent_backend().await;
    let cutoff = deadline();
    let probe = Arc::new(DeadlineProbe::new(cutoff));
    let backend = backend.with_fault_probe(probe.clone());
    let (sent, received) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let result = backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadWrite, cutoff.class).started(cutoff),
                |attempt| {
                    Box::pin(async move {
                        let page = attempt
                            .query(
                                statement("SELECT pg_backend_pid()::bigint"),
                                ReadProtection::Snapshot,
                                bytes(),
                            )
                            .await?;
                        let crate::SqlValue::Integer(pid) =
                            page.rows[0].value(crate::ColumnIndex::new(0))?
                        else {
                            panic!("backend pid")
                        };
                        sent.send(*pid as i32).unwrap();
                        let shape = crate::write_batch::UpsertShape::try_new(
                            vec!["value".into(), "payload".into()],
                            vec![false, false],
                            vec!["value".into()],
                        )?;
                        let writes = (0..=crate::constants::POSTGRES_PIPELINE_STATEMENTS)
                            .map(|value| {
                                shape.render_counted(
                                    crate::SqlEngine::Postgres,
                                    "counted_deadline",
                                    vec![vec![
                                        crate::SqlValue::Integer(value as i64),
                                        crate::SqlValue::Integer(7),
                                    ]],
                                )
                            })
                            .collect::<StorageResult<Vec<_>>>()?;
                        attempt
                            .finish_write_batch(writes, bytes())
                            .await?
                            .map_err(|_| StorageError::internal("unexpected counted failure"))?;
                        Ok(())
                    })
                },
            )
            .await;
        (backend, result)
    });
    let pid = received.await.unwrap();
    let blocked = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let blocked: bool = fixture
                .admin
                .query_one(
                    "SELECT pg_backend_pid() = ANY(pg_blocking_pids($1))",
                    &[&pid],
                )
                .await
                .unwrap()
                .get(0);
            if blocked {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let outcome = tokio::time::timeout(Duration::from_secs(60), task).await;
    fixture
        .admin
        .query_one("SELECT pg_advisory_unlock($1)", &[&lock])
        .await
        .unwrap();
    blocked.expect("first counted window reached actual server lock");
    let (backend, result) = outcome.unwrap().unwrap();
    let error = result.unwrap_err();
    assert!(
        error
            .attempt_kind()
            .is_some_and(storage_types::AttemptKind::is_unavailable),
        "{error:?}"
    );
    assert_eq!(probe.aborts.load(Ordering::Relaxed), 0);
    assert_eq!(probe.commits.load(Ordering::Relaxed), 0);
    assert_eq!(
        fixture
            .admin
            .query_one(
                &format!("SELECT count(*) FROM {}.counted_deadline", fixture.schema),
                &[]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    drop(backend);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_terminal_read_held_before_preparation_when_deadline_expires_then_no_native_abort_or_commit_is_needed()
 {
    let fixture = NativeFixture::open().await;
    let backend = fixture.open_independent_backend().await;
    let cutoff = deadline();
    let mut probe = DeadlineProbe::new(cutoff);
    probe.hold_before_preparation = true;
    let probe = Arc::new(probe);
    let backend = backend.with_fault_probe(probe.clone());
    let result = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, cutoff.class).started(cutoff),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .finish_read_batch(
                            statement("SELECT value FROM native_commit_probe"),
                            vec![],
                            bytes(),
                        )
                        .await?;
                    Ok(())
                })
            },
        )
        .await;
    let error = result.expect_err("deadline before preparation");
    assert!(
        error
            .attempt_kind()
            .is_some_and(storage_types::AttemptKind::is_unavailable)
    );
    assert_eq!(probe.aborts.load(Ordering::Relaxed), 0);
    assert_eq!(probe.commits.load(Ordering::Relaxed), 0);
    drop(backend);
    fixture.close().await;
}

/// Waits for real Postgres work without starting a Tokio timer.
async fn until(condition: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("condition reached");
}

/// Manual-clock attempts: the test moves the deadline past expiry. Each
/// attempt starts exactly one timer (`sleeps`); expiry before COMMIT starts
/// no grace timer.
async fn expire_manually(read_only: bool) {
    let fixture = NativeFixture::open().await;
    let clock = Arc::new(crate::test_support::ManualClock::default());
    let probe = Arc::new(DeadlineProbe::new(deadline()));
    let backend = Arc::new(
        fixture
            .open_independent_backend()
            .await
            .with_fault_probe(probe.clone())
            .with_clock(clock.clone()),
    );
    let cutoff = AttemptDeadline {
        class: DeadlineClass::Write,
        expires_at: tokio::time::Instant::now() + Duration::from_secs(30),
        commit_grace: Duration::from_secs(2),
    };
    let reached = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let callback_reached = reached.clone();
    let owner = backend.clone();
    let task = tokio::spawn(async move {
        let mode = if read_only {
            AttemptMode::ReadOnly
        } else {
            AttemptMode::ReadWrite
        };
        owner
            .with_attempt(
                AttemptPlan::new(mode, cutoff.class).started(cutoff),
                move |attempt| {
                    Box::pin(async move {
                        if read_only {
                            callback_reached.store(true, Ordering::SeqCst);
                            // BEGIN, the read and COMMIT are sent in one
                            // pipeline.
                            attempt
                                .finish_read_batch(
                                    statement("SELECT 1::bigint FROM pg_sleep(30)"),
                                    vec![],
                                    bytes(),
                                )
                                .await?;
                        } else {
                            attempt
                                .execute(statement("INSERT INTO native_commit_probe VALUES (1)"))
                                .await?;
                            callback_reached.store(true, Ordering::SeqCst);
                            std::future::pending::<()>().await;
                        }
                        Ok(())
                    })
                },
            )
            .await
    });
    until(|| reached.load(Ordering::SeqCst)).await;
    tokio::time::pause();
    clock.pass(cutoff.expires_at).await;
    let error = task.await.unwrap().unwrap_err();
    tokio::time::resume();
    assert!(!error.is_commit_unknown(), "{error:?}");
    assert!(
        error
            .attempt_kind()
            .is_some_and(storage_types::AttemptKind::is_unavailable),
        "{error:?}"
    );
    assert_eq!(clock.sleeps(), 1, "one deadline timer and no grace timer");
    assert_eq!(probe.aborts.load(Ordering::Relaxed), 0);
    assert_eq!(probe.commits.load(Ordering::Relaxed), 0);
    assert_eq!(
        backend.pool.status().size,
        0,
        "the connection is not returned to the pool"
    );
    assert_eq!(fixture.count_records().await, 0);
    drop(backend);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_write_before_commit_when_manual_deadline_expires_then_known_abort_and_connection_dropped()
 {
    expire_manually(false).await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_slow_read_only_batch_when_manual_deadline_expires_then_deadline_and_never_unknown() {
    expire_manually(true).await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_statement_longer_than_budget_when_client_timer_never_fires_then_server_ends_it() {
    let fixture = NativeFixture::open().await;
    // The manual clock never moves: only the server's statement_timeout from
    // the BEGIN batch can end the statement.
    let backend = fixture
        .open_independent_backend()
        .await
        .with_clock(Arc::new(crate::test_support::ManualClock::default()));
    let budget = AttemptDeadline {
        class: DeadlineClass::PointRead,
        expires_at: tokio::time::Instant::now() + Duration::from_millis(800),
        commit_grace: Duration::from_millis(500),
    };
    let started = std::time::Instant::now();
    let result = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, budget.class).started(budget),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .query(
                            statement("SELECT 1::bigint FROM pg_sleep(5)"),
                            ReadProtection::Snapshot,
                            bytes(),
                        )
                        .await
                })
            },
        )
        .await;
    let elapsed = started.elapsed();
    let error = result.err().expect("server statement timeout");
    assert!(!error.is_commit_unknown(), "{error:?}");
    assert!(
        error
            .attempt_kind()
            .is_some_and(storage_types::AttemptKind::is_unavailable),
        "{error:?}"
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "server ended the statement after {elapsed:?}"
    );
    drop(backend);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_held_pool_when_manual_deadline_expires_then_start_phase_deadline_without_callback() {
    let fixture = NativeFixture::open().await;
    let clock = Arc::new(crate::test_support::ManualClock::default());
    let backend = Arc::new(
        fixture
            .open_independent_backend()
            .await
            .with_clock(clock.clone()),
    );
    let held = backend.pool.get().await.unwrap();
    let cutoff = AttemptDeadline {
        class: DeadlineClass::Write,
        expires_at: tokio::time::Instant::now() + Duration::from_secs(30),
        commit_grace: Duration::from_secs(2),
    };
    let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let callback_called = called.clone();
    let owner = backend.clone();
    let task = tokio::spawn(async move {
        owner
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadWrite, cutoff.class).started(cutoff),
                move |_| {
                    Box::pin(async move {
                        callback_called.store(true, Ordering::SeqCst);
                        Ok(())
                    })
                },
            )
            .await
    });
    until(|| clock.sleeps() == 1).await;
    tokio::time::pause();
    clock.pass(cutoff.expires_at).await;
    let error = task.await.unwrap().unwrap_err();
    tokio::time::resume();
    assert!(
        error.deadline_phase() == Some(storage_types::AttemptPhase::Acquire),
        "{error:?}"
    );
    assert!(!called.load(Ordering::SeqCst));
    drop(held);
    drop(backend);
    fixture.close().await;
}

/// A write whose COMMIT is held by a deferred constraint trigger that waits for
/// an advisory lock owned by the test. Returns the held lock key, the backend
/// connection pid and the running attempt.
async fn start_held_commit(
    fixture: &NativeFixture,
    backend: &Arc<super::PostgresBackend>,
    cutoff: AttemptDeadline,
) -> (i64, i32, tokio::task::JoinHandle<StorageResult<()>>) {
    let lock = i64::from_le_bytes(uuid::Uuid::new_v4().as_bytes()[..8].try_into().unwrap());
    fixture
        .admin
        .query_one("SELECT pg_advisory_lock($1)", &[&lock])
        .await
        .unwrap();
    fixture
        .admin
        .batch_execute(&format!(
            "CREATE TABLE {0}.deferred_commit (value bigint NOT NULL); CREATE FUNCTION \
             {0}.hold_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM \
             pg_advisory_xact_lock({lock}); RETURN NULL; END $$; CREATE CONSTRAINT TRIGGER \
             hold_commit AFTER INSERT ON {0}.deferred_commit DEFERRABLE INITIALLY DEFERRED FOR \
             EACH ROW EXECUTE FUNCTION {0}.hold_commit()",
            fixture.schema,
        ))
        .await
        .unwrap();
    let (sent, received) = tokio::sync::oneshot::channel();
    let owner = backend.clone();
    let task = tokio::spawn(async move {
        owner
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadWrite, cutoff.class).started(cutoff),
                |attempt| {
                    Box::pin(async move {
                        let page = attempt
                            .query(
                                statement("SELECT pg_backend_pid()::bigint"),
                                ReadProtection::Snapshot,
                                bytes(),
                            )
                            .await?;
                        let crate::SqlValue::Integer(pid) =
                            page.rows[0].value(crate::ColumnIndex::new(0))?
                        else {
                            panic!("backend pid")
                        };
                        sent.send(*pid as i32).unwrap();
                        attempt
                            .execute(statement("INSERT INTO deferred_commit VALUES (1)"))
                            .await?;
                        Ok(())
                    })
                },
            )
            .await
    });
    let pid = received.await.unwrap();
    // Only the COMMIT can wait on the lock: the trigger is deferred.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let blocked: bool = fixture
                .admin
                .query_one(
                    "SELECT pg_backend_pid() = ANY(pg_blocking_pids($1))",
                    &[&pid],
                )
                .await
                .unwrap()
                .get(0);
            if blocked {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("COMMIT reached the actual server lock wait");
    (lock, pid, task)
}

fn manual_write_deadline() -> AttemptDeadline {
    AttemptDeadline {
        class: DeadlineClass::Write,
        expires_at: tokio::time::Instant::now() + Duration::from_secs(30),
        commit_grace: Duration::from_secs(2),
    }
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_commit_held_by_deferred_trigger_when_answer_arrives_in_grace_then_completed_and_connection_reused()
 {
    let fixture = NativeFixture::open().await;
    let clock = Arc::new(crate::test_support::ManualClock::default());
    let backend = Arc::new(
        fixture
            .open_independent_backend()
            .await
            .with_clock(clock.clone()),
    );
    let cutoff = manual_write_deadline();
    let (lock, pid, task) = start_held_commit(&fixture, &backend, cutoff).await;
    tokio::time::pause();
    clock.pass(cutoff.expires_at).await;
    until(|| clock.sleeps() == 2).await;
    // The grace timer runs and the manual clock never passes it: the answer
    // arrives inside the grace.
    fixture
        .admin
        .query_one("SELECT pg_advisory_unlock($1)", &[&lock])
        .await
        .unwrap();
    let result = task.await.unwrap();
    tokio::time::resume();
    result.expect("COMMIT answered inside the grace completes");
    assert_eq!(
        fixture
            .admin
            .query_one(
                &format!("SELECT count(*) FROM {}.deferred_commit", fixture.schema),
                &[]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    assert_eq!(
        backend.pool.status().available,
        1,
        "the connection returned to the pool"
    );
    assert_eq!(backend.pool.status().size, 1);
    let next = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    let page = attempt
                        .query(
                            statement("SELECT pg_backend_pid()::bigint"),
                            ReadProtection::Snapshot,
                            bytes(),
                        )
                        .await?;
                    let crate::SqlValue::Integer(pid) =
                        page.rows[0].value(crate::ColumnIndex::new(0))?
                    else {
                        panic!("backend pid")
                    };
                    Ok(*pid as i32)
                })
            },
        )
        .await
        .unwrap();
    assert_eq!(
        next, pid,
        "the same native connection served the next attempt"
    );
    drop(backend);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_commit_held_by_deferred_trigger_when_grace_passes_then_unknown_connection_dropped_and_write_present()
 {
    let fixture = NativeFixture::open().await;
    let clock = Arc::new(crate::test_support::ManualClock::default());
    let backend = Arc::new(
        fixture
            .open_independent_backend()
            .await
            .with_clock(clock.clone()),
    );
    let cutoff = manual_write_deadline();
    let (lock, _pid, task) = start_held_commit(&fixture, &backend, cutoff).await;
    tokio::time::pause();
    clock.pass(cutoff.expires_at).await;
    until(|| clock.sleeps() == 2).await;
    clock.pass(cutoff.settle_by()).await;
    let error = task.await.unwrap().unwrap_err();
    tokio::time::resume();
    assert!(error.is_commit_unknown(), "{error:?}");
    assert_eq!(
        backend.pool.status().size,
        0,
        "the connection with an unanswered COMMIT is dropped"
    );
    fixture
        .admin
        .query_one("SELECT pg_advisory_unlock($1)", &[&lock])
        .await
        .unwrap();
    // The server backend finishes the COMMIT after the client is gone.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let rows: i64 = fixture
                .admin
                .query_one(
                    &format!("SELECT count(*) FROM {}.deferred_commit", fixture.schema),
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            if rows == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the unknown write is present");
    drop(backend);
    fixture.close().await;
}

/// Decides finding R1 of specs/C4-corrections.md section 9r. The clock never
/// moves, so only the server can end the transaction. The BEGIN batch sets
/// `transaction_timeout` to remaining + grace (500 ms). If that SET LOCAL has
/// no effect the session value (2 x the longest budget) applies and all four
/// sleeps complete.
// expected to fail if R1 is true; runner copies the `transaction_timeout_probe` stderr line
// (completed sleeps and the returned error) and the assertion message.
#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_attempt_transaction_timeout_when_statements_outlast_it_then_server_ends_transaction()
{
    let fixture = NativeFixture::open().await;
    let clock = Arc::new(crate::test_support::ManualClock::default());
    let backend = fixture.open_independent_backend().await.with_clock(clock);
    // Warm the pool so the 300 ms budget is not spent connecting.
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .query(
                            statement("SELECT 1::bigint"),
                            ReadProtection::Snapshot,
                            bytes(),
                        )
                        .await
                        .map(|_| ())
                })
            },
        )
        .await
        .unwrap();
    let budget = AttemptDeadline {
        class: DeadlineClass::PointRead,
        expires_at: tokio::time::Instant::now() + Duration::from_millis(300),
        commit_grace: Duration::from_millis(200),
    };
    let completed = Arc::new(AtomicUsize::new(0));
    let counter = completed.clone();
    let result = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, budget.class).started(budget),
            move |attempt| {
                Box::pin(async move {
                    for _ in 0..4 {
                        // Each sleep is shorter than statement_timeout (300
                        // ms); only the transaction bound can end the series.
                        attempt
                            .query(
                                statement("SELECT 1::bigint FROM pg_sleep(0.25)"),
                                ReadProtection::Snapshot,
                                bytes(),
                            )
                            .await?;
                        counter.fetch_add(1, Ordering::SeqCst);
                    }
                    Ok(())
                })
            },
        )
        .await;
    let sleeps = completed.load(Ordering::SeqCst);
    eprintln!("transaction_timeout_probe completed_sleeps={sleeps} result={result:?}");
    let error = result.err().unwrap_or_else(|| {
        panic!("transaction_timeout did not end the transaction: {sleeps} of 4 sleeps completed")
    });
    assert!(!error.is_commit_unknown(), "{error:?}");
    assert!(
        error
            .attempt_kind()
            .is_some_and(storage_types::AttemptKind::is_unavailable),
        "{error:?}"
    );
    assert!(
        sleeps <= 2,
        "transaction_timeout must fire at the per-attempt 500 ms, not the session value: {sleeps} \
         sleeps completed"
    );
    drop(backend);

    // The per-attempt timer stops with the transaction: a pooled connection
    // survives an idle period longer than the bound.
    let backend = fixture.open_independent_backend().await;
    let short = || AttemptDeadline {
        class: DeadlineClass::Write,
        expires_at: tokio::time::Instant::now() + Duration::from_millis(300),
        commit_grace: Duration::from_millis(200),
    };
    let first = write_pid(&backend, short()).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let second = write_pid(&backend, short())
        .await
        .expect("the next attempt succeeds after the idle period");
    assert_eq!(
        first, second,
        "the pooled connection was not killed after COMMIT"
    );
    assert_eq!(backend.pool.status().size, 1);
    assert_eq!(fixture.count_records().await, 2);
    drop(backend);
    fixture.close().await;
}

async fn write_pid(
    backend: &super::PostgresBackend,
    budget: AttemptDeadline,
) -> StorageResult<i32> {
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, budget.class).started(budget),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .execute(statement("INSERT INTO native_commit_probe VALUES (1)"))
                        .await?;
                    let page = attempt
                        .query(
                            statement("SELECT pg_backend_pid()::bigint"),
                            ReadProtection::Snapshot,
                            bytes(),
                        )
                        .await?;
                    let crate::SqlValue::Integer(pid) =
                        page.rows[0].value(crate::ColumnIndex::new(0))?
                    else {
                        panic!("backend pid")
                    };
                    Ok(*pid as i32)
                })
            },
        )
        .await
}

/// Finding R2: a COMMIT answered with a conflict ends the transaction and
/// leaves the session usable, so the connection stays in the pool. Needs
/// `fail_answered_commit` in `attempt.rs`.
#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_serializable_conflict_at_commit_when_attempt_fails_then_known_conflict_and_connection_kept()
 {
    let fixture = NativeFixture::open().await;
    fixture
        .admin
        .batch_execute(&format!(
            "CREATE TABLE {0}.skew (k bigint PRIMARY KEY, v bigint NOT NULL); INSERT INTO \
             {0}.skew VALUES (1, 0), (2, 0)",
            fixture.schema
        ))
        .await
        .unwrap();
    let first = Arc::new(fixture.open_independent_backend().await);
    let second = Arc::new(fixture.open_independent_backend().await);
    let (ready, ready_rx) = tokio::sync::oneshot::channel();
    let (committed, committed_rx) = tokio::sync::oneshot::channel::<()>();
    let reached_commit = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = reached_commit.clone();
    // The late attempt reads the whole table, writes row 2 and waits for the
    // other attempt to commit; its own COMMIT then completes the SSI cycle.
    let late_owner = second.clone();
    let late = tokio::spawn(async move {
        late_owner
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                    .start(late_owner.attempt_deadlines()),
                move |attempt| {
                    Box::pin(async move {
                        attempt
                            .query(
                                statement("SELECT sum(v)::bigint FROM skew"),
                                ReadProtection::Snapshot,
                                bytes(),
                            )
                            .await?;
                        attempt
                            .execute(statement("UPDATE skew SET v = 1 WHERE k = 2"))
                            .await?;
                        let page = attempt
                            .query(
                                statement("SELECT pg_backend_pid()::bigint"),
                                ReadProtection::Snapshot,
                                bytes(),
                            )
                            .await?;
                        let crate::SqlValue::Integer(pid) =
                            page.rows[0].value(crate::ColumnIndex::new(0))?
                        else {
                            panic!("backend pid")
                        };
                        ready.send(()).unwrap();
                        let _ = committed_rx.await;
                        flag.store(true, Ordering::SeqCst);
                        Ok(*pid as i32)
                    })
                },
            )
            .await
    });
    ready_rx.await.unwrap();
    first
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(first.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .query(
                            statement("SELECT sum(v)::bigint FROM skew"),
                            ReadProtection::Snapshot,
                            bytes(),
                        )
                        .await?;
                    attempt
                        .execute(statement("UPDATE skew SET v = 1 WHERE k = 1"))
                        .await?;
                    Ok(())
                })
            },
        )
        .await
        .expect("the first committer succeeds");
    committed.send(()).unwrap();
    let error = late.await.unwrap().unwrap_err();
    assert!(
        reached_commit.load(Ordering::SeqCst),
        "the callback finished, so the conflict was answered at COMMIT"
    );
    assert!(!error.is_commit_unknown(), "{error:?}");
    assert_eq!(
        error.attempt_kind(),
        Some(storage_types::AttemptKind::Conflict),
        "{error:?}"
    );
    assert_eq!(
        second.pool.status().size,
        1,
        "the answered conflict keeps its connection"
    );
    assert_eq!(second.pool.status().available, 1);
    assert_eq!(
        fixture
            .admin
            .query_one(
                &format!("SELECT sum(v)::bigint FROM {}.skew", fixture.schema),
                &[]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        1,
        "only the first write committed"
    );
    drop(first);
    drop(second);
    fixture.close().await;
}

/// Stops a write attempt at `boundary` on its own runtime, then reads the row
/// through a fresh backend of the same schema. The fixture lives on the
/// verifying runtime, which outlasts the stop.
fn stopped_postgres_write(boundary: SqlCommitBoundary) -> (StorageError, usize) {
    let verifier = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let fixture = verifier.block_on(NativeFixture::open());
    let error = crate::fault_injection_tests::stopped_write(boundary, |hold| async {
        fixture
            .open_independent_backend()
            .await
            .with_fault_probe(hold)
    });
    let rows = verifier.block_on(async {
        let backend = fixture.open_independent_backend().await;
        let rows = crate::fault_injection_tests::value_one_rows(&backend).await;
        drop(backend);
        rows
    });
    verifier.block_on(fixture.close());
    (error, rows)
}

#[test]
#[ignore = "requires TEST_POSTGRES_DSN"]
fn given_write_attempt_stopped_before_commit_submission_when_supervised_then_known_and_not_committed()
 {
    let (error, rows) = stopped_postgres_write(SqlCommitBoundary::BeforeCommit);
    assert!(
        !error.is_commit_unknown(),
        "a stop before commit submission is a known failure: {error:?}"
    );
    assert_eq!(rows, 0, "no COMMIT was sent");
}

#[test]
#[ignore = "requires TEST_POSTGRES_DSN"]
fn given_write_attempt_stopped_after_commit_submission_when_supervised_then_unknown_and_committed()
{
    let (error, rows) = stopped_postgres_write(SqlCommitBoundary::AfterDurableAcknowledgement);
    assert!(
        error.is_commit_unknown(),
        "a stop after commit submission is unknown: {error:?}"
    );
    assert_eq!(rows, 1, "the commit reached the schema");
}

/// The follow-on `25P02` of the statements that share the failed statement's
/// pipeline must not outrank the root cause: the sleep ends by
/// `statement_timeout`, the read behind it in the same pipeline receives
/// `in_failed_sql_transaction`, and the step ignores the batch error.
#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_statement_timeout_inside_pipelined_wave_when_caught_then_deadline_not_conflict() {
    let fixture = NativeFixture::open().await;
    let backend = fixture
        .open_independent_backend()
        .await
        .with_clock(Arc::new(crate::test_support::ManualClock::default()));
    let budget = AttemptDeadline {
        class: DeadlineClass::Write,
        expires_at: tokio::time::Instant::now() + Duration::from_millis(800),
        commit_grace: Duration::from_millis(500),
    };
    let result: StorageResult<()> = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, budget.class).started(budget),
            |attempt| {
                Box::pin(async move {
                    let batch = attempt
                        .execute_batch(vec![
                            statement("SELECT 1::bigint FROM pg_sleep(5)"),
                            statement("SELECT 2::bigint"),
                        ])
                        .await;
                    assert!(batch.is_err(), "the sleep must end by statement_timeout");
                    let _ = attempt
                        .query(
                            statement("SELECT 3::bigint"),
                            ReadProtection::Snapshot,
                            bytes(),
                        )
                        .await;
                    Ok(())
                })
            },
        )
        .await;
    let error = result.expect_err("rejected attempt");
    assert_eq!(
        error.deadline_phase(),
        Some(storage_types::AttemptPhase::Statement)
    );
    assert_eq!(
        error.attempt_kind(),
        Some(storage_types::AttemptKind::Deadline)
    );
    drop(backend);
    fixture.close().await;
}
