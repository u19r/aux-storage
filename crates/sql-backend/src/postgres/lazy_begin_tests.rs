//! Lazy BEGIN (PG-BEGIN): the BEGIN batch rides in the attempt's first wave.
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::future::BoxFuture;
use storage_types::{
    AttemptDeadline, AttemptDurability, AttemptMode, AttemptPlan, DeadlineClass, StorageError,
    StorageResult,
};

use super::{
    driver::{PostgresBackend, PostgresTransaction},
    test_support::NativeFixture,
};
use crate::{
    ReadProtection, SqlAttempt, SqlBackend, SqlFaultCall, SqlFaultProbe, SqlNativeEvent,
    SqlStatement, SqlValue, SqlWaveStatement,
};

fn statement(sql: &'static str) -> SqlStatement {
    SqlStatement::new(sql, vec![])
}

fn bytes() -> NonZeroUsize {
    NonZeroUsize::new(4096).unwrap()
}

const INSERT: &str = "INSERT INTO native_commit_probe VALUES (1)";

/// Records native events and the statements of every wave.
#[derive(Default)]
struct WaveProbe {
    events: Mutex<Vec<SqlNativeEvent>>,
    waves: Mutex<Vec<Vec<String>>>,
    hold_first_wave_until: Option<tokio::time::Instant>,
}

impl SqlFaultProbe for WaveProbe {
    fn fail(&self, _: SqlFaultCall<'_>) -> Option<StorageError> {
        None
    }
    fn observe(&self, event: SqlNativeEvent) {
        self.events.lock().unwrap().push(event);
    }
    fn before_wave<'a>(
        &'a self,
        statements: &'a [SqlWaveStatement<'a>],
    ) -> BoxFuture<'a, StorageResult<()>> {
        Box::pin(async move {
            let first = {
                let mut waves = self.waves.lock().unwrap();
                waves.push(
                    statements
                        .iter()
                        .map(|statement| statement.sql.to_owned())
                        .collect(),
                );
                waves.len() == 1
            };
            if first && let Some(until) = self.hold_first_wave_until {
                tokio::time::sleep_until(until).await;
            }
            Ok(())
        })
    }
}

impl WaveProbe {
    fn count(&self, wanted: fn(&SqlNativeEvent) -> bool) -> usize {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| wanted(event))
            .count()
    }
    fn begins(&self) -> usize {
        self.count(|event| matches!(event, SqlNativeEvent::Begin))
    }
    fn commits(&self) -> usize {
        self.count(|event| matches!(event, SqlNativeEvent::Commit))
    }
    fn rollbacks(&self) -> usize {
        self.count(|event| matches!(event, SqlNativeEvent::Rollback))
    }
    fn waves(&self) -> Vec<Vec<String>> {
        self.waves.lock().unwrap().clone()
    }
    fn lone_begins(&self) -> usize {
        self.waves()
            .iter()
            .filter(|wave| wave.len() == 1 && wave[0].starts_with("START TRANSACTION"))
            .count()
    }
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_first_query_when_attempt_commits_then_begin_rides_in_its_wave() {
    let fixture = NativeFixture::open().await;
    let probe = Arc::new(WaveProbe::default());
    let backend = fixture
        .open_independent_backend()
        .await
        .with_fault_probe(probe.clone());
    let result = backend
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
        .await;
    assert_eq!(result.unwrap(), ());
    let waves = probe.waves();
    assert_eq!(waves.len(), 2, "{waves:?}");
    assert_eq!(waves[0].len(), 2, "{waves:?}");
    assert!(waves[0][0].starts_with("START TRANSACTION"), "{waves:?}");
    assert_eq!(waves[0][1], "SELECT 1::bigint");
    assert_eq!(waves[1], vec!["COMMIT".to_owned()]);
    assert_eq!(
        (probe.begins(), probe.commits(), probe.rollbacks()),
        (1, 1, 0)
    );
    assert_eq!(probe.lone_begins(), 0);
    assert_eq!(backend.pool.status().available, 1);
    drop(backend);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_first_execute_when_attempt_commits_then_write_is_durable_in_two_waves() {
    let fixture = NativeFixture::open().await;
    let probe = Arc::new(WaveProbe::default());
    let backend = fixture
        .open_independent_backend()
        .await
        .with_fault_probe(probe.clone());
    let result = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            |attempt| Box::pin(async move { attempt.execute(statement(INSERT)).await.map(|_| ()) }),
        )
        .await;
    assert_eq!(result.unwrap(), ());
    let waves = probe.waves();
    assert_eq!(waves.len(), 2, "{waves:?}");
    assert_eq!(waves[0].len(), 2, "{waves:?}");
    assert!(waves[0][0].starts_with("START TRANSACTION"), "{waves:?}");
    assert_eq!(waves[0][1], INSERT);
    assert_eq!(waves[1], vec!["COMMIT".to_owned()]);
    assert_eq!((probe.begins(), probe.commits()), (1, 1));
    assert_eq!(fixture.count_records().await, 1);
    assert_eq!(backend.pool.status().available, 1);
    drop(backend);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_first_query_batch_then_execute_batch_when_attempt_commits_then_one_begin_and_three_waves()
 {
    let fixture = NativeFixture::open().await;
    let probe = Arc::new(WaveProbe::default());
    let backend = fixture
        .open_independent_backend()
        .await
        .with_fault_probe(probe.clone());
    let result = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .query_batch_protected(
                            vec![
                                (statement("SELECT 1::bigint"), ReadProtection::Snapshot),
                                (statement("SELECT 2::bigint"), ReadProtection::Snapshot),
                            ],
                            bytes(),
                        )
                        .await?;
                    attempt
                        .execute_batch(vec![statement(INSERT), statement(INSERT)])
                        .await?;
                    Ok(())
                })
            },
        )
        .await;
    result.unwrap();
    let waves = probe.waves();
    assert_eq!(waves.len(), 3, "{waves:?}");
    assert_eq!(waves[0].len(), 3, "{waves:?}");
    assert!(waves[0][0].starts_with("START TRANSACTION"), "{waves:?}");
    assert_eq!(&waves[0][1..], ["SELECT 1::bigint", "SELECT 2::bigint"]);
    assert_eq!(waves[1], vec![INSERT.to_owned(), INSERT.to_owned()]);
    assert_eq!(waves[2], vec!["COMMIT".to_owned()]);
    assert_eq!(probe.begins(), 1);
    assert_eq!(probe.lone_begins(), 0);
    assert_eq!(fixture.count_records().await, 2);
    drop(backend);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_empty_callback_when_attempt_ends_then_nothing_is_sent() {
    let fixture = NativeFixture::open().await;
    let probe = Arc::new(WaveProbe::default());
    let backend = fixture
        .open_independent_backend()
        .await
        .with_fault_probe(probe.clone());
    let completed = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            |_| Box::pin(async { Ok(5) }),
        )
        .await;
    assert_eq!(completed.unwrap(), 5);
    let refused: StorageResult<()> = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            |_| Box::pin(async { Err(StorageError::internal("callback refused")) }),
        )
        .await;
    assert!(refused.is_err());
    assert!(probe.waves().is_empty());
    assert_eq!(probe.events.lock().unwrap().len(), 0);
    let status = backend.pool.status();
    assert_eq!((status.size, status.available), (1, 1));
    drop(backend);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_first_query_failing_at_parse_when_attempt_ends_then_no_begin_or_rollback() {
    let fixture = NativeFixture::open().await;
    let probe = Arc::new(WaveProbe::default());
    let backend = fixture
        .open_independent_backend()
        .await
        .with_fault_probe(probe.clone());
    let result: StorageResult<()> = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .query(
                            statement("SELECT value FROM native_missing_relation"),
                            ReadProtection::Snapshot,
                            bytes(),
                        )
                        .await?;
                    Ok(())
                })
            },
        )
        .await;
    let error = result.unwrap_err();
    assert!(!error.is_commit_unknown(), "{error:?}");
    assert_eq!(
        (probe.begins(), probe.rollbacks(), probe.commits()),
        (0, 0, 0)
    );
    assert_eq!(backend.pool.status().available, 1);
    drop(backend);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_first_query_failing_at_execution_when_attempt_ends_then_rollback_follows_pipelined_begin()
 {
    let fixture = NativeFixture::open().await;
    let probe = Arc::new(WaveProbe::default());
    let backend = fixture
        .open_independent_backend()
        .await
        .with_fault_probe(probe.clone());
    let result: StorageResult<()> = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    let divide = SqlStatement::new(
                        "SELECT 1::bigint / $1::bigint",
                        vec![SqlValue::Integer(0)],
                    );
                    attempt
                        .query(divide, ReadProtection::Snapshot, bytes())
                        .await?;
                    Ok(())
                })
            },
        )
        .await;
    let error = result.unwrap_err();
    assert!(!error.is_commit_unknown(), "{error:?}");
    assert!(
        format!("{:?}", error.as_ref()).contains("22012"),
        "{error:?}"
    );
    let waves = probe.waves();
    assert_eq!(waves.len(), 2, "{waves:?}");
    assert!(waves[0][0].starts_with("START TRANSACTION"), "{waves:?}");
    assert_eq!(waves[0].len(), 2, "{waves:?}");
    assert_eq!(waves[1], vec!["ROLLBACK".to_owned()]);
    assert_eq!(
        (probe.begins(), probe.rollbacks(), probe.commits()),
        (1, 1, 0)
    );
    assert_eq!(backend.pool.status().available, 1);
    drop(backend);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_failing_begin_batch_when_statement_rides_with_it_then_begin_error_is_reported() {
    let fixture = NativeFixture::open().await;
    fixture
        .admin
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS {}.sys_system_stream_scope",
            fixture.schema
        ))
        .await
        .unwrap();
    let probe = Arc::new(WaveProbe::default());
    let backend = fixture.open_independent_backend().await;
    let result = PostgresBackend::run_attempt(
        backend.pool.clone(),
        std::sync::Arc::new(storage_common::native_attempt_lifecycle::AttemptLifecycle::new()),
        (PostgresTransaction::SerializableData, None),
        AttemptDurability::Durable,
        (
            AttemptDeadline {
                class: DeadlineClass::Write,
                expires_at: tokio::time::Instant::now() + Duration::from_secs(30),
                commit_grace: Duration::from_secs(2),
            },
            Arc::new(crate::test_support::ManualClock::default()),
        ),
        Some(probe.clone()),
        |attempt| {
            Box::pin(async move {
                attempt.execute(statement(INSERT)).await?;
                Ok(())
            })
        },
    )
    .await;
    let Err(error) = result else {
        panic!("the failing BEGIN batch must fail the attempt")
    };
    let text = format!("{:?}", error.as_ref());
    assert!(text.contains("42P01"), "{text}");
    assert!(!text.contains("25P02"), "{text}");
    assert!(!error.is_commit_unknown(), "{text}");
    assert_eq!(
        (probe.begins(), probe.commits(), probe.rollbacks()),
        (0, 0, 1)
    );
    assert_eq!(fixture.count_records().await, 0);
    assert_eq!(backend.pool.status().size, 1);
    drop(backend);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_deadline_passed_before_first_wave_when_statement_is_sent_then_known_begin_deadline_and_nothing_written()
 {
    let fixture = NativeFixture::open().await;
    // Opened before the budget starts: a cold connection under load can take
    // longer than the whole 300 ms budget and expire it at acquire.
    let backend = fixture.open_independent_backend().await;
    let cutoff = tokio::time::Instant::now() + Duration::from_millis(300);
    let probe = Arc::new(WaveProbe {
        hold_first_wave_until: Some(cutoff + Duration::from_millis(50)),
        ..Default::default()
    });
    // The manual clock never moves, so the client timer never fires.
    let backend = backend
        .with_clock(Arc::new(crate::test_support::ManualClock::default()))
        .with_fault_probe(probe.clone());
    let deadline = AttemptDeadline {
        class: DeadlineClass::Write,
        expires_at: cutoff,
        commit_grace: Duration::from_millis(200),
    };
    let result: StorageResult<()> = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, deadline.class).started(deadline),
            |attempt| {
                Box::pin(async move {
                    attempt.execute(statement(INSERT)).await?;
                    Ok(())
                })
            },
        )
        .await;
    let error = result.unwrap_err();
    assert!(
        error
            .attempt_kind()
            .is_some_and(storage_types::AttemptKind::is_unavailable),
        "{error:?}"
    );
    assert_eq!(probe.waves().len(), 1, "{error:?}");
    assert!(format!("{error:?}").contains("Begin"), "{error:?}");
    assert!(!error.is_commit_unknown(), "{error:?}");
    assert_eq!(
        (probe.begins(), probe.rollbacks(), probe.commits()),
        (0, 0, 0)
    );
    assert_eq!(fixture.count_records().await, 0);
    assert_eq!(backend.pool.status().available, 1);
    drop(backend);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_savepoint_as_first_statement_when_rolled_back_to_then_begin_preceded_it() {
    let fixture = NativeFixture::open().await;
    let probe = Arc::new(WaveProbe::default());
    let backend = fixture
        .open_independent_backend()
        .await
        .with_fault_probe(probe.clone());
    let result = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt.execute(statement("SAVEPOINT pg_begin")).await?;
                    attempt.execute(statement(INSERT)).await?;
                    attempt
                        .execute(statement("ROLLBACK TO SAVEPOINT pg_begin"))
                        .await?;
                    Ok(())
                })
            },
        )
        .await;
    result.unwrap();
    let waves = probe.waves();
    assert_eq!(waves.len(), 4, "{waves:?}");
    assert!(waves[0][0].starts_with("START TRANSACTION"), "{waves:?}");
    assert_eq!(waves[0][1], "SAVEPOINT pg_begin");
    assert_eq!(fixture.count_records().await, 0);
    drop(backend);
    fixture.close().await;
}
