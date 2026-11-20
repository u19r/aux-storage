use std::{num::NonZeroUsize, time::Duration};

use storage_types::{
    AttemptPlan, DeadlineClass, InfrastructureContext, InfrastructureResultKind, StorageEnum,
    StorageError, StorageResult,
};

use super::test_support::NativeFixture;
use crate::{
    AttemptMode, ColumnIndex, ReadProtection, SqlAttempt, SqlBackend, SqlStatement, SqlValue,
};

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_caught_execute_error_when_callback_returns_success_then_native_attempt_reports_abort()
 {
    let fixture = NativeFixture::open().await;
    let result = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .execute(SqlStatement::new(
                            "INSERT INTO native_commit_probe VALUES (1)",
                            vec![],
                        ))
                        .await?;
                    assert!(
                        attempt
                            .execute(SqlStatement::new(
                                "INSERT INTO native_commit_probe VALUES (1 / 0)",
                                vec![]
                            ))
                            .await
                            .is_err()
                    );
                    Ok(())
                })
            },
        )
        .await;
    let count = fixture.count_records().await;
    fixture.close().await;
    assert_eq!(count, 0);
    assert_division_failure(result);
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_caught_query_error_when_callback_returns_success_then_native_attempt_reports_abort()
{
    let fixture = NativeFixture::open().await;
    let result = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .execute(SqlStatement::new(
                            "INSERT INTO native_commit_probe VALUES (1)",
                            vec![],
                        ))
                        .await?;
                    assert!(
                        attempt
                            .query(
                                SqlStatement::new("SELECT 1 / 0", vec![]),
                                ReadProtection::Snapshot,
                                NonZeroUsize::new(4096).unwrap()
                            )
                            .await
                            .is_err()
                    );
                    Ok(())
                })
            },
        )
        .await;
    let count = fixture.count_records().await;
    fixture.close().await;
    assert_eq!(count, 0);
    assert_division_failure(result);
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_canceled_native_statement_when_callback_returns_success_then_all_effects_roll_back()
{
    let fixture = NativeFixture::open().await;
    let result = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .execute(SqlStatement::new(
                            "INSERT INTO native_commit_probe VALUES (1)",
                            vec![],
                        ))
                        .await?;
                    let canceled = tokio::time::timeout(
                        Duration::from_millis(10),
                        attempt.execute(SqlStatement::new(
                            "INSERT INTO native_commit_probe SELECT 2 FROM (SELECT pg_sleep(0.1)) \
                             AS delayed",
                            vec![],
                        )),
                    )
                    .await;
                    assert!(canceled.is_err());
                    Ok(())
                })
            },
        )
        .await;
    let count = fixture.count_records().await;
    fixture.close().await;
    assert_eq!(
        count, 0,
        "cancellation cannot leave partial committed effects"
    );
    assert!(result.unwrap_err().is_attempt_abandoned());
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_late_native_row_error_when_page_fills_then_native_attempt_reports_abort() {
    let fixture = NativeFixture::open().await;
    let result = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .execute(SqlStatement::new(
                            "INSERT INTO native_commit_probe VALUES (1)",
                            vec![],
                        ))
                        .await?;
                    let _ = attempt
                        .query(
                            SqlStatement::new(
                                "SELECT 1 / (3 - value) FROM generate_series(1, 3) AS value",
                                vec![],
                            ),
                            ReadProtection::Snapshot,
                            NonZeroUsize::MIN,
                        )
                        .await;
                    Ok(())
                })
            },
        )
        .await;
    let count = fixture.count_records().await;
    fixture.close().await;
    assert_eq!(count, 0);
    assert_division_failure(result);
}

fn assert_division_failure(result: StorageResult<()>) {
    let error = result.expect_err("the original native failure must survive rollback");
    assert!(
        matches!(error.as_ref(), StorageEnum::InternalServerError { message }
        if message == "Postgres native operation failed (SQLSTATE 22012)"),
        "{error:?}"
    );
    assert!(
        !error
            .attempt_kind()
            .is_some_and(|kind| kind != storage_types::AttemptKind::CommitUnknown)
    );
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_failed_native_statement_when_infrastructure_callback_rejects_then_error_and_safe_pool_reuse_survive()
 {
    let fixture = NativeFixture::open().await;
    let (observed_pid, native_pid) = tokio::sync::oneshot::channel();
    let result = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    let page = attempt
                        .query(
                            SqlStatement::new("SELECT pg_backend_pid()::bigint", vec![]),
                            ReadProtection::Snapshot,
                            NonZeroUsize::new(4096).unwrap(),
                        )
                        .await?;
                    let SqlValue::Integer(pid) = page.rows[0].value(ColumnIndex::new(0)).unwrap()
                    else {
                        panic!("native pid");
                    };
                    observed_pid.send(*pid).unwrap();
                    attempt
                        .execute(SqlStatement::new(
                            "INSERT INTO native_commit_probe VALUES (1)",
                            vec![],
                        ))
                        .await?;
                    assert!(
                        attempt
                            .execute(SqlStatement::new("SELECT 1 / 0", vec![]))
                            .await
                            .is_err()
                    );
                    Err::<(), StorageError>(
                        InfrastructureResultKind::InvalidBound {
                            context: InfrastructureContext::Revision,
                        }
                        .into(),
                    )
                })
            },
        )
        .await;
    assert_division_failure(result);
    let pid = native_pid.await.unwrap();
    let page = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .query(
                            SqlStatement::new("SELECT pg_backend_pid()::bigint", vec![]),
                            ReadProtection::Snapshot,
                            NonZeroUsize::new(4096).unwrap(),
                        )
                        .await
                })
            },
        )
        .await
        .unwrap();
    assert!(
        matches!(page.rows[0].value(ColumnIndex::new(0)).unwrap(), SqlValue::Integer(current) if *current == pid)
    );
    assert_eq!(fixture.count_records().await, 0);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_attempt_durability_when_transaction_runs_then_synchronous_commit_follows_it() {
    let fixture = NativeFixture::open().await;
    let mut observed = Vec::new();
    for durability in [
        storage_types::AttemptDurability::Relaxed,
        storage_types::AttemptDurability::Durable,
    ] {
        let request = storage_types::AttemptPlan::new(
            AttemptMode::ReadWrite,
            storage_types::DeadlineClass::Write,
        )
        .with_durability(durability)
        .start(fixture.backend.attempt_deadlines());
        let setting = fixture
            .backend
            .with_attempt(request, |attempt| {
                Box::pin(async move {
                    let page = attempt
                        .query(
                            SqlStatement::new(
                                "SELECT current_setting('synchronous_commit')",
                                vec![],
                            ),
                            ReadProtection::Snapshot,
                            NonZeroUsize::new(4096).unwrap(),
                        )
                        .await?;
                    let SqlValue::Text(setting) = page.rows[0].value(ColumnIndex::new(0)).unwrap()
                    else {
                        panic!("synchronous_commit setting");
                    };
                    Ok(setting.clone())
                })
            })
            .await
            .unwrap();
        observed.push(setting);
    }
    fixture.close().await;
    assert_eq!(observed, ["off", "on"]);
}

#[cfg(feature = "fault-injection")]
async fn text_setting<A: SqlAttempt>(attempt: &mut A, sql: &'static str) -> StorageResult<String> {
    let page = attempt
        .query(
            SqlStatement::new(sql, vec![]),
            ReadProtection::Snapshot,
            NonZeroUsize::new(4096).unwrap(),
        )
        .await?;
    let SqlValue::Text(value) = page.rows[0].value(ColumnIndex::new(0)).unwrap() else {
        panic!("text setting");
    };
    Ok(value.clone())
}

/// W2 D2 and D3: an item attempt runs `READ COMMITTED` with no table lock,
/// and a `Claim` read skips a row that another attempt holds instead of
/// waiting for it.
#[cfg(feature = "fault-injection")]
#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_item_attempt_when_a_claim_meets_a_locked_row_then_it_skips_the_row_without_waiting()
{
    let fixture = NativeFixture::open().await;
    fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .execute(SqlStatement::new(
                            "INSERT INTO native_commit_probe VALUES (1), (2)",
                            vec![],
                        ))
                        .await?;
                    Ok(())
                })
            },
        )
        .await
        .unwrap();
    let isolation = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    text_setting(attempt, "SELECT current_setting('transaction_isolation')").await
                })
            },
        )
        .await
        .unwrap();
    assert_eq!(isolation, "read committed");

    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let (holding, held) = tokio::sync::oneshot::channel::<()>();
    let other = fixture.open_independent_backend().await;
    let holder = other.with_attempt(
        AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
            .start(other.attempt_deadlines()),
        move |attempt| {
            Box::pin(async move {
                attempt
                    .query(
                        SqlStatement::new(
                            "SELECT value FROM native_commit_probe WHERE value = 1",
                            vec![],
                        ),
                        ReadProtection::Conflict,
                        NonZeroUsize::new(4096).unwrap(),
                    )
                    .await?;
                let _ = holding.send(());
                let _ = released.await;
                Ok(())
            })
        },
    );
    let claimer = async {
        held.await.unwrap();
        let claimed = tokio::time::timeout(
            Duration::from_secs(2),
            fixture.backend.with_attempt(
                AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                    .start(fixture.backend.attempt_deadlines()),
                |attempt| {
                    Box::pin(async move {
                        let page = attempt
                            .query(
                                SqlStatement::new(
                                    "SELECT value FROM native_commit_probe ORDER BY value LIMIT 1",
                                    vec![],
                                ),
                                ReadProtection::Claim,
                                NonZeroUsize::new(4096).unwrap(),
                            )
                            .await?;
                        Ok(page
                            .rows
                            .iter()
                            .map(|row| match row.value(ColumnIndex::new(0)).unwrap() {
                                SqlValue::Integer(value) => *value,
                                _ => panic!("probe value"),
                            })
                            .collect::<Vec<_>>())
                    })
                },
            ),
        )
        .await;
        let _ = release.send(());
        claimed
    };
    let (held_result, claimed) = tokio::join!(holder, claimer);
    held_result.unwrap();
    let claimed = claimed.expect("a claim waited for a locked row").unwrap();
    fixture.close().await;
    assert_eq!(claimed, [2]);
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_read_only_attempt_when_query_writes_then_server_refuses_it_and_nothing_commits() {
    let fixture = NativeFixture::open().await;
    let result = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .query(
                            SqlStatement::new(
                                "INSERT INTO native_commit_probe VALUES (1) RETURNING value",
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
    let count = fixture.count_records().await;
    fixture.close().await;
    let error = result.err().expect("the server refuses the write");
    assert!(format!("{:?}", error.as_ref()).contains("25006"));
    assert!(!error.is_commit_unknown());
    assert_eq!(count, 0);
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_read_only_final_read_batch_when_a_statement_writes_then_server_refuses_it_and_nothing_commits()
 {
    let fixture = NativeFixture::open().await;
    let result = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .finish_read_batch(
                            SqlStatement::new("SELECT 1", vec![]),
                            vec![(
                                SqlStatement::new(
                                    "INSERT INTO native_commit_probe VALUES (1) RETURNING value",
                                    vec![],
                                ),
                                ReadProtection::Snapshot,
                            )],
                            NonZeroUsize::new(1024).unwrap(),
                        )
                        .await
                })
            },
        )
        .await;
    let count = fixture.count_records().await;
    fixture.close().await;
    let error = match result {
        Err(error) => error,
        Ok(batch) => {
            batch
                .reads
                .err()
                .expect("the batch statement is refused")
                .error
        }
    };
    assert!(format!("{:?}", error.as_ref()).contains("25006"));
    assert_eq!(count, 0);
}
