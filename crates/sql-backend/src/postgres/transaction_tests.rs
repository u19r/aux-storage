use std::{num::NonZeroUsize, time::Duration};

use storage_types::{
    InfrastructureContext, InfrastructureResultKind, StorageEnum, StorageError, StorageResult,
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
        .with_attempt(AttemptMode::ReadWrite, |attempt| {
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
        })
        .await;
    let count = fixture.count_records().await;
    fixture.close().await;
    assert_eq!(count, 0);
    assert_native_abort(result);
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_caught_query_error_when_callback_returns_success_then_native_attempt_reports_abort()
{
    let fixture = NativeFixture::open().await;
    let result = fixture
        .backend
        .with_attempt(AttemptMode::ReadWrite, |attempt| {
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
        })
        .await;
    let count = fixture.count_records().await;
    fixture.close().await;
    assert_eq!(count, 0);
    assert_native_abort(result);
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_canceled_native_statement_when_callback_returns_success_then_all_effects_roll_back()
{
    let fixture = NativeFixture::open().await;
    let result = fixture
        .backend
        .with_attempt(AttemptMode::ReadWrite, |attempt| {
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
                        "INSERT INTO native_commit_probe SELECT 2 FROM (SELECT pg_sleep(0.1)) AS \
                         delayed",
                        vec![],
                    )),
                )
                .await;
                assert!(canceled.is_err());
                Ok(())
            })
        })
        .await;
    let count = fixture.count_records().await;
    fixture.close().await;
    assert_eq!(
        count, 0,
        "cancellation cannot leave partial committed effects"
    );
    assert_native_abort(result);
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_late_native_row_error_when_page_fills_then_native_attempt_reports_abort() {
    let fixture = NativeFixture::open().await;
    let result = fixture
        .backend
        .with_attempt(AttemptMode::ReadWrite, |attempt| {
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
        })
        .await;
    let count = fixture.count_records().await;
    fixture.close().await;
    assert_eq!(count, 0);
    assert_native_abort(result);
}

fn assert_native_abort(result: StorageResult<()>) {
    let error = result.expect_err("the native attempt must not acknowledge success after rollback");
    assert!(matches!(
        error.as_ref(),
        StorageEnum::TransactionConflict { .. }
    ));
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_failed_native_statement_when_infrastructure_callback_rejects_then_error_and_safe_pool_reuse_survive()
 {
    let fixture = NativeFixture::open().await;
    let (observed_pid, native_pid) = tokio::sync::oneshot::channel();
    let result = fixture
        .backend
        .with_attempt(AttemptMode::ReadWrite, |attempt| {
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
                    InfrastructureResultKind::ProtectedConflict {
                        context: InfrastructureContext::Revision,
                    }
                    .into(),
                )
            })
        })
        .await;
    let error = result.expect_err("callback rejection must survive rollback");
    assert!(matches!(
        error.as_ref(),
        StorageEnum::InfrastructureResult(InfrastructureResultKind::ProtectedConflict {
            context: InfrastructureContext::Revision
        })
    ));
    let pid = native_pid.await.unwrap();
    let page = fixture
        .backend
        .with_attempt(AttemptMode::ReadOnly, |attempt| {
            Box::pin(async move {
                attempt
                    .query(
                        SqlStatement::new("SELECT pg_backend_pid()::bigint", vec![]),
                        ReadProtection::Snapshot,
                        NonZeroUsize::new(4096).unwrap(),
                    )
                    .await
            })
        })
        .await
        .unwrap();
    assert!(
        matches!(page.rows[0].value(ColumnIndex::new(0)).unwrap(), SqlValue::Integer(current) if *current == pid)
    );
    assert_eq!(fixture.count_records().await, 0);
    fixture.close().await;
}
