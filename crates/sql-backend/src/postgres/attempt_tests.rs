use std::borrow::Cow;

use storage_types::{AttemptPlan, DeadlineClass};

use super::statements::append_query_protection;
use crate::ReadProtection;

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_reused_sql_when_binds_and_table_change_then_observe_current_rows() {
    use crate::{AttemptMode, SqlAttempt, SqlBackend, SqlStatement, SqlValue};

    let fixture = super::test_support::NativeFixture::open().await;
    for _ in 0..2 {
        fixture
            .backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                    .start(fixture.backend.attempt_deadlines()),
                |attempt| {
                    Box::pin(async move {
                        for value in [1, 2] {
                            attempt
                                .execute(SqlStatement::new(
                                    "INSERT INTO native_commit_probe (value) VALUES ($1)",
                                    vec![SqlValue::Integer(value)],
                                ))
                                .await?;
                        }
                        for (threshold, expected) in [(0, 2), (1, 1)] {
                            let page = attempt
                                .query(
                                    SqlStatement::new(
                                        "SELECT value FROM native_commit_probe WHERE value > $1",
                                        vec![SqlValue::Integer(threshold)],
                                    ),
                                    ReadProtection::Snapshot,
                                    std::num::NonZeroUsize::new(4096).unwrap(),
                                )
                                .await?;
                            assert_eq!(page.rows.len(), expected);
                        }
                        attempt
                            .execute(SqlStatement::new("DROP TABLE native_commit_probe", vec![]))
                            .await?;
                        attempt
                            .execute(SqlStatement::new(
                                "CREATE TABLE native_commit_probe (value bigint NOT NULL)",
                                vec![],
                            ))
                            .await?;
                        Ok(())
                    })
                },
            )
            .await
            .unwrap();
    }
    fixture.close().await;
}

#[test]
fn given_borrowed_or_owned_query_when_protected_then_preserve_lock_sql() {
    for (protection, suffix) in [
        (ReadProtection::Snapshot, ""),
        (ReadProtection::Shared, " FOR SHARE"),
        (ReadProtection::Conflict, " FOR UPDATE"),
        (ReadProtection::Claim, " FOR UPDATE SKIP LOCKED"),
    ] {
        for sql in [Cow::Borrowed("SELECT 1"), Cow::Owned("SELECT 1".to_owned())] {
            assert_eq!(
                append_query_protection(sql, protection).unwrap(),
                format!("SELECT 1{suffix}")
            );
        }
    }
    assert!(matches!(
        append_query_protection(Cow::Borrowed("SELECT 1"), ReadProtection::Snapshot).unwrap(),
        Cow::Borrowed(_)
    ));
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_failed_native_pipeline_when_settled_then_preserve_origin_and_reuse_connection() {
    use storage_types::{StorageEnum, context::WrappedError};

    use crate::{AttemptMode, SqlAttempt, SqlBackend, SqlStatement};

    let dsn = std::env::var("TEST_POSTGRES_DSN").unwrap();
    let backend = super::PostgresBackend::initialize(
        &dsn,
        1,
        false,
        storage_types::AttemptDeadlines::default(),
    )
    .await
    .unwrap();
    for _ in 0..8 {
        let error = backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                    .start(backend.attempt_deadlines()),
                |attempt| {
                    Box::pin(async move {
                        attempt
                            .execute_batch(vec![
                                SqlStatement::new(
                                    "DO $$ BEGIN RAISE EXCEPTION 'abort' USING ERRCODE='40001'; \
                                     END $$",
                                    vec![],
                                ),
                                SqlStatement::new("SELECT 1", vec![]),
                                SqlStatement::new("SELECT 2", vec![]),
                            ])
                            .await
                    })
                },
            )
            .await
            .err()
            .unwrap();
        assert_eq!(
            error.attempt_kind(),
            Some(storage_types::AttemptKind::Conflict)
        );
        assert!(error.to_string().contains("40001"), "{error}");
        let error = backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                    .start(backend.attempt_deadlines()),
                |attempt| {
                    Box::pin(async move {
                        attempt
                            .query_batch_protected(
                                vec![
                                    (
                                        SqlStatement::new("SELECT 1 / 0", vec![]),
                                        ReadProtection::Snapshot,
                                    ),
                                    (
                                        SqlStatement::new("SELECT 3", vec![]),
                                        ReadProtection::Snapshot,
                                    ),
                                ],
                                std::num::NonZeroUsize::new(4096).unwrap(),
                            )
                            .await
                    })
                },
            )
            .await
            .err()
            .unwrap();
        assert!(
            matches!(error.to_enum(), StorageEnum::InternalServerError { message } if message.contains("22012"))
        );
    }
    let page = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .query(
                            SqlStatement::new("SELECT 4::bigint", vec![]),
                            ReadProtection::Snapshot,
                            std::num::NonZeroUsize::new(4096).unwrap(),
                        )
                        .await
                })
            },
        )
        .await
        .unwrap();
    assert_eq!(page.rows.len(), 1);
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_warning_before_ordinary_commit_when_acknowledged_then_result_is_unknown() {
    use crate::{AttemptMode, SqlAttempt, SqlBackend, SqlStatement};
    let fixture = super::test_support::NativeFixture::open().await;
    let result = fixture
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(fixture.backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .execute(SqlStatement::new(
                            "DO $$ BEGIN RAISE WARNING 'fixture'; END $$",
                            vec![],
                        ))
                        .await
                })
            },
        )
        .await;
    assert!(result.unwrap_err().is_commit_unknown());
    fixture.close().await;
}
