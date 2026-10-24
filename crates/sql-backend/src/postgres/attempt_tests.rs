use std::{borrow::Cow, hint::black_box};

use alloc_counter::AllocationGuard;

use super::statements::append_query_protection;
use crate::{ReadProtection, SqlEngine, sql_template::sql_template};

#[test]
fn given_borrowed_or_owned_query_when_protected_then_preserve_lock_sql() {
    for (protection, suffix) in [
        (ReadProtection::Snapshot, ""),
        (ReadProtection::Shared, " FOR SHARE"),
        (ReadProtection::Conflict, " FOR UPDATE"),
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

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_query_protection_when_sql_built_then_compare_dynamic_and_static_templates() {
    for (protection, label) in [
        (ReadProtection::Snapshot, "snapshot"),
        (ReadProtection::Shared, "shared"),
        (ReadProtection::Conflict, "conflict"),
    ] {
        let baseline = || {
            let mut sql = format!(
                "SELECT row_blob FROM sys_user_stream_catalog WHERE name = {}",
                SqlEngine::Postgres.placeholder(black_box(1))
            );
            match protection {
                ReadProtection::Snapshot => (),
                ReadProtection::Shared => sql.push_str(" FOR SHARE"),
                ReadProtection::Conflict => sql.push_str(" FOR UPDATE"),
            }
            Cow::Owned(sql)
        };
        let optimized = || {
            append_query_protection(
                sql_template!(SqlEngine::Postgres, "SELECT row_blob FROM sys_user_stream_catalog WHERE name = " {1}),
                black_box(protection),
            ).unwrap()
        };
        assert_eq!(baseline(), optimized());
        report_sql(&format!("postgres_{label}_dynamic"), baseline);
        report_sql(&format!("postgres_{label}_static"), optimized);
    }
}

fn report_sql(label: &str, render: impl Fn() -> Cow<'static, str>) {
    for _ in 0..100 {
        drop(black_box(render()));
    }
    let guard = AllocationGuard::start(
        module_path!(),
        "sql_protection",
        file!(),
        line!(),
        Some(label),
    );
    let started = cpu_time::ProcessTime::now();
    for _ in 0..100_000 {
        drop(black_box(render()));
    }
    let cpu = started.elapsed();
    alloc_counter::emit_report(&guard.finish()).unwrap();
    eprintln!(
        "sql_protection label={label} iterations=100000 cpu_ns={}",
        cpu.as_nanos()
    );
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_failed_native_pipeline_when_settled_then_preserve_origin_and_reuse_connection() {
    use storage_types::{StorageEnum, context::WrappedError};

    use crate::{AttemptMode, SqlAttempt, SqlBackend, SqlStatement};

    let dsn = std::env::var("TEST_POSTGRES_DSN").unwrap();
    let backend = super::PostgresBackend::initialize(&dsn, 1, false)
        .await
        .unwrap();
    for _ in 0..8 {
        let error = backend
            .with_attempt(AttemptMode::ReadWrite, |attempt| {
                Box::pin(async move {
                    attempt
                        .execute_batch(vec![
                            SqlStatement::new(
                                "DO $$ BEGIN RAISE EXCEPTION 'abort' USING ERRCODE='40001'; END $$",
                                vec![],
                            ),
                            SqlStatement::new("SELECT 1", vec![]),
                            SqlStatement::new("SELECT 2", vec![]),
                        ])
                        .await
                })
            })
            .await
            .err()
            .unwrap();
        assert!(
            matches!(error.to_enum(), StorageEnum::TransactionConflict { message } if message.contains("40001"))
        );
        let error = backend
            .with_attempt(AttemptMode::ReadWrite, |attempt| {
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
            })
            .await
            .err()
            .unwrap();
        assert!(
            matches!(error.to_enum(), StorageEnum::InternalServerError { message } if message.contains("22012"))
        );
    }
    let page = backend
        .with_attempt(AttemptMode::ReadOnly, |attempt| {
            Box::pin(async move {
                attempt
                    .query(
                        SqlStatement::new("SELECT 4::bigint", vec![]),
                        ReadProtection::Snapshot,
                        std::num::NonZeroUsize::new(4096).unwrap(),
                    )
                    .await
            })
        })
        .await
        .unwrap();
    assert_eq!(page.rows.len(), 1);
}
