//! A native TCP forwarding fixture loses selected replies after forwarding the
//! actual command. Independent native reads still establish its durable result.
use std::{sync::atomic::Ordering, time::Duration};

use storage_types::{AttemptDeadline, AttemptDurability, DeadlineClass, StorageError};

use super::{
    driver::{PostgresBackend, PostgresTransaction},
    pool::{Manager, Pool},
    test_support::NativeFixture,
};
use crate::{SqlAttempt, SqlStatement};

async fn open_reply_loss_pool(
    schema: &str,
    command: &'static [u8],
) -> (Pool, super::reply_loss::ReplyLossForwarder) {
    let dsn = std::env::var("TEST_POSTGRES_DSN").unwrap();
    let upstream: tokio_postgres::Config = dsn.parse().unwrap();
    let forwarder = super::reply_loss::ReplyLossForwarder::bind(&dsn, command).await;
    let port = forwarder.port;
    let mut config = tokio_postgres::Config::new();
    config
        .host("127.0.0.1")
        .port(port)
        .user(upstream.get_user().unwrap())
        .dbname(upstream.get_dbname().unwrap())
        .ssl_mode(tokio_postgres::config::SslMode::Disable)
        .options(format!("-csearch_path={schema}"));
    if let Some(password) = upstream.get_password() {
        config.password(password);
    }
    let pool = Pool::builder(
        Manager::try_new(
            config,
            tokio_postgres::NoTls,
            storage_types::AttemptDeadlines::default(),
        )
        .unwrap(),
    )
    .max_size(1)
    .build()
    .unwrap();
    drop(pool.get().await.unwrap());
    forwarder.arm();
    (pool, forwarder)
}

/// The manual clock moves the deadline: after a sent COMMIT the attempt waits
/// one grace timer for the answer and then reports unknown; before COMMIT
/// (a lost ROLLBACK answer) it reports the known abort at once.
async fn check_lost_reply(commit: bool) {
    let fixture = NativeFixture::open().await;
    let command: &'static [u8] = if commit { b"COMMIT\0" } else { b"ROLLBACK\0" };
    let (pool, forwarder) = open_reply_loss_pool(&fixture.schema, command).await;
    let client = pool.get().await.unwrap();
    let stopped = client.observe_cleanup();
    drop(client);
    let clock = std::sync::Arc::new(crate::test_support::ManualClock::default());
    let deadline = AttemptDeadline {
        class: DeadlineClass::Write,
        expires_at: tokio::time::Instant::now() + Duration::from_secs(30),
        commit_grace: Duration::from_secs(2),
    };
    let task = tokio::spawn(PostgresBackend::run_attempt(
        pool.clone(),
        std::sync::Arc::new(storage_common::native_attempt_lifecycle::AttemptLifecycle::new()),
        (PostgresTransaction::Serializable, None),
        AttemptDurability::Durable,
        (deadline, clock.clone()),
        None,
        move |attempt| {
            Box::pin(async move {
                attempt
                    .execute(SqlStatement::new(
                        "INSERT INTO native_commit_probe VALUES (1)",
                        vec![],
                    ))
                    .await?;
                if commit {
                    Ok(())
                } else {
                    Err(StorageError::internal("request abort after native write"))
                }
            })
        },
    ));
    tokio::time::timeout(Duration::from_secs(10), async {
        while forwarder.commands.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("settlement command forwarded");
    tokio::time::pause();
    clock.pass(deadline.expires_at).await;
    if commit {
        tokio::time::timeout(Duration::from_secs(10), async {
            while clock.sleeps() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("grace timer started");
        clock.pass(deadline.settle_by()).await;
    }
    let result = task.await.unwrap();
    tokio::time::resume();
    let error = result.err().unwrap();
    assert_eq!(error.is_commit_unknown(), commit, "{error:?}");
    if !commit {
        assert!(
            error
                .attempt_kind()
                .is_some_and(storage_types::AttemptKind::is_unavailable),
            "{error:?}"
        );
    }
    assert_eq!(
        clock.sleeps(),
        1 + usize::from(commit),
        "a grace timer only after a sent COMMIT"
    );
    assert_eq!(
        forwarder.commands.load(Ordering::Acquire),
        1,
        "no second settlement command"
    );
    assert!(*stopped.borrow(), "response follows actual socket cleanup");
    assert_eq!(pool.status().size, 0);
    tokio::time::timeout(Duration::from_secs(10), forwarder.close())
        .await
        .unwrap();
    assert_eq!(fixture.count_records().await, i64::from(commit));
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_submitted_commit_with_lost_reply_when_grace_expires_then_unknown_and_one_durable_write()
 {
    check_lost_reply(true).await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_submitted_rollback_with_lost_reply_when_deadline_expires_then_known_abort_without_second_abort()
 {
    check_lost_reply(false).await;
}

/// The pipelined COMMIT of `finish_write_batch` is a simple query like the
/// separate COMMIT, so the forwarder loses its reply the same way. The sent
/// COMMIT makes the lost answer unknown, and the write is durable.
#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_pipelined_batch_commit_with_lost_reply_when_grace_expires_then_unknown_and_one_durable_write()
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
    let (pool, forwarder) = open_reply_loss_pool(&fixture.schema, b"COMMIT\0").await;
    let client = pool.get().await.unwrap();
    let stopped = client.observe_cleanup();
    drop(client);
    let clock = std::sync::Arc::new(crate::test_support::ManualClock::default());
    let deadline = AttemptDeadline {
        class: DeadlineClass::Write,
        expires_at: tokio::time::Instant::now() + Duration::from_secs(30),
        commit_grace: Duration::from_secs(2),
    };
    let task = tokio::spawn(PostgresBackend::run_attempt(
        pool.clone(),
        std::sync::Arc::new(storage_common::native_attempt_lifecycle::AttemptLifecycle::new()),
        (PostgresTransaction::Serializable, None),
        AttemptDurability::Durable,
        (deadline, clock.clone()),
        None,
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
                    .finish_write_batch(vec![counted], std::num::NonZeroUsize::new(4096).unwrap())
                    .await?
                    .map_err(|_| StorageError::internal("unexpected counted failure"))?;
                Ok(())
            })
        },
    ));
    tokio::time::timeout(Duration::from_secs(10), async {
        while forwarder.commands.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("pipelined COMMIT forwarded");
    let wave_count_sql = format!(
        "SELECT count(*)::bigint FROM {}.counted_deadline",
        fixture.schema
    );
    let wave_started = std::time::Instant::now();
    let mut wave_count = fixture
        .admin
        .query_one(&wave_count_sql, &[])
        .await
        .unwrap()
        .get::<_, i64>(0);
    while wave_count != 1 && wave_started.elapsed() < Duration::from_secs(10) {
        tokio::time::sleep(Duration::from_millis(20)).await;
        wave_count = fixture
            .admin
            .query_one(&wave_count_sql, &[])
            .await
            .unwrap()
            .get::<_, i64>(0);
    }
    assert_eq!(
        wave_count,
        1,
        "server did not commit the pipelined wave before the client gave up (a code question, not \
         a test race); last count {wave_count} after {:?}",
        wave_started.elapsed()
    );
    tokio::time::pause();
    clock.pass(deadline.expires_at).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while clock.sleeps() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("grace timer started");
    clock.pass(deadline.settle_by()).await;
    let result = task.await.unwrap();
    tokio::time::resume();
    let error = result.err().unwrap();
    assert!(error.is_commit_unknown(), "{error:?}");
    assert_eq!(clock.sleeps(), 2, "a deadline timer and one grace timer");
    assert_eq!(
        forwarder.commands.load(Ordering::Acquire),
        1,
        "no second settlement command"
    );
    assert!(*stopped.borrow(), "response follows actual socket cleanup");
    assert_eq!(pool.status().size, 0);
    tokio::time::timeout(Duration::from_secs(10), forwarder.close())
        .await
        .unwrap();
    let count_sql = format!(
        "SELECT count(*)::bigint FROM {}.counted_deadline",
        fixture.schema
    );
    let started = std::time::Instant::now();
    let mut count = fixture
        .admin
        .query_one(&count_sql, &[])
        .await
        .unwrap()
        .get::<_, i64>(0);
    while count != 1 && started.elapsed() < Duration::from_secs(10) {
        tokio::time::sleep(Duration::from_millis(20)).await;
        count = fixture
            .admin
            .query_one(&count_sql, &[])
            .await
            .unwrap()
            .get::<_, i64>(0);
    }
    assert_eq!(
        count,
        1,
        "durable count after waiting {:?}; last count {count}",
        started.elapsed()
    );
    fixture.close().await;
}
