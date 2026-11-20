use futures::FutureExt as _;

use super::driver::{PostgresBackend, PostgresTransaction};
use crate::{
    SqlAttempt as _,
    postgres::pool::{Manager, Pool},
};

async fn open_pool() -> (Pool, tokio::sync::watch::Receiver<bool>) {
    let dsn = std::env::var("TEST_POSTGRES_DSN").unwrap();
    let pool = Pool::builder(
        Manager::try_new(
            dsn.parse().unwrap(),
            tokio_postgres::NoTls,
            storage_types::AttemptDeadlines::default(),
        )
        .unwrap(),
    )
    .max_size(1)
    .build()
    .unwrap();
    let client = pool.get().await.unwrap();
    let stopped = client.observe_cleanup();
    drop(client);
    (pool, stopped)
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_unknown_attempt_when_result_returns_then_local_connection_cleanup_has_finished() {
    let (pool, stopped) = open_pool().await;
    let result = PostgresBackend::run_attempt(
        pool.clone(),
        std::sync::Arc::new(storage_common::native_attempt_lifecycle::AttemptLifecycle::new()),
        (PostgresTransaction::Snapshot, None),
        storage_types::AttemptDurability::Durable,
        (
            storage_types::AttemptDeadlines::default()
                .start(storage_types::DeadlineClass::PointRead),
            std::sync::Arc::new(storage_types::SystemClock),
        ),
        None,
        |attempt| {
            Box::pin(async move {
                let error = storage_types::StorageError::commit_unknown("test unknown settlement");
                attempt.reject_settlement_error(&error);
                Err::<(), _>(error)
            })
        },
    )
    .await;
    assert!(result.err().unwrap().is_commit_unknown());
    assert!(
        *stopped.borrow(),
        "native result must await actual connection destruction"
    );
    assert_eq!(pool.status().size, 0);
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_panicking_callback_when_unwind_resumes_then_local_connection_cleanup_has_finished() {
    let (pool, stopped) = open_pool().await;
    let result = std::panic::AssertUnwindSafe(PostgresBackend::run_attempt::<(), _>(
        pool.clone(),
        std::sync::Arc::new(storage_common::native_attempt_lifecycle::AttemptLifecycle::new()),
        (PostgresTransaction::Snapshot, None),
        storage_types::AttemptDurability::Durable,
        (
            storage_types::AttemptDeadlines::default()
                .start(storage_types::DeadlineClass::PointRead),
            std::sync::Arc::new(storage_types::SystemClock),
        ),
        None,
        |_| Box::pin(async { panic!("callback panic") }),
    ))
    .catch_unwind()
    .await;
    assert!(result.is_err());
    assert!(
        *stopped.borrow(),
        "unwind must await actual connection destruction"
    );
    assert_eq!(pool.status().size, 0);
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_acknowledged_attempt_when_result_returns_then_healthy_connection_remains_reusable() {
    let (pool, stopped) = open_pool().await;
    let result = PostgresBackend::run_attempt(
        pool.clone(),
        std::sync::Arc::new(storage_common::native_attempt_lifecycle::AttemptLifecycle::new()),
        (PostgresTransaction::Snapshot, None),
        storage_types::AttemptDurability::Durable,
        (
            storage_types::AttemptDeadlines::default()
                .start(storage_types::DeadlineClass::PointRead),
            std::sync::Arc::new(storage_types::SystemClock),
        ),
        None,
        |_| Box::pin(async { Ok(()) }),
    )
    .await;
    assert!(result.is_ok());
    assert!(!*stopped.borrow());
    assert_eq!(pool.status().available, 1);
    super::pool::Client::take(pool.get().await.unwrap())
        .discard()
        .await;
    assert!(*stopped.borrow());
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_panicking_callback_construction_when_unwind_resumes_then_cleanup_has_finished() {
    let (pool, stopped) = open_pool().await;
    let result = std::panic::AssertUnwindSafe(PostgresBackend::run_attempt::<(), _>(
        pool.clone(),
        std::sync::Arc::new(storage_common::native_attempt_lifecycle::AttemptLifecycle::new()),
        (PostgresTransaction::Snapshot, None),
        storage_types::AttemptDurability::Durable,
        (
            storage_types::AttemptDeadlines::default()
                .start(storage_types::DeadlineClass::PointRead),
            std::sync::Arc::new(storage_types::SystemClock),
        ),
        None,
        |_| panic!("callback construction panic"),
    ))
    .catch_unwind()
    .await;
    assert!(result.is_err());
    assert!(*stopped.borrow());
    assert_eq!(pool.status().size, 0);
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_dropped_response_waiter_when_native_callback_is_held_then_exclusive_client_remains_owned()
 {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    use crate::{ReadProtection, SqlBackend as _, SqlStatement};
    let backend = Arc::new(
        PostgresBackend::initialize(
            &std::env::var("TEST_POSTGRES_DSN").unwrap(),
            1,
            false,
            storage_types::AttemptDeadlines::default(),
        )
        .await
        .unwrap(),
    );
    let pool = backend.pool.clone();
    let client = pool.get().await.unwrap();
    let stopped = client.observe_cleanup();
    drop(client);
    let release = Arc::new(tokio::sync::Notify::new());
    let callback_release = release.clone();
    let finished = Arc::new(AtomicBool::new(false));
    let callback_finished = finished.clone();
    let (started, received) = tokio::sync::oneshot::channel();
    let owner = backend.clone();
    let waiter = tokio::spawn(async move {
        let deadline = storage_types::AttemptDeadline {
            class: storage_types::DeadlineClass::PointRead,
            expires_at: tokio::time::Instant::now() + Duration::from_secs(30),
            commit_grace: Duration::from_secs(30),
        };
        owner
            .with_attempt(
                storage_types::AttemptPlan::new(
                    storage_types::AttemptMode::ReadOnly,
                    deadline.class,
                )
                .started(deadline),
                |attempt| {
                    Box::pin(async move {
                        attempt
                            .query(
                                SqlStatement::new("SELECT 1::bigint", vec![]),
                                ReadProtection::Snapshot,
                                std::num::NonZeroUsize::new(4096).unwrap(),
                            )
                            .await?;
                        started.send(()).unwrap();
                        callback_release.notified().await;
                        callback_finished.store(true, Ordering::Release);
                        Ok(())
                    })
                },
            )
            .await
    });
    received.await.unwrap();
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert_eq!(
        pool.status().available,
        0,
        "dropping the response waiter cannot release the native owner"
    );
    assert!(!finished.load(Ordering::Acquire));
    assert!(!*stopped.borrow());
    release.notify_one();
    let client = tokio::time::timeout(Duration::from_secs(60), pool.get())
        .await
        .unwrap()
        .unwrap();
    assert!(finished.load(Ordering::Acquire));
    assert!(
        !*stopped.borrow(),
        "normal settlement still reuses the native connection"
    );
    super::pool::Client::take(client).discard().await;
    assert!(*stopped.borrow());
}

#[test]
fn given_confirmed_write_when_callback_returns_retry_control_in_either_shape_then_result_is_nonretryable()
 {
    for error in [
        storage_types::StorageEnum::Attempt(storage_types::AttemptFailure::Conflict(
            storage_types::ConflictCause::Logical("post-commit callback conflict"),
        ))
        .into(),
        storage_types::AttemptFailure::Unavailable(storage_types::NativeUnavailable::Deadline(
            storage_types::AttemptPhase::Statement,
        ))
        .into(),
    ] {
        let result = super::driver::normalize_committed_result::<()>(Err(error), true);
        let error = result.expect_err("no success after confirmed write failure");
        assert!(!error.is_commit_unknown());
        assert!(
            !error
                .attempt_kind()
                .is_some_and(|kind| kind != storage_types::AttemptKind::CommitUnknown)
        );
        assert!(!error.class().is_retryable());
    }
    let unknown = storage_types::StorageError::commit_unknown("earlier native unknown");
    assert!(
        super::driver::normalize_committed_result::<()>(Err(unknown), true)
            .err()
            .unwrap()
            .is_commit_unknown()
    );
    let permanent = storage_types::StorageError::internal("earlier permanent native cause");
    let expected = format!("{:?}", permanent.as_ref());
    let actual = super::driver::normalize_committed_result::<()>(Err(permanent), true)
        .err()
        .unwrap();
    assert_eq!(format!("{:?}", actual.as_ref()), expected);
}
