use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use storage_types::{
    AttemptDeadline, AttemptMode, AttemptPlan, AttemptRequest, DeadlineClass,
    context::WrappedError as _,
};

use super::{driver::PostgresBackend, owned_server_test_support::OwnedServer};
use crate::{SqlAttempt as _, SqlBackend as _, SqlStatement};

fn request() -> AttemptRequest {
    AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write).started(AttemptDeadline {
        class: DeadlineClass::Write,
        expires_at: tokio::time::Instant::now() + Duration::from_secs(20),
        commit_grace: Duration::from_secs(10),
    })
}

async fn observe_wait(admin: &tokio_postgres::Client, pid: i32, event: &str) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if admin
                .query_one(
                    "SELECT coalesce(wait_event, '') FROM pg_stat_activity WHERE pid=$1",
                    &[&pid],
                )
                .await
                .unwrap()
                .get::<_, String>(0)
                == event
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("exact backend native wait");
    eprintln!("owned_wait pid={pid} event={event}");
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_BIN; starts an isolated owned server"]
async fn given_missing_sync_standby_when_commit_wait_cancelled_then_raw_success_but_backend_unknown()
 {
    let server = OwnedServer::start().await;
    let admin = server.connect().await;
    admin
        .client
        .batch_execute(
            "CREATE TABLE native_commit_probe(value bigint NOT NULL); CREATE TABLE \
             counted_owned(value bigint PRIMARY KEY, payload bigint NOT NULL); CREATE TABLE \
             sys_system_stream_scope(slot bigint NOT NULL)",
        )
        .await
        .unwrap();
    let backend = Arc::new(
        PostgresBackend::initialize(&server.dsn(), 1, false, Default::default())
            .await
            .unwrap(),
    );
    let held = backend.pool.get().await.unwrap();
    let backend_pid: i32 = held
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let cleanup = held.observe_cleanup();
    drop(held);
    admin
        .client
        .batch_execute(
            "ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (aux_missing_owned_standby)'",
        )
        .await
        .unwrap();
    admin
        .client
        .query_one("SELECT pg_reload_conf()", &[])
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let names: String = admin
                .client
                .query_one("SHOW synchronous_standby_names", &[])
                .await
                .unwrap()
                .get(0);
            if names == "FIRST 1 (aux_missing_owned_standby)" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        admin
            .client
            .query_one(
                "SELECT count(*) FROM pg_stat_replication WHERE sync_state IN ('sync','quorum')",
                &[]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    let raw = server.connect().await;
    let raw_pid: i32 = raw
        .client
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    raw.client
        .batch_execute("BEGIN; INSERT INTO native_commit_probe VALUES(1)")
        .await
        .unwrap();
    let raw_commit = raw.client.batch_execute("COMMIT");
    let control = async {
        observe_wait(&admin.client, raw_pid, "SyncRep").await;
        assert_eq!(
            admin
                .client
                .query_one("SELECT count(*) FROM native_commit_probe", &[])
                .await
                .unwrap()
                .get::<_, i64>(0),
            0
        );
        eprintln!("owned_raw_wait externally_visible_rows=0");
        assert!(
            admin
                .client
                .query_one("SELECT pg_cancel_backend($1)", &[&raw_pid])
                .await
                .unwrap()
                .get::<_, bool>(0)
        );
    };
    let (raw_result, ()) = tokio::join!(raw_commit, control);
    raw_result.expect("generic COMMIT succeeds despite cancelled replication wait");
    assert_eq!(raw.warnings.load(Ordering::Relaxed), 1);
    eprintln!("owned_raw_commit success=true warning01000=1 locally_visible_rows=1");
    let operation_backend = backend.clone();
    let operation = tokio::spawn(async move {
        operation_backend
            .with_attempt(request(), |attempt| {
                Box::pin(async move {
                    attempt
                        .execute(SqlStatement::new(
                            "INSERT INTO native_commit_probe VALUES(2)",
                            vec![],
                        ))
                        .await
                })
            })
            .await
    });
    observe_wait(&admin.client, backend_pid, "SyncRep").await;
    assert_eq!(
        admin
            .client
            .query_one("SELECT count(*) FROM native_commit_probe", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    assert!(
        admin
            .client
            .query_one("SELECT pg_cancel_backend($1)", &[&backend_pid])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    let error = tokio::time::timeout(Duration::from_secs(10), operation)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(error.is_commit_unknown(), "{error:?}");
    assert!(
        !error
            .attempt_kind()
            .is_some_and(|kind| kind != storage_types::AttemptKind::CommitUnknown)
    );
    assert!(
        *cleanup.borrow(),
        "actual client connection destroyed before result"
    );
    assert_eq!(backend.pool.status().size, 0);
    assert_eq!(
        admin
            .client
            .query_one("SELECT count(*) FROM native_commit_probe", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        2
    );
    eprintln!("owned_hardened_commit unknown=true retry=false rows=2 pool_size=0 cleanup=true");
    let held = backend.pool.get().await.unwrap();
    let counted_pid: i32 = held
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    assert_ne!(
        counted_pid, backend_pid,
        "flagged physical connection cannot be reused"
    );
    let counted_cleanup = held.observe_cleanup();
    drop(held);
    let operation_backend = backend.clone();
    let operation = tokio::spawn(async move {
        operation_backend
            .with_attempt(request(), |attempt| {
                Box::pin(async move {
                    let counted = crate::write_batch::UpsertShape::try_new(
                        vec!["value".into(), "payload".into()],
                        vec![false, false],
                        vec!["value".into()],
                    )?
                    .render_counted(
                        crate::SqlEngine::Postgres,
                        "counted_owned",
                        vec![vec![
                            crate::SqlValue::Integer(1),
                            crate::SqlValue::Integer(7),
                        ]],
                    )?;
                    attempt
                        .finish_write_batch(
                            vec![counted],
                            std::num::NonZeroUsize::new(4096).unwrap(),
                        )
                        .await?
                        .map_err(|_| {
                            storage_types::StorageError::internal("unexpected counted rejection")
                        })?;
                    Ok(())
                })
            })
            .await
    });
    observe_wait(&admin.client, counted_pid, "SyncRep").await;
    assert!(
        admin
            .client
            .query_one("SELECT pg_cancel_backend($1)", &[&counted_pid])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    let error = tokio::time::timeout(Duration::from_secs(10), operation)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(error.is_commit_unknown(), "{error:?}");
    assert!(
        !error
            .attempt_kind()
            .is_some_and(|kind| kind != storage_types::AttemptKind::CommitUnknown)
    );
    assert!(*counted_cleanup.borrow());
    assert_eq!(backend.pool.status().size, 0);
    assert_eq!(
        admin
            .client
            .query_one("SELECT count(*) FROM counted_owned", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    eprintln!(
        "owned_counted_commit unknown=true retry=false rows=1 pool_size=0 cleanup=true \
         fresh_pid=true"
    );
    drop(backend);
    raw.close().await;
    admin.close().await;
    server.finish();
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_BIN; starts an isolated owned server"]
async fn given_stopped_owned_server_when_acquiring_then_no_callback_and_connection_unavailable() {
    let mut server = OwnedServer::start().await;
    let invalid_dsn = format!("{} user=aux_missing_owned_user", server.dsn());
    let error = PostgresBackend::initialize(&invalid_dsn, 1, false, Default::default())
        .await
        .err()
        .expect("missing role rejected");
    assert!(
        !error
            .attempt_kind()
            .is_some_and(|kind| kind != storage_types::AttemptKind::CommitUnknown),
        "permanent startup failure is not unavailability"
    );
    eprintln!("owned_missing_role permanent=true unavailable=false");
    let backend = PostgresBackend::initialize(&server.dsn(), 1, false, Default::default())
        .await
        .unwrap();
    // Remove the idle connection so this operation must create a physical
    // session.
    let held = backend.pool.get().await.unwrap();
    super::pool::Client::take(held).discard().await;
    server.stop();
    let called = Arc::new(AtomicBool::new(false));
    let callback_called = called.clone();
    let error = backend
        .with_attempt(request(), move |_| {
            Box::pin(async move {
                callback_called.store(true, Ordering::Relaxed);
                Ok(())
            })
        })
        .await
        .unwrap_err();
    eprintln!(
        "owned_acquisition callback={} kind={:?} error={error:?}",
        called.load(Ordering::Relaxed),
        error.attempt_kind()
    );
    assert!(!called.load(Ordering::Relaxed));
    assert!(!error.is_commit_unknown());
    assert!(matches!(
        error.to_enum(),
        storage_types::StorageEnum::Attempt(storage_types::AttemptFailure::Unavailable(
            storage_types::NativeUnavailable::ConnectionLost
        ))
    ));
    assert!(
        error
            .attempt_kind()
            .is_some_and(storage_types::AttemptKind::is_unavailable)
    );
    drop(backend);
    server.finish();
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_BIN; starts an isolated owned server"]
async fn given_uncommitted_write_and_blocked_query_when_owned_server_stops_then_known_and_row_absent()
 {
    let mut server = OwnedServer::start().await;
    let admin = server.connect().await;
    admin
        .client
        .batch_execute("CREATE TABLE native_commit_probe(value bigint NOT NULL)")
        .await
        .unwrap();
    let backend = Arc::new(
        PostgresBackend::initialize(&server.dsn(), 1, false, Default::default())
            .await
            .unwrap(),
    );
    let held = backend.pool.get().await.unwrap();
    let pid: i32 = held
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let cleanup = held.observe_cleanup();
    drop(held);
    let operation_backend = backend.clone();
    let operation = tokio::spawn(async move {
        operation_backend
            .with_attempt(request(), |attempt| {
                Box::pin(async move {
                    attempt
                        .execute(SqlStatement::new(
                            "INSERT INTO native_commit_probe VALUES(1)",
                            vec![],
                        ))
                        .await?;
                    attempt
                        .execute(SqlStatement::new("SELECT pg_sleep(120)", vec![]))
                        .await
                })
            })
            .await
    });
    observe_wait(&admin.client, pid, "PgSleep").await;
    server.stop();
    let error = tokio::time::timeout(Duration::from_secs(35), operation)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(
        !error.is_commit_unknown(),
        "no COMMIT was sent, so the outcome is a known abort: {error:?}"
    );
    assert!(*cleanup.borrow());
    assert_eq!(backend.pool.status().size, 0);
    admin.close().await;
    drop(backend);
    server.restart().await;
    let admin = server.connect().await;
    assert_eq!(
        admin
            .client
            .query_one("SELECT count(*) FROM native_commit_probe", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    eprintln!("owned_stopped_active unknown=false cleanup=true restarted_rows=0");
    admin.close().await;
    server.finish();
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_BIN; starts an isolated owned server"]
async fn given_owned_server_when_shutdown_command_cannot_spawn_then_child_stays_owned_until_reaped()
{
    let mut server = OwnedServer::start().await;
    let admin = server.connect().await;
    let pid = server.child.as_ref().unwrap().id();
    let missing_bin = tempfile::Builder::new()
        .prefix("auxpg-missing-")
        .tempdir_in("/tmp")
        .unwrap();
    let original_bin = std::mem::replace(&mut server.bin, missing_bin.path().to_owned());
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| server.stop()));
    // Restore the actual explicit binary path before assertions or teardown.
    server.bin = original_bin;
    assert!(failure.is_err(), "missing owned pg_ctl must fail to spawn");
    let child = server
        .child
        .as_mut()
        .expect("shutdown failure retains child ownership");
    assert_eq!(child.id(), pid);
    assert!(
        child.try_wait().unwrap().is_none(),
        "same owned server is still live"
    );
    assert_eq!(
        admin
            .client
            .query_one("SELECT 1::bigint", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    eprintln!(
        "owned_shutdown_spawn_failure pid={pid} same_child_owned=true alive=true \
         directory_retained=true"
    );
    admin.close().await;
    server.stop();
    assert!(
        server.child.is_none(),
        "successful shutdown joined and released child"
    );
    server.finish();
    missing_bin.close().unwrap();
}
