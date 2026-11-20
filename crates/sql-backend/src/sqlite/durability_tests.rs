//! The SQLite WAL barrier: off the writer connection, on the current WAL
//! file after a WAL reset and a reopen, and a failed sync stops the store.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
    mpsc,
};

use storage_types::{AttemptPlan, DeadlineClass, NativeUnavailable, StorageEnum};

use super::{
    driver::SqliteBackend,
    durability::{SyncHook, initialize_batch, initialize_batch_with_sync_hook},
};
use crate::{AttemptMode, SqlAttempt as _, SqlBackend as _, SqlStatement};

fn temp_path(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("{label}-{}.db", uuid::Uuid::now_v7()))
}

fn remove(path: &std::path::Path) {
    for suffix in ["", "-wal", "-shm"] {
        let mut file = path.as_os_str().to_owned();
        file.push(suffix);
        let _ = std::fs::remove_file(file);
    }
}

async fn open(path: &std::path::Path) -> SqliteBackend {
    SqliteBackend::initialize(path, storage_types::AttemptDeadlines::default())
        .await
        .unwrap()
}

async fn execute(backend: &SqliteBackend, sql: &'static str) -> storage_types::StorageResult<u64> {
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move { attempt.execute(SqlStatement::new(sql, vec![])).await })
            },
        )
        .await
}

fn rows(path: &std::path::Path) -> i64 {
    rusqlite::Connection::open(path)
        .unwrap()
        .query_row("SELECT count(*) FROM proof", [], |row| row.get(0))
        .unwrap()
}

async fn replace_batch(backend: &mut SqliteBackend, hook: SyncHook) {
    let batch = initialize_batch_with_sync_hook(backend.connections[0].clone(), hook)
        .await
        .unwrap()
        .unwrap();
    while batch.stats().barriers_completed < 1 {
        tokio::task::yield_now().await;
    }
    backend.durability = Some(batch);
}

#[tokio::test]
async fn given_memory_database_when_initialized_then_has_no_durability_batch() {
    let connection = tokio_rusqlite::Connection::open_in_memory().await.unwrap();
    assert!(
        initialize_batch(connection, super::durability::BarrierObserver::default(),)
            .await
            .unwrap()
            .is_none()
    );
}

// A2: the WAL sync does not occupy the writer connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_blocked_barrier_when_second_write_runs_then_it_commits_before_the_barrier_opens() {
    let path = temp_path("sqlite-durability-off-writer");
    let mut backend = open(&path).await;
    execute(&backend, "CREATE TABLE proof (id INTEGER PRIMARY KEY)")
        .await
        .unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let (entered, sync_entered) = mpsc::channel::<()>();
    let (release, released) = mpsc::channel::<()>();
    let released = Arc::new(Mutex::new(released));
    replace_batch(&mut backend, {
        let armed = Arc::clone(&armed);
        Arc::new(move || {
            if armed.swap(false, Ordering::SeqCst) {
                entered.send(()).unwrap();
                released.lock().unwrap().recv().unwrap();
            }
            Ok(())
        })
    })
    .await;
    let backend = Arc::new(backend);
    armed.store(true, Ordering::SeqCst);

    let first = tokio::spawn({
        let backend = Arc::clone(&backend);
        async move { execute(&backend, "INSERT INTO proof VALUES (1)").await }
    });
    tokio::task::spawn_blocking(move || sync_entered.recv().unwrap())
        .await
        .unwrap();
    let second = tokio::spawn({
        let backend = Arc::clone(&backend);
        async move { execute(&backend, "INSERT INTO proof VALUES (2)").await }
    });
    // Both commits are visible while the first sync is still blocked.
    let path_for_poll = path.clone();
    tokio::task::spawn_blocking(move || {
        while rows(&path_for_poll) < 2 {
            std::thread::yield_now();
        }
    })
    .await
    .unwrap();
    assert!(!first.is_finished(), "the first write waits for its sync");
    release.send(()).unwrap();
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    drop(backend);
    assert_eq!(rows(&path), 2);
    remove(&path);
}

#[tokio::test]
async fn given_store_reopened_when_write_waits_then_sync_uses_the_current_wal_file() {
    let path = temp_path("sqlite-durability-reopen");
    let backend = open(&path).await;
    execute(&backend, "CREATE TABLE proof (id INTEGER PRIMARY KEY)")
        .await
        .unwrap();
    execute(&backend, "INSERT INTO proof VALUES (1)")
        .await
        .unwrap();
    drop(backend);

    let backend = open(&path).await;
    execute(&backend, "INSERT INTO proof VALUES (2)")
        .await
        .unwrap();
    // A WAL reset rewrites the same file; the barrier's descriptor stays valid.
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    execute(&backend, "INSERT INTO proof VALUES (3)")
        .await
        .unwrap();
    let stats = backend.durability.as_deref().unwrap().stats();
    assert!(!stats.stopped && stats.pending == 0);
    drop(backend);
    assert_eq!(rows(&path), 3);
    remove(&path);
}

#[tokio::test]
async fn given_failed_wal_sync_when_attempts_run_then_store_reports_unavailable() {
    let path = temp_path("sqlite-durability-failed");
    let mut backend = open(&path).await;
    execute(&backend, "CREATE TABLE proof (id INTEGER PRIMARY KEY)")
        .await
        .unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    replace_batch(&mut backend, {
        let armed = Arc::clone(&armed);
        Arc::new(move || {
            if armed.load(Ordering::SeqCst) {
                return Err(std::io::Error::other("injected fsync failure"));
            }
            Ok(())
        })
    })
    .await;
    armed.store(true, Ordering::SeqCst);
    let written = execute(&backend, "INSERT INTO proof VALUES (1)").await;
    assert!(written.unwrap_err().is_commit_unknown());
    let refused = execute(&backend, "INSERT INTO proof VALUES (2)").await;
    assert!(matches!(
        refused.unwrap_err().as_ref(),
        StorageEnum::Attempt(storage_types::AttemptFailure::Unavailable(
            NativeUnavailable::DurabilityFailed
        ))
    ));
    drop(backend);
    remove(&path);
}

/// W2 D11: on a standard queue only the send waits for the disk; receive and
/// delete commit Relaxed and answer without a barrier wait.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn given_queue_operations_when_they_settle_then_send_waits_and_receive_and_delete_do_not() {
    use queue_provider::{Queue, QueueOperations as _, SendMessageRequest, queue_message_for_send};
    let path = temp_path("sqlite-durability-queue");
    let backend = open(&path).await;
    let batch = backend.durability.clone().unwrap();
    let store = crate::SqlStore::initialize_schema(
        backend,
        storage_types::InitialStreamLineages::ONE,
        true,
    )
    .await
    .unwrap();
    let url = store
        .create_queue(Queue {
            queue_name: "durability".into(),
            queue_url: "http://localhost/queue/durability".into(),
            attributes: Default::default(),
            created_at: storage_types::TimestampMillis::now(),
            ..Default::default()
        })
        .await
        .unwrap()
        .queue_url;
    let waits = || batch.stats().waits;
    let before = waits();
    store
        .send_message(queue_message_for_send(
            SendMessageRequest {
                queue_url: url.clone(),
                message_body: "durable".into(),
                delay_seconds: None,
                message_attributes: None,
                ..Default::default()
            },
            storage_types::TimestampMillis::now(),
        ))
        .await
        .unwrap();
    let after_send = waits();
    let received = store
        .receive_messages(&url, 1, None, None)
        .await
        .unwrap()
        .messages;
    let after_receive = waits();
    store
        .delete_message(&url, received[0].receipt_handle.as_str().into())
        .await
        .unwrap();
    let after_delete = waits();
    assert_eq!(
        (
            after_send - before,
            after_receive - after_send,
            after_delete - after_receive
        ),
        (1, 0, 0)
    );
    drop(store);
    remove(&path);
}
