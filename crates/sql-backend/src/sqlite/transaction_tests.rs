use crate::{AttemptMode, SqlAttempt as _, SqlBackend as _, SqlStatement};

#[tokio::test]
async fn given_memory_backend_when_write_is_followed_by_read_then_both_use_the_same_database() {
    use crate::{ReadProtection, SqlAttempt as _, SqlBackend as _, SqlStatement};
    let backend = super::driver::SqliteBackend::initialize(":memory:")
        .await
        .unwrap();
    backend
        .with_attempt(AttemptMode::ReadWrite, |attempt| {
            Box::pin(async move {
                attempt
                    .execute(SqlStatement::new(
                        "CREATE TABLE proof (id INTEGER PRIMARY KEY)",
                        vec![],
                    ))
                    .await?;
                attempt
                    .execute(SqlStatement::new("INSERT INTO proof VALUES (1)", vec![]))
                    .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    let page = backend
        .with_attempt(AttemptMode::ReadOnly, |attempt| {
            Box::pin(async move {
                attempt
                    .query(
                        SqlStatement::new("SELECT id FROM proof", vec![]),
                        ReadProtection::Snapshot,
                        std::num::NonZeroUsize::new(1024).unwrap(),
                    )
                    .await
            })
        })
        .await
        .unwrap();
    assert_eq!(page.rows.len(), 1);
}

async fn open_backend() -> super::driver::SqliteBackend {
    let backend = super::driver::SqliteBackend::initialize(":memory:")
        .await
        .unwrap();
    backend
        .with_attempt(AttemptMode::ReadWrite, |attempt| {
            Box::pin(async move {
                attempt
                    .execute(SqlStatement::new(
                        "CREATE TABLE proof (id INTEGER PRIMARY KEY)",
                        vec![],
                    ))
                    .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    backend
}

async fn count_rows(backend: &super::driver::SqliteBackend) -> usize {
    backend
        .with_attempt(AttemptMode::ReadOnly, |attempt| {
            Box::pin(async move {
                let page = attempt
                    .query(
                        SqlStatement::new("SELECT id FROM proof", vec![]),
                        crate::ReadProtection::Snapshot,
                        std::num::NonZeroUsize::new(1024).unwrap(),
                    )
                    .await?;
                Ok(page.rows.len())
            })
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn given_write_attempt_when_operation_fails_then_rows_are_rolled_back() {
    let backend = open_backend().await;
    let result: storage_types::StorageResult<()> = backend
        .with_attempt(AttemptMode::ReadWrite, |attempt| {
            Box::pin(async move {
                attempt
                    .execute(SqlStatement::new("INSERT INTO proof VALUES (1)", vec![]))
                    .await?;
                Err(storage_types::StorageError::internal("deliberate failure"))
            })
        })
        .await;
    assert!(result.is_err());
    assert_eq!(count_rows(&backend).await, 0);
}

#[tokio::test]
async fn given_write_attempt_when_operation_panics_then_rows_are_rolled_back() {
    let backend = open_backend().await;
    let result: storage_types::StorageResult<()> = backend
        .with_attempt(AttemptMode::ReadWrite, |attempt| {
            Box::pin(async move {
                attempt
                    .execute(SqlStatement::new("INSERT INTO proof VALUES (1)", vec![]))
                    .await?;
                panic!("deliberate failure");
            })
        })
        .await;
    assert!(result.is_err());
    assert_eq!(count_rows(&backend).await, 0);
}

#[tokio::test]
async fn given_external_await_when_worker_resumes_then_transaction_commits() {
    let backend = open_backend().await;
    backend
        .with_attempt(AttemptMode::ReadWrite, |attempt| {
            Box::pin(async move {
                attempt
                    .execute(SqlStatement::new("INSERT INTO proof VALUES (1)", vec![]))
                    .await?;
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                attempt
                    .execute(SqlStatement::new("INSERT INTO proof VALUES (2)", vec![]))
                    .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    assert_eq!(count_rows(&backend).await, 2);
}

// Foreign-key enforcement must be enabled before BEGIN; public schema callbacks
// already run inside an attempt. This configured native connection proves an
// actual COMMIT failure, which injected boundary failures do not exercise.
#[test]
fn given_failed_native_commit_when_attempt_settles_then_unknown_classification_survives() {
    let mut connection = rusqlite::Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "PRAGMA foreign_keys = ON; CREATE TABLE parent (id INTEGER PRIMARY KEY); CREATE TABLE \
             child (id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)",
        )
        .unwrap();
    let (session, commands) = super::transaction::Session::open();
    let result: storage_types::StorageResult<()> = super::transaction::execute_attempt(
        &mut connection,
        AttemptMode::ReadWrite,
        Box::pin(async move {
            session
                .call(|native| {
                    native
                        .execute("INSERT INTO child VALUES (1)", [])
                        .map_err(super::error::map_driver_error)?;
                    Ok(())
                })
                .await?;
            Ok(())
        }),
        commands,
    );
    assert!(matches!(
        (result).as_ref().err().and_then(|error| error.base_error()),
        Some(storage_types::StorageEnum::SyncSubmissionAmbiguous { .. })
    ));
    let rows: i64 = connection
        .query_row("SELECT count(*) FROM child", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 0);
}

#[cfg(feature = "fault-injection")]
struct ReadProgressGate {
    armed: std::sync::atomic::AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(feature = "fault-injection")]
impl crate::SqlFaultProbe for ReadProgressGate {
    fn fail(&self, _call: crate::SqlFaultCall<'_>) -> Option<storage_types::StorageError> {
        None
    }

    fn at_commit_boundary(
        &self,
        boundary: crate::SqlCommitBoundary,
    ) -> futures::future::BoxFuture<'_, storage_types::StorageResult<()>> {
        Box::pin(async move {
            if boundary == crate::SqlCommitBoundary::BeforeCommit
                && self.armed.swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                self.entered.notify_one();
                self.release.notified().await;
            }
            Ok(())
        })
    }
}

#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn given_pending_write_receipt_when_a_file_backed_reader_runs_then_it_progresses_without_uncommitted_data()
 {
    let path = std::env::temp_dir().join(format!(
        "sqlite-reader-progress-{}.db",
        uuid::Uuid::now_v7()
    ));
    let backend = super::driver::SqliteBackend::initialize(&path)
        .await
        .unwrap();
    backend
        .with_attempt(AttemptMode::ReadWrite, |attempt| {
            Box::pin(async move {
                attempt
                    .execute(SqlStatement::new(
                        "CREATE TABLE proof (id INTEGER PRIMARY KEY)",
                        vec![],
                    ))
                    .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    let gate = std::sync::Arc::new(ReadProgressGate {
        armed: std::sync::atomic::AtomicBool::new(true),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let backend = std::sync::Arc::new(backend.with_fault_probe(gate.clone()));
    let writer_backend = backend.clone();
    let writer = tokio::spawn(async move {
        writer_backend
            .with_attempt(AttemptMode::ReadWrite, |attempt| {
                Box::pin(async move {
                    attempt
                        .execute(SqlStatement::new("INSERT INTO proof VALUES (1)", vec![]))
                        .await?;
                    Ok(())
                })
            })
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), gate.entered.notified())
        .await
        .unwrap();
    let rows = tokio::time::timeout(std::time::Duration::from_secs(10), count_rows(&backend))
        .await
        .expect("the independent WAL reader must not wait for the writer receipt");
    assert_eq!(
        rows, 0,
        "uncommitted state stays private to the write attempt"
    );
    assert!(!writer.is_finished());
    gate.release.notify_one();
    writer
        .await
        .unwrap()
        .expect("write receives its real durable acknowledgment");
    assert_eq!(count_rows(&backend).await, 1);
    drop(backend);
    let reopened = super::driver::SqliteBackend::initialize(&path)
        .await
        .unwrap();
    assert_eq!(count_rows(&reopened).await, 1);
    drop(reopened);
    std::fs::remove_file(path).unwrap();
}
