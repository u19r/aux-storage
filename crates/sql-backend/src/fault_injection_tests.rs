use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use futures::future::BoxFuture;
use storage_types::{StorageEnum, StorageError, StorageResult};

use crate::{
    AttemptMode, ReadProtection, SqlAttempt, SqlBackend, SqlCommitBoundary, SqlFaultCall,
    SqlFaultProbe, SqlStatement,
};

struct CommitFailure {
    boundary: SqlCommitBoundary,
    pending: AtomicBool,
}

impl CommitFailure {
    fn new(boundary: SqlCommitBoundary) -> Arc<Self> {
        Arc::new(Self {
            boundary,
            pending: AtomicBool::new(true),
        })
    }
}

impl SqlFaultProbe for CommitFailure {
    fn fail(&self, _call: SqlFaultCall<'_>) -> Option<StorageError> {
        None
    }

    fn at_commit_boundary(&self, boundary: SqlCommitBoundary) -> BoxFuture<'_, StorageResult<()>> {
        Box::pin(async move {
            if boundary == self.boundary && self.pending.swap(false, Ordering::SeqCst) {
                return Err(StorageError::sync_submission_ambiguous(
                    None,
                    "deliberate boundary failure",
                ));
            }
            Ok(())
        })
    }
}

#[cfg(any(feature = "sqlite", feature = "turso"))]
async fn initialize_probe_table(backend: &impl SqlBackend) {
    backend
        .with_schema_attempt(|attempt| {
            Box::pin(async move {
                attempt
                    .execute(SqlStatement::new(
                        "CREATE TABLE native_commit_probe (value BIGINT NOT NULL)",
                        vec![],
                    ))
                    .await
            })
        })
        .await
        .unwrap();
}

async fn assert_native_boundary(backend: &impl SqlBackend, boundary: SqlCommitBoundary) {
    let result = backend
        .with_attempt(AttemptMode::ReadWrite, |attempt| {
            Box::pin(async move {
                attempt
                    .execute(SqlStatement::new(
                        "INSERT INTO native_commit_probe VALUES (1)",
                        vec![],
                    ))
                    .await
            })
        })
        .await;
    let error = result.expect_err("the boundary suppresses the native result");
    let expected_rows = match boundary {
        SqlCommitBoundary::BeforeCommit => {
            assert!(matches!(
                error.as_ref(),
                StorageEnum::TransactionConflict { .. }
            ));
            0
        }
        SqlCommitBoundary::AfterDurableAcknowledgement => {
            assert!(matches!(
                error.as_ref(),
                StorageEnum::SyncSubmissionAmbiguous { .. }
            ));
            1
        }
    };
    let page = backend
        .with_attempt(AttemptMode::ReadOnly, |attempt| {
            Box::pin(async move {
                attempt
                    .query(
                        SqlStatement::new("SELECT value FROM native_commit_probe", vec![]),
                        ReadProtection::Snapshot,
                        NonZeroUsize::new(4096).unwrap(),
                    )
                    .await
            })
        })
        .await
        .unwrap();
    assert_eq!(page.rows.len(), expected_rows);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn given_sqlite_commit_boundary_failure_when_attempt_settles_then_outcome_matches_durable_rows()
 {
    for boundary in [
        SqlCommitBoundary::BeforeCommit,
        SqlCommitBoundary::AfterDurableAcknowledgement,
    ] {
        let backend = crate::sqlite::SqliteBackend::initialize(":memory:")
            .await
            .unwrap();
        initialize_probe_table(&backend).await;
        let backend = backend.with_fault_probe(CommitFailure::new(boundary));
        assert_native_boundary(&backend, boundary).await;
    }
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn given_turso_commit_boundary_failure_when_attempt_settles_then_outcome_matches_durable_rows()
 {
    for boundary in [
        SqlCommitBoundary::BeforeCommit,
        SqlCommitBoundary::AfterDurableAcknowledgement,
    ] {
        let backend = crate::turso::TursoBackend::initialize(":memory:")
            .await
            .unwrap();
        initialize_probe_table(&backend).await;
        let backend = backend.with_fault_probe(CommitFailure::new(boundary));
        assert_native_boundary(&backend, boundary).await;
    }
}

#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_postgres_commit_boundary_failure_when_attempt_settles_then_outcome_matches_durable_rows()
 {
    for boundary in [
        SqlCommitBoundary::BeforeCommit,
        SqlCommitBoundary::AfterDurableAcknowledgement,
    ] {
        let fixture = crate::postgres::test_support::NativeFixture::open().await;
        let backend = fixture
            .open_independent_backend()
            .await
            .with_fault_probe(CommitFailure::new(boundary));
        assert_native_boundary(&backend, boundary).await;
        drop(backend);
        fixture.close().await;
    }
}
