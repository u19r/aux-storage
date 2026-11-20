//! A COMMIT in the last wave of `finish_write_batch` that Postgres answers
//! with the command tag `ROLLBACK` (a statement of the same wave aborted the
//! transaction) is a known abort, never a commit.

use std::{
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use futures::future::BoxFuture;
use storage_types::{AttemptPlan, DeadlineClass, StorageError, StorageResult};

use super::test_support::NativeFixture;
use crate::{
    AttemptMode, SqlAttempt, SqlBackend, SqlCommitBoundary, SqlEngine, SqlFaultCall, SqlFaultProbe,
    SqlNativeEvent, SqlValue,
};

/// Records the commit-side events of every attempt of one backend.
#[derive(Default)]
struct CommitRecorder {
    commit_events: AtomicUsize,
    durable_acknowledgements: AtomicUsize,
    boundaries: Mutex<Vec<SqlCommitBoundary>>,
}

impl SqlFaultProbe for CommitRecorder {
    fn fail(&self, _call: SqlFaultCall<'_>) -> Option<StorageError> {
        None
    }

    fn at_commit_boundary(
        &self,
        boundary: SqlCommitBoundary,
        _mode: storage_types::AttemptMode,
    ) -> BoxFuture<'_, StorageResult<()>> {
        if boundary == SqlCommitBoundary::AfterDurableAcknowledgement {
            self.durable_acknowledgements.fetch_add(1, Ordering::SeqCst);
        }
        self.boundaries.lock().unwrap().push(boundary);
        Box::pin(async { Ok(()) })
    }

    fn observe(&self, event: SqlNativeEvent) {
        if event == SqlNativeEvent::Commit {
            self.commit_events.fetch_add(1, Ordering::SeqCst);
        }
    }
}

fn upsert(value: i64) -> crate::SqlCountedWrite {
    crate::write_batch::UpsertShape::try_new(
        vec!["value".into(), "payload".into()],
        vec![false, false],
        vec!["value".into()],
    )
    .unwrap()
    .render_counted(
        SqlEngine::Postgres,
        "counted_primary",
        vec![vec![SqlValue::Integer(value), SqlValue::Integer(7)]],
    )
    .unwrap()
}

/// The only statement of the terminal wave misses its count (the trigger
/// swallows the insert), so Postgres aborts the transaction and answers the
/// COMMIT of the same wave with the tag `ROLLBACK`.
#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_failed_statement_in_last_wave_when_commit_answers_rollback_tag_then_attempt_is_a_known_abort()
 {
    let fixture = NativeFixture::open().await;
    fixture
        .admin
        .batch_execute(&format!(
            "SET search_path TO {}; CREATE TABLE sys_system_stream_scope (slot bigint NOT NULL); \
             CREATE TABLE counted_primary (value bigint PRIMARY KEY, payload bigint NOT NULL); \
             CREATE FUNCTION skip_counted() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN \
             NULL; END $$; CREATE TRIGGER skip_primary BEFORE INSERT ON counted_primary FOR EACH \
             ROW EXECUTE FUNCTION skip_counted()",
            fixture.schema,
        ))
        .await
        .unwrap();
    let recorder = Arc::new(CommitRecorder::default());
    let backend = fixture
        .open_independent_backend()
        .await
        .with_fault_probe(Arc::clone(&recorder) as Arc<dyn SqlFaultProbe>);
    let result = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                .scoped(storage_types::AttemptScope::Data)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .execute(crate::SqlStatement::new(
                            "INSERT INTO native_commit_probe VALUES (2)",
                            vec![],
                        ))
                        .await?;
                    let failure = attempt
                        .finish_write_batch(vec![upsert(1)], NonZeroUsize::new(4096).unwrap())
                        .await?
                        .err()
                        .expect("the count assertion fails");
                    assert!(matches!(
                        failure,
                        crate::SqlWriteBatchFailure::CountAssertion {
                            statement_index: 0,
                            ..
                        }
                    ));
                    Ok(())
                })
            },
        )
        .await;
    let error = result.expect_err("a failed terminal statement fails the attempt");
    assert!(
        !error.is_commit_unknown(),
        "the transaction is known aborted, not unknown"
    );
    assert_eq!(
        recorder.commit_events.load(Ordering::SeqCst),
        0,
        "no Commit event: the COMMIT answered ROLLBACK"
    );
    assert_eq!(
        recorder.durable_acknowledgements.load(Ordering::SeqCst),
        0,
        "no durable acknowledgement boundary for an aborted transaction; saw {:?}",
        recorder.boundaries.lock().unwrap()
    );
    assert_eq!(
        fixture.count_records().await,
        0,
        "the earlier insert is gone"
    );
    drop(backend);
    fixture.close().await;
}
