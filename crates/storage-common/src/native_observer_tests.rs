use std::sync::Mutex;

use futures::future::BoxFuture;
use storage_types::{AttemptKind, AttemptMode, StorageError, StorageResult};

use super::native_observer::*;

/// Records each boundary with its attempt mode and answers with `fail`.
pub(crate) struct RecordingObserver {
    pub(crate) seen: Mutex<Vec<(CommitBoundary, AttemptMode)>>,
    pub(crate) fail: bool,
}

impl RecordingObserver {
    pub(crate) fn new(fail: bool) -> Self {
        Self {
            seen: Mutex::new(Vec::new()),
            fail,
        }
    }
}

impl NativeObserver for RecordingObserver {
    fn at_commit_boundary(
        &self,
        boundary: CommitBoundary,
        mode: AttemptMode,
    ) -> BoxFuture<'_, StorageResult<()>> {
        self.seen.lock().unwrap().push((boundary, mode));
        let fail = self.fail;
        Box::pin(async move {
            if fail {
                Err(StorageError::internal("observer failure"))
            } else {
                Ok(())
            }
        })
    }
}

#[test]
fn before_commit_failure_is_a_known_conflict() {
    let observer = RecordingObserver::new(true);
    let error = futures::executor::block_on(observe_commit_boundary(
        &observer,
        CommitBoundary::BeforeCommit,
        AttemptMode::ReadWrite,
    ))
    .expect_err("observer fails");
    assert_eq!(error.attempt_kind(), Some(AttemptKind::Conflict));
    assert!(!error.is_commit_unknown());
    assert_eq!(
        *observer.seen.lock().unwrap(),
        vec![(CommitBoundary::BeforeCommit, AttemptMode::ReadWrite)]
    );
}

#[test]
fn after_acknowledgement_failure_of_a_write_is_unknown() {
    let observer = RecordingObserver::new(true);
    let error = futures::executor::block_on(observe_commit_boundary(
        &observer,
        CommitBoundary::AfterDurableAcknowledgement,
        AttemptMode::ReadWriteItems,
    ))
    .expect_err("observer fails");
    assert!(error.is_commit_unknown());
    assert_eq!(
        *observer.seen.lock().unwrap(),
        vec![(
            CommitBoundary::AfterDurableAcknowledgement,
            AttemptMode::ReadWriteItems
        )]
    );
}

#[test]
fn after_acknowledgement_failure_of_a_read_is_a_known_failure() {
    let observer = RecordingObserver::new(true);
    let error = futures::executor::block_on(observe_commit_boundary(
        &observer,
        CommitBoundary::AfterDurableAcknowledgement,
        AttemptMode::ReadOnly,
    ))
    .expect_err("observer fails");
    assert!(!error.is_commit_unknown());
    assert_eq!(
        *observer.seen.lock().unwrap(),
        vec![(
            CommitBoundary::AfterDurableAcknowledgement,
            AttemptMode::ReadOnly
        )]
    );
}

#[test]
fn passing_observer_returns_ok() {
    let observer = RecordingObserver::new(false);
    for boundary in [
        CommitBoundary::BeforeCommit,
        CommitBoundary::AfterDurableAcknowledgement,
    ] {
        futures::executor::block_on(observe_commit_boundary(
            &observer,
            boundary,
            AttemptMode::ReadOnly,
        ))
        .expect("observer passes");
    }
    assert_eq!(
        *observer.seen.lock().unwrap(),
        vec![
            (CommitBoundary::BeforeCommit, AttemptMode::ReadOnly),
            (
                CommitBoundary::AfterDurableAcknowledgement,
                AttemptMode::ReadOnly
            )
        ]
    );
}
