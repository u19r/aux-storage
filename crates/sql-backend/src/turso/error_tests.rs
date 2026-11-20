use storage_common::native_outcome::NativeCommitError;
use storage_types::{AttemptKind, NativeCallPhase, NativeCode};

use super::error::*;

fn conflict_code(error: turso::Error) -> NativeCode {
    match commit_error(error) {
        NativeCommitError::Conflict(cause) => {
            assert_eq!(cause.phase, NativeCallPhase::Commit);
            cause.code
        }
        other => panic!("expected a conflict, got {other:?}"),
    }
}

#[test]
fn given_turso_errors_when_commit_error_then_conflict_only_for_engine_conflicts() {
    assert_eq!(
        conflict_code(turso::Error::Busy("busy".into())),
        NativeCode::Named("Busy")
    );
    assert_eq!(
        conflict_code(turso::Error::Error("Write-write conflict".into())),
        NativeCode::Named("WriteWriteConflict")
    );
    assert!(matches!(
        commit_error(turso::Error::IoError(
            std::io::ErrorKind::Other,
            "synthetic io failure"
        )),
        NativeCommitError::Unproven { .. }
    ));
    assert!(matches!(
        commit_error(turso::Error::Corrupt("synthetic".into())),
        NativeCommitError::Unproven { .. }
    ));
    let capacity = map_driver_error(turso::Error::Error("Out of memory".into()));
    assert_eq!(capacity.attempt_kind(), Some(AttemptKind::Capacity));
}
