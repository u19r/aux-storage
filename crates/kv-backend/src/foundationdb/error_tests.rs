use foundationdb::FdbError;
use storage_types::{
    AttemptFailure, AttemptKind, AttemptPhase, CapacityKind, ConflictCause, ErrorClass,
    NativeCallPhase, NativeCause, NativeCode, NativeEngine, NativeUnavailable,
};

use super::error::map_operation_error;

#[test]
fn given_fdb_codes_when_operation_error_then_typed() {
    let conflict = map_operation_error(FdbError::from_code(1020));
    assert_eq!(
        conflict.attempt_failure(),
        Some(&AttemptFailure::Conflict(ConflictCause::Native(
            NativeCause {
                engine: NativeEngine::FoundationDb,
                code: NativeCode::Fdb(1020),
                phase: NativeCallPhase::Statement,
            }
        )))
    );
    let large = map_operation_error(FdbError::from_code(2101));
    assert_eq!(
        large.attempt_failure(),
        Some(&AttemptFailure::Capacity {
            kind: CapacityKind::TransactionBytes,
            code: NativeCode::Fdb(2101),
        })
    );
    for code in [1031, 1025] {
        let deadline = map_operation_error(FdbError::from_code(code));
        assert_eq!(
            deadline.attempt_failure(),
            Some(&AttemptFailure::Unavailable(NativeUnavailable::Deadline(
                AttemptPhase::Statement
            ))),
            "code {code}"
        );
        assert_eq!(deadline.attempt_kind(), Some(AttemptKind::Deadline));
    }
    let internal = map_operation_error(FdbError::from_code(1500));
    assert_eq!(internal.class(), ErrorClass::Internal);
}

#[test]
fn given_commit_codes_when_classified_then_only_proven_aborts_are_known() {
    use storage_common::native_outcome::NativeCommitError;

    use super::error::commit_error;

    for code in [1025, 1031, 1021] {
        let classified = commit_error(FdbError::from_code(code));
        assert!(
            matches!(classified, NativeCommitError::Unproven { .. }),
            "code {code} is not Unproven"
        );
    }
    assert!(
        matches!(
            commit_error(FdbError::from_code(1020)),
            NativeCommitError::Conflict(_)
        ),
        "code 1020 is not Conflict"
    );
    assert!(
        matches!(
            commit_error(FdbError::from_code(2101)),
            NativeCommitError::Capacity {
                kind: CapacityKind::TransactionBytes,
                ..
            }
        ),
        "code 2101 is not Capacity(TransactionBytes)"
    );
    // 2000 (client_invalid_operation) is neither maybe-committed nor retryable.
    assert!(
        matches!(
            commit_error(FdbError::from_code(2000)),
            NativeCommitError::NotCommitted(_)
        ),
        "code 2000 is not NotCommitted"
    );
}
