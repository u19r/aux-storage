use storage_types::{
    AttemptKind, CapacityKind, ErrorClass, NativeCallPhase, NativeCause, NativeCode, NativeEngine,
    StorageError,
};

use super::native_outcome::*;

fn cause() -> NativeCause {
    NativeCause {
        engine: NativeEngine::FoundationDb,
        code: NativeCode::Fdb(1021),
        phase: NativeCallPhase::Commit,
    }
}

#[test]
fn given_commit_error_table_when_classified_then_known_or_unknown() {
    for submitted in [true, false] {
        let conflict = classify_commit(submitted, NativeCommitError::Conflict(cause()));
        assert_eq!(conflict.attempt_kind(), Some(AttemptKind::Conflict));
        assert!(!conflict.is_commit_unknown());

        let limit = classify_commit(
            submitted,
            NativeCommitError::Capacity {
                kind: CapacityKind::NativeLimit,
                cause: cause(),
            },
        );
        assert_eq!(limit.attempt_kind(), Some(AttemptKind::Capacity));
        let bytes = classify_commit(
            submitted,
            NativeCommitError::Capacity {
                kind: CapacityKind::TransactionBytes,
                cause: cause(),
            },
        );
        assert_eq!(bytes.attempt_kind(), Some(AttemptKind::TransactionBytes));

        let not_committed = classify_commit(submitted, NativeCommitError::NotCommitted(cause()));
        assert_eq!(not_committed.class(), ErrorClass::Internal);
        assert!(!not_committed.is_commit_unknown());

        for native in [Some(cause()), None] {
            let unproven = classify_commit(
                submitted,
                NativeCommitError::Unproven {
                    reason: "reply lost",
                    cause: native,
                },
            );
            assert_eq!(unproven.is_commit_unknown(), submitted, "{unproven}");
            if !submitted {
                assert_eq!(unproven.class(), ErrorClass::Internal);
            }
        }

        let unknown = classify_commit(
            submitted,
            NativeCommitError::Classified(StorageError::commit_unknown("lost")),
        );
        assert_eq!(unknown.is_commit_unknown(), submitted, "{unknown}");
        if !submitted {
            assert_eq!(unknown.class(), ErrorClass::Internal);
        }

        let validation = StorageError::raw_validation("bad");
        let passed = classify_commit(submitted, NativeCommitError::Classified(validation.clone()));
        assert_eq!(passed.to_string(), validation.to_string());
        assert_eq!(passed.class(), ErrorClass::Client);
    }
}
