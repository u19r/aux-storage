use foundationdb::{FdbError, TransactionCommitError};
use storage_common::native_outcome::{NativeCommitError, classify_commit};
use storage_types::{
    AttemptFailure, CapacityKind, NativeCallPhase, NativeCause, NativeCode, NativeEngine,
    StorageError,
};

use crate::constants::{
    FOUNDATIONDB_TRANSACTION_CANCELLED_CODE, FOUNDATIONDB_TRANSACTION_TIMED_OUT_CODE,
    FOUNDATIONDB_TRANSACTION_TOO_LARGE_CODE,
};

fn is_deadline_code(code: i32) -> bool {
    code == FOUNDATIONDB_TRANSACTION_TIMED_OUT_CODE
        || code == FOUNDATIONDB_TRANSACTION_CANCELLED_CODE
}

fn cause(code: i32, phase: NativeCallPhase) -> NativeCause {
    NativeCause {
        engine: NativeEngine::FoundationDb,
        code: NativeCode::Fdb(code),
        phase,
    }
}

/// Native errors before commit submission. The owner destroys the
/// transaction before returning, which discards its staged mutations.
pub(super) fn map_operation_error(error: FdbError) -> StorageError {
    let code = error.code();
    if code == FOUNDATIONDB_TRANSACTION_TOO_LARGE_CODE {
        AttemptFailure::Capacity {
            kind: CapacityKind::TransactionBytes,
            code: NativeCode::Fdb(code),
        }
        .into()
    } else if is_deadline_code(code) {
        storage_types::expired_in(storage_types::AttemptPhase::Statement)
    } else if error.is_retryable_not_committed() {
        StorageError::native_conflict(cause(code, NativeCallPhase::Statement))
    } else {
        StorageError::internal(&format!(
            "FoundationDB native operation failed ({})",
            cause(code, NativeCallPhase::Statement)
        ))
    }
}

/// A failed COMMIT. The FoundationDB client states the commit state of each
/// error: only `is_maybe_committed` and a cancelled or timed-out submitted
/// commit may still apply.
pub(super) fn commit_error(error: FdbError) -> NativeCommitError {
    let code = error.code();
    let cause = cause(code, NativeCallPhase::Commit);
    if error.is_maybe_committed() || is_deadline_code(code) {
        return NativeCommitError::Unproven {
            reason: "FoundationDB commit was not acknowledged",
            cause: Some(cause),
        };
    }
    if code == FOUNDATIONDB_TRANSACTION_TOO_LARGE_CODE {
        return NativeCommitError::Capacity {
            kind: CapacityKind::TransactionBytes,
            cause,
        };
    }
    if error.is_retryable_not_committed() {
        storage_common::telemetry::record_provider_count(
            "foundationdb",
            "native_commit_conflicts",
            1,
        );
        return NativeCommitError::Conflict(cause);
    }
    NativeCommitError::NotCommitted(cause)
}

/// A read-only commit (watch registration): it writes nothing, so it is
/// never unknown.
pub(super) fn map_unticketed_commit_error(error: TransactionCommitError) -> StorageError {
    classify_commit(false, commit_error(error.into()))
}
