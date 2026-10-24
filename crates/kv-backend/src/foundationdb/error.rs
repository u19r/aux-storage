use foundationdb::{FdbError, TransactionCommitError};
use storage_types::{StorageEnum, StorageError};

pub(super) fn map_operation_error(error: FdbError) -> StorageError {
    if error.code() == crate::constants::FOUNDATIONDB_TRANSACTION_TOO_LARGE_CODE {
        storage_types::InfrastructureResultKind::NativeCapacityRejected.into()
    } else if error.is_retryable_not_committed() {
        StorageEnum::TransactionConflict {
            message: format!("FoundationDB transaction conflict (code {})", error.code()),
        }
        .into()
    } else {
        StorageError::internal(&format!(
            "FoundationDB native operation failed (code {})",
            error.code()
        ))
    }
}

pub(super) fn map_commit_error(error: TransactionCommitError) -> StorageError {
    if error.is_maybe_committed() {
        StorageError::sync_submission_ambiguous(
            None,
            format!(
                "FoundationDB commit was not acknowledged (code {})",
                error.code()
            ),
        )
    } else {
        let error = map_operation_error(error.into());
        if matches!(error.as_ref(), StorageEnum::TransactionConflict { .. }) {
            storage_common::telemetry::record_provider_count(
                "foundationdb",
                "native_commit_conflicts",
                1,
            );
        }
        error
    }
}
