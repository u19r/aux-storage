use rusqlite::{Error, ErrorCode};
use storage_common::native_outcome::NativeCommitError;
use storage_types::{
    AttemptFailure, CapacityKind, NativeCallPhase, NativeCause, NativeCode, NativeEngine,
    StorageError,
};

fn cause(extended_code: i32, phase: NativeCallPhase) -> NativeCause {
    NativeCause {
        engine: NativeEngine::Sqlite,
        code: NativeCode::Sqlite(extended_code),
        phase,
    }
}

pub(super) fn map_driver_error(error: Error) -> StorageError {
    map_native_error(error, NativeCallPhase::Statement)
}

pub(super) fn map_native_error(error: Error, phase: NativeCallPhase) -> StorageError {
    match error {
        Error::SqliteFailure(failure, _) => match failure.code {
            ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked => {
                StorageError::native_conflict(cause(failure.extended_code, phase))
            }
            ErrorCode::TooBig | ErrorCode::DiskFull | ErrorCode::OutOfMemory => {
                AttemptFailure::Capacity {
                    kind: CapacityKind::NativeLimit,
                    code: NativeCode::Sqlite(failure.extended_code),
                }
                .into()
            }
            _ => StorageError::internal(&format!(
                "SQLite native operation failed ({})",
                cause(failure.extended_code, phase)
            )),
        },
        _ => StorageError::internal("SQLite native operation failed"),
    }
}

/// A failed COMMIT. Busy or locked: SQLite left the transaction open and
/// wrote nothing; the caller's rollback proves the abort.
pub(super) fn commit_error(error: &Error) -> NativeCommitError {
    match error {
        Error::SqliteFailure(failure, _)
            if matches!(
                failure.code,
                ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked
            ) =>
        {
            NativeCommitError::Conflict(cause(failure.extended_code, NativeCallPhase::Commit))
        }
        Error::SqliteFailure(failure, _) => NativeCommitError::Unproven {
            reason: "SQLite commit outcome is unknown",
            cause: Some(cause(failure.extended_code, NativeCallPhase::Commit)),
        },
        _ => NativeCommitError::Unproven {
            reason: "SQLite commit outcome is unknown",
            cause: None,
        },
    }
}

pub(super) fn map_worker_error(error: tokio_rusqlite::Error) -> StorageError {
    match error {
        tokio_rusqlite::Error::Rusqlite(native) => map_driver_error(native),
        _ => StorageError::internal("SQLite worker failed"),
    }
}
