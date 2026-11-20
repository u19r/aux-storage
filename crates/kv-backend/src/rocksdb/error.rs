use rocksdb::{Error as NativeError, ErrorKind};
use storage_common::native_outcome::NativeCommitError;
use storage_types::{NativeCallPhase, NativeCause, NativeCode, NativeEngine, StorageError};

const fn kind_name(kind: ErrorKind) -> &'static str {
    match kind {
        ErrorKind::NotFound => "NotFound",
        ErrorKind::Corruption => "Corruption",
        ErrorKind::NotSupported => "NotSupported",
        ErrorKind::InvalidArgument => "InvalidArgument",
        ErrorKind::IOError => "IOError",
        ErrorKind::MergeInProgress => "MergeInProgress",
        ErrorKind::Incomplete => "Incomplete",
        ErrorKind::ShutdownInProgress => "ShutdownInProgress",
        ErrorKind::TimedOut => "TimedOut",
        ErrorKind::Aborted => "Aborted",
        ErrorKind::Busy => "Busy",
        ErrorKind::Expired => "Expired",
        ErrorKind::TryAgain => "TryAgain",
        ErrorKind::CompactionTooLarge => "CompactionTooLarge",
        ErrorKind::ColumnFamilyDropped => "ColumnFamilyDropped",
        ErrorKind::Unknown => "Unknown",
    }
}

fn cause(error: &NativeError, phase: NativeCallPhase) -> NativeCause {
    NativeCause {
        engine: NativeEngine::RocksDb,
        code: NativeCode::Named(kind_name(error.kind())),
        phase,
    }
}

pub(super) fn map_operation_error(error: NativeError) -> StorageError {
    if error.kind() == ErrorKind::Busy {
        StorageError::native_conflict(cause(&error, NativeCallPhase::Statement))
    } else {
        StorageError::internal(&format!(
            "RocksDB native operation failed ({})",
            cause(&error, NativeCallPhase::Statement)
        ))
    }
}

/// A failed COMMIT: `Busy` is RocksDB's conflict validation, which runs
/// before the WAL write.
pub(super) fn commit_error(error: NativeError) -> NativeCommitError {
    let cause = cause(&error, NativeCallPhase::Commit);
    if error.kind() == ErrorKind::Busy {
        NativeCommitError::Conflict(cause)
    } else {
        NativeCommitError::Unproven {
            reason: "RocksDB commit was not acknowledged",
            cause: Some(cause),
        }
    }
}
