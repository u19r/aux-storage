use rocksdb::{Error as NativeError, ErrorKind};
use storage_types::{StorageEnum, StorageError};

pub(super) fn map_operation_error(error: NativeError) -> StorageError {
    if error.kind() == ErrorKind::Busy {
        StorageEnum::TransactionConflict {
            message: format!("RocksDB transaction conflict (kind {:?})", error.kind()),
        }
        .into()
    } else {
        StorageError::internal(&format!(
            "RocksDB native operation failed (kind {:?})",
            error.kind()
        ))
    }
}

pub(super) fn map_commit_error(error: NativeError) -> StorageError {
    if error.kind() == ErrorKind::Busy {
        StorageEnum::TransactionConflict {
            message: "RocksDB transaction conflict".to_owned(),
        }
        .into()
    } else {
        StorageError::sync_submission_ambiguous(None, "RocksDB commit was not acknowledged")
    }
}
