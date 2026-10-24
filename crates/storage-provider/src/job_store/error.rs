use storage_types::StorageError;

use crate::RawOrderedJobStoreError;

/// Keeps a checked-commit contract error typed until the native attempt has
/// rolled back. Native drivers themselves return `StorageError`.
pub enum JobCommitError {
    Storage(StorageError),
    Contract(RawOrderedJobStoreError),
}

impl From<StorageError> for JobCommitError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

impl From<RawOrderedJobStoreError> for JobCommitError {
    fn from(error: RawOrderedJobStoreError) -> Self {
        match error {
            RawOrderedJobStoreError::Backend(error) => Self::Storage(error),
            error => Self::Contract(error),
        }
    }
}
