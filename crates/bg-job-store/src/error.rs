use std::fmt;

use scheduled_bg_jobs::store::{StoreContractError, StoreDataError, StoreFailure};
use storage_provider::RawOrderedJobStoreError;
use storage_types::{StorageEnum, StorageError, context::WrappedError};

/// Safe error returned by the provider-backed `JobStore` adapter.
///
/// Backend errors are intentionally projected to the fixed upstream
/// categories.  Their diagnostic text, including keys and values, never
/// reaches `Display` or `Debug` here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduledJobStoreError {
    Contract(StoreContractError),
    OperationReuseMismatch,
    Backend(StoreFailure),
}

impl fmt::Display for ScheduledJobStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract(error) => error.fmt(formatter),
            Self::OperationReuseMismatch => {
                formatter.write_str("commit operation ID was reused with different content")
            }
            Self::Backend(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ScheduledJobStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Contract(error) => Some(error),
            Self::Backend(error) => Some(error),
            Self::OperationReuseMismatch => None,
        }
    }
}

impl From<StoreContractError> for ScheduledJobStoreError {
    fn from(error: StoreContractError) -> Self {
        Self::Contract(error)
    }
}

impl From<RawOrderedJobStoreError> for ScheduledJobStoreError {
    fn from(error: RawOrderedJobStoreError) -> Self {
        match error {
            RawOrderedJobStoreError::OperationReuseMismatch => Self::OperationReuseMismatch,
            RawOrderedJobStoreError::Timeout => Self::Backend(StoreFailure::Timeout),
            RawOrderedJobStoreError::Corrupt(field) => {
                Self::Backend(StoreFailure::Corrupt(StoreDataError::Malformed(field)))
            }
            RawOrderedJobStoreError::Backend(error) => Self::Backend(map_storage_error(&error)),
        }
    }
}

fn map_storage_error(error: &StorageError) -> StoreFailure {
    match error.to_enum() {
        StorageEnum::Unsupported { .. } => StoreFailure::Unsupported,
        StorageEnum::ServiceUnavailable { .. }
        | StorageEnum::TransactionConflict { .. }
        | StorageEnum::TransactionInProgress { .. }
        | StorageEnum::ProvisionedThroughputExceeded { .. }
        | StorageEnum::Throttled { .. }
        | StorageEnum::LimitExceeded { .. }
        | StorageEnum::RequestLimitExceeded => StoreFailure::Unavailable,
        StorageEnum::Serialization(_) | StorageEnum::AwsSerialization(_) => {
            StoreFailure::Corrupt(StoreDataError::Malformed("serialized value"))
        }
        _ => StoreFailure::Permanent,
    }
}
