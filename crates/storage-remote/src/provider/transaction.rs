//! Journal-owned transaction acknowledgement and retry classification.

use http_request::{HttpRequestDispatchStatus, HttpRequestError};
use storage_types::{
    StorageEnum, StorageError, TransactWriteItemsRequest, TransactWriteItemsResponse,
    context::WrappedError,
};

use super::implementation::{RemoteInvocationError, RemoteStorageProvider};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ReplayPolicy {
    Transport,
    // Once a request may have applied, only the journal can establish its
    // result. A transport retry could turn that ambiguity into a later abort.
    Journal,
}

impl RemoteStorageProvider {
    pub(super) async fn invoke_transaction(
        &self,
        request: &TransactWriteItemsRequest,
    ) -> Result<TransactWriteItemsResponse, RemoteInvocationError> {
        let bytes = self
            .invoke_bytes_with_dispatch_status(
                "DynamoDB_20120810.TransactWriteItems",
                request,
                ReplayPolicy::Journal,
            )
            .await?;
        serde_json::from_slice(&bytes).map_err(|err| {
            RemoteInvocationError::new(
                StorageError::from(err),
                HttpRequestError::Decode.dispatch_status(),
            )
        })
    }
}

impl RemoteInvocationError {
    pub(super) fn is_ambiguous(&self) -> bool {
        self.dispatch_status == HttpRequestDispatchStatus::PossiblyDispatched
            && !self.is_known_not_applied()
    }

    fn is_known_not_applied(&self) -> bool {
        if matches!(self.error.to_enum(), StorageEnum::AwsService { code: Some(code), .. } if code == storage_types::STORAGE_NOT_LEADER_ERROR_TYPE)
        {
            return true;
        }
        matches!(
            self.error.to_enum(),
            StorageEnum::KeyValidation(_)
                | StorageEnum::ResourceNotFound { .. }
                | StorageEnum::IndexNotFound { .. }
                | StorageEnum::Validation { .. }
                | StorageEnum::RawValidation { .. }
                | StorageEnum::Unsupported { .. }
                | StorageEnum::TableNotFound { .. }
                | StorageEnum::TableAlreadyExists { .. }
                | StorageEnum::AccessDenied { .. }
                | StorageEnum::Authentication { .. }
                | StorageEnum::MissingAuthenticationToken
                | StorageEnum::TransactionCanceled { .. }
                | StorageEnum::TransactionConflict { .. }
                | StorageEnum::ConditionalCheckFailed
                | StorageEnum::ConditionalCheckFailedWithItem { .. }
                | StorageEnum::IdempotentParameterMismatch
                | StorageEnum::AwsSerialization(_)
                | StorageEnum::Database(_)
        )
    }
}
