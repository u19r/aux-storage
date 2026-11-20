//! The journal's transaction send. Replay classification is in `replay_policy`.

use http_request::HttpRequestError;
use storage_types::{StorageError, TransactWriteItemsRequest, TransactWriteItemsResponse};

use super::implementation::{RemoteInvocationError, RemoteStorageProvider};

impl RemoteStorageProvider {
    pub(super) async fn invoke_transaction(
        &self,
        request: &TransactWriteItemsRequest,
    ) -> Result<TransactWriteItemsResponse, RemoteInvocationError> {
        let bytes = self
            .invoke_bytes_with_dispatch_status("DynamoDB_20120810.TransactWriteItems", request)
            .await?;
        serde_json::from_slice(&bytes).map_err(|err| {
            RemoteInvocationError::new(
                StorageError::from(err),
                HttpRequestError::Decode.dispatch_status(),
            )
        })
    }
}
