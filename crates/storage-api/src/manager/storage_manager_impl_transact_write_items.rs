use http_error::HttpApiError;
use storage_sync::SyncWriteRequest;
use storage_types::{TransactWriteItemsRequest, TransactWriteItemsResponse};

use crate::{
    manager::{StorageApiManagerImpl, storage_manager_impl_sync_write_proposer::sync_response_at},
    types::Response,
};

/// Handles a `DynamoDB` `TransactWriteItems` operation
///
/// # Errors
/// Returns `HttpApiError` if:
/// - More than 100 items are provided
/// - Any of the transactional operations fail
/// - Condition checks fail
impl StorageApiManagerImpl {
    pub(super) async fn transact_write_items_internal(
        &self,
        request: TransactWriteItemsRequest,
    ) -> Result<Response, HttpApiError> {
        if let Some(response) = self
            .propose_sync_write_if_configured(SyncWriteRequest::TransactWriteItems(request.clone()))
            .await?
        {
            return Ok(Response::TransactWriteItems(sync_response_at(
                &response,
                0,
                TransactWriteItemsResponse {
                    consumed_capacity: None,
                    item_collection_metrics: None,
                },
            )?));
        }

        let response = self.db().transact_write_items(request).await?;

        Ok(Response::TransactWriteItems(response))
    }
}
