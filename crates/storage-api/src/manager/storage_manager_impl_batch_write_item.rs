use http_error::HttpApiError;
use storage_sync::SyncWriteRequest;
use storage_types::{BatchWriteItemRequest, BatchWriteItemResponse};

use crate::{
    manager::{StorageApiManagerImpl, storage_manager_impl_sync_write_proposer::sync_response_at},
    types::Response,
};

impl StorageApiManagerImpl {
    pub(super) async fn batch_write_item_internal(
        &self,
        request: BatchWriteItemRequest,
    ) -> Result<Response, HttpApiError> {
        if let Some(response) = self
            .propose_sync_write_if_configured(SyncWriteRequest::BatchWriteItem(request.clone()))
            .await?
        {
            return Ok(Response::BatchWriteItem(sync_response_at(
                &response,
                0,
                BatchWriteItemResponse {
                    unprocessed_items: None,
                    item_collection_metrics: None,
                    consumed_capacity: None,
                },
            )?));
        }

        Ok(Response::BatchWriteItem(
            self.db().batch_write_item(request).await?,
        ))
    }
}
