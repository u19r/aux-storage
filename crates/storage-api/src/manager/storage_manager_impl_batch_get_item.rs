use http_error::HttpApiError;
use storage_types::BatchGetItemRequest;

use crate::{
    batch_get_wire_response::BatchGetWireResponse, manager::StorageApiManagerImpl, types::Response,
};

impl StorageApiManagerImpl {
    pub(super) async fn batch_get_item_internal(
        &self,
        request: BatchGetItemRequest,
    ) -> Result<Response, HttpApiError> {
        let needs_barrier = request
            .request_items
            .values()
            .any(|keys| keys.consistent_read.unwrap_or(false));
        self.ensure_sync_read_barrier(needs_barrier).await?;
        let wire_response = self.db().batch_get_item(request).await?;
        Ok(Response::BatchGetWire(BatchGetWireResponse::from(
            wire_response,
        )))
    }
}
