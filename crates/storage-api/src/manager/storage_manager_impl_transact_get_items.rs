use http_error::HttpApiError;
use storage_types::TransactGetItemsRequest;

use crate::{manager::StorageApiManagerImpl, types::Response};

impl StorageApiManagerImpl {
    pub(super) async fn transact_get_items_internal(
        &self,
        request: TransactGetItemsRequest,
    ) -> Result<Response, HttpApiError> {
        let response = self.db().transact_get_items(request).await?;
        Ok(Response::TransactGetItems(response))
    }
}
