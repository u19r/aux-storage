use http_error::HttpApiError;
use storage_sync::SyncWriteRequest;
use storage_types::{UpdateTableRequest, UpdateTableResponse};

use crate::{
    manager::{
        StorageApiManagerImpl, storage_manager_impl_sync_write_proposer::required_sync_response_at,
    },
    types::Response,
};

impl StorageApiManagerImpl {
    pub(super) async fn update_table_internal(
        &self,
        request: UpdateTableRequest,
    ) -> Result<Response, HttpApiError> {
        if let Some(requested) = request.max_indexers {
            let current = self.db().get_table_info(&request.table_name).await?;
            if requested < current.max_indexers {
                return Err(HttpApiError::from(storage_types::StorageError::validation(
                    "MaxIndexers:cannot_decrease",
                )));
            }
        }
        if let Some(response) = self
            .propose_sync_write_if_configured(|| SyncWriteRequest::UpdateTable(request.clone()))
            .await?
        {
            return Ok(Response::UpdateTable(required_sync_response_at::<
                UpdateTableResponse,
            >(
                &response, 0, "UpdateTable"
            )?));
        }

        let resp = self.db().update_table(request).await?;
        Ok(Response::UpdateTable(resp))
    }
}
