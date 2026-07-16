use http_error::HttpApiError;
use storage_sync::SyncWriteRequest;
use storage_types::{UpdateTimeToLiveRequest, UpdateTimeToLiveResponse};

use crate::{
    manager::{
        StorageApiManagerImpl, storage_manager_impl_sync_write_proposer::required_sync_response_at,
    },
    types::Response,
};

impl StorageApiManagerImpl {
    pub(super) async fn update_time_to_live_internal(
        &self,
        request: UpdateTimeToLiveRequest,
    ) -> Result<Response, HttpApiError> {
        if let Some(response) = self
            .propose_sync_write_if_configured(|| {
                SyncWriteRequest::UpdateTimeToLive(request.clone())
            })
            .await?
        {
            return Ok(Response::UpdateTimeToLive(required_sync_response_at::<
                UpdateTimeToLiveResponse,
            >(
                &response,
                0,
                "UpdateTimeToLive",
            )?));
        }

        let response = self.db().update_time_to_live(request).await?;
        Ok(Response::UpdateTimeToLive(response))
    }
}
