use http_error::HttpApiError;
use storage_provider::{ListChangeIndexMarkersRequest, ListChangeIndexMarkersResponse};

use crate::{manager::StorageApiManagerImpl, types::Response};

impl StorageApiManagerImpl {
    pub(super) async fn list_change_index_markers_internal(
        &self,
        request: ListChangeIndexMarkersRequest,
    ) -> Result<Response, HttpApiError> {
        self.db()
            .list_change_index_markers(request)
            .await
            .map(|markers| {
                Response::ListChangeIndexMarkers(ListChangeIndexMarkersResponse { markers })
            })
            .map_err(Into::into)
    }
}
