use http_error::HttpApiError;
use stream_provider::{SystemStreamTrimPageRequest, SystemStreamTrimPageResult};

use crate::manager::StorageApiManagerImpl;

impl StorageApiManagerImpl {
    pub(super) async fn run_system_stream_trim_page_internal(
        &self,
        request: SystemStreamTrimPageRequest,
    ) -> Result<SystemStreamTrimPageResult, HttpApiError> {
        self.db()
            .run_system_stream_trim_page(
                request.scope().clone(),
                request.cutoff_timestamp(),
                request.page_limit(),
                request.continuation().cloned(),
            )
            .await
            .map_err(HttpApiError::from)
    }
}
