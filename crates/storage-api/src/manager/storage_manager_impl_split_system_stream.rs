use http_error::HttpApiError;
use storage::AdmissionClass;
use stream_provider::{SystemStreamSplitRequest, SystemStreamSplitResult};

use crate::manager::StorageApiManagerImpl;

impl StorageApiManagerImpl {
    pub(super) async fn split_system_stream_internal(
        &self,
        request: SystemStreamSplitRequest,
    ) -> Result<SystemStreamSplitResult, HttpApiError> {
        self.db()
            .run_stream_admitted("default", AdmissionClass::Write, |stream| async move {
                stream
                    .split_system_stream(request)
                    .await
                    .map_err(|error| storage_types::StorageError::from(error.into_storage_enum()))
            })
            .await
            .map_err(HttpApiError::from)
    }
}
