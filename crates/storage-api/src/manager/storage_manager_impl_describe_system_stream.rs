use http_error::HttpApiError;
use storage::AdmissionClass;
use storage_types::{DescribeSystemStreamRequest, DescribeSystemStreamResponse};

use crate::{manager::StorageApiManagerImpl, types::Response};

impl StorageApiManagerImpl {
    pub(super) async fn describe_system_stream_internal(
        &self,
        _request: DescribeSystemStreamRequest,
    ) -> Result<Response, HttpApiError> {
        let scope =
            self.db()
                .run_stream_admitted("default", AdmissionClass::RangeRead, |stream| async move {
                    stream.system_stream_scope().await.map_err(|error| {
                        storage_types::StorageError::from(error.into_storage_enum())
                    })
                })
                .await
                .map_err(HttpApiError::from)?;
        Ok(Response::DescribeSystemStream(
            DescribeSystemStreamResponse::new(scope),
        ))
    }
}
