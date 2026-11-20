use http_error::HttpApiError;
use storage::{AdmissionClass, DatabaseManager};
use stream_provider::{SystemStreamSplitRequest, SystemStreamSplitResult};

pub(crate) async fn split_system_stream(
    db: &DatabaseManager,
    request: SystemStreamSplitRequest,
) -> Result<SystemStreamSplitResult, HttpApiError> {
    db.run_stream_admitted("default", AdmissionClass::Write, |stream| async move {
        stream
            .split_system_stream(request)
            .await
            .map_err(|error| storage_types::StorageError::from(error.into_storage_enum()))
    })
    .await
    .map_err(crate::errors::dynamodb_storage_error)
}
