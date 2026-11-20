use http_error::HttpApiError;
use storage::{AdmissionClass, DatabaseManager};
use storage_types::{DescribeSystemStreamRequest, DescribeSystemStreamResponse};

use crate::types::Response;

pub async fn describe_system_stream(
    db: &DatabaseManager,
    _request: DescribeSystemStreamRequest,
) -> Result<Response, HttpApiError> {
    let scope = db
        .run_stream_admitted("default", AdmissionClass::RangeRead, |stream| async move {
            stream
                .system_stream_scope()
                .await
                .map_err(|error| storage_types::StorageError::from(error.into_storage_enum()))
        })
        .await
        .map_err(crate::errors::dynamodb_storage_error)?;
    Ok(Response::DescribeSystemStream(
        DescribeSystemStreamResponse::new(scope),
    ))
}
