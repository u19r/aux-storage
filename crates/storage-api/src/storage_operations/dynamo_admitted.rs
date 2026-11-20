use std::{future::Future, sync::Arc};

use http_error::HttpApiError;
use storage::{AdmissionClass, DatabaseManager};
use storage_provider::DynamoOperations;
use storage_types::StorageResult;

use crate::types::Response;

/// Runs one DynamoDB operation under the default connection's foreground
/// admission and maps the result to the protocol response.
pub async fn dynamo_admitted<T, F, Fut>(
    db: &DatabaseManager,
    class: AdmissionClass,
    respond: fn(T) -> Response,
    operation: F,
) -> Result<Response, HttpApiError>
where
    F: FnOnce(Arc<dyn DynamoOperations>) -> Fut,
    Fut: Future<Output = StorageResult<T>> + Send + 'static,
    T: Send + 'static,
{
    db.run_dynamo_admitted("default", class, operation)
        .await
        .map(respond)
        .map_err(crate::errors::dynamodb_storage_error)
}
