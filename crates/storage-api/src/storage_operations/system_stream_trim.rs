use http_error::HttpApiError;
use storage::DatabaseManager;
use stream_provider::{SystemStreamTrimPageRequest, SystemStreamTrimPageResult};

pub(crate) async fn run_system_stream_trim_page(
    db: &DatabaseManager,
    request: SystemStreamTrimPageRequest,
) -> Result<SystemStreamTrimPageResult, HttpApiError> {
    db.run_system_stream_trim_page(
        request.scope().clone(),
        request.cutoff_timestamp(),
        request.page_limit(),
        request.continuation().cloned(),
    )
    .await
    .map_err(crate::errors::dynamodb_storage_error)
}
