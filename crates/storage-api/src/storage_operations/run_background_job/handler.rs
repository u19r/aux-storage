use http_error::HttpApiError;
use storage::DatabaseManager;
use storage_common::StorageJob;

use super::TtlSweepPayload;
use crate::types::Response;

pub(crate) async fn run_background_job(
    db: &DatabaseManager,
    payload: serde_json::Value,
) -> Result<Response, HttpApiError> {
    let TtlSweepPayload { request } = TtlSweepPayload::try_from(payload)?;
    let outcome = db
        .run_ttl_sweep(request)
        .await
        .map_err(crate::errors::dynamodb_storage_error)?;
    let mut response = serde_json::json!({
        "Message": format!("Background job executed: {}", StorageJob::TtlSweep)
    });
    let next_cursor = outcome
        .next_cursor
        .as_ref()
        .map(|cursor| cursor.token.as_str());
    if let Some(cursor) = next_cursor {
        response["NextCursor"] = serde_json::Value::String(cursor.to_owned());
    }
    Ok(Response::Raw(response))
}
