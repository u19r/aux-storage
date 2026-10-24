use http_error::HttpApiError;
use storage_common::StorageJob;

use super::TtlSweepPayload;
use crate::{manager::StorageApiManagerImpl, types::Response};

impl StorageApiManagerImpl {
    pub(in crate::manager) async fn run_background_job_internal(
        &self,
        payload: serde_json::Value,
    ) -> Result<Response, HttpApiError> {
        let TtlSweepPayload { request } = TtlSweepPayload::try_from(payload)?;
        let outcome = self
            .db()
            .run_ttl_sweep(request)
            .await
            .map_err(HttpApiError::from)?;
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
}
