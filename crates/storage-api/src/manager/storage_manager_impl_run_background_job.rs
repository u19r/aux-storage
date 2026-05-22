use bg_jobs::BackgroundJobName;
use http_error::HttpApiError;

use crate::{manager::StorageApiManagerImpl, types::Response};

impl StorageApiManagerImpl {
    pub(super) async fn run_background_job_internal(
        &self,
        payload: serde_json::Value,
    ) -> Result<Response, HttpApiError> {
        let job_name = payload
            .get("JobName")
            .and_then(serde_json::Value::as_str)
            .filter(|name| !name.trim().is_empty())
            .ok_or_else(|| {
                HttpApiError::validation_error("JobName is required for TestUtil.RunBackgroundJob")
            })?;

        let job_name: BackgroundJobName = job_name.parse().map_err(|err| {
            HttpApiError::validation_error(format!("Unsupported background job: {err}"))
        })?;

        self.db().run_job(job_name).await;

        let response = serde_json::json!({
            "Message": format!("Background job executed: {job_name}")
        });
        Ok(Response::Raw(response))
    }
}
