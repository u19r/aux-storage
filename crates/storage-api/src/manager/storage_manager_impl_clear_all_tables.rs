use http_error::HttpApiError;

use crate::{manager::StorageApiManagerImpl, types::Response};

impl StorageApiManagerImpl {
    pub(super) async fn clear_all_tables_internal(
        &self,
        _payload: serde_json::Value,
    ) -> Result<Response, HttpApiError> {
        self.db().clear_all_tables().await?;
        let response = serde_json::json!({
            "Message": "All tables cleared successfully"
        });
        Ok(Response::Raw(response))
    }
}
