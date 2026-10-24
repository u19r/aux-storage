//! Public canonical history uses the one native parser and owns its encoded
//! response.
use std::sync::Arc;

use http_error::HttpApiError;

use super::StorageApiManagerImpl;
use crate::types::Response;
impl StorageApiManagerImpl {
    pub(super) async fn read_canonical_history_response(
        &self,
        payload: serde_json::Value,
    ) -> Result<Response, HttpApiError> {
        self.db()
            .read_canonical_history_response(payload)
            .await
            .map(|response| Response::CanonicalHistory(Arc::new(response)))
            .map_err(HttpApiError::from)
    }
}
