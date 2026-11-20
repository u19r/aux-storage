use http_error::HttpApiError;
use storage::DatabaseManager;

use crate::types::Response;

pub(crate) async fn clear_all_tables(db: &DatabaseManager) -> Result<Response, HttpApiError> {
    db.clear_all_tables().await?;
    let response = serde_json::json!({
        "Message": "All tables cleared successfully"
    });
    Ok(Response::Raw(response))
}
