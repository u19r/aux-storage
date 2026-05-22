use http_error::HttpApiError;
use storage_types::{StreamName, TableName};

use crate::{manager::StorageApiManagerImpl, types::Response};

impl StorageApiManagerImpl {
    pub(super) async fn append_table_stream_record_internal(
        &self,
        payload: serde_json::Value,
    ) -> Result<Response, HttpApiError> {
        let table_name = payload
            .get("TableName")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                HttpApiError::validation_error(
                    "TableName is required for TestUtil.AppendTableStreamRecord",
                )
            })?;
        let data = payload
            .get("Data")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                HttpApiError::validation_error(
                    "Data is required for TestUtil.AppendTableStreamRecord",
                )
            })?;

        let table = TableName::new(table_name);
        let stream_name = StreamName::table_stream(&table);
        let stream_item_id = self
            .db()
            .stream_provider()
            .append_item(stream_name, data.as_bytes(), None)
            .await
            .map_err(|err| {
                HttpApiError::internal_server_error(format!(
                    "failed to append table stream record: {err}"
                ))
            })?;

        let response = serde_json::json!({
            "Message": "Table stream record appended",
            "StreamItemId": stream_item_id.to_string()
        });
        Ok(Response::Raw(response))
    }
}
