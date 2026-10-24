use http_error::HttpApiError;
use storage_types::TableName;
use stream_provider::{StreamAppendOperationId, TableAppendReconciliation};

use crate::{manager::StorageApiManagerImpl, types::Response};

impl StorageApiManagerImpl {
    pub(super) async fn append_table_stream_record_internal(
        &self,
        payload: serde_json::Value,
    ) -> Result<Response, HttpApiError> {
        let (table, data) = read_table_append(&payload)?;
        let operation = match payload.get("OperationId") {
            Some(value) => read_append_operation(value)?,
            None => StreamAppendOperationId::fresh(),
        };
        let id = self
            .db()
            .append_table_stream_record_for_operation(operation, &table, data.as_bytes())
            .await
            .map_err(HttpApiError::from)?;
        Ok(Response::Raw(serde_json::json!({
            "Message": "Table stream record appended", "StreamItemId": id.to_string(), "OperationId": operation.token()
        })))
    }

    pub(super) async fn reconcile_table_stream_record_internal(
        &self,
        payload: serde_json::Value,
    ) -> Result<Response, HttpApiError> {
        let (table, data) = read_table_append(&payload)?;
        let operation = payload
            .get("OperationId")
            .ok_or_else(|| {
                HttpApiError::validation_error("OperationId is required for append reconciliation")
            })
            .and_then(read_append_operation)?;
        let result = self
            .db()
            .reconcile_table_stream_record(operation, table, data.as_bytes().to_vec())
            .await
            .map_err(HttpApiError::from)?;
        let response = match result {
            TableAppendReconciliation::Durable(id) => {
                serde_json::json!({"Outcome": "Durable", "StreamItemId": id.to_string(), "OperationId": operation.token()})
            }
            TableAppendReconciliation::Unresolved => {
                serde_json::json!({"Outcome": "Unresolved", "OperationId": operation.token()})
            }
        };
        Ok(Response::Raw(response))
    }
}

fn read_table_append(payload: &serde_json::Value) -> Result<(TableName, &str), HttpApiError> {
    let table = payload
        .get("TableName")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            HttpApiError::validation_error("TableName is required for a table stream append")
        })?;
    let data = payload
        .get("Data")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            HttpApiError::validation_error("Data is required for a table stream append")
        })?;
    Ok((TableName::new(table), data))
}

fn read_append_operation(
    value: &serde_json::Value,
) -> Result<StreamAppendOperationId, HttpApiError> {
    let token = value.as_str().ok_or_else(|| {
        HttpApiError::validation_error("OperationId must be an append operation UUID")
    })?;
    StreamAppendOperationId::parse(token).map_err(|_| {
        HttpApiError::validation_error("OperationId must be a nonzero append operation UUID")
    })
}
