use std::collections::HashMap;

use storage_backfill::LogicalBackfillChecksum;
use storage_types::{StorageError, StorageResult};
use turso::Value as TursoValue;

pub(super) fn row_text<'a>(
    row: &'a HashMap<String, TursoValue>,
    column: &str,
) -> StorageResult<&'a str> {
    match row.get(column) {
        Some(TursoValue::Text(value)) => Ok(value),
        _ => Err(StorageError::internal(&format!(
            "missing text column {column} in turso sync log row"
        ))),
    }
}

pub(super) fn row_optional_text(
    row: &HashMap<String, TursoValue>,
    column: &str,
) -> StorageResult<Option<String>> {
    match row.get(column) {
        Some(TursoValue::Text(value)) => Ok(Some(value.clone())),
        Some(TursoValue::Null) | None => Ok(None),
        _ => Err(StorageError::internal(&format!(
            "invalid text column {column} in turso logical row"
        ))),
    }
}

pub(super) fn row_i64(row: &HashMap<String, TursoValue>, column: &str) -> StorageResult<i64> {
    row.get(column)
        .map(super::provider::value_to_i64)
        .transpose()?
        .ok_or_else(|| StorageError::internal(&format!("missing integer column {column}")))
}

pub(super) fn row_optional_i64(
    row: &HashMap<String, TursoValue>,
    column: &str,
) -> StorageResult<Option<i64>> {
    match row.get(column) {
        Some(TursoValue::Null) | None => Ok(None),
        Some(value) => super::provider::value_to_i64(value).map(Some),
    }
}

pub(super) fn row_blob(row: &HashMap<String, TursoValue>, column: &str) -> StorageResult<Vec<u8>> {
    match row.get(column) {
        Some(TursoValue::Blob(value)) => Ok(value.clone()),
        Some(TursoValue::Text(value)) => Ok(value.as_bytes().to_vec()),
        Some(TursoValue::Null) | None => Ok(Vec::new()),
        Some(TursoValue::Integer(_)) | Some(TursoValue::Real(_)) => Err(StorageError::internal(
            &format!("invalid blob column {column} in turso logical row"),
        )),
    }
}

pub(super) fn log_component_u64(value: Option<&TursoValue>, label: &str) -> StorageResult<u64> {
    let value = value
        .map(super::provider::value_to_i64)
        .transpose()?
        .ok_or_else(|| StorageError::internal(&format!("missing sync apply log {label} column")))?;
    u64::try_from(value)
        .map_err(|_| StorageError::validation(format!("sync apply log {label} is negative")))
}

pub(super) fn log_component_i64(value: u64, label: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| {
        StorageError::validation(format!("sync apply log {label} does not fit turso integer"))
    })
}

pub(super) fn unchecked_checksum() -> StorageResult<LogicalBackfillChecksum> {
    LogicalBackfillChecksum::new("unchecked").map_err(|error| {
        StorageError::internal(&format!("logical export checksum failed: {error}"))
    })
}

pub(super) fn payload_string(payload: &serde_json::Value, field: &str) -> StorageResult<String> {
    payload
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| StorageError::validation(format!("logical domain record missing {field}")))
}

pub(super) fn payload_i64(payload: &serde_json::Value, field: &str) -> StorageResult<i64> {
    payload
        .get(field)
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| StorageError::validation(format!("logical domain record missing {field}")))
}

pub(super) fn payload_optional_i64(
    payload: &serde_json::Value,
    field: &str,
) -> StorageResult<Option<i64>> {
    match payload.get(field) {
        Some(serde_json::Value::Null) | None => Ok(None),
        Some(value) => value.as_i64().map(Some).ok_or_else(|| {
            StorageError::validation(format!("logical domain record {field} must be integer"))
        }),
    }
}

pub(super) fn payload_optional_string(
    payload: &serde_json::Value,
    field: &str,
) -> StorageResult<Option<String>> {
    match payload.get(field) {
        Some(serde_json::Value::Null) | None => Ok(None),
        Some(value) => value
            .as_str()
            .map(|value| Some(value.to_string()))
            .ok_or_else(|| {
                StorageError::validation(format!("logical domain record {field} must be string"))
            }),
    }
}

pub(super) fn u64_to_i64(value: u64, label: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| {
        StorageError::validation(format!(
            "logical table metadata {label} does not fit integer"
        ))
    })
}
