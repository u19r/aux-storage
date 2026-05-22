use storage_backfill::LogicalBackfillChecksum;
use storage_types::{StorageError, StorageResult};

use super::PostgresStorageProvider;

pub(super) fn log_component_u64(
    value: Result<i64, tokio_postgres::Error>,
    label: &str,
) -> StorageResult<u64> {
    let value = value
        .map_err(|err| PostgresStorageProvider::map_postgres_error("decode sync log id", err))?;
    u64::try_from(value)
        .map_err(|_| StorageError::validation(format!("sync apply log {label} is negative")))
}

pub(super) fn log_component_i64(value: u64, label: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| {
        StorageError::validation(format!(
            "sync apply log {label} does not fit postgres integer"
        ))
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
