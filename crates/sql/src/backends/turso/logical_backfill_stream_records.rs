use std::collections::HashMap;

use storage_backfill::{LogicalBackfillDomain, LogicalBackfillRecord};
use storage_types::StorageResult;
use turso::Value as TursoValue;

use super::logical_backfill::{row_blob, row_i64, row_optional_i64, row_text};

pub(super) fn turso_stream_format_record_from_row(
    row: &HashMap<String, TursoValue>,
) -> StorageResult<LogicalBackfillRecord> {
    let format_key = row_text(row, "format_key")?.to_string();
    let format_version = row_i64(row, "format_version")?;
    let payload_json = serde_json::json!({
        "stream_table": "sys_stream_format_metadata",
        "format_key": format_key,
        "format_version": format_version,
    })
    .to_string();
    Ok(LogicalBackfillRecord::DomainRecord {
        domain: LogicalBackfillDomain::StreamRecords,
        record_key_json: serde_json::json!({
            "stream_table": "sys_stream_format_metadata",
            "format_key": format_key,
        })
        .to_string(),
        payload_json,
    })
}

pub(super) fn turso_user_stream_record_from_row(
    row: &HashMap<String, TursoValue>,
) -> StorageResult<LogicalBackfillRecord> {
    let stream_name = row_text(row, "stream_name")?.to_string();
    let internal_id = row_text(row, "internal_id")?.to_string();
    let ttl_seconds = row_optional_i64(row, "ttl_seconds")?;
    let created_at = row_i64(row, "created_at")?;
    let updated_at = row_i64(row, "updated_at")?;
    let payload_json = serde_json::json!({
        "stream_table": "sys_user_streams",
        "stream_name": stream_name,
        "internal_id": internal_id,
        "ttl_seconds": ttl_seconds,
        "created_at": created_at,
        "updated_at": updated_at,
    })
    .to_string();
    Ok(LogicalBackfillRecord::DomainRecord {
        domain: LogicalBackfillDomain::StreamRecords,
        record_key_json: serde_json::json!({
            "stream_table": "sys_user_streams",
            "stream_name": stream_name,
        })
        .to_string(),
        payload_json,
    })
}

pub(super) fn turso_stream_item_record_from_row(
    row: &HashMap<String, TursoValue>,
) -> StorageResult<LogicalBackfillRecord> {
    let stream_name = row_text(row, "stream_name")?.to_string();
    let item_id = row_text(row, "item_id")?.to_string();
    let data = row_blob(row, "data")?;
    let created_at = row_i64(row, "created_at")?;
    let data_type = row_i64(row, "data_type")?;
    let payload_json = serde_json::json!({
        "stream_table": "sys_stream_items",
        "stream_name": stream_name,
        "item_id": item_id,
        "data": data,
        "created_at": created_at,
        "data_type": data_type,
    })
    .to_string();
    Ok(LogicalBackfillRecord::DomainRecord {
        domain: LogicalBackfillDomain::StreamRecords,
        record_key_json: serde_json::json!({
            "stream_table": "sys_stream_items",
            "stream_name": stream_name,
            "item_id": item_id,
        })
        .to_string(),
        payload_json,
    })
}

pub(super) fn turso_stream_cursor_record_from_row(
    row: &HashMap<String, TursoValue>,
) -> StorageResult<LogicalBackfillRecord> {
    let cursor_name = row_text(row, "cursor_name")?.to_string();
    let stream_name = row_text(row, "stream_name")?.to_string();
    let position = row_text(row, "position")?.to_string();
    let created_at = row_i64(row, "created_at")?;
    let payload_json = serde_json::json!({
        "stream_table": "sys_stream_cursors",
        "cursor_name": cursor_name,
        "stream_name": stream_name,
        "position": position,
        "created_at": created_at,
    })
    .to_string();
    Ok(LogicalBackfillRecord::DomainRecord {
        domain: LogicalBackfillDomain::StreamRecords,
        record_key_json: serde_json::json!({
            "stream_table": "sys_stream_cursors",
            "cursor_name": cursor_name,
            "stream_name": stream_name,
        })
        .to_string(),
        payload_json,
    })
}
