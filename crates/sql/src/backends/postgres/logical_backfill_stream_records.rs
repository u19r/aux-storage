use storage_backfill::{LogicalBackfillDomain, LogicalBackfillRecord};
use storage_types::StorageResult;

use super::PostgresStorageProvider;

pub(super) fn postgres_stream_format_record_from_row(
    row: &tokio_postgres::Row,
) -> StorageResult<LogicalBackfillRecord> {
    let format_key = row
        .try_get::<_, String>(0)
        .map_err(|err| PostgresStorageProvider::map_postgres_error("decode format_key", err))?;
    let format_version = row
        .try_get::<_, i64>(1)
        .map_err(|err| PostgresStorageProvider::map_postgres_error("decode format_version", err))?;
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

pub(super) fn postgres_user_stream_record_from_row(
    row: &tokio_postgres::Row,
) -> StorageResult<LogicalBackfillRecord> {
    let stream_name = row
        .try_get::<_, String>(0)
        .map_err(|err| PostgresStorageProvider::map_postgres_error("decode stream_name", err))?;
    let internal_id = row
        .try_get::<_, String>(1)
        .map_err(|err| PostgresStorageProvider::map_postgres_error("decode internal_id", err))?;
    let ttl_seconds = row
        .try_get::<_, Option<i64>>(2)
        .map_err(|err| PostgresStorageProvider::map_postgres_error("decode ttl_seconds", err))?;
    let created_at = row
        .try_get::<_, i64>(3)
        .map_err(|err| PostgresStorageProvider::map_postgres_error("decode created_at", err))?;
    let updated_at = row
        .try_get::<_, i64>(4)
        .map_err(|err| PostgresStorageProvider::map_postgres_error("decode updated_at", err))?;
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

pub(super) fn postgres_stream_item_record_from_row(
    row: &tokio_postgres::Row,
) -> StorageResult<LogicalBackfillRecord> {
    let stream_name = row
        .try_get::<_, String>(0)
        .map_err(|err| PostgresStorageProvider::map_postgres_error("decode stream_name", err))?;
    let item_id = row
        .try_get::<_, String>(1)
        .map_err(|err| PostgresStorageProvider::map_postgres_error("decode item_id", err))?;
    let data = row
        .try_get::<_, Vec<u8>>(2)
        .map_err(|err| PostgresStorageProvider::map_postgres_error("decode data", err))?;
    let created_at = row
        .try_get::<_, i64>(3)
        .map_err(|err| PostgresStorageProvider::map_postgres_error("decode created_at", err))?;
    let data_type = row
        .try_get::<_, i32>(4)
        .map(i64::from)
        .map_err(|err| PostgresStorageProvider::map_postgres_error("decode data_type", err))?;
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

pub(super) fn postgres_stream_cursor_record_from_row(
    row: &tokio_postgres::Row,
) -> StorageResult<LogicalBackfillRecord> {
    let cursor_name = row
        .try_get::<_, String>(0)
        .map_err(|err| PostgresStorageProvider::map_postgres_error("decode cursor_name", err))?;
    let stream_name = row
        .try_get::<_, String>(1)
        .map_err(|err| PostgresStorageProvider::map_postgres_error("decode stream_name", err))?;
    let position = row
        .try_get::<_, String>(2)
        .map_err(|err| PostgresStorageProvider::map_postgres_error("decode position", err))?;
    let created_at = row
        .try_get::<_, i64>(3)
        .map_err(|err| PostgresStorageProvider::map_postgres_error("decode created_at", err))?;
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
