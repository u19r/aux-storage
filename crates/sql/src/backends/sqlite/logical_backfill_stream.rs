use storage_backfill::{LogicalBackfillDomain, LogicalBackfillRecord};
use storage_types::{StorageError, StorageResult};

use super::logical_backfill::{payload_i64, payload_optional_i64, payload_string};
use crate::{error_handler::map_sqlite_error, utils::SqliteConn};

pub(super) fn append_stream_format_records(
    conn: &rusqlite::Connection,
    records: &mut Vec<LogicalBackfillRecord>,
    limit: i64,
) -> StorageResult<()> {
    let mut stmt = conn
        .prepare(
            "SELECT format_key, format_version FROM sys_stream_format_metadata ORDER BY \
             format_key LIMIT ?1",
        )
        .map_err(map_sqlite_error)?;
    let rows = stmt
        .query_map([limit], stream_format_record_from_row)
        .map_err(map_sqlite_error)?;
    for row in rows {
        records.push(row.map_err(map_sqlite_error)?);
    }
    Ok(())
}

pub(super) fn resolve_stream_record_filter(
    conn: &rusqlite::Connection,
    stream_filter: Option<&str>,
) -> StorageResult<Option<Vec<String>>> {
    let Some(stream_name) = stream_filter else {
        return Ok(None);
    };
    let mut filters = vec![stream_name.to_string()];
    let mut stmt = conn
        .prepare(
            "SELECT internal_id FROM sys_user_streams WHERE stream_name = ?1 OR internal_id = ?1",
        )
        .map_err(map_sqlite_error)?;
    let rows = stmt
        .query_map([stream_name], |row| row.get::<_, String>(0))
        .map_err(map_sqlite_error)?;
    for row in rows {
        let internal_id = row.map_err(map_sqlite_error)?;
        if !filters.iter().any(|filter| filter == &internal_id) {
            filters.push(internal_id);
        }
    }
    Ok(Some(filters))
}

pub(super) fn append_user_stream_records(
    conn: &rusqlite::Connection,
    records: &mut Vec<LogicalBackfillRecord>,
    stream_filter: Option<&str>,
    limit: i64,
) -> StorageResult<()> {
    if let Some(stream_name) = stream_filter {
        let mut stmt = conn
            .prepare(
                "SELECT stream_name, internal_id, ttl_seconds, created_at, updated_at FROM \
                 sys_user_streams WHERE stream_name = ?1 OR internal_id = ?1 ORDER BY stream_name \
                 LIMIT ?2",
            )
            .map_err(map_sqlite_error)?;
        let rows = stmt
            .query_map((stream_name, limit), user_stream_record_from_row)
            .map_err(map_sqlite_error)?;
        for row in rows {
            records.push(row.map_err(map_sqlite_error)?);
        }
    } else {
        let mut stmt = conn
            .prepare(
                "SELECT stream_name, internal_id, ttl_seconds, created_at, updated_at FROM \
                 sys_user_streams ORDER BY stream_name LIMIT ?1",
            )
            .map_err(map_sqlite_error)?;
        let rows = stmt
            .query_map([limit], user_stream_record_from_row)
            .map_err(map_sqlite_error)?;
        for row in rows {
            records.push(row.map_err(map_sqlite_error)?);
        }
    }
    Ok(())
}

pub(super) fn append_stream_item_records(
    conn: &rusqlite::Connection,
    records: &mut Vec<LogicalBackfillRecord>,
    stream_filter: Option<&[String]>,
    limit: i64,
) -> StorageResult<()> {
    if let Some(stream_names) = stream_filter {
        let mut stmt = conn
            .prepare(
                "SELECT stream_name, item_id, data, created_at, data_type FROM sys_stream_items \
                 WHERE stream_name = ?1 OR stream_name = ?2 ORDER BY stream_name, item_id LIMIT ?3",
            )
            .map_err(map_sqlite_error)?;
        let first = stream_names
            .first()
            .ok_or_else(|| StorageError::validation("stream filter cannot be empty"))?;
        let second = stream_names.get(1).unwrap_or(first);
        let rows = stmt
            .query_map(
                (first.as_str(), second.as_str(), limit),
                stream_item_record_from_row,
            )
            .map_err(map_sqlite_error)?;
        for row in rows {
            records.push(row.map_err(map_sqlite_error)?);
        }
    } else {
        let mut stmt = conn
            .prepare(
                "SELECT stream_name, item_id, data, created_at, data_type FROM sys_stream_items \
                 ORDER BY stream_name, item_id LIMIT ?1",
            )
            .map_err(map_sqlite_error)?;
        let rows = stmt
            .query_map([limit], stream_item_record_from_row)
            .map_err(map_sqlite_error)?;
        for row in rows {
            records.push(row.map_err(map_sqlite_error)?);
        }
    }
    Ok(())
}

pub(super) fn append_stream_cursor_records(
    conn: &rusqlite::Connection,
    records: &mut Vec<LogicalBackfillRecord>,
    stream_filter: Option<&[String]>,
    limit: i64,
) -> StorageResult<()> {
    if let Some(stream_names) = stream_filter {
        let mut stmt = conn
            .prepare(
                "SELECT cursor_name, stream_name, position, created_at FROM sys_stream_cursors \
                 WHERE stream_name = ?1 OR stream_name = ?2 ORDER BY stream_name, cursor_name \
                 LIMIT ?3",
            )
            .map_err(map_sqlite_error)?;
        let first = stream_names
            .first()
            .ok_or_else(|| StorageError::validation("stream filter cannot be empty"))?;
        let second = stream_names.get(1).unwrap_or(first);
        let rows = stmt
            .query_map(
                (first.as_str(), second.as_str(), limit),
                stream_cursor_record_from_row,
            )
            .map_err(map_sqlite_error)?;
        for row in rows {
            records.push(row.map_err(map_sqlite_error)?);
        }
    } else {
        let mut stmt = conn
            .prepare(
                "SELECT cursor_name, stream_name, position, created_at FROM sys_stream_cursors \
                 ORDER BY stream_name, cursor_name LIMIT ?1",
            )
            .map_err(map_sqlite_error)?;
        let rows = stmt
            .query_map([limit], stream_cursor_record_from_row)
            .map_err(map_sqlite_error)?;
        for row in rows {
            records.push(row.map_err(map_sqlite_error)?);
        }
    }
    Ok(())
}

fn stream_format_record_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<LogicalBackfillRecord> {
    let format_key: String = row.get(0)?;
    let format_version: i64 = row.get(1)?;
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

fn user_stream_record_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<LogicalBackfillRecord> {
    let stream_name: String = row.get(0)?;
    let internal_id: String = row.get(1)?;
    let ttl_seconds: Option<i64> = row.get(2)?;
    let created_at: i64 = row.get(3)?;
    let updated_at: i64 = row.get(4)?;
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

fn stream_item_record_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<LogicalBackfillRecord> {
    let stream_name: String = row.get(0)?;
    let item_id: String = row.get(1)?;
    let data: Vec<u8> = row.get(2)?;
    let created_at: i64 = row.get(3)?;
    let data_type: i64 = row.get(4)?;
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

fn stream_cursor_record_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<LogicalBackfillRecord> {
    let cursor_name: String = row.get(0)?;
    let stream_name: String = row.get(1)?;
    let position: String = row.get(2)?;
    let created_at: i64 = row.get(3)?;
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

pub(super) fn import_stream_format_record(
    payload: &serde_json::Value,
    sqlite: &SqliteConn<'_>,
) -> StorageResult<()> {
    let format_key = payload_string(payload, "format_key")?;
    let format_version = payload_i64(payload, "format_version")?;
    sqlite
        .execute(
            r"INSERT INTO sys_stream_format_metadata (format_key, format_version)
              VALUES (?1, ?2)
              ON CONFLICT(format_key)
              DO UPDATE SET format_version = excluded.format_version",
            (format_key.as_str(), format_version),
        )
        .map_err(map_sqlite_error)?;
    Ok(())
}

pub(super) fn import_user_stream_record(
    payload: &serde_json::Value,
    sqlite: &SqliteConn<'_>,
) -> StorageResult<()> {
    let stream_name = payload_string(payload, "stream_name")?;
    let internal_id = payload_string(payload, "internal_id")?;
    let ttl_seconds = payload_optional_i64(payload, "ttl_seconds")?;
    let created_at = payload_i64(payload, "created_at")?;
    let updated_at = payload_i64(payload, "updated_at")?;
    sqlite
        .execute(
            r"INSERT INTO sys_user_streams (
                stream_name, internal_id, ttl_seconds, created_at, updated_at
              )
              VALUES (?1, ?2, ?3, ?4, ?5)
              ON CONFLICT(stream_name)
              DO UPDATE SET
                internal_id = excluded.internal_id,
                ttl_seconds = excluded.ttl_seconds,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at",
            (
                stream_name.as_str(),
                internal_id.as_str(),
                ttl_seconds,
                created_at,
                updated_at,
            ),
        )
        .map_err(map_sqlite_error)?;
    Ok(())
}

pub(super) fn import_stream_item_record(
    payload: &serde_json::Value,
    sqlite: &SqliteConn<'_>,
) -> StorageResult<()> {
    let stream_name = payload_string(payload, "stream_name")?;
    let item_id = payload_string(payload, "item_id")?;
    let data = payload
        .get("data")
        .cloned()
        .ok_or_else(|| StorageError::validation("stream item record missing data"))
        .and_then(|value| serde_json::from_value::<Vec<u8>>(value).map_err(Into::into))?;
    let created_at = payload_i64(payload, "created_at")?;
    let data_type = payload_i64(payload, "data_type")?;
    sqlite
        .execute(
            r"INSERT INTO sys_stream_items (stream_name, item_id, data, created_at, data_type)
              VALUES (?1, ?2, ?3, ?4, ?5)
              ON CONFLICT(stream_name, item_id)
              DO UPDATE SET
                data = excluded.data,
                created_at = excluded.created_at,
                data_type = excluded.data_type",
            (
                stream_name.as_str(),
                item_id.as_str(),
                data,
                created_at,
                data_type,
            ),
        )
        .map_err(map_sqlite_error)?;
    Ok(())
}

pub(super) fn import_stream_cursor_record(
    payload: &serde_json::Value,
    sqlite: &SqliteConn<'_>,
) -> StorageResult<()> {
    let cursor_name = payload_string(payload, "cursor_name")?;
    let stream_name = payload_string(payload, "stream_name")?;
    let position = payload_string(payload, "position")?;
    let created_at = payload_i64(payload, "created_at")?;
    sqlite
        .execute(
            r"INSERT INTO sys_stream_cursors (cursor_name, stream_name, position, created_at)
              VALUES (?1, ?2, ?3, ?4)
              ON CONFLICT(cursor_name, stream_name)
              DO UPDATE SET
                position = excluded.position,
                created_at = excluded.created_at",
            (
                cursor_name.as_str(),
                stream_name.as_str(),
                position.as_str(),
                created_at,
            ),
        )
        .map_err(map_sqlite_error)?;
    Ok(())
}
