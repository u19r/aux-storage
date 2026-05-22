use storage_backfill::{
    LogicalBackfillDomain, LogicalBackfillRecord, LogicalExportPage, LogicalExportRequest,
};
use storage_types::{StorageError, StorageResult};
use turso::Value as TursoValue;

use super::{
    TursoStorageProvider,
    logical_backfill::{
        payload_i64, payload_optional_i64, payload_string, row_text, unchecked_checksum,
    },
    logical_backfill_stream_records::{
        turso_stream_cursor_record_from_row, turso_stream_format_record_from_row,
        turso_stream_item_record_from_row, turso_user_stream_record_from_row,
    },
};

pub(super) async fn export_stream_records(
    provider: &TursoStorageProvider,
    request: LogicalExportRequest,
) -> StorageResult<LogicalExportPage> {
    let conn = provider.connect().await?;
    let limit = i64::from(request.limit);
    let mut records = Vec::new();
    append_turso_stream_format_records(provider, &conn, &mut records, limit).await?;
    append_turso_user_stream_records(provider, &conn, &mut records, None, limit).await?;
    let stream_filters = resolve_turso_stream_record_filter(provider, &conn, None).await?;
    append_turso_stream_item_records(
        provider,
        &conn,
        &mut records,
        stream_filters.as_deref(),
        limit,
    )
    .await?;
    append_turso_stream_cursor_records(
        provider,
        &conn,
        &mut records,
        stream_filters.as_deref(),
        limit,
    )
    .await?;
    Ok(LogicalExportPage {
        domain: LogicalBackfillDomain::StreamRecords,
        records,
        next_cursor: None,
        checksum: unchecked_checksum()?,
    })
}

pub(super) async fn import_stream_record<C>(
    provider: &TursoStorageProvider,
    conn: &C,
    payload_json: &str,
) -> StorageResult<()>
where
    C: super::provider::TursoSqlConnection + ?Sized,
{
    let payload = serde_json::from_str::<serde_json::Value>(payload_json)?;
    match payload_string(&payload, "stream_table")?.as_str() {
        "sys_stream_format_metadata" => {
            import_turso_stream_format_record(provider, conn, &payload).await
        }
        "sys_user_streams" => import_turso_user_stream_record(provider, conn, &payload).await,
        "sys_stream_items" => import_turso_stream_item_record(provider, conn, &payload).await,
        "sys_stream_cursors" => import_turso_stream_cursor_record(provider, conn, &payload).await,
        other => Err(StorageError::validation(format!(
            "unsupported turso logical stream table {other}"
        ))),
    }
}

async fn append_turso_stream_format_records<C>(
    provider: &TursoStorageProvider,
    conn: &C,
    records: &mut Vec<LogicalBackfillRecord>,
    limit: i64,
) -> StorageResult<()>
where
    C: super::provider::TursoSqlConnection + ?Sized,
{
    let rows = provider
        .query_rows(
            conn,
            r"SELECT format_key, format_version
              FROM sys_stream_format_metadata
              ORDER BY format_key
              LIMIT ?1",
            vec![TursoValue::Integer(limit)],
        )
        .await?;
    for row in &rows {
        records.push(turso_stream_format_record_from_row(row)?);
    }
    Ok(())
}

async fn resolve_turso_stream_record_filter<C>(
    provider: &TursoStorageProvider,
    conn: &C,
    stream_filter: Option<&str>,
) -> StorageResult<Option<Vec<String>>>
where
    C: super::provider::TursoSqlConnection + ?Sized,
{
    let Some(stream_name) = stream_filter else {
        return Ok(None);
    };
    let rows = provider
        .query_rows(
            conn,
            "SELECT internal_id FROM sys_user_streams WHERE stream_name = ?1 OR internal_id = ?1",
            vec![TursoValue::Text(stream_name.to_string())],
        )
        .await?;
    let mut filters = vec![stream_name.to_string()];
    for row in rows {
        let internal_id = row_text(&row, "internal_id")?.to_string();
        if !filters.iter().any(|filter| filter == &internal_id) {
            filters.push(internal_id);
        }
    }
    Ok(Some(filters))
}

async fn append_turso_user_stream_records<C>(
    provider: &TursoStorageProvider,
    conn: &C,
    records: &mut Vec<LogicalBackfillRecord>,
    stream_filter: Option<&str>,
    limit: i64,
) -> StorageResult<()>
where
    C: super::provider::TursoSqlConnection + ?Sized,
{
    let (sql, params) = if let Some(stream_name) = stream_filter {
        (
            r"SELECT stream_name, internal_id, ttl_seconds, created_at, updated_at
              FROM sys_user_streams
              WHERE stream_name = ?1 OR internal_id = ?1
              ORDER BY stream_name
              LIMIT ?2",
            vec![
                TursoValue::Text(stream_name.to_string()),
                TursoValue::Integer(limit),
            ],
        )
    } else {
        (
            r"SELECT stream_name, internal_id, ttl_seconds, created_at, updated_at
              FROM sys_user_streams
              ORDER BY stream_name
              LIMIT ?1",
            vec![TursoValue::Integer(limit)],
        )
    };
    let rows = provider.query_rows(conn, sql, params).await?;
    for row in &rows {
        records.push(turso_user_stream_record_from_row(row)?);
    }
    Ok(())
}

async fn append_turso_stream_item_records<C>(
    provider: &TursoStorageProvider,
    conn: &C,
    records: &mut Vec<LogicalBackfillRecord>,
    stream_filter: Option<&[String]>,
    limit: i64,
) -> StorageResult<()>
where
    C: super::provider::TursoSqlConnection + ?Sized,
{
    let (sql, params) = if let Some(stream_names) = stream_filter {
        let first = stream_names
            .first()
            .ok_or_else(|| StorageError::validation("stream filter cannot be empty"))?;
        let second = stream_names.get(1).unwrap_or(first);
        (
            r"SELECT stream_name, item_id, data, created_at, data_type
              FROM sys_stream_items
              WHERE stream_name = ?1 OR stream_name = ?2
              ORDER BY stream_name, item_id
              LIMIT ?3",
            vec![
                TursoValue::Text(first.clone()),
                TursoValue::Text(second.clone()),
                TursoValue::Integer(limit),
            ],
        )
    } else {
        (
            r"SELECT stream_name, item_id, data, created_at, data_type
              FROM sys_stream_items
              ORDER BY stream_name, item_id
              LIMIT ?1",
            vec![TursoValue::Integer(limit)],
        )
    };
    let rows = provider.query_rows(conn, sql, params).await?;
    for row in &rows {
        records.push(turso_stream_item_record_from_row(row)?);
    }
    Ok(())
}

async fn append_turso_stream_cursor_records<C>(
    provider: &TursoStorageProvider,
    conn: &C,
    records: &mut Vec<LogicalBackfillRecord>,
    stream_filter: Option<&[String]>,
    limit: i64,
) -> StorageResult<()>
where
    C: super::provider::TursoSqlConnection + ?Sized,
{
    let (sql, params) = if let Some(stream_names) = stream_filter {
        let first = stream_names
            .first()
            .ok_or_else(|| StorageError::validation("stream filter cannot be empty"))?;
        let second = stream_names.get(1).unwrap_or(first);
        (
            r"SELECT cursor_name, stream_name, position, created_at
              FROM sys_stream_cursors
              WHERE stream_name = ?1 OR stream_name = ?2
              ORDER BY stream_name, cursor_name
              LIMIT ?3",
            vec![
                TursoValue::Text(first.clone()),
                TursoValue::Text(second.clone()),
                TursoValue::Integer(limit),
            ],
        )
    } else {
        (
            r"SELECT cursor_name, stream_name, position, created_at
              FROM sys_stream_cursors
              ORDER BY stream_name, cursor_name
              LIMIT ?1",
            vec![TursoValue::Integer(limit)],
        )
    };
    let rows = provider.query_rows(conn, sql, params).await?;
    for row in &rows {
        records.push(turso_stream_cursor_record_from_row(row)?);
    }
    Ok(())
}

async fn import_turso_stream_format_record<C>(
    provider: &TursoStorageProvider,
    conn: &C,
    payload: &serde_json::Value,
) -> StorageResult<()>
where
    C: super::provider::TursoSqlConnection + ?Sized,
{
    let format_key = payload_string(payload, "format_key")?;
    let format_version = payload_i64(payload, "format_version")?;
    let _ = provider
        .execute(
            conn,
            r"INSERT INTO sys_stream_format_metadata (format_key, format_version)
              VALUES (?1, ?2)
              ON CONFLICT(format_key)
              DO UPDATE SET format_version = excluded.format_version",
            vec![
                TursoValue::Text(format_key),
                TursoValue::Integer(format_version),
            ],
        )
        .await?;
    Ok(())
}

async fn import_turso_user_stream_record<C>(
    provider: &TursoStorageProvider,
    conn: &C,
    payload: &serde_json::Value,
) -> StorageResult<()>
where
    C: super::provider::TursoSqlConnection + ?Sized,
{
    let stream_name = payload_string(payload, "stream_name")?;
    let internal_id = payload_string(payload, "internal_id")?;
    let ttl_seconds = payload_optional_i64(payload, "ttl_seconds")?;
    let created_at = payload_i64(payload, "created_at")?;
    let updated_at = payload_i64(payload, "updated_at")?;
    let _ = provider
        .execute(
            conn,
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
            vec![
                TursoValue::Text(stream_name),
                TursoValue::Text(internal_id),
                ttl_seconds.map_or(TursoValue::Null, TursoValue::Integer),
                TursoValue::Integer(created_at),
                TursoValue::Integer(updated_at),
            ],
        )
        .await?;
    Ok(())
}

async fn import_turso_stream_item_record<C>(
    provider: &TursoStorageProvider,
    conn: &C,
    payload: &serde_json::Value,
) -> StorageResult<()>
where
    C: super::provider::TursoSqlConnection + ?Sized,
{
    let stream_name = payload_string(payload, "stream_name")?;
    let item_id = payload_string(payload, "item_id")?;
    let data = payload
        .get("data")
        .cloned()
        .ok_or_else(|| StorageError::validation("stream item record missing data"))
        .and_then(|value| serde_json::from_value::<Vec<u8>>(value).map_err(Into::into))?;
    let created_at = payload_i64(payload, "created_at")?;
    let data_type = payload_i64(payload, "data_type")?;
    let _ = provider
        .execute(
            conn,
            "DELETE FROM sys_stream_items WHERE stream_name = ?1 AND item_id = ?2",
            vec![
                TursoValue::Text(stream_name.clone()),
                TursoValue::Text(item_id.clone()),
            ],
        )
        .await?;
    let _ = provider
        .execute(
            conn,
            r"INSERT INTO sys_stream_items (stream_name, item_id, data, created_at, data_type)
              VALUES (?1, ?2, ?3, ?4, ?5)",
            vec![
                TursoValue::Text(stream_name),
                TursoValue::Text(item_id),
                TursoValue::Blob(data),
                TursoValue::Integer(created_at),
                TursoValue::Integer(data_type),
            ],
        )
        .await?;
    Ok(())
}

async fn import_turso_stream_cursor_record<C>(
    provider: &TursoStorageProvider,
    conn: &C,
    payload: &serde_json::Value,
) -> StorageResult<()>
where
    C: super::provider::TursoSqlConnection + ?Sized,
{
    let cursor_name = payload_string(payload, "cursor_name")?;
    let stream_name = payload_string(payload, "stream_name")?;
    let position = payload_string(payload, "position")?;
    let created_at = payload_i64(payload, "created_at")?;
    let _ = provider
        .execute(
            conn,
            r"INSERT INTO sys_stream_cursors (cursor_name, stream_name, position, created_at)
              VALUES (?1, ?2, ?3, ?4)
              ON CONFLICT(cursor_name, stream_name)
              DO UPDATE SET
                position = excluded.position,
                created_at = excluded.created_at",
            vec![
                TursoValue::Text(cursor_name),
                TursoValue::Text(stream_name),
                TursoValue::Text(position),
                TursoValue::Integer(created_at),
            ],
        )
        .await?;
    Ok(())
}
