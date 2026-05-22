use storage_backfill::{
    LogicalBackfillDomain, LogicalBackfillRecord, LogicalExportPage, LogicalExportRequest,
};
use storage_types::{StorageError, StorageResult};

use super::{
    PostgresStorageProvider,
    logical_backfill::{payload_i64, payload_optional_i64, payload_string, unchecked_checksum},
    logical_backfill_stream_records::{
        postgres_stream_cursor_record_from_row, postgres_stream_format_record_from_row,
        postgres_stream_item_record_from_row, postgres_user_stream_record_from_row,
    },
};

pub(super) async fn export_stream_records(
    provider: &PostgresStorageProvider,
    request: LogicalExportRequest,
) -> StorageResult<LogicalExportPage> {
    let client = provider
        .pool
        .get()
        .await
        .map_err(PostgresStorageProvider::map_postgres_client_acquire_error)?;
    let mut records = Vec::new();
    let limit = i64::from(request.limit);
    append_postgres_stream_format_records(provider, &client, &mut records, limit).await?;
    append_postgres_user_stream_records(provider, &client, &mut records, None, limit).await?;
    let stream_filters = resolve_postgres_stream_record_filter(provider, &client, None).await?;
    append_postgres_stream_item_records(
        provider,
        &client,
        &mut records,
        stream_filters.as_deref(),
        limit,
    )
    .await?;
    append_postgres_stream_cursor_records(
        provider,
        &client,
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

pub(super) async fn import_stream_record(
    provider: &PostgresStorageProvider,
    payload_json: &str,
) -> StorageResult<()> {
    let payload = serde_json::from_str::<serde_json::Value>(payload_json)?;
    match payload_string(&payload, "stream_table")?.as_str() {
        "sys_stream_format_metadata" => {
            import_postgres_stream_format_record(provider, &payload).await
        }
        "sys_user_streams" => import_postgres_user_stream_record(provider, &payload).await,
        "sys_stream_items" => import_postgres_stream_item_record(provider, &payload).await,
        "sys_stream_cursors" => import_postgres_stream_cursor_record(provider, &payload).await,
        other => Err(StorageError::validation(format!(
            "unsupported postgres logical stream table {other}"
        ))),
    }
}

async fn append_postgres_stream_format_records<C>(
    provider: &PostgresStorageProvider,
    client: &C,
    records: &mut Vec<LogicalBackfillRecord>,
    limit: i64,
) -> StorageResult<()>
where
    C: deadpool_postgres::GenericClient + Sync,
{
    let rows = client
        .query(
            r"SELECT format_key, format_version
              FROM sys_stream_format_metadata
              ORDER BY format_key
              LIMIT $1",
            &[&limit],
        )
        .await
        .map_err(|err| {
            PostgresStorageProvider::map_postgres_error("export stream format records", err)
        })?;
    for row in &rows {
        records.push(postgres_stream_format_record_from_row(row)?);
    }
    let _ = provider;
    Ok(())
}

async fn resolve_postgres_stream_record_filter<C>(
    provider: &PostgresStorageProvider,
    client: &C,
    stream_filter: Option<&str>,
) -> StorageResult<Option<Vec<String>>>
where
    C: deadpool_postgres::GenericClient + Sync,
{
    let Some(stream_name) = stream_filter else {
        return Ok(None);
    };
    let rows = client
        .query(
            "SELECT internal_id FROM sys_user_streams WHERE stream_name = $1 OR internal_id = $1",
            &[&stream_name],
        )
        .await
        .map_err(|err| PostgresStorageProvider::map_postgres_error("resolve stream filter", err))?;
    let mut filters = vec![stream_name.to_string()];
    for row in rows {
        let internal_id = row
            .try_get::<_, String>(0)
            .map_err(|err| PostgresStorageProvider::map_postgres_error("decode stream id", err))?;
        if !filters.iter().any(|filter| filter == &internal_id) {
            filters.push(internal_id);
        }
    }
    let _ = provider;
    Ok(Some(filters))
}

async fn append_postgres_user_stream_records<C>(
    provider: &PostgresStorageProvider,
    client: &C,
    records: &mut Vec<LogicalBackfillRecord>,
    stream_filter: Option<&str>,
    limit: i64,
) -> StorageResult<()>
where
    C: deadpool_postgres::GenericClient + Sync,
{
    let rows = if let Some(stream_name) = stream_filter {
        client
            .query(
                r"SELECT stream_name, internal_id, ttl_seconds, created_at, updated_at
                  FROM sys_user_streams
                  WHERE stream_name = $1 OR internal_id = $1
                  ORDER BY stream_name
                  LIMIT $2",
                &[&stream_name, &limit],
            )
            .await
    } else {
        client
            .query(
                r"SELECT stream_name, internal_id, ttl_seconds, created_at, updated_at
                  FROM sys_user_streams
                  ORDER BY stream_name
                  LIMIT $1",
                &[&limit],
            )
            .await
    }
    .map_err(|err| PostgresStorageProvider::map_postgres_error("export user streams", err))?;
    for row in &rows {
        records.push(postgres_user_stream_record_from_row(row)?);
    }
    let _ = provider;
    Ok(())
}

async fn append_postgres_stream_item_records<C>(
    provider: &PostgresStorageProvider,
    client: &C,
    records: &mut Vec<LogicalBackfillRecord>,
    stream_filter: Option<&[String]>,
    limit: i64,
) -> StorageResult<()>
where
    C: deadpool_postgres::GenericClient + Sync,
{
    let rows = if let Some(stream_names) = stream_filter {
        let first = stream_names
            .first()
            .ok_or_else(|| StorageError::validation("stream filter cannot be empty"))?;
        let second = stream_names.get(1).unwrap_or(first);
        client
            .query(
                r"SELECT stream_name, item_id, data, created_at, data_type
                  FROM sys_stream_items
                  WHERE stream_name = $1 OR stream_name = $2
                  ORDER BY stream_name, item_id
                  LIMIT $3",
                &[first, second, &limit],
            )
            .await
    } else {
        client
            .query(
                r"SELECT stream_name, item_id, data, created_at, data_type
                  FROM sys_stream_items
                  ORDER BY stream_name, item_id
                  LIMIT $1",
                &[&limit],
            )
            .await
    }
    .map_err(|err| PostgresStorageProvider::map_postgres_error("export stream items", err))?;
    for row in &rows {
        records.push(postgres_stream_item_record_from_row(row)?);
    }
    let _ = provider;
    Ok(())
}

async fn append_postgres_stream_cursor_records<C>(
    provider: &PostgresStorageProvider,
    client: &C,
    records: &mut Vec<LogicalBackfillRecord>,
    stream_filter: Option<&[String]>,
    limit: i64,
) -> StorageResult<()>
where
    C: deadpool_postgres::GenericClient + Sync,
{
    let rows = if let Some(stream_names) = stream_filter {
        let first = stream_names
            .first()
            .ok_or_else(|| StorageError::validation("stream filter cannot be empty"))?;
        let second = stream_names.get(1).unwrap_or(first);
        client
            .query(
                r"SELECT cursor_name, stream_name, position, created_at
                  FROM sys_stream_cursors
                  WHERE stream_name = $1 OR stream_name = $2
                  ORDER BY stream_name, cursor_name
                  LIMIT $3",
                &[first, second, &limit],
            )
            .await
    } else {
        client
            .query(
                r"SELECT cursor_name, stream_name, position, created_at
                  FROM sys_stream_cursors
                  ORDER BY stream_name, cursor_name
                  LIMIT $1",
                &[&limit],
            )
            .await
    }
    .map_err(|err| PostgresStorageProvider::map_postgres_error("export stream cursors", err))?;
    for row in &rows {
        records.push(postgres_stream_cursor_record_from_row(row)?);
    }
    let _ = provider;
    Ok(())
}

async fn import_postgres_stream_format_record(
    provider: &PostgresStorageProvider,
    payload: &serde_json::Value,
) -> StorageResult<()> {
    let format_key = payload_string(payload, "format_key")?;
    let format_version = payload_i64(payload, "format_version")?;
    let client = provider
        .pool
        .get()
        .await
        .map_err(PostgresStorageProvider::map_postgres_client_acquire_error)?;
    client
        .execute(
            r"INSERT INTO sys_stream_format_metadata (format_key, format_version)
              VALUES ($1, $2)
              ON CONFLICT(format_key)
              DO UPDATE SET format_version = excluded.format_version",
            &[&format_key, &format_version],
        )
        .await
        .map_err(|err| {
            PostgresStorageProvider::map_postgres_write_error("import stream format", err)
        })?;
    Ok(())
}

async fn import_postgres_user_stream_record(
    provider: &PostgresStorageProvider,
    payload: &serde_json::Value,
) -> StorageResult<()> {
    let stream_name = payload_string(payload, "stream_name")?;
    let internal_id = payload_string(payload, "internal_id")?;
    let ttl_seconds = payload_optional_i64(payload, "ttl_seconds")?;
    let created_at = payload_i64(payload, "created_at")?;
    let updated_at = payload_i64(payload, "updated_at")?;
    let client = provider
        .pool
        .get()
        .await
        .map_err(PostgresStorageProvider::map_postgres_client_acquire_error)?;
    client
        .execute(
            r"INSERT INTO sys_user_streams (
                stream_name, internal_id, ttl_seconds, created_at, updated_at
              )
              VALUES ($1, $2, $3, $4, $5)
              ON CONFLICT(stream_name)
              DO UPDATE SET
                internal_id = excluded.internal_id,
                ttl_seconds = excluded.ttl_seconds,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at",
            &[
                &stream_name,
                &internal_id,
                &ttl_seconds,
                &created_at,
                &updated_at,
            ],
        )
        .await
        .map_err(|err| {
            PostgresStorageProvider::map_postgres_write_error("import user stream", err)
        })?;
    Ok(())
}

async fn import_postgres_stream_item_record(
    provider: &PostgresStorageProvider,
    payload: &serde_json::Value,
) -> StorageResult<()> {
    let stream_name = payload_string(payload, "stream_name")?;
    let item_id = payload_string(payload, "item_id")?;
    let data = payload
        .get("data")
        .cloned()
        .ok_or_else(|| StorageError::validation("stream item record missing data"))
        .and_then(|value| serde_json::from_value::<Vec<u8>>(value).map_err(Into::into))?;
    let created_at = payload_i64(payload, "created_at")?;
    let data_type = i32::try_from(payload_i64(payload, "data_type")?).map_err(|_| {
        StorageError::validation("stream item data_type does not fit postgres integer")
    })?;
    let client = provider
        .pool
        .get()
        .await
        .map_err(PostgresStorageProvider::map_postgres_client_acquire_error)?;
    client
        .execute(
            r"INSERT INTO sys_stream_items (stream_name, item_id, data, created_at, data_type)
              VALUES ($1, $2, $3, $4, $5)
              ON CONFLICT(stream_name, item_id)
              DO UPDATE SET
                data = excluded.data,
                created_at = excluded.created_at,
                data_type = excluded.data_type",
            &[&stream_name, &item_id, &data, &created_at, &data_type],
        )
        .await
        .map_err(|err| {
            PostgresStorageProvider::map_postgres_write_error("import stream item", err)
        })?;
    Ok(())
}

async fn import_postgres_stream_cursor_record(
    provider: &PostgresStorageProvider,
    payload: &serde_json::Value,
) -> StorageResult<()> {
    let cursor_name = payload_string(payload, "cursor_name")?;
    let stream_name = payload_string(payload, "stream_name")?;
    let position = payload_string(payload, "position")?;
    let created_at = payload_i64(payload, "created_at")?;
    let client = provider
        .pool
        .get()
        .await
        .map_err(PostgresStorageProvider::map_postgres_client_acquire_error)?;
    client
        .execute(
            r"INSERT INTO sys_stream_cursors (cursor_name, stream_name, position, created_at)
              VALUES ($1, $2, $3, $4)
              ON CONFLICT(cursor_name, stream_name)
              DO UPDATE SET
                position = excluded.position,
                created_at = excluded.created_at",
            &[&cursor_name, &stream_name, &position, &created_at],
        )
        .await
        .map_err(|err| {
            PostgresStorageProvider::map_postgres_write_error("import stream cursor", err)
        })?;
    Ok(())
}
