use rusqlite::OptionalExtension as _;
use storage_backfill::{LogicalBackfillDomain, LogicalBackfillRecord};
use storage_types::{StorageError, StorageResult, TableName};

use super::{
    SQLiteStorageProvider,
    logical_backfill::{payload_i64, payload_optional_string, payload_string},
};
use crate::{
    error_handler::map_sqlite_error,
    utils::{SqliteConn, SqliteTableRowidMode, build_gsi_creation_sqls},
};

pub(super) fn append_gsi_backfill_records(
    conn: &rusqlite::Connection,
    records: &mut Vec<LogicalBackfillRecord>,
    table_filter: Option<&str>,
    limit: i64,
) -> StorageResult<()> {
    if let Some(table_name) = table_filter {
        let mut stmt = conn
            .prepare(
                "SELECT table_name, index_name, status, scan_lek, captured_stream_tail, \
                 created_at, updated_at FROM gsi_backfill WHERE table_name = ?1 ORDER BY \
                 table_name, index_name LIMIT ?2",
            )
            .map_err(map_sqlite_error)?;
        let rows = stmt
            .query_map((table_name, limit), gsi_backfill_record_from_row)
            .map_err(map_sqlite_error)?;
        for row in rows {
            records.push(row.map_err(map_sqlite_error)?);
        }
    } else {
        let mut stmt = conn
            .prepare(
                "SELECT table_name, index_name, status, scan_lek, captured_stream_tail, \
                 created_at, updated_at FROM gsi_backfill ORDER BY table_name, index_name LIMIT ?1",
            )
            .map_err(map_sqlite_error)?;
        let rows = stmt
            .query_map([limit], gsi_backfill_record_from_row)
            .map_err(map_sqlite_error)?;
        for row in rows {
            records.push(row.map_err(map_sqlite_error)?);
        }
    }
    Ok(())
}

pub(super) fn append_physical_gsi_records(
    conn: &rusqlite::Connection,
    records: &mut Vec<LogicalBackfillRecord>,
    table_filter: Option<&str>,
    limit: i64,
) -> StorageResult<()> {
    let table_infos = load_gsi_table_infos(conn, table_filter)?;
    for table_info in table_infos {
        let Some(gsis) = table_info.global_secondary_indexes.as_ref() else {
            continue;
        };
        for gsi in gsis {
            let physical_name =
                crate::naming::physical_gsi_table_name(&table_info.table_name, &gsi.index_name);
            let sql = format!(
                "SELECT * FROM \"{}\" LIMIT ?1",
                quote_identifier_part(&physical_name)
            );
            let mut stmt = conn.prepare(&sql).map_err(map_sqlite_error)?;
            let column_names = stmt
                .column_names()
                .into_iter()
                .map(str::to_string)
                .collect::<Vec<_>>();
            let rows = stmt
                .query_map([limit], |row| {
                    physical_gsi_record_from_row(
                        &table_info.table_name,
                        &gsi.index_name,
                        &column_names,
                        row,
                    )
                })
                .map_err(map_sqlite_error)?;
            for row in rows {
                records.push(row.map_err(map_sqlite_error)?);
            }
        }
    }
    Ok(())
}

fn load_gsi_table_infos(
    conn: &rusqlite::Connection,
    table_filter: Option<&str>,
) -> StorageResult<Vec<storage_types::StoredTableInfo>> {
    if let Some(table_name) = table_filter {
        let mut stmt = conn
            .prepare(
                "SELECT id, table_name, table_status, created_at, attribute_definitions, \
                 key_schema, max_indexers, global_secondary_indexes, table_size_bytes, \
                 item_count, stream_specification, deletion_protection_enabled, \
                 table_stream_duration_hours, default_item_stream_duration_hours FROM tables \
                 WHERE table_name = ?1 AND global_secondary_indexes IS NOT NULL ORDER BY \
                 table_name",
            )
            .map_err(map_sqlite_error)?;
        let rows = stmt
            .query_map([table_name], crate::utils::sql_row_to_stored_stable_info)
            .map_err(map_sqlite_error)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(map_sqlite_error)
    } else {
        let mut stmt = conn
            .prepare(
                "SELECT id, table_name, table_status, created_at, attribute_definitions, \
                 key_schema, max_indexers, global_secondary_indexes, table_size_bytes, \
                 item_count, stream_specification, deletion_protection_enabled, \
                 table_stream_duration_hours, default_item_stream_duration_hours FROM tables \
                 WHERE global_secondary_indexes IS NOT NULL ORDER BY table_name",
            )
            .map_err(map_sqlite_error)?;
        let rows = stmt
            .query_map([], crate::utils::sql_row_to_stored_stable_info)
            .map_err(map_sqlite_error)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(map_sqlite_error)
    }
}

fn gsi_backfill_record_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<LogicalBackfillRecord> {
    let table_name: String = row.get(0)?;
    let index_name: String = row.get(1)?;
    let status: String = row.get(2)?;
    let scan_lek: Option<String> = row.get(3)?;
    let captured_stream_tail: Option<String> = row.get(4)?;
    let created_at: i64 = row.get(5)?;
    let updated_at: i64 = row.get(6)?;
    let payload_json = serde_json::json!({
        "gsi_record_type": "backfill_state",
        "table_name": table_name,
        "index_name": index_name,
        "status": status,
        "scan_lek": scan_lek,
        "captured_stream_tail": captured_stream_tail,
        "created_at": created_at,
        "updated_at": updated_at,
    })
    .to_string();
    Ok(LogicalBackfillRecord::DomainRecord {
        domain: LogicalBackfillDomain::GsiRecords,
        record_key_json: serde_json::json!({
            "gsi_record_type": "backfill_state",
            "table_name": table_name,
            "index_name": index_name,
        })
        .to_string(),
        payload_json,
    })
}

fn physical_gsi_record_from_row(
    table_name: &TableName,
    index_name: &storage_types::IndexName,
    column_names: &[String],
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<LogicalBackfillRecord> {
    let mut columns = serde_json::Map::new();
    for (index, name) in column_names.iter().enumerate() {
        columns.insert(name.clone(), sqlite_value_to_json(row.get_ref(index)?));
    }
    let payload_json = serde_json::json!({
        "gsi_record_type": "physical_row",
        "table_name": table_name.as_ref(),
        "index_name": index_name.as_ref(),
        "columns": columns,
    })
    .to_string();
    Ok(LogicalBackfillRecord::DomainRecord {
        domain: LogicalBackfillDomain::GsiRecords,
        record_key_json: serde_json::json!({
            "gsi_record_type": "physical_row",
            "table_name": table_name.as_ref(),
            "index_name": index_name.as_ref(),
            "columns": columns,
        })
        .to_string(),
        payload_json,
    })
}

pub(super) fn import_gsi_backfill_record(
    payload: &serde_json::Value,
    sqlite: &SqliteConn<'_>,
) -> StorageResult<()> {
    let table_name = payload_string(payload, "table_name")?;
    let index_name = payload_string(payload, "index_name")?;
    let status = payload_string(payload, "status")?;
    let scan_lek = payload_optional_string(payload, "scan_lek")?;
    let captured_stream_tail = payload_optional_string(payload, "captured_stream_tail")?;
    let created_at = payload_i64(payload, "created_at")?;
    let updated_at = payload_i64(payload, "updated_at")?;
    sqlite
        .execute(
            r"INSERT INTO gsi_backfill (
                table_name, index_name, status, scan_lek, captured_stream_tail, created_at,
                updated_at
              )
              VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
              ON CONFLICT(table_name, index_name)
              DO UPDATE SET
                status = excluded.status,
                scan_lek = excluded.scan_lek,
                captured_stream_tail = excluded.captured_stream_tail,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at",
            (
                table_name.as_str(),
                index_name.as_str(),
                status.as_str(),
                scan_lek.as_deref(),
                captured_stream_tail.as_deref(),
                created_at,
                updated_at,
            ),
        )
        .map_err(map_sqlite_error)?;
    Ok(())
}

pub(super) fn import_physical_gsi_record(
    payload: &serde_json::Value,
    sqlite: &SqliteConn<'_>,
) -> StorageResult<()> {
    let table_name = TableName::new(&payload_string(payload, "table_name")?);
    let index_name = storage_types::IndexName::new(&payload_string(payload, "index_name")?);
    ensure_physical_gsi_table(sqlite, &table_name, &index_name)?;
    let columns = payload
        .get("columns")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| StorageError::validation("gsi physical row missing columns"))?;
    let mut column_names = columns.keys().cloned().collect::<Vec<_>>();
    column_names.sort();
    let values = column_names
        .iter()
        .map(|name| json_to_sqlite_value(&columns[name]))
        .collect::<StorageResult<Vec<_>>>()?;
    let placeholders = (1..=column_names.len())
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let quoted_columns = column_names
        .iter()
        .map(|name| format!("\"{}\"", quote_identifier_part(name)))
        .collect::<Vec<_>>()
        .join(", ");
    let physical_name = crate::naming::physical_gsi_table_name(&table_name, &index_name);
    let sql = format!(
        "INSERT OR REPLACE INTO \"{}\" ({quoted_columns}) VALUES ({placeholders})",
        quote_identifier_part(&physical_name)
    );
    sqlite
        .execute(&sql, rusqlite::params_from_iter(values.iter()))
        .map_err(map_sqlite_error)?;
    Ok(())
}

fn ensure_physical_gsi_table(
    sqlite: &SqliteConn<'_>,
    table_name: &TableName,
    index_name: &storage_types::IndexName,
) -> StorageResult<()> {
    let table_info = SQLiteStorageProvider::do_get_table_info(table_name, sqlite)?;
    let Some(gsis) = table_info.global_secondary_indexes.as_ref() else {
        return Err(StorageError::validation(format!(
            "table {} has no gsi metadata",
            table_name.as_ref()
        )));
    };
    let Some(gsi) = gsis.iter().find(|gsi| gsi.index_name == *index_name) else {
        return Err(StorageError::validation(format!(
            "table {} has no gsi {}",
            table_name.as_ref(),
            index_name.as_ref()
        )));
    };
    let physical_name = crate::naming::physical_gsi_table_name(table_name, index_name);
    let exists = sqlite
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [physical_name.as_str()],
            |_| Ok(true),
        )
        .optional()
        .map_err(map_sqlite_error)?
        .unwrap_or(false);
    if exists {
        return Ok(());
    }
    let create_sqls = build_gsi_creation_sqls(
        table_name,
        &table_info.attribute_definitions,
        &table_info.key_schema,
        std::slice::from_ref(gsi),
        table_info.max_indexers,
        SqliteTableRowidMode::WithoutRowid,
    );
    for sql in create_sqls {
        sqlite.execute(&sql, []).map_err(map_sqlite_error)?;
    }
    Ok(())
}

fn sqlite_value_to_json(value: rusqlite::types::ValueRef<'_>) -> serde_json::Value {
    match value {
        rusqlite::types::ValueRef::Null => serde_json::json!({ "type": "null" }),
        rusqlite::types::ValueRef::Integer(value) => {
            serde_json::json!({ "type": "integer", "value": value })
        }
        rusqlite::types::ValueRef::Real(value) => {
            serde_json::json!({ "type": "real", "value": value })
        }
        rusqlite::types::ValueRef::Text(value) => serde_json::json!({
            "type": "text",
            "value": String::from_utf8_lossy(value),
        }),
        rusqlite::types::ValueRef::Blob(value) => {
            serde_json::json!({ "type": "blob", "value": value })
        }
    }
}

fn json_to_sqlite_value(value: &serde_json::Value) -> StorageResult<rusqlite::types::Value> {
    let value_type = value
        .get("type")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| StorageError::validation("sqlite value missing type"))?;
    match value_type {
        "null" => Ok(rusqlite::types::Value::Null),
        "integer" => value
            .get("value")
            .and_then(serde_json::Value::as_i64)
            .map(rusqlite::types::Value::Integer)
            .ok_or_else(|| StorageError::validation("sqlite integer value missing value")),
        "real" => value
            .get("value")
            .and_then(serde_json::Value::as_f64)
            .map(rusqlite::types::Value::Real)
            .ok_or_else(|| StorageError::validation("sqlite real value missing value")),
        "text" => value
            .get("value")
            .and_then(serde_json::Value::as_str)
            .map(|value| rusqlite::types::Value::Text(value.to_string()))
            .ok_or_else(|| StorageError::validation("sqlite text value missing value")),
        "blob" => value
            .get("value")
            .cloned()
            .ok_or_else(|| StorageError::validation("sqlite blob value missing value"))
            .and_then(|value| serde_json::from_value::<Vec<u8>>(value).map_err(Into::into))
            .map(rusqlite::types::Value::Blob),
        other => Err(StorageError::validation(format!(
            "unsupported sqlite value type {other}"
        ))),
    }
}

fn quote_identifier_part(identifier: &str) -> String {
    identifier.replace('"', "\"\"")
}
