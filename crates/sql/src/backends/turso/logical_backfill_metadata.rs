use storage_backfill::{LogicalBackfillDomain, LogicalBackfillRecord};
use storage_types::{StorageResult, StoredTableInfo};
use turso::Value as TursoValue;

use super::{
    TursoStorageProvider, logical_backfill_values::u64_to_i64, provider::option_string_to_value,
    sql_statements,
};
use crate::utils::{SqliteTableRowidMode, build_gsi_creation_sqls, build_table_creation_sql};

pub(super) async fn import_table_metadata_record<C>(
    provider: &TursoStorageProvider,
    conn: &C,
    payload_json: &str,
) -> StorageResult<()>
where
    C: super::provider::TursoSqlConnection + ?Sized,
{
    let table_info = serde_json::from_str::<StoredTableInfo>(payload_json)?;
    let table_name = table_info.table_name.clone();
    if provider.table_exists_conn(conn, &table_name).await? {
        return Ok(());
    }
    let global_secondary_indexes_json = table_info
        .global_secondary_indexes
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    let stream_specification_json = table_info
        .stream_specification
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;

    let _ = provider
        .execute(
            conn,
            sql_statements::insert_table(),
            vec![
                TursoValue::Text(uuid::Uuid::now_v7().to_string()),
                TursoValue::Text(table_name.to_string()),
                TursoValue::Text(String::from(&table_info.table_status)),
                TursoValue::Integer(table_info.created_at.timestamp_millis()),
                TursoValue::Text(serde_json::to_string(&table_info.attribute_definitions)?),
                TursoValue::Text(serde_json::to_string(&table_info.key_schema)?),
                option_string_to_value(global_secondary_indexes_json),
                TursoValue::Integer(u64_to_i64(table_info.table_size_bytes, "table size")?),
                TursoValue::Integer(u64_to_i64(table_info.item_count, "item count")?),
                option_string_to_value(stream_specification_json),
                TursoValue::Integer(if table_info.deletion_protection_enabled {
                    1
                } else {
                    0
                }),
                TursoValue::Integer(table_info.table_stream_duration.as_hours_wire_value()),
                TursoValue::Integer(
                    table_info
                        .default_item_stream_duration
                        .as_hours_wire_value(),
                ),
            ],
        )
        .await?;

    let rowid_mode = SqliteTableRowidMode::WithRowid;
    let create_sql = build_table_creation_sql(
        &table_name,
        &table_info.attribute_definitions,
        &table_info.key_schema,
        table_info.global_secondary_indexes.as_deref(),
        rowid_mode,
    );
    let _ = provider.execute(conn, &create_sql, Vec::new()).await?;
    if let Some(gsis) = table_info.global_secondary_indexes.as_ref() {
        for sql in build_gsi_creation_sqls(
            &table_name,
            &table_info.attribute_definitions,
            &table_info.key_schema,
            gsis,
            rowid_mode,
        ) {
            let _ = provider.execute(conn, &sql, Vec::new()).await?;
        }
    }
    Ok(())
}

pub(super) fn table_metadata_record(
    table_info: StoredTableInfo,
) -> StorageResult<LogicalBackfillRecord> {
    Ok(LogicalBackfillRecord::DomainRecord {
        domain: LogicalBackfillDomain::TableMetadata,
        record_key_json: serde_json::json!({
            "table_name": table_info.table_name,
        })
        .to_string(),
        payload_json: serde_json::to_string(&table_info)?,
    })
}
