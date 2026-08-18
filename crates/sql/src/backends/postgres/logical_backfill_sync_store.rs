use std::collections::HashMap;

use storage_types::{AttributeValue, KeyAttributes, StorageError, StorageResult};
use tokio_postgres::types::ToSql;

use super::{
    PostgresStorageProvider, logical_backfill_values::log_component_i64, physical_names,
    sql_statements,
};

pub(super) async fn ensure_sync_apply_markers_table<C>(
    _provider: &PostgresStorageProvider,
    client: &C,
) -> StorageResult<()>
where
    C: deadpool_postgres::GenericClient + Sync,
{
    client
        .execute(
            r"CREATE TABLE IF NOT EXISTS sys_sync_apply_markers (
                marker_key TEXT PRIMARY KEY,
                term BIGINT NOT NULL,
                log_index BIGINT NOT NULL,
                applied_at BIGINT NOT NULL,
                leader_node_id TEXT NOT NULL
            )",
            &[],
        )
        .await
        .map_err(|err| {
            PostgresStorageProvider::map_postgres_write_error("create sync markers table", err)
        })?;
    Ok(())
}

pub(super) async fn ensure_sync_log_entries_table<C>(
    _provider: &PostgresStorageProvider,
    client: &C,
) -> StorageResult<()>
where
    C: deadpool_postgres::GenericClient + Sync,
{
    client
        .execute(
            r"CREATE TABLE IF NOT EXISTS sys_sync_log_entries (
                term BIGINT NOT NULL,
                log_index BIGINT NOT NULL,
                metadata_json TEXT NOT NULL,
                batch_json TEXT NOT NULL,
                PRIMARY KEY (term, log_index)
            )",
            &[],
        )
        .await
        .map_err(|err| {
            PostgresStorageProvider::map_postgres_write_error("create sync log table", err)
        })?;
    Ok(())
}

pub(super) async fn sync_apply_marker_exists<C>(
    provider: &PostgresStorageProvider,
    client: &C,
    mutation_id: &str,
) -> StorageResult<bool>
where
    C: deadpool_postgres::GenericClient + Sync,
{
    let marker_key = sync_mutation_marker_key(mutation_id);
    let row = client
        .query_one(
            "SELECT COUNT(*) FROM sys_sync_apply_markers WHERE marker_key = $1",
            &[&marker_key],
        )
        .await
        .map_err(|err| PostgresStorageProvider::map_postgres_error("read sync marker", err))?;
    let count = row
        .try_get::<_, i64>(0)
        .map_err(|err| PostgresStorageProvider::map_postgres_error("decode sync marker", err))?;
    let _ = provider;
    Ok(count > 0)
}

pub(super) async fn insert_sync_apply_marker<C>(
    _provider: &PostgresStorageProvider,
    client: &C,
    marker: &str,
    log_id: storage_sync::SyncLogId,
    metadata: &storage_sync::SyncCommitMetadata,
) -> StorageResult<()>
where
    C: deadpool_postgres::GenericClient + Sync,
{
    let marker_key = if marker == "__last_applied__" {
        marker.to_string()
    } else {
        sync_mutation_marker_key(marker)
    };
    client
        .execute(
            r"INSERT INTO sys_sync_apply_markers (
                marker_key, term, log_index, applied_at, leader_node_id
            )
              VALUES ($1, $2, $3, $4, $5)
              ON CONFLICT(marker_key)
              DO UPDATE SET
                term = excluded.term,
                log_index = excluded.log_index,
                applied_at = excluded.applied_at,
                leader_node_id = excluded.leader_node_id",
            &[
                &marker_key,
                &log_component_i64(log_id.term, "term")?,
                &log_component_i64(log_id.index, "index")?,
                &metadata.committed_at.timestamp_millis(),
                &metadata.leader_node_id,
            ],
        )
        .await
        .map_err(|err| {
            PostgresStorageProvider::map_postgres_write_error("write sync marker", err)
        })?;
    Ok(())
}

pub(super) async fn upsert_main_row<C>(
    _provider: &PostgresStorageProvider,
    client: &C,
    table_info: &storage_types::StoredTableInfo,
    key_attributes: &KeyAttributes,
    full_item: &HashMap<String, AttributeValue>,
    payload_item: &HashMap<String, AttributeValue>,
    indexers: &[String],
) -> StorageResult<()>
where
    C: deadpool_postgres::GenericClient + Sync,
{
    let prepared = PostgresStorageProvider::prepare_main_row_write(
        table_info,
        key_attributes,
        full_item,
        payload_item,
        Some(indexers),
    )?;
    let sql = sql_statements::upsert_main_row(
        &physical_names::physical_table_name(&table_info.table_name),
        &prepared.columns_sql,
        &prepared.values_sql,
        &prepared.conflict_target,
        &prepared.assignments,
    );
    let params = prepared
        .bind_values
        .iter()
        .map(|value| value as &(dyn ToSql + Sync))
        .collect::<Vec<_>>();
    client.execute(&sql, &params).await.map_err(|err| {
        PostgresStorageProvider::map_postgres_write_error("sync apply put row", err)
    })?;
    Ok(())
}

pub(super) async fn delete_main_row<C>(
    _provider: &PostgresStorageProvider,
    client: &C,
    table_info: &storage_types::StoredTableInfo,
    key_attributes: &KeyAttributes,
) -> StorageResult<()>
where
    C: deadpool_postgres::GenericClient + Sync,
{
    let key_bindings = PostgresStorageProvider::key_column_bindings_for_schema(
        table_info,
        &table_info.key_schema,
        key_attributes,
        None,
    )?;
    let mut bind_values = Vec::with_capacity(key_bindings.len());
    let where_sql =
        PostgresStorageProvider::where_clause_for_bindings(&key_bindings, &mut bind_values);
    let sql = sql_statements::delete_main_row(
        &physical_names::physical_table_name(&table_info.table_name),
        &where_sql,
    );
    let params = bind_values
        .iter()
        .map(|value| value as &(dyn ToSql + Sync))
        .collect::<Vec<_>>();
    client.execute(&sql, &params).await.map_err(|err| {
        PostgresStorageProvider::map_postgres_write_error("sync apply delete row", err)
    })?;
    Ok(())
}

pub(super) async fn set_item_revision<C>(
    _provider: &PostgresStorageProvider,
    client: &C,
    table_name: &storage_types::TableName,
    key: &KeyAttributes,
    version: storage_types::ItemStreamVersion,
) -> StorageResult<()>
where
    C: deadpool_postgres::GenericClient + Sync,
{
    let revision = log_component_i64(version.get(), "item stream version")?;
    let key_json = key.canonical_dynamo_json().map_err(|err| {
        StorageError::validation(format!("revision key must be Dynamo JSON encodable: {err}"))
    })?;
    client
        .execute(
            r"INSERT INTO item_revisions (table_name, key_json, revision)
              VALUES ($1, $2, $3)
              ON CONFLICT(table_name, key_json)
              DO UPDATE SET revision = excluded.revision",
            &[&table_name.to_string(), &key_json, &revision],
        )
        .await
        .map_err(|err| {
            PostgresStorageProvider::map_postgres_write_error("set item revision", err)
        })?;
    Ok(())
}

pub(super) fn old_item_map(
    value: Option<&str>,
) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
    value
        .map(serde_json::from_str::<HashMap<String, AttributeValue>>)
        .transpose()
        .map_err(Into::into)
}

pub(super) fn resolved_sync_log_entry_from_row(
    row: &tokio_postgres::Row,
) -> StorageResult<storage_sync::ResolvedSyncLogEntry> {
    let metadata_json = row
        .try_get::<_, String>(0)
        .map_err(|err| PostgresStorageProvider::map_postgres_error("decode sync metadata", err))?;
    let batch_json = row
        .try_get::<_, String>(1)
        .map_err(|err| PostgresStorageProvider::map_postgres_error("decode sync batch", err))?;
    Ok(storage_sync::ResolvedSyncLogEntry::new(
        serde_json::from_str(&metadata_json)?,
        serde_json::from_str(&batch_json)?,
    ))
}

fn sync_mutation_marker_key(mutation_id: &str) -> String {
    format!("mutation#{mutation_id}")
}
