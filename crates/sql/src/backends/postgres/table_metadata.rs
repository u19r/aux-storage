use std::sync::Arc;

use deadpool_postgres::GenericClient;
use serde::de::DeserializeOwned;
use storage_types::{
    StorageError, StorageResult, StoredTableInfo, StreamRetentionDuration, TableName, TableStatus,
    TimestampMillis,
};
use tokio_postgres::Row;

use crate::backends::postgres::{PostgresStorageProvider, sql_statements};

impl PostgresStorageProvider {
    pub(super) fn is_json_null_like(raw: &str) -> bool {
        let trimmed = raw.trim();
        trimmed.is_empty() || trimmed.eq_ignore_ascii_case("null") || trimmed == "\"null\""
    }

    pub(super) fn parse_json_required<T: DeserializeOwned>(
        raw: &str,
        field: &str,
    ) -> StorageResult<T> {
        serde_json::from_str(raw).map_err(|err| {
            StorageError::internal(&format!(
                "postgres failed to parse field {field} json: {err}"
            ))
        })
    }

    pub(super) fn parse_json_optional<T: DeserializeOwned>(
        raw: Option<String>,
        field: &str,
    ) -> StorageResult<Option<T>> {
        match raw {
            Some(value) if !Self::is_json_null_like(&value) => {
                Self::parse_json_required(&value, field).map(Some)
            }
            _ => Ok(None),
        }
    }

    pub(super) fn row_to_stored_table_info(row: &Row) -> StorageResult<StoredTableInfo> {
        let table_name: String = row
            .try_get("table_name")
            .map_err(|err| Self::map_postgres_error("row decode table_name", err))?;
        let table_status: String = row
            .try_get("table_status")
            .map_err(|err| Self::map_postgres_error("row decode table_status", err))?;
        let created_at: i64 = row
            .try_get("created_at")
            .map_err(|err| Self::map_postgres_error("row decode created_at", err))?;
        let attribute_definitions: String = row
            .try_get("attribute_definitions")
            .map_err(|err| Self::map_postgres_error("row decode attribute_definitions", err))?;
        let key_schema: String = row
            .try_get("key_schema")
            .map_err(|err| Self::map_postgres_error("row decode key_schema", err))?;
        let global_secondary_indexes: Option<String> = row
            .try_get("global_secondary_indexes")
            .map_err(|err| Self::map_postgres_error("row decode global_secondary_indexes", err))?;
        let stream_specification: Option<String> = row
            .try_get("stream_specification")
            .map_err(|err| Self::map_postgres_error("row decode stream_specification", err))?;
        let table_size_bytes: i64 = row
            .try_get("table_size_bytes")
            .map_err(|err| Self::map_postgres_error("row decode table_size_bytes", err))?;
        let item_count: i64 = row
            .try_get("item_count")
            .map_err(|err| Self::map_postgres_error("row decode item_count", err))?;
        let deletion_protection_enabled: bool =
            row.try_get("deletion_protection_enabled").map_err(|err| {
                Self::map_postgres_error("row decode deletion_protection_enabled", err)
            })?;
        let table_stream_duration_hours: i64 =
            row.try_get("table_stream_duration_hours").map_err(|err| {
                Self::map_postgres_error("row decode table_stream_duration_hours", err)
            })?;
        let default_item_stream_duration_hours: i64 = row
            .try_get("default_item_stream_duration_hours")
            .map_err(|err| {
                Self::map_postgres_error("row decode default_item_stream_duration_hours", err)
            })?;

        let attribute_definitions =
            Self::parse_json_required(&attribute_definitions, "attribute_definitions")?;
        let key_schema = Self::parse_json_required(&key_schema, "key_schema")?;
        let global_secondary_indexes =
            Self::parse_json_optional(global_secondary_indexes, "global_secondary_indexes")?;
        let stream_specification =
            Self::parse_json_optional(stream_specification, "stream_specification")?;

        Ok(StoredTableInfo {
            table_name: TableName::new(&table_name),
            table_status: TableStatus::from(table_status.as_str()),
            created_at: TimestampMillis::from(created_at),
            attribute_definitions,
            key_schema,
            global_secondary_indexes,
            table_size_bytes: u64::try_from(table_size_bytes).unwrap_or_default(),
            item_count: u64::try_from(item_count).unwrap_or_default(),
            stream_specification,
            table_stream_duration: StreamRetentionDuration::try_from(table_stream_duration_hours)
                .map_err(|err| {
                StorageError::validation(format!("invalid table stream duration metadata: {err}"))
            })?,
            default_item_stream_duration: StreamRetentionDuration::try_from(
                default_item_stream_duration_hours,
            )
            .map_err(|err| {
                StorageError::validation(format!(
                    "invalid default item stream duration metadata: {err}"
                ))
            })?,
            deletion_protection_enabled,
        })
    }

    pub(super) async fn invalidate_table_info_cache(&self, table_name: &TableName) {
        self.table_info_cache.write().await.remove(table_name);
    }

    pub(super) async fn get_table_info_with_client<C: GenericClient + Sync>(
        &self,
        client: &C,
        table_name: &TableName,
    ) -> StorageResult<StoredTableInfo> {
        let table_name_value = table_name.to_string();
        let row = client
            .query_opt(sql_statements::get_table_info(), &[&table_name_value])
            .await
            .map_err(|err| Self::map_postgres_error("get_table_info query", err))?;
        let Some(row) = row else {
            return Err(StorageError::table_not_found(table_name_value.as_str()));
        };
        Self::row_to_stored_table_info(&row)
    }

    pub(super) async fn get_table_info_cached_arc(
        &self,
        table_name: &TableName,
    ) -> StorageResult<Arc<StoredTableInfo>> {
        if let Some(cached) = self.table_info_cache.read().await.get(table_name).cloned() {
            return Ok(cached);
        }
        let client = self.acquire_client("get_table_info_cached").await?;
        let _connection_hold = self.connection_hold_timer("get_table_info_cached");
        let table_info = Arc::new(self.get_table_info_with_client(&client, table_name).await?);
        self.table_info_cache
            .write()
            .await
            .insert(table_name.clone(), Arc::clone(&table_info));
        Ok(table_info)
    }
}
