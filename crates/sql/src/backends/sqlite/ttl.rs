use std::collections::HashMap;

use rusqlite::OptionalExtension as _;
use storage_common::ttl::{
    TtlConfigRecord, parse_ttl_index_key, ttl_index_key_for_item, ttl_index_key_for_wire_item,
    ttl_index_prefix,
};
use storage_types::{
    AttributeValue, StorageError, StorageResult, StoredTableInfo, TableName, WireItem,
};

use crate::{
    SQLiteStorageProvider,
    error_handler::map_sqlite_error,
    naming, sql_statements,
    utils::{SqliteConn, call_sqlite},
};

impl SQLiteStorageProvider {
    pub(crate) async fn save_ttl_config(
        &self,
        table_name: &TableName,
        config: &TtlConfigRecord,
    ) -> StorageResult<()> {
        let blob = storage_types::storage_serde::to_bytes(config)?;
        let table = table_name.clone();
        call_sqlite(&self.connection, move |conn| {
            let (sql, params) = sql_statements::upsert_ttl_config(&table, &blob);
            conn.execute(sql, params).map_err(map_sqlite_error)
        })
        .await?;
        Ok(())
    }

    pub(crate) async fn delete_ttl_config(&self, table_name: &TableName) -> StorageResult<()> {
        let table = table_name.clone();
        call_sqlite(&self.connection, move |conn| {
            let (sql, params) = sql_statements::delete_ttl_config(&table);
            conn.execute(sql, params).map_err(map_sqlite_error)
        })
        .await?;
        Ok(())
    }

    pub(crate) async fn load_ttl_config(
        &self,
        table_name: &TableName,
    ) -> StorageResult<Option<TtlConfigRecord>> {
        let table = table_name.clone();
        let blob = call_sqlite(&self.connection, move |conn| {
            let (sql, params) = sql_statements::get_ttl_config(&table);
            conn.query_row(sql, params, |row| row.get::<_, Vec<u8>>(0))
                .optional()
                .map_err(map_sqlite_error)
        })
        .await?;
        match blob {
            Some(bytes) => Ok(Some(storage_types::storage_serde::from_bytes(&bytes)?)),
            None => Ok(None),
        }
    }

    pub(crate) async fn list_ttl_configs(
        &self,
    ) -> StorageResult<Vec<(TableName, TtlConfigRecord)>> {
        call_sqlite(&self.connection, |conn| {
            let (sql, params) = sql_statements::list_ttl_configs();
            let mut stmt = conn.prepare(sql).map_err(map_sqlite_error)?;
            let mut rows = stmt.query(params).map_err(map_sqlite_error)?;
            let mut configs = Vec::new();
            while let Some(row) = rows.next().map_err(map_sqlite_error)? {
                let table_name: String = row.get(0).map_err(map_sqlite_error)?;
                let blob: Vec<u8> = row.get(1).map_err(map_sqlite_error)?;
                match storage_types::storage_serde::from_bytes::<TtlConfigRecord>(&blob) {
                    Ok(record) => configs.push((TableName::new(&table_name), record)),
                    Err(err) => {
                        tracing::warn!(table=%table_name, error = %err, "ttl.config.decode_failed")
                    }
                }
            }
            Ok(configs)
        })
        .await
    }

    pub(crate) fn load_ttl_config_txn(
        sqlite: &SqliteConn<'_>,
        table_name: &TableName,
    ) -> StorageResult<Option<TtlConfigRecord>> {
        let (sql, params) = sql_statements::get_ttl_config(table_name);
        let blob = sqlite
            .query_row(sql, params, |row| row.get::<_, Vec<u8>>(0))
            .optional()
            .map_err(map_sqlite_error)?;
        match blob {
            Some(bytes) => Ok(Some(storage_types::storage_serde::from_bytes(&bytes)?)),
            None => Ok(None),
        }
    }

    pub(crate) async fn create_ttl_index_table(&self, table_name: &TableName) -> StorageResult<()> {
        let table = table_name.clone();
        call_sqlite(&self.connection, move |conn| {
            let ttl_table = naming::physical_ttl_index_table_name(&table);
            let sql = format!(
                "CREATE TABLE IF NOT EXISTS \"{ttl_table}\" (ttl_value INTEGER NOT NULL,key_token \
                 TEXT NOT NULL,PRIMARY KEY (ttl_value, key_token))"
            );
            conn.execute(&sql, []).map_err(map_sqlite_error)
        })
        .await?;
        Ok(())
    }

    pub(crate) async fn drop_ttl_index_table(&self, table_name: &TableName) -> StorageResult<()> {
        let table = table_name.clone();
        call_sqlite(&self.connection, move |conn| {
            let ttl_table = naming::physical_ttl_index_table_name(&table);
            let sql = format!("DROP TABLE IF EXISTS \"{ttl_table}\"");
            conn.execute(&sql, []).map_err(map_sqlite_error)
        })
        .await?;
        Ok(())
    }

    pub(crate) fn update_ttl_index_entries(
        sqlite: &SqliteConn<'_>,
        table_info: &StoredTableInfo,
        ttl_config: Option<&TtlConfigRecord>,
        old_item: Option<&HashMap<String, AttributeValue>>,
        new_item: Option<&HashMap<String, AttributeValue>>,
    ) -> StorageResult<()> {
        let Some(config) = ttl_config else {
            return Ok(());
        };
        if !matches!(
            config.status,
            storage_types::TimeToLiveStatus::Enabled | storage_types::TimeToLiveStatus::Enabling
        ) {
            return Ok(());
        }

        let table_name = &table_info.table_name;
        let old_item = old_item.filter(|item| !item.is_empty());
        let new_item = new_item.filter(|item| !item.is_empty());

        let old_key = if let Some(item) = old_item {
            ttl_index_key_for_item(table_name, table_info, &config.attribute_name, item)?
        } else {
            None
        };
        let new_key = if let Some(item) = new_item {
            ttl_index_key_for_item(table_name, table_info, &config.attribute_name, item)?
        } else {
            None
        };

        if old_key.is_some() && old_key == new_key {
            return Ok(());
        }

        let ttl_table = naming::physical_ttl_index_table_name(table_name);

        let prefix = ttl_index_prefix(table_name);

        if let Some(key) = old_key {
            let (ttl_value, key_token) = parse_ttl_index_key(&key, &prefix)
                .ok_or_else(|| StorageError::internal("ttl index key parse failed"))?;
            let sql =
                format!("DELETE FROM \"{ttl_table}\" WHERE ttl_value = ?1 AND key_token = ?2");
            sqlite
                .execute(&sql, rusqlite::params![ttl_value, key_token])
                .map_err(map_sqlite_error)?;
        }

        if let Some(key) = new_key {
            let (ttl_value, key_token) = parse_ttl_index_key(&key, &prefix)
                .ok_or_else(|| StorageError::internal("ttl index key parse failed"))?;
            let sql = format!(
                "INSERT OR REPLACE INTO \"{ttl_table}\" (ttl_value, key_token) VALUES (?1, ?2)"
            );
            sqlite
                .execute(&sql, rusqlite::params![ttl_value, key_token])
                .map_err(map_sqlite_error)?;
        }

        Ok(())
    }

    pub(crate) fn update_ttl_index_entries_wire(
        sqlite: &SqliteConn<'_>,
        table_info: &StoredTableInfo,
        ttl_config: Option<&TtlConfigRecord>,
        old_item: Option<&WireItem>,
        new_item: Option<&WireItem>,
    ) -> StorageResult<()> {
        let Some(config) = ttl_config else {
            return Ok(());
        };
        if !matches!(
            config.status,
            storage_types::TimeToLiveStatus::Enabled | storage_types::TimeToLiveStatus::Enabling
        ) {
            return Ok(());
        }

        let table_name = &table_info.table_name;
        let old_key = if let Some(item) = old_item {
            ttl_index_key_for_wire_item(table_name, table_info, &config.attribute_name, item)?
        } else {
            None
        };
        let new_key = if let Some(item) = new_item {
            ttl_index_key_for_wire_item(table_name, table_info, &config.attribute_name, item)?
        } else {
            None
        };

        if old_key.is_some() && old_key == new_key {
            return Ok(());
        }

        let ttl_table = naming::physical_ttl_index_table_name(table_name);
        let prefix = ttl_index_prefix(table_name);

        if let Some(key) = old_key {
            let (ttl_value, key_token) = parse_ttl_index_key(&key, &prefix)
                .ok_or_else(|| StorageError::internal("ttl index key parse failed"))?;
            let sql =
                format!("DELETE FROM \"{ttl_table}\" WHERE ttl_value = ?1 AND key_token = ?2");
            sqlite
                .execute(&sql, rusqlite::params![ttl_value, key_token])
                .map_err(map_sqlite_error)?;
        }

        if let Some(key) = new_key {
            let (ttl_value, key_token) = parse_ttl_index_key(&key, &prefix)
                .ok_or_else(|| StorageError::internal("ttl index key parse failed"))?;
            let sql = format!(
                "INSERT OR REPLACE INTO \"{ttl_table}\" (ttl_value, key_token) VALUES (?1, ?2)"
            );
            sqlite
                .execute(&sql, rusqlite::params![ttl_value, key_token])
                .map_err(map_sqlite_error)?;
        }

        Ok(())
    }
}
