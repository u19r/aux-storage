use std::collections::HashMap;

use storage_condition::{Condition, evaluate_condition};
use storage_types::{
    AttributeValue, KeyAttributes, ReplicationEventMetadata, StorageError,
    StorageResult, StreamRetentionDuration, TableName,
};

use crate::{
    SQLiteStorageProvider,
    error_handler::map_sqlite_error,
    stream_writer::{should_write_stream_entries_for_gsi_mode, write_stream_entries},
};

impl SQLiteStorageProvider {
    pub fn do_delete_item(
        table_name: &TableName,
        key: &KeyAttributes,
        condition: &Option<Condition>,
        sqlite: &crate::utils::SqliteConn<'_>,
        immediate_gsi_consistency: bool,
        return_old_on_condition_failure: bool,
        replication: Option<&ReplicationEventMetadata>,
        item_stream_ttl_hours: Option<StreamRetentionDuration>,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        let table_name_safe = table_name.sanitized_name();
        let key_item = key_item_map(key);

        if key.is_empty() {
            return Err(StorageError::validation(
                "Delete request must specify a key",
            ));
        }

        // First, get the item to return it if it exists
        let existing_item = Self::do_get_item(table_name, key, sqlite)?;
        let table_info = Self::do_get_table_info(table_name, sqlite)?;
        let should_write_stream =
            should_write_stream_entries_for_gsi_mode(&table_info, immediate_gsi_consistency);

        let item = match &existing_item {
            Some(item) => item.clone(),
            None => {
                if condition.is_none() && replication.is_some() && should_write_stream {
                    let item_stream_version = storage_types::ItemStreamVersion::try_from(
                        Self::do_bump_item_revision(table_name, key, sqlite)?,
                    )?;
                    write_stream_entries(
                        sqlite,
                        &table_info,
                        &key_item,
                        None,
                        true,
                        item_stream_version,
                        replication,
                    )?;
                    return Ok(None);
                }
                return if condition.is_some() {
                    Ok(Some(HashMap::new()))
                } else {
                    Ok(None)
                };
            }
        };

        if let Some(condition) = condition
            && !evaluate_condition(&item, condition)
        {
            return Err(crate::provider_core::write::conditional_failure(
                existing_item.as_ref(),
                return_old_on_condition_failure,
            ));
        }

        // Delete from main table
        let conditions: Vec<String> = key
            .iter()
            .enumerate()
            .map(|(i, (name, _))| format!("{name} = ?{}", i + 1))
            .collect();
        let conditions_str = conditions.join(" AND ");
        let sql = format!("DELETE FROM \"table_{table_name_safe}\" WHERE {conditions_str}");

        let values = key_values_borrowed(key)?;

        let item_for_stream = item.clone();

        // Delete from main table
        sqlite
            .execute(&sql, rusqlite::params_from_iter(values))
            .map_err(map_sqlite_error)?;
        let item_stream_version = storage_types::ItemStreamVersion::try_from(
            Self::do_bump_item_revision(table_name, key, sqlite)?,
        )?;

        if should_write_stream {
            write_stream_entries(
                sqlite,
                &table_info,
                &key_item,
                Some(&item_for_stream),
                true,
                item_stream_version,
                replication,
            )?;
        }

        if immediate_gsi_consistency {
            SQLiteStorageProvider::apply_immediate_gsi_updates(
                sqlite,
                &table_info,
                existing_item.as_ref(),
                None,
                item_stream_version,
            )?;
        }

        let ttl_config = SQLiteStorageProvider::load_ttl_config_txn(sqlite, table_name)?;
        SQLiteStorageProvider::update_ttl_index_entries(
            sqlite,
            &table_info,
            ttl_config.as_ref(),
            existing_item.as_ref(),
            None,
        )?;
        SQLiteStorageProvider::apply_item_stream_duration_tx(
            sqlite,
            &table_info,
            key,
            item_stream_ttl_hours,
        )?;

        Ok(existing_item)
    }
}

fn key_values_borrowed(key: &KeyAttributes) -> StorageResult<Vec<&str>> {
    key.iter()
        .map(|(_, value)| value)
        .map(|value| {
            value.inner_str().map_err(|err| {
                StorageError::validation(format!("key attribute must be scalar: {err}"))
            })
        })
        .collect::<StorageResult<Vec<_>>>()
}

fn key_item_map(key: &KeyAttributes) -> HashMap<String, AttributeValue> {
    key.iter()
        .map(|(name, value)| (name.to_string(), value.clone()))
        .collect()
}

#[cfg(all(test, not(feature = "turso-backend")))]
pub(crate) fn key_values_borrowed_for_tests(key: &KeyAttributes) -> StorageResult<Vec<&str>> {
    key_values_borrowed(key)
}
