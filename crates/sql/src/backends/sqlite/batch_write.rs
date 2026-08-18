use std::collections::HashMap;

use storage_common::ttl::TtlConfigRecord;
use storage_types::{
    KeyAttributes, PreparedBatchOperation, StorageError, TableName, TimeToLiveStatus,
    context::ErrorContext as _,
};

use crate::{
    SQLiteStorageProvider,
    error_handler::map_sqlite_error,
    indexed_item::SqlIndexedItem,
    stream_writer::{SqliteWriteStreamEntriesInput, write_stream_entries},
    utils::{SqliteConn, main_table_payload},
};

pub(crate) fn execute_prepared_batch_operation(
    sqlite: &SqliteConn<'_>,
    prepared_op: &PreparedBatchOperation,
    state: &mut BatchWriteTxnState,
    should_write_to_stream: bool,
) -> Result<(), StorageError> {
    match prepared_op {
        PreparedBatchOperation::Put {
            table_name,
            table_info,

            key_attributes,
            non_key_attributes,
            full_item,
            indexers,
            aux_item_stream_ttl_hours,
            ..
        } => {
            let ttl_config = state.ttl_config(sqlite, table_name)?;
            let existing = if should_write_to_stream || ttl_tracking_enabled(ttl_config.as_ref()) {
                SQLiteStorageProvider::do_get_item_with_indexers(
                    table_name,
                    key_attributes,
                    sqlite,
                )?
            } else {
                None
            };
            let (existing_item, existing_indexers) = existing.map_or_else(
                || (None, Vec::new()),
                |(item, indexers)| (Some(item), indexers),
            );

            let payload = main_table_payload(key_attributes, non_key_attributes);
            let indexed = SqlIndexedItem::extract(
                full_item,
                payload.as_ref(),
                indexers.as_deref(),
                table_info.max_indexers,
            )?;
            super::put_item_impl::execute_put_item(
                sqlite,
                &table_name.sanitized_name(),
                key_attributes,
                &indexed,
                table_info.max_indexers,
            )?;
            let item_stream_version = storage_types::ItemStreamVersion::try_from(
                SQLiteStorageProvider::do_bump_item_revision(table_name, key_attributes, sqlite)?,
            )?;

            if should_write_to_stream {
                write_stream_entries(
                    sqlite,
                    table_info,
                    full_item,
                    SqliteWriteStreamEntriesInput {
                        old_item: existing_item.as_ref(),
                        indexers: indexers.as_deref().unwrap_or_default(),
                        old_indexers: existing_item.as_ref().map(|_| existing_indexers.as_slice()),
                        is_deleted: false,
                        item_stream_version,
                        replication: None,
                    },
                )?;
            }
            SQLiteStorageProvider::update_ttl_index_entries(
                sqlite,
                table_info,
                ttl_config.as_ref(),
                existing_item.as_ref(),
                Some(full_item),
            )?;
            SQLiteStorageProvider::apply_item_stream_duration_tx(
                sqlite,
                table_info,
                key_attributes,
                *aux_item_stream_ttl_hours,
            )?;
        }
        PreparedBatchOperation::Delete {
            table_name,
            table_info,
            key,
            existing_item,
            aux_item_stream_ttl_hours,
            ..
        } => {
            let ttl_config = state.ttl_config(sqlite, table_name)?;
            let mut existing_item_from_db = None;
            let mut existing_indexers = Vec::new();
            if (should_write_to_stream || ttl_tracking_enabled(ttl_config.as_ref()))
                && let Some((item, indexers)) =
                    SQLiteStorageProvider::do_get_item_with_indexers(table_name, key, sqlite)?
            {
                existing_item_from_db = Some(item);
                existing_indexers = indexers;
            }
            let existing_item_ref = existing_item.as_ref().or(existing_item_from_db.as_ref());

            execute_delete_item_sql(sqlite, table_name, key)?;

            if should_write_to_stream && let Some(item) = existing_item_ref {
                let item_stream_version = storage_types::ItemStreamVersion::try_from(
                    SQLiteStorageProvider::do_bump_item_revision(table_name, key, sqlite)?,
                )?;
                write_stream_entries(
                    sqlite,
                    table_info,
                    item,
                    SqliteWriteStreamEntriesInput {
                        old_item: Some(item),
                        indexers: &[],
                        old_indexers: Some(&existing_indexers),
                        is_deleted: true,
                        item_stream_version,
                        replication: None,
                    },
                )?;
            }

            SQLiteStorageProvider::update_ttl_index_entries(
                sqlite,
                table_info,
                ttl_config.as_ref(),
                existing_item_ref,
                None,
            )?;
            SQLiteStorageProvider::apply_item_stream_duration_tx(
                sqlite,
                table_info,
                key,
                *aux_item_stream_ttl_hours,
            )?;
        }
    }
    Ok(())
}

#[derive(Default)]
pub(crate) struct BatchWriteTxnState {
    ttl_configs: HashMap<TableName, Option<TtlConfigRecord>>,
}

impl BatchWriteTxnState {
    fn ttl_config(
        &mut self,
        sqlite: &SqliteConn<'_>,
        table_name: &TableName,
    ) -> Result<Option<TtlConfigRecord>, StorageError> {
        if !self.ttl_configs.contains_key(table_name) {
            self.ttl_configs.insert(
                table_name.clone(),
                SQLiteStorageProvider::load_ttl_config_txn(sqlite, table_name)?,
            );
        }
        self.ttl_configs.get(table_name).cloned().ok_or_else(|| {
            StorageError::internal(&format!("batch write TTL config missing for {table_name}"))
        })
    }
}

fn ttl_tracking_enabled(config: Option<&storage_common::ttl::TtlConfigRecord>) -> bool {
    config.is_some_and(|config| {
        matches!(
            config.status,
            TimeToLiveStatus::Enabled | TimeToLiveStatus::Enabling
        )
    })
}

pub(crate) fn execute_delete_item_sql(
    sqlite: &SqliteConn<'_>,
    table_name: &TableName,
    key: &KeyAttributes,
) -> Result<(), StorageError> {
    let conditions: Vec<String> = key
        .iter()
        .enumerate()
        .map(|(i, (name, _))| format!("{name} = ?{}", i + 1))
        .collect();
    let conditions_str = conditions.join(" AND ");
    let sql = format!(
        "DELETE FROM \"table_{}\" WHERE {conditions_str}",
        table_name.sanitized_name()
    );
    let values: Vec<String> = key
        .iter()
        .map(|(_, value)| value)
        .map(|value| {
            value.inner_string().map_err(|err| {
                StorageError::validation(format!("key attribute must be scalar: {err}"))
            })
        })
        .collect::<Result<Vec<_>, StorageError>>()?;

    sqlite
        .execute(&sql, rusqlite::params_from_iter(values.iter()))
        .map_err(map_sqlite_error)
        .context("Delete item execute")?;

    Ok(())
}
