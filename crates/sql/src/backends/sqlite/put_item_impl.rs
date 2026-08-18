use std::{collections::HashMap, sync::LazyLock};

use storage_condition::{Condition, evaluate_condition};
use storage_provider::split_item_into_key_and_attributes_sync;
use storage_types::{
    AttributeValue, KeyAttributes, ReplicationEventMetadata, SplitDynamoItem, StorageError,
    StorageResult, StreamRetentionDuration, TableName, context::ErrorContext as _,
};

use crate::{
    SQLiteStorageProvider,
    error_handler::map_sqlite_error,
    indexed_item::SqlIndexedItem,
    stream_writer::{
        SqliteWriteStreamEntriesInput, should_write_stream_entries_for_gsi_mode,
        write_stream_entries,
    },
    utils::{SqliteConn, main_table_payload},
};

pub(crate) struct PutItemInput<'a> {
    pub(crate) table_name: &'a TableName,
    pub(crate) item: &'a HashMap<String, AttributeValue>,
    pub(crate) condition: &'a Option<Condition>,
    pub(crate) indexers: Option<&'a [String]>,
    pub(crate) immediate_gsi_consistency: bool,
    pub(crate) return_old_on_condition_failure: bool,
    pub(crate) replication: Option<&'a ReplicationEventMetadata>,
    pub(crate) item_stream_ttl_hours: Option<StreamRetentionDuration>,
}

pub(crate) fn condition_item_ref(
    old_item: Option<&HashMap<String, AttributeValue>>,
) -> &HashMap<String, AttributeValue> {
    static EMPTY_ITEM: LazyLock<HashMap<String, AttributeValue>> = LazyLock::new(HashMap::new);
    old_item.unwrap_or(&EMPTY_ITEM)
}

impl SQLiteStorageProvider {
    pub(crate) fn do_put_item(
        sqlite: &SqliteConn<'_>,
        input: PutItemInput<'_>,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        let PutItemInput {
            table_name,
            item,
            condition,
            indexers,
            immediate_gsi_consistency,
            return_old_on_condition_failure,
            replication,
            item_stream_ttl_hours,
        } = input;
        let table_name_safe = table_name.sanitized_name();
        let table_info = Self::do_get_table_info(table_name, sqlite)?;

        if item.is_empty() {
            return Err(StorageError::validation("Item is empty"));
        }

        let SplitDynamoItem {
            key_attributes,
            non_key_attributes,
            all_attributes,
        } = split_item_into_key_and_attributes_sync(item.clone(), &table_info)?;

        let (old_item, old_indexers) =
            Self::do_get_item_with_indexers(table_name, &key_attributes, sqlite)?.map_or_else(
                || (None, Vec::new()),
                |(item, indexers)| (Some(item), indexers),
            );

        if let Some(condition) = condition
            && !evaluate_condition(condition_item_ref(old_item.as_ref()), condition)
        {
            return Err(crate::provider_core::write::conditional_failure(
                old_item.as_ref(),
                return_old_on_condition_failure,
            ));
        }

        let payload = main_table_payload(&key_attributes, &non_key_attributes);
        let indexed = SqlIndexedItem::extract(
            &all_attributes,
            payload.as_ref(),
            indexers,
            table_info.max_indexers,
        )?;
        let rows_affected = execute_put_item(
            sqlite,
            &table_name_safe,
            &key_attributes,
            &indexed,
            table_info.max_indexers,
        )?;
        let item_stream_version = storage_types::ItemStreamVersion::try_from(
            Self::do_bump_item_revision(table_name, &key_attributes, sqlite)?,
        )?;

        tracing::Span::current().record("rows_affected", rows_affected);

        if should_write_stream_entries_for_gsi_mode(&table_info, immediate_gsi_consistency) {
            write_stream_entries(
                sqlite,
                &table_info,
                &all_attributes,
                SqliteWriteStreamEntriesInput {
                    old_item: old_item.as_ref(),
                    indexers: indexers.unwrap_or_default(),
                    old_indexers: old_item.as_ref().map(|_| old_indexers.as_slice()),
                    is_deleted: false,
                    item_stream_version,
                    replication,
                },
            )
            .context("write stream entries")?;
        }

        if immediate_gsi_consistency {
            SQLiteStorageProvider::apply_immediate_gsi_updates(
                sqlite,
                &table_info,
                old_item.as_ref(),
                Some(&all_attributes),
                indexers.unwrap_or_default(),
                item_stream_version,
            )?;
        }

        let ttl_config = SQLiteStorageProvider::load_ttl_config_txn(sqlite, table_name)?;
        SQLiteStorageProvider::update_ttl_index_entries(
            sqlite,
            &table_info,
            ttl_config.as_ref(),
            old_item.as_ref(),
            Some(&all_attributes),
        )?;
        SQLiteStorageProvider::apply_item_stream_duration_tx(
            sqlite,
            &table_info,
            &key_attributes,
            item_stream_ttl_hours,
        )?;

        Ok(old_item)
    }
}

pub(crate) fn execute_put_item(
    sqlite: &SqliteConn<'_>,
    table_name_safe: &str,
    key_attributes: &KeyAttributes,
    indexed: &SqlIndexedItem,
    capacity: storage_types::MaxIndexers,
) -> StorageResult<usize> {
    let mut columns = Vec::with_capacity(key_attributes.len() + 1 + capacity.as_usize());
    let mut values = Vec::with_capacity(columns.capacity());

    for (attr_name, attr_value) in key_attributes.iter() {
        columns.push(attr_name.to_string());
        values.push(rusqlite::types::Value::Text(
            attr_value.inner_string().map_err(|err| {
                StorageError::validation(format!("key attribute must be scalar: {err}"))
            })?,
        ));
    }

    columns.push("attributes_blob".to_string());
    values.push(rusqlite::types::Value::Text(
        indexed.residual_json().to_owned(),
    ));
    for ordinal in 0..capacity.as_usize() {
        columns.push(crate::utils::indexer_column_name(ordinal));
        values.push(
            match indexed.slots().get(ordinal).and_then(Option::as_ref) {
                Some(value) => rusqlite::types::Value::Text(value.clone()),
                None => rusqlite::types::Value::Null,
            },
        );
    }

    let placeholders: String = (1..=values.len())
        .map(|i| format!("?{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let columns_str = columns.join(", ");
    let sql = format!(
        "INSERT OR REPLACE INTO \"table_{table_name_safe}\" ({columns_str}) VALUES \
         ({placeholders})"
    );

    sqlite
        .execute(&sql, rusqlite::params_from_iter(values.iter()))
        .map_err(map_sqlite_error)
        .context("put item transaction insert execute")
}
