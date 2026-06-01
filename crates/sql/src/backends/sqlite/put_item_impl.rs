use std::{collections::HashMap, sync::LazyLock};

use storage_condition::{
    Condition, condition_has_repeated_root_field, evaluate_condition,
    try_evaluate_condition_with_cached_roots, try_evaluate_condition_with_root,
};
use storage_provider::split_item_into_key_and_attributes_sync;
use storage_types::{
    AttributeValue, KeyAttributes, KeySchemaElement, ReplicationEventMetadata, SplitDynamoItem,
    StorageEnum, StorageError, StorageResult, TableName, WireItem, context::ErrorContext as _,
};

use crate::{
    SQLiteStorageProvider,
    error_handler::map_sqlite_error,
    stream_writer::{
        WriteWireStreamEntriesInput, should_write_stream_entries_for_gsi_mode,
        write_stream_entries, write_stream_entries_wire,
    },
    utils::{SqliteConn, main_table_attributes_blob},
};

pub(crate) fn condition_item_ref(
    old_item: Option<&HashMap<String, AttributeValue>>,
) -> &HashMap<String, AttributeValue> {
    static EMPTY_ITEM: LazyLock<HashMap<String, AttributeValue>> = LazyLock::new(HashMap::new);
    old_item.unwrap_or(&EMPTY_ITEM)
}

fn evaluate_wire_condition(
    old_item: Option<&WireItem>,
    condition: &Condition,
) -> StorageResult<bool> {
    if condition_has_repeated_root_field(condition) {
        return evaluate_wire_condition_cached(old_item, condition);
    }
    let mut root_value = |field: &str| match old_item {
        Some(item) => item.attribute_value(field),
        None => Ok(None),
    };
    try_evaluate_condition_with_root(condition, &mut root_value)
}

fn evaluate_wire_condition_cached(
    old_item: Option<&WireItem>,
    condition: &Condition,
) -> StorageResult<bool> {
    let mut root_value = |field: &str| {
        Ok(match old_item {
            Some(item) => item.attribute_value(field)?,
            None => None,
        })
    };
    try_evaluate_condition_with_cached_roots(condition, &mut root_value)
}

impl SQLiteStorageProvider {
    pub fn do_put_wire_item(
        table_name: &TableName,
        item: &WireItem,
        condition: &Option<Condition>,
        sqlite: &SqliteConn<'_>,
        immediate_gsi_consistency: bool,
        should_return_old: bool,
        replication: Option<&ReplicationEventMetadata>,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        let table_name_safe = table_name.sanitized_name();
        let table_info = Self::do_get_table_info(table_name, sqlite)?;
        let key_attributes = extract_wire_item_key_attributes(item, &table_info.key_schema)?;

        let ttl_config = SQLiteStorageProvider::load_ttl_config_txn(sqlite, table_name)?;
        let should_write_stream =
            should_write_stream_entries_for_gsi_mode(&table_info, immediate_gsi_consistency);
        let should_track_ttl = ttl_tracking_enabled(ttl_config.as_ref());
        let needs_old_item =
            should_return_old || condition.is_some() || should_write_stream || should_track_ttl;

        let old_item = if needs_old_item {
            Self::do_get_wire_item(table_name, &key_attributes, sqlite)?
        } else {
            None
        };

        if let Some(condition) = condition
            && !evaluate_wire_condition(old_item.as_ref(), condition)?
        {
            return Err(StorageEnum::ConditionalCheckFailed.into());
        }

        let attributes_blob = wire_item_attributes_blob(item)?;
        execute_put_item_with_blob(sqlite, &table_name_safe, &key_attributes, &attributes_blob)?;
        let item_stream_version = storage_types::ItemStreamVersion::try_from(
            Self::do_bump_item_revision(table_name, &key_attributes, sqlite)?,
        )?;

        if should_write_stream {
            write_stream_entries_wire(
                sqlite,
                &table_info,
                item,
                WriteWireStreamEntriesInput {
                    old_item: old_item.as_ref(),
                    is_deleted: false,
                    item_stream_version,
                    replication,
                },
            )
            .context("write stream entries")?;
        }
        if immediate_gsi_consistency {
            let old_item_map = old_item
                .as_ref()
                .map(WireItem::to_attribute_map)
                .transpose()?;
            let new_item_map = item.to_attribute_map()?;
            SQLiteStorageProvider::apply_immediate_gsi_updates(
                sqlite,
                &table_info,
                old_item_map.as_ref(),
                Some(&new_item_map),
                item_stream_version,
            )?;
        }
        if should_track_ttl {
            SQLiteStorageProvider::update_ttl_index_entries_wire(
                sqlite,
                &table_info,
                ttl_config.as_ref(),
                old_item.as_ref(),
                Some(item),
            )?;
        }

        if should_return_old {
            old_item.map(WireItem::into_attribute_map).transpose()
        } else {
            Ok(None)
        }
    }

    pub fn do_put_item(
        table_name: &TableName,
        item: &HashMap<String, AttributeValue>,
        condition: &Option<Condition>,
        sqlite: &SqliteConn<'_>,
        immediate_gsi_consistency: bool,
        replication: Option<&ReplicationEventMetadata>,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
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

        let old_item = Self::do_get_item(table_name, &key_attributes, sqlite)?;

        if let Some(condition) = condition
            && !evaluate_condition(condition_item_ref(old_item.as_ref()), condition)
        {
            return Err(StorageEnum::ConditionalCheckFailed.into());
        }

        // Build the SQL statement for main table
        let mut columns = Vec::new();
        let mut values = Vec::new();

        // Add key attributes as individual columns
        for (attr_name, attr_value) in key_attributes.iter() {
            columns.push(attr_name.to_string());
            values.push(attr_value.inner_string().map_err(|err| {
                StorageError::validation(format!("key attribute must be scalar: {err}"))
            })?);
        }

        // Add non-key attributes as JSON blob
        columns.push("attributes_blob".to_string());
        let blob_json = main_table_attributes_blob(&key_attributes, &non_key_attributes)?;
        values.push(blob_json.clone());

        let placeholders: String = (1..=values.len())
            .map(|i| format!("?{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let columns_str = columns.join(", ");

        let sql = format!(
            "INSERT OR REPLACE INTO \"table_{table_name_safe}\" ({columns_str}) VALUES \
             ({placeholders})"
        );

        let rows_affected = sqlite
            .execute(&sql, rusqlite::params_from_iter(values.iter()))
            .map_err(map_sqlite_error)
            .context("put item transaction insert execute")?;
        let item_stream_version = storage_types::ItemStreamVersion::try_from(
            Self::do_bump_item_revision(table_name, &key_attributes, sqlite)?,
        )?;

        tracing::Span::current().record("rows_affected", rows_affected);

        if should_write_stream_entries_for_gsi_mode(&table_info, immediate_gsi_consistency) {
            write_stream_entries(
                sqlite,
                &table_info,
                &all_attributes,
                old_item.as_ref(),
                false,
                item_stream_version,
                replication,
            )
            .context("write stream entries")?;
        }

        if immediate_gsi_consistency {
            SQLiteStorageProvider::apply_immediate_gsi_updates(
                sqlite,
                &table_info,
                old_item.as_ref(),
                Some(&all_attributes),
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

        Ok(old_item)
    }
}

fn extract_wire_item_key_attributes(
    item: &WireItem,
    key_schema: &[KeySchemaElement],
) -> StorageResult<KeyAttributes> {
    let mut key_attributes = KeyAttributes::with_capacity(key_schema.len());
    for key in key_schema {
        let attr = item
            .attribute_value(&key.attribute_name)?
            .ok_or_else(StorageError::invalid_or_missing_key)?;
        key_attributes.insert(key.attribute_name.clone(), attr);
    }
    Ok(key_attributes)
}

fn wire_item_attributes_blob(item: &WireItem) -> StorageResult<String> {
    let bytes = match item {
        WireItem::DynamoJson { data } => data.clone(),
        WireItem::LocalSplit {
            non_key_attributes_blob,
            ..
        } => non_key_attributes_blob
            .clone()
            .unwrap_or_else(|| b"{}".to_vec()),
    };
    String::from_utf8(bytes).map_err(|_| StorageError::internal("wire item payload is not utf-8"))
}

fn execute_put_item_with_blob(
    sqlite: &SqliteConn<'_>,
    table_name_safe: &str,
    key_attributes: &KeyAttributes,
    attributes_blob: &str,
) -> StorageResult<()> {
    let mut columns = Vec::with_capacity(key_attributes.len() + 1);
    let mut values = Vec::with_capacity(key_attributes.len() + 1);

    for (attr_name, attr_value) in key_attributes.iter() {
        columns.push(attr_name.to_string());
        values.push(attr_value.inner_string().map_err(|err| {
            StorageError::validation(format!("key attribute must be scalar: {err}"))
        })?);
    }

    columns.push("attributes_blob".to_string());
    values.push(attributes_blob.to_string());

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
        .context("put item transaction insert execute")?;
    Ok(())
}

fn ttl_tracking_enabled(config: Option<&storage_common::ttl::TtlConfigRecord>) -> bool {
    config.is_some_and(|config| {
        matches!(
            config.status,
            storage_types::TimeToLiveStatus::Enabled | storage_types::TimeToLiveStatus::Enabling
        )
    })
}
