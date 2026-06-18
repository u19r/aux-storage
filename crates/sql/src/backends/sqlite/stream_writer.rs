use std::collections::HashMap;

use storage_common::ttl::is_ttl_index;
use storage_types::{
    AttributeValue, ItemKey, ItemStreamVersion, ReplicationEventMetadata, StorageError,
    StoredTableInfo, StreamName, TimestampMillis, WireItem,
};
use stream_provider::{EmbeddedStreamItem, StoredStreamPointer, StreamDataType};
use uuid::Uuid;

use crate::{
    SQLiteStorageProvider, change_index, error_handler::map_sqlite_error, sql_statements,
    utils::SqliteConn,
};

const STREAM_EMBEDDED_MAX_BYTES: usize = 1024;

#[derive(Clone, Copy)]
pub struct WriteWireStreamEntriesInput<'a> {
    pub old_item: Option<&'a WireItem>,
    pub is_deleted: bool,
    pub item_stream_version: ItemStreamVersion,
    pub replication: Option<&'a ReplicationEventMetadata>,
}

pub fn write_stream_entries(
    sqlite: &SqliteConn<'_>,
    table_info: &StoredTableInfo,
    item_data: &HashMap<String, AttributeValue>,
    old_item: Option<&HashMap<String, AttributeValue>>,
    is_deleted: bool,
    item_stream_version: ItemStreamVersion,
    replication: Option<&ReplicationEventMetadata>,
) -> Result<(), StorageError> {
    if !should_write_stream_entries(table_info) {
        return Ok(());
    }

    // Check if stream tables exist - if not, skip stream tracking
    let (sql, params) = sql_statements::check_stream_tables_exist();
    let stream_tables_exist = sqlite
        .prepare(sql)
        .and_then(|mut stmt| stmt.exists(params))
        .unwrap_or(false);

    if !stream_tables_exist {
        // Stream tracking is not enabled, skip silently
        return Ok(());
    }

    let created_at = TimestampMillis::now();
    let item_key = ItemKey::from_key_schema(
        table_info.table_name.clone(),
        &table_info.key_schema,
        item_data,
    )
    .map_err(|err| StorageError::internal(&format!("stream item key error: {err}")))?;

    let item_stream = StreamName::table_item_stream(&table_info.table_name, &item_key)
        .map_err(|err| StorageError::internal(&format!("stream name error: {err}")))?;
    let table_stream_name = StreamName::table_stream(&table_info.table_name);
    let system_stream_name = StreamName::system_table_stream();

    // 1. First, write to the item stream with full data
    let data = if is_deleted {
        storage_types::storage_serde::to_bytes(item_data).map_err(|err| {
            StorageError::internal(&format!("stream delete marker encode error: {err}"))
        })?
    } else {
        storage_types::storage_serde::to_bytes(item_data).map_err(|err| {
            StorageError::internal(&format!("stream new image encode error: {err}"))
        })?
    };
    let old_bytes = match old_item {
        Some(old) if !old.is_empty() => {
            Some(storage_types::storage_serde::to_bytes(old).map_err(|err| {
                StorageError::internal(&format!("stream old image encode error: {err}"))
            })?)
        }
        _ => None,
    };
    let embedded_bytes = old_bytes.as_ref().map_or(0, std::vec::Vec::len) + data.len();

    let item_stream_row_id = storage_types::StreamItemId::from(item_stream_version);
    let data_type = if is_deleted {
        StreamDataType::DeleteMarker
    } else {
        StreamDataType::DynamoDbJson
    };

    let (sql, params) = sql_statements::insert_stream_entry(
        &item_stream,
        &item_stream_row_id,
        data.as_slice(),
        &created_at,
        data_type,
    );

    // Use the original SQL with params! for BLOB data
    sqlite.execute(sql, params).map_err(|err| {
        let stream_name: String = (&item_stream).into();
        tracing::error!(
            stream_name = %stream_name,
            data_len = data.len(),
            error = ?err,
            "failed to insert item stream entry"
        );
        map_sqlite_error(err)
    })?;

    let stored_pointer = if embedded_bytes <= STREAM_EMBEDDED_MAX_BYTES {
        let mut items = Vec::with_capacity(1 + usize::from(old_bytes.is_some()));
        items.push(EmbeddedStreamItem {
            data: data.clone(),
            data_type,
        });
        if let Some(old) = old_bytes {
            items.push(EmbeddedStreamItem {
                data: old,
                data_type: StreamDataType::DynamoDbJson,
            });
        }
        StoredStreamPointer::embedded(
            item_stream.clone(),
            table_info.table_name.clone(),
            item_stream_version,
            items,
        )
    } else {
        StoredStreamPointer::pointer(
            item_stream.clone(),
            table_info.table_name.clone(),
            item_stream_version,
        )
    };
    let stored_pointer = if let Some(replication) = replication.cloned() {
        stored_pointer.with_replication_metadata(replication)
    } else {
        stored_pointer
    };

    let pointer_data = storage_types::storage_serde::to_bytes(&stored_pointer)?;

    let table_pointer_stream_item_id = Uuid::now_v7().into();
    let system_pointer_stream_item_id = Uuid::now_v7().into();
    for (stream_name, pointer_stream_item_id) in [
        (table_stream_name, table_pointer_stream_item_id),
        (system_stream_name, system_pointer_stream_item_id),
    ] {
        let pointer_data = pointer_data.clone();
        let (sql, params) = sql_statements::insert_stream_entry(
            &stream_name,
            &pointer_stream_item_id,
            pointer_data.as_slice(),
            &created_at,
            StreamDataType::StreamPointer,
        );

        // Use original SQL with params! for consistent BLOB handling
        sqlite.execute(sql, params).map_err(|err| {
            let name: String = (&stream_name).into();
            tracing::error!(
                stream_name = %name,
                data_len = pointer_data.len(),
                error = ?err,
                "failed to insert stream pointer entry"
            );
            map_sqlite_error(err)
        })?;
    }
    SQLiteStorageProvider::insert_stream_pointer_index_tx(
        sqlite,
        &table_info.table_name,
        &item_stream,
        item_stream_version,
        table_pointer_stream_item_id,
        system_pointer_stream_item_id,
        created_at,
    )?;
    insert_change_index_marker(sqlite, table_info, table_pointer_stream_item_id, created_at)?;

    Ok(())
}

pub fn write_stream_entries_wire(
    sqlite: &SqliteConn<'_>,
    table_info: &StoredTableInfo,
    item_data: &WireItem,
    input: WriteWireStreamEntriesInput<'_>,
) -> Result<(), StorageError> {
    if !should_write_stream_entries(table_info) {
        return Ok(());
    }
    let WriteWireStreamEntriesInput {
        old_item,
        is_deleted,
        item_stream_version,
        replication,
    } = input;

    let (sql, params) = sql_statements::check_stream_tables_exist();
    let stream_tables_exist = sqlite
        .prepare(sql)
        .and_then(|mut stmt| stmt.exists(params))
        .unwrap_or(false);
    if !stream_tables_exist {
        return Ok(());
    }

    let created_at = TimestampMillis::now();
    let mut key_attributes = HashMap::with_capacity(table_info.key_schema.len());
    for key in &table_info.key_schema {
        let value = item_data
            .attribute_value(&key.attribute_name)?
            .ok_or_else(|| StorageError::internal("stream item missing key attributes"))?;
        key_attributes.insert(key.attribute_name.clone(), value);
    }
    let item_key = ItemKey::from_key_schema(
        table_info.table_name.clone(),
        &table_info.key_schema,
        &key_attributes,
    )
    .map_err(|err| StorageError::internal(&format!("stream item key error: {err}")))?;

    let item_stream = StreamName::table_item_stream(&table_info.table_name, &item_key)
        .map_err(|err| StorageError::internal(&format!("stream name error: {err}")))?;
    let table_stream_name = StreamName::table_stream(&table_info.table_name);
    let system_stream_name = StreamName::system_table_stream();

    let data = encode_wire_item_stream_bytes(item_data)?;
    let old_bytes = old_item.map(encode_wire_item_stream_bytes).transpose()?;
    let embedded_bytes = old_bytes.as_ref().map_or(0, std::vec::Vec::len) + data.len();

    let item_stream_row_id = storage_types::StreamItemId::from(item_stream_version);
    let data_type = if is_deleted {
        StreamDataType::DeleteMarker
    } else {
        StreamDataType::DynamoDbJson
    };

    let (sql, params) = sql_statements::insert_stream_entry(
        &item_stream,
        &item_stream_row_id,
        data.as_slice(),
        &created_at,
        data_type,
    );
    sqlite.execute(sql, params).map_err(|err| {
        let stream_name: String = (&item_stream).into();
        tracing::error!(
            stream_name = %stream_name,
            data_len = data.len(),
            error = ?err,
            "failed to insert item stream entry (wire)"
        );
        map_sqlite_error(err)
    })?;

    let stored_pointer = if embedded_bytes <= STREAM_EMBEDDED_MAX_BYTES {
        let mut items = Vec::with_capacity(1 + usize::from(old_bytes.is_some()));
        items.push(EmbeddedStreamItem {
            data: data.clone(),
            data_type,
        });
        if let Some(old) = old_bytes {
            items.push(EmbeddedStreamItem {
                data: old,
                data_type: StreamDataType::DynamoDbJson,
            });
        }
        StoredStreamPointer::embedded(
            item_stream.clone(),
            table_info.table_name.clone(),
            item_stream_version,
            items,
        )
    } else {
        StoredStreamPointer::pointer(
            item_stream.clone(),
            table_info.table_name.clone(),
            item_stream_version,
        )
    };
    let stored_pointer = if let Some(replication) = replication.cloned() {
        stored_pointer.with_replication_metadata(replication)
    } else {
        stored_pointer
    };

    let pointer_data = storage_types::storage_serde::to_bytes(&stored_pointer)?;

    let table_pointer_stream_item_id = Uuid::now_v7().into();
    let system_pointer_stream_item_id = Uuid::now_v7().into();
    for (stream_name, pointer_stream_item_id) in [
        (table_stream_name, table_pointer_stream_item_id),
        (system_stream_name, system_pointer_stream_item_id),
    ] {
        let pointer_data = pointer_data.clone();
        let (sql, params) = sql_statements::insert_stream_entry(
            &stream_name,
            &pointer_stream_item_id,
            pointer_data.as_slice(),
            &created_at,
            StreamDataType::StreamPointer,
        );
        sqlite.execute(sql, params).map_err(|err| {
            let name: String = (&stream_name).into();
            tracing::error!(
                stream_name = %name,
                data_len = pointer_data.len(),
                error = ?err,
                "failed to insert stream pointer entry (wire)"
            );
            map_sqlite_error(err)
        })?;
    }
    SQLiteStorageProvider::insert_stream_pointer_index_tx(
        sqlite,
        &table_info.table_name,
        &item_stream,
        item_stream_version,
        table_pointer_stream_item_id,
        system_pointer_stream_item_id,
        created_at,
    )?;
    insert_change_index_marker(sqlite, table_info, table_pointer_stream_item_id, created_at)?;

    Ok(())
}

fn insert_change_index_marker(
    sqlite: &SqliteConn<'_>,
    table_info: &StoredTableInfo,
    pointer_stream_item_id: storage_types::StreamItemId,
    created_at: TimestampMillis,
) -> Result<(), StorageError> {
    let slot = change_index::slot_for_table(&table_info.table_name);
    let versionstamp = change_index::sortable_version(pointer_stream_item_id);
    let table_id = table_info.table_name.as_ref();
    let (sql, params) =
        sql_statements::insert_change_index_marker(slot, &versionstamp, table_id, &created_at);
    sqlite.execute(sql, params).map_err(map_sqlite_error)?;
    Ok(())
}

fn encode_wire_item_stream_bytes(item: &WireItem) -> Result<Vec<u8>, StorageError> {
    match item {
        WireItem::DynamoJson { data } => {
            Ok(storage_types::storage_serde::compress_json_bytes(data))
        }
        WireItem::LocalSplit { .. } => {
            let map = item.to_attribute_map()?;
            storage_types::storage_serde::to_bytes(&map)
        }
    }
}

pub(crate) fn should_write_stream_entries(table_info: &StoredTableInfo) -> bool {
    should_write_stream_entries_for_gsi_mode(table_info, false)
}

pub(crate) fn should_write_stream_entries_for_gsi_mode(
    table_info: &StoredTableInfo,
    immediate_gsi_consistency: bool,
) -> bool {
    let stream_enabled = table_info
        .stream_specification
        .as_ref()
        .is_some_and(|spec| spec.stream_enabled);

    let has_gsi = table_info
        .global_secondary_indexes
        .as_ref()
        .is_some_and(|indexes| indexes.iter().any(|idx| !is_ttl_index(&idx.index_name)));

    stream_enabled || (has_gsi && !immediate_gsi_consistency)
}
