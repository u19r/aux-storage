use storage_types::{IndexName, ItemKey, StorageError, StorageResult};

use crate::keyspace::{
    compact::{self, IndexStorageId, KeyRange, TableStorageId},
    table_identity::TableIdentity,
};

pub(crate) fn item_key(table: &TableIdentity, item_key: &ItemKey) -> StorageResult<Vec<u8>> {
    match item_key.index_id() {
        Some(index_name) => {
            let index_id =
                index_id(table, index_name).ok_or_else(|| missing_index_identity(index_name))?;
            Ok(compact::gsi_item_key(
                table.table_id,
                index_id,
                &sorted_key_suffix(item_key)?,
            ))
        }
        None => Ok(compact::primary_item_key(
            table.table_id,
            &sorted_key_suffix(item_key)?,
        )),
    }
}

pub(crate) fn item_key_increment(table: &TableIdentity, key: &ItemKey) -> StorageResult<Vec<u8>> {
    increment_bytes(item_key(table, key)?)
}

pub(crate) fn item_key_decrement(table: &TableIdentity, key: &ItemKey) -> StorageResult<Vec<u8>> {
    decrement_bytes(item_key(table, key)?)
}

pub(crate) fn primary_item_prefix(table_id: TableStorageId) -> KeyRange {
    compact::primary_item_prefix(table_id)
}

pub(crate) fn gsi_prefix(table: &TableIdentity, index_name: &IndexName) -> Option<KeyRange> {
    index_id(table, index_name).map(|index_id| compact::gsi_prefix(table.table_id, index_id))
}

pub(crate) fn gsi_tombstone_key(
    table: &TableIdentity,
    index_name: &IndexName,
    index_key: &ItemKey,
) -> StorageResult<Vec<u8>> {
    let index_id = index_id(table, index_name).ok_or_else(|| missing_index_identity(index_name))?;
    Ok(compact::gsi_tombstone_key(
        table.table_id,
        index_id,
        &sorted_key_suffix(index_key)?,
    ))
}

pub(crate) fn gsi_tombstone_prefix(
    table: &TableIdentity,
    index_name: &IndexName,
) -> Option<KeyRange> {
    index_id(table, index_name)
        .map(|index_id| compact::gsi_tombstone_prefix(table.table_id, index_id))
}

pub(crate) fn gsi_backfill_key(table: &TableIdentity, index_name: &IndexName) -> Option<Vec<u8>> {
    index_id(table, index_name).map(|index_id| compact::gsi_backfill_key(table.table_id, index_id))
}

fn index_id(table: &TableIdentity, index_name: &IndexName) -> Option<IndexStorageId> {
    table
        .indexes
        .iter()
        .find(|index| &index.index_name == index_name)
        .map(|index| index.index_id)
}

fn sorted_key_suffix(item_key: &ItemKey) -> StorageResult<Vec<u8>> {
    item_key
        .sorted_storage_suffix()
        .map_err(|err| StorageError::internal(&format!("item key serialization failed: {err}")))
}

fn missing_index_identity(index_name: &IndexName) -> StorageError {
    StorageError::internal(&format!("missing storage identity for index {index_name}"))
}

fn increment_bytes(mut bytes: Vec<u8>) -> StorageResult<Vec<u8>> {
    for index in (0..bytes.len()).rev() {
        if bytes[index] < 0xFF {
            bytes[index] += 1;
            return Ok(bytes);
        }
        bytes[index] = 0x00;
    }
    bytes.push(0x00);
    Ok(bytes)
}

fn decrement_bytes(mut bytes: Vec<u8>) -> StorageResult<Vec<u8>> {
    for index in (0..bytes.len()).rev() {
        if bytes[index] > 0x00 {
            bytes[index] -= 1;
            return Ok(bytes);
        }
        bytes[index] = 0xFF;
    }
    if !bytes.is_empty() {
        bytes.pop();
        return Ok(bytes);
    }
    Err(StorageError::internal("cannot decrement empty item key"))
}
