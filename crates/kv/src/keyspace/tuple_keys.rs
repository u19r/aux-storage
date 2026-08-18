//! Canonical FoundationDB Tuple keys for item and GSI rows.
//!
//! This module is deliberately compiled only for the FoundationDB backend.  The
//! RocksDB provider keeps its existing sorted suffix codec; FoundationDB data
//! is cut over as one format and is never decoded through the compact item/GSI
//! constructors.

use std::borrow::Cow;

use foundationdb::tuple::{Bytes, Element, pack, unpack};
use storage_types::{
    AttributeValue, IndexKey, IndexKeyPrefix, ItemKey, StorageError, StorageResult, TableKey,
};

use crate::keyspace::{
    compact::{IndexStorageId, KeyRange, TableStorageId},
    table_identity::TableIdentity,
};

pub(super) const FORMAT: i64 = 2;
pub(super) const PRIMARY: &str = "item";
pub(super) const GSI: &str = "gsi";
pub(super) const GSI_TOMBSTONE: &str = "gsi_tombstone";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TupleKeyElement {
    pub(crate) tag: usize,
    pub(crate) value: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum TupleMapperElement {
    Key(TupleKeyElement),
    Value(usize),
    Literal(AttributeValue),
}

pub(crate) fn item_mapper_elements(
    table_id: TableStorageId,
    hash: &TupleMapperElement,
    range: Option<&TupleMapperElement>,
) -> StorageResult<Vec<u8>> {
    let (range_tag, range_value) = if let Some(range) = range {
        (mapper_element_tag(range)?, mapper_element_value(range)?)
    } else {
        (Element::Nil, Element::Nil)
    };
    let elements = [
        Element::Int(FORMAT),
        mapper_key_element(2)?,
        Element::String(Cow::Borrowed(PRIMARY)),
        Element::Int(i64::from(table_id.get())),
        mapper_element_tag(hash)?,
        mapper_element_value(hash)?,
        range_tag,
        range_value,
        Element::String(Cow::Borrowed("{...}")),
    ];
    Ok(pack(&elements.as_slice()))
}

/// Build a mapper for a primary-table partition query.  Unlike a point-item
/// mapper, this leaves the tuple expansion marker immediately after the hash
/// pair so FoundationDB scans every range key in the mapped partition.
pub(crate) fn item_partition_mapper_elements(
    table_id: TableStorageId,
    hash: &TupleMapperElement,
) -> StorageResult<Vec<u8>> {
    let elements = [
        Element::Int(FORMAT),
        mapper_key_element(2)?,
        Element::String(Cow::Borrowed(PRIMARY)),
        Element::Int(i64::from(table_id.get())),
        mapper_element_tag(hash)?,
        mapper_element_value(hash)?,
        Element::String(Cow::Borrowed("{...}")),
    ];
    Ok(pack(&elements.as_slice()))
}

fn mapper_element_tag(element: &TupleMapperElement) -> StorageResult<Element<'static>> {
    match element {
        TupleMapperElement::Key(element) => mapper_key_element(element.tag),
        TupleMapperElement::Value(_) => Ok(Element::String(Cow::Borrowed("S"))),
        TupleMapperElement::Literal(value) => {
            Ok(Element::String(Cow::Borrowed(scalar_key_tag(value)?)))
        }
    }
}

fn mapper_element_value(element: &TupleMapperElement) -> StorageResult<Element<'static>> {
    match element {
        TupleMapperElement::Key(element) => mapper_key_element(element.value),
        TupleMapperElement::Value(index) => mapper_value_element(*index),
        TupleMapperElement::Literal(value) => {
            let bytes = ItemKey::serialize_attribute_value_to_bytes(value).map_err(|error| {
                StorageError::internal(&format!(
                    "mapped Tuple literal serialization failed: {error}"
                ))
            })?;
            Ok(Element::Bytes(Bytes::from(bytes)))
        }
    }
}

fn scalar_key_tag(value: &AttributeValue) -> StorageResult<&'static str> {
    match value {
        AttributeValue::S(_) => Ok("S"),
        AttributeValue::N(_) => Ok("N"),
        AttributeValue::B(_) => Ok("B"),
        _ => Err(StorageError::validation(
            "mapped Tuple literals must be scalar key values",
        )),
    }
}

fn mapper_key_element(index: usize) -> StorageResult<Element<'static>> {
    const KEYS: [&str; 14] = [
        "{K[0]}", "{K[1]}", "{K[2]}", "{K[3]}", "{K[4]}", "{K[5]}", "{K[6]}", "{K[7]}", "{K[8]}",
        "{K[9]}", "{K[10]}", "{K[11]}", "{K[12]}", "{K[13]}",
    ];
    KEYS.get(index)
        .copied()
        .map(|key| Element::String(Cow::Borrowed(key)))
        .ok_or_else(|| StorageError::internal("mapped Tuple key element is out of range"))
}

fn mapper_value_element(index: usize) -> StorageResult<Element<'static>> {
    const VALUES: [&str; 34] = [
        "{V[0]}", "{V[1]}", "{V[2]}", "{V[3]}", "{V[4]}", "{V[5]}", "{V[6]}", "{V[7]}", "{V[8]}",
        "{V[9]}", "{V[10]}", "{V[11]}", "{V[12]}", "{V[13]}", "{V[14]}", "{V[15]}", "{V[16]}",
        "{V[17]}", "{V[18]}", "{V[19]}", "{V[20]}", "{V[21]}", "{V[22]}", "{V[23]}", "{V[24]}",
        "{V[25]}", "{V[26]}", "{V[27]}", "{V[28]}", "{V[29]}", "{V[30]}", "{V[31]}", "{V[32]}",
        "{V[33]}",
    ];
    VALUES
        .get(index)
        .copied()
        .map(|value| Element::String(Cow::Borrowed(value)))
        .ok_or_else(|| StorageError::internal("mapped Tuple value element is out of range"))
}

pub(crate) fn item_key(table: &TableIdentity, item_key: &ItemKey) -> StorageResult<Vec<u8>> {
    match item_key {
        ItemKey::Table(key) => pack_primary(table, key),
        ItemKey::Index(key) => {
            let index_id = index_id(table, &key.index_id)
                .ok_or_else(|| missing_index_identity(&key.index_id))?;
            pack_gsi(table, index_id, key)
        }
        ItemKey::IndexPrefix(key) => {
            let index_id = index_id(table, &key.index_id)
                .ok_or_else(|| missing_index_identity(&key.index_id))?;
            pack_gsi_prefix_key(table, index_id, key)
        }
    }
}

/// Build the physical prefix for a query which fixes only the partition key.
/// A `TableKey` without a sort key is also used for a hash-only point key, so
/// it cannot double as this prefix: hash-range rows sort after the hash
/// element and before the prefix end, not after fixed nil sort sentinels.
pub(crate) fn item_key_prefix(table: &TableIdentity, key: &ItemKey) -> StorageResult<Vec<u8>> {
    match key {
        ItemKey::Table(key) => {
            let mut elements = pack_header_elements(table, PRIMARY);
            elements.push(Element::Int(i64::from(table.table_id.get())));
            append_key_pair(&mut elements, &key.hash_key)?;
            if let Some(range_key) = key.range_key.as_ref() {
                append_key_pair(&mut elements, range_key)?;
            }
            Ok(pack(&elements))
        }
        ItemKey::IndexPrefix(key) => {
            let index_id = index_id(table, &key.index_id)
                .ok_or_else(|| missing_index_identity(&key.index_id))?;
            let mut elements = pack_header_elements(table, GSI);
            elements.push(Element::Int(i64::from(table.table_id.get())));
            elements.push(Element::Int(i64::from(index_id.get())));
            append_key_pair(&mut elements, &key.hash_key)?;
            if let Some(range_key) = key.range_key.as_ref() {
                append_key_pair(&mut elements, range_key)?;
            }
            Ok(pack(&elements))
        }
        ItemKey::Index(_) => item_key(table, key),
    }
}

pub(crate) fn item_key_prefix_end(
    table: &TableIdentity,
    item_key: &ItemKey,
) -> StorageResult<Vec<u8>> {
    let prefix = item_key_prefix(table, item_key)?;
    let allows_longer_range_values = match item_key {
        ItemKey::Table(key) => key.range_key.is_some(),
        ItemKey::IndexPrefix(key) => key.range_key.is_some(),
        ItemKey::Index(_) => false,
    };
    Ok(prefix_range_end(&prefix, allows_longer_range_values))
}

pub(crate) fn primary_item_prefix(table: &TableIdentity) -> KeyRange {
    range_for_prefix(pack_header(table, PRIMARY, None))
}

pub(crate) fn gsi_prefix(
    table: &TableIdentity,
    index_name: &storage_types::IndexName,
) -> Option<KeyRange> {
    index_id(table, index_name)
        .map(|index_id| range_for_prefix(pack_header(table, GSI, Some(index_id))))
}

pub(crate) fn gsi_tombstone_key(
    table: &TableIdentity,
    index_name: &storage_types::IndexName,
    index_key: &ItemKey,
) -> StorageResult<Vec<u8>> {
    let index_id = index_id(table, index_name).ok_or_else(|| missing_index_identity(index_name))?;
    let ItemKey::Index(key) = index_key else {
        return Err(StorageError::validation(
            "GSI tombstone keys require a complete index key",
        ));
    };
    pack_gsi_with_family(GSI_TOMBSTONE, table, index_id, key)
}

pub(crate) fn gsi_tombstone_prefix(
    table: &TableIdentity,
    index_name: &storage_types::IndexName,
) -> Option<KeyRange> {
    index_id(table, index_name)
        .map(|index_id| range_for_prefix(pack_header(table, GSI_TOMBSTONE, Some(index_id))))
}

/// Whether a physical key belongs to the canonical GSI item/tombstone families.
/// This is intentionally a prefix check: malformed rows are still rejected by
/// the owner that decodes them, while mutation coalescing can classify them
/// without inventing a legacy compact decoder.
pub(crate) fn is_gsi_key(key: &[u8]) -> bool {
    let Ok(elements) = unpack::<Vec<Element<'_>>>(key) else {
        return false;
    };
    matches!(elements.first(), Some(Element::Int(value)) if *value == FORMAT)
        && matches!(elements.get(1), Some(Element::Bytes(_)))
        && matches!(elements.get(2), Some(Element::String(value)) if value == GSI || value == GSI_TOMBSTONE)
}

fn pack_primary(table: &TableIdentity, key: &TableKey) -> StorageResult<Vec<u8>> {
    let mut elements = pack_header_elements(table, PRIMARY);
    elements.push(Element::Int(i64::from(table.table_id.get())));
    append_key_pair(&mut elements, &key.hash_key)?;
    append_optional_key_pair(&mut elements, key.range_key.as_ref())?;
    Ok(pack(&elements))
}

fn pack_gsi(
    table: &TableIdentity,
    index_id: IndexStorageId,
    key: &IndexKey,
) -> StorageResult<Vec<u8>> {
    pack_gsi_with_family(GSI, table, index_id, key)
}

fn pack_gsi_with_family(
    family: &str,
    table: &TableIdentity,
    index_id: IndexStorageId,
    key: &IndexKey,
) -> StorageResult<Vec<u8>> {
    let mut elements = pack_header_elements(table, family);
    elements.push(Element::Int(i64::from(table.table_id.get())));
    elements.push(Element::Int(i64::from(index_id.get())));
    append_key_pair(&mut elements, &key.hash_key)?;
    append_optional_key_pair(&mut elements, key.range_key.as_ref())?;
    append_key_pair(&mut elements, &key.table_key.hash_key)?;
    append_optional_key_pair(&mut elements, key.table_key.range_key.as_ref())?;
    Ok(pack(&elements))
}

fn pack_gsi_prefix_key(
    table: &TableIdentity,
    index_id: IndexStorageId,
    key: &IndexKeyPrefix,
) -> StorageResult<Vec<u8>> {
    let mut elements = pack_header_elements(table, GSI);
    elements.push(Element::Int(i64::from(table.table_id.get())));
    elements.push(Element::Int(i64::from(index_id.get())));
    append_key_pair(&mut elements, &key.hash_key)?;
    append_optional_key_pair(&mut elements, key.range_key.as_ref())?;
    Ok(pack(&elements))
}

fn pack_header(table: &TableIdentity, family: &str, index_id: Option<IndexStorageId>) -> Vec<u8> {
    let mut elements = pack_header_elements(table, family);
    elements.push(Element::Int(i64::from(table.table_id.get())));
    if let Some(index_id) = index_id {
        elements.push(Element::Int(i64::from(index_id.get())));
    }
    pack(&elements)
}

fn pack_header_elements(table: &TableIdentity, family: &str) -> Vec<Element<'static>> {
    vec![
        Element::Int(FORMAT),
        Element::Bytes(Bytes::from(table.tenant_keyspace.clone())),
        Element::String(Cow::Owned(family.to_string())),
    ]
}

fn append_key_pair(
    elements: &mut Vec<Element<'static>>,
    value: &AttributeValue,
) -> StorageResult<()> {
    let (tag, bytes) = key_bytes(value)?;
    elements.push(Element::String(Cow::Borrowed(tag)));
    elements.push(Element::Bytes(Bytes::from(bytes)));
    Ok(())
}

fn append_optional_key_pair(
    elements: &mut Vec<Element<'static>>,
    value: Option<&AttributeValue>,
) -> StorageResult<()> {
    if let Some(value) = value {
        append_key_pair(elements, value)?;
    } else {
        elements.push(Element::Nil);
        elements.push(Element::Nil);
    }
    Ok(())
}

fn key_bytes(value: &AttributeValue) -> StorageResult<(&'static str, Vec<u8>)> {
    let tag = match value {
        AttributeValue::S(_) => "S",
        AttributeValue::N(_) => "N",
        AttributeValue::B(_) => "B",
        _ => {
            return Err(StorageError::validation(
                "only S, N, and B types are supported for physical keys",
            ));
        }
    };
    let bytes = ItemKey::serialize_attribute_value_to_bytes(value).map_err(|error| {
        StorageError::internal(&format!("item key serialization failed: {error}"))
    })?;
    Ok((tag, bytes))
}

fn index_id(table: &TableIdentity, name: &storage_types::IndexName) -> Option<IndexStorageId> {
    table
        .indexes
        .iter()
        .find(|index| &index.index_name == name)
        .map(|index| index.index_id)
}

fn missing_index_identity(name: &storage_types::IndexName) -> StorageError {
    StorageError::internal(&format!("missing storage identity for index {name}"))
}

fn range_for_prefix(prefix: Vec<u8>) -> KeyRange {
    let end = prefix_range_end(&prefix, false);
    KeyRange { start: prefix, end }
}

fn prefix_range_end(prefix: &[u8], allows_longer_range_values: bool) -> Vec<u8> {
    // A hash-only query fixes the complete encoded hash element, so its
    // terminal zero must remain part of the prefix.  A range `begins_with`
    // query instead fixes only the bytes before that terminal and must admit
    // longer values with the same encoded prefix.  In both cases the upper
    // bound is the ordinary lexicographic successor, not an appended 0xff:
    // the latter would also admit a different hash such as `grp-2`.
    let candidate = if allows_longer_range_values && prefix.last() == Some(&0x00) {
        &prefix[..prefix.len().saturating_sub(1)]
    } else {
        prefix
    };
    let mut end = candidate.to_vec();
    for index in (0..end.len()).rev() {
        if end[index] < 0xff {
            end[index] += 1;
            return end;
        }
        end[index] = 0;
    }
    end.push(0);
    end
}
