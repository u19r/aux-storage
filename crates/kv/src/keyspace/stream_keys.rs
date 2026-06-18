use storage_types::{ItemKey, StorageError, StorageResult, StreamItemId, StreamName, TableName};

use crate::keyspace::{
    compact::{self, KeyRange},
    table_identity::TableIdentity,
};

const TABLE_STREAM_SUFFIX: &[u8] = b"/stream-table";
const ITEM_STREAM_SEGMENT: &[u8] = b"/stream-item/";

pub(crate) enum CompactStreamRange {
    System(KeyRange),
    Table(KeyRange),
    Item(KeyRange),
    Legacy,
}

pub(crate) fn stream_write_keys(
    table: &TableIdentity,
    item_key: &ItemKey,
    _stream_item_id: StreamItemId,
) -> StorageResult<CompactStreamWriteKeys> {
    let item_scope = item_stream_scope(item_key)?;
    Ok(CompactStreamWriteKeys {
        system_row: compact::system_stream_prefix().start,
        table_row: compact::table_stream_prefix(table.table_id).start,
        item_row: compact::item_stream_prefix(table.table_id, &item_scope).start,
        table_pointer: compact::stream_pointer_table_prefix(table.table_id).start,
        item_pointer: compact::stream_pointer_item_prefix(table.table_id, &item_scope).start,
    })
}

pub(crate) fn stream_row_key(
    stream_name: &StreamName,
    table: Option<&TableIdentity>,
    stream_item_id: StreamItemId,
) -> StorageResult<Option<Vec<u8>>> {
    let range = match compact_stream_range(stream_name, table)? {
        CompactStreamRange::System(range)
        | CompactStreamRange::Table(range)
        | CompactStreamRange::Item(range) => range,
        CompactStreamRange::Legacy => return Ok(None),
    };
    let mut key = range.start;
    key.extend_from_slice(stream_item_id.as_bytes());
    Ok(Some(key))
}

pub(crate) fn stream_pointer_item_prefix_for_stream(
    table: &TableIdentity,
    item_stream: &StreamName,
) -> StorageResult<KeyRange> {
    Ok(compact::stream_pointer_item_prefix(
        table.table_id,
        &item_stream_scope_from_name(item_stream)?,
    ))
}

pub(crate) fn stream_pointer_item_key_for_stream(
    table: &TableIdentity,
    item_stream: &StreamName,
    stream_item_id: StreamItemId,
) -> StorageResult<Vec<u8>> {
    let item_scope = item_stream_scope_from_name(item_stream)?;
    let mut scope_and_id = item_scope;
    scope_and_id.extend_from_slice(stream_item_id.as_bytes());
    Ok(compact::stream_pointer_item_key(
        table.table_id,
        &scope_and_id,
    ))
}

pub(crate) fn stream_pointer_table_key_for_stream(
    table: &TableIdentity,
    stream_item_id: StreamItemId,
) -> Vec<u8> {
    compact::stream_pointer_table_key(table.table_id, stream_item_id.as_bytes())
}

pub(crate) struct CompactStreamWriteKeys {
    pub(crate) system_row: Vec<u8>,
    pub(crate) table_row: Vec<u8>,
    pub(crate) item_row: Vec<u8>,
    pub(crate) table_pointer: Vec<u8>,
    pub(crate) item_pointer: Vec<u8>,
}

pub(crate) fn compact_stream_range(
    stream_name: &StreamName,
    table: Option<&TableIdentity>,
) -> StorageResult<CompactStreamRange> {
    if *stream_name == StreamName::system_table_stream() {
        return Ok(CompactStreamRange::System(compact::system_stream_prefix()));
    }

    let Some((table_name, kind)) = parse_table_stream_name(stream_name) else {
        return Ok(CompactStreamRange::Legacy);
    };
    let table = table.ok_or_else(|| StorageError::table_not_found(&table_name))?;
    match kind {
        TableStreamKind::Table => Ok(CompactStreamRange::Table(compact::table_stream_prefix(
            table.table_id,
        ))),
        TableStreamKind::Item { item_scope } => {
            let range = compact::item_stream_prefix(table.table_id, &item_scope);
            Ok(CompactStreamRange::Item(range))
        }
    }
}

pub(crate) fn table_name_for_stream(stream_name: &StreamName) -> Option<TableName> {
    parse_table_stream_name(stream_name).map(|(table_name, _kind)| table_name)
}

pub(crate) fn stream_item_id_from_compact_key(key: &[u8]) -> Option<StreamItemId> {
    match compact::parse_compact_key(key).ok()? {
        compact::ParsedCompactKey::SystemStreamRow { stream_item_id }
        | compact::ParsedCompactKey::TableStreamRow { stream_item_id, .. } => {
            StreamItemId::try_from(stream_item_id).ok()
        }
        compact::ParsedCompactKey::ItemStreamRow { item_scope, .. }
        | compact::ParsedCompactKey::StreamPointerItemIndex { item_scope, .. } => {
            let id_start = item_scope.len().checked_sub(12)?;
            StreamItemId::try_from(&item_scope[id_start..]).ok()
        }
        compact::ParsedCompactKey::StreamPointerTableIndex { stream_item_id, .. } => {
            StreamItemId::try_from(stream_item_id).ok()
        }
        _ => None,
    }
}

pub(crate) fn item_stream_scope(item_key: &ItemKey) -> StorageResult<Vec<u8>> {
    item_key
        .hash_range_key_part()
        .map_err(|err| StorageError::internal(&format!("item stream scope build failed: {err}")))
}

enum TableStreamKind {
    Table,
    Item { item_scope: Vec<u8> },
}

fn parse_table_stream_name(stream_name: &StreamName) -> Option<(TableName, TableStreamKind)> {
    let bytes = stream_name.as_ref();
    if let Some(table) = bytes.strip_suffix(TABLE_STREAM_SUFFIX) {
        return Some((
            TableName::new(&String::from_utf8_lossy(table)),
            TableStreamKind::Table,
        ));
    }
    let (table, item_scope) = split_once(bytes, ITEM_STREAM_SEGMENT)?;
    Some((
        TableName::new(&String::from_utf8_lossy(table)),
        TableStreamKind::Item {
            item_scope: item_scope.to_vec(),
        },
    ))
}

fn item_stream_scope_from_name(stream_name: &StreamName) -> StorageResult<Vec<u8>> {
    match parse_table_stream_name(stream_name) {
        Some((_table_name, TableStreamKind::Item { item_scope })) => Ok(item_scope),
        _ => Err(StorageError::validation(
            "stream name is not a table item stream",
        )),
    }
}

fn split_once<'a>(bytes: &'a [u8], needle: &[u8]) -> Option<(&'a [u8], &'a [u8])> {
    let position = bytes
        .windows(needle.len())
        .position(|window| window == needle)?;
    Some((&bytes[..position], &bytes[(position + needle.len())..]))
}
