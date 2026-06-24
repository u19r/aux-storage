use std::collections::HashMap;

use storage_types::{
    AttributeValue, ItemKey, ReplicationEventMetadata, StorageResult, StreamItemId, StreamName,
    TableName, TimestampMillis,
};
use stream_provider::{EmbeddedStreamItem, StreamDataType};

use crate::{
    key_template::{KeyTemplate, PlaceholderBinding},
    keyspace::{stream_keys, table_identity::TableIdentity},
    stream::{
        constants::STREAM_EMBEDDED_MAX_BYTES,
        item_codec::encode_stored_stream_item_parts,
        pointer_codec::{CompactStoredStreamPointer, encode_compact_pointer},
    },
};

#[derive(Clone, Copy)]
pub struct StreamEntryContext<'a> {
    pub table_identity: &'a TableIdentity,
    pub table_name: &'a TableName,
    pub item_key: &'a ItemKey,
}

/// Create stream items for item updates (put, update, delete operations).
///
/// This function creates 3 key/value entries for stream
/// writes:
/// 1. System stream entry
/// 2. Table stream entry
/// 3. Item stream entry
///
/// # Arguments
/// * `table_name` - The name of the table
/// * `item_key` - The key of the item being operated on
/// * `item` - The item data (for put/update) or key (for delete)
/// * `old_item` - The previous item image (if available)
/// * `stream_item_id` - Unique ID for this stream item
/// * `is_delete` - Whether this is a delete operation
///
/// # Returns
/// A vector of serialized key/value pairs for stream inserts
fn should_embed_stream_items(item_bytes: &[u8], old_item_bytes: Option<&[u8]>) -> bool {
    old_item_bytes.map_or(item_bytes.len(), |old| old.len() + item_bytes.len())
        <= STREAM_EMBEDDED_MAX_BYTES
}

pub fn create_item_update_stream_entries(
    context: StreamEntryContext<'_>,
    item: &HashMap<String, AttributeValue>,
    old_item: Option<&HashMap<String, AttributeValue>>,
    stream_item_id: StreamItemId,
    is_delete: bool,
    replication: Option<&ReplicationEventMetadata>,
) -> StorageResult<Vec<(KeyTemplate, Vec<u8>)>> {
    let item_bytes = storage_types::storage_serde::to_bytes(item)?;
    let old_item_bytes = match old_item {
        Some(old) if !old.is_empty() => Some(storage_types::storage_serde::to_bytes(old)?),
        _ => None,
    };

    create_item_update_stream_entries_wire_encoded(
        context,
        item_bytes.as_slice(),
        old_item_bytes.as_deref(),
        stream_item_id,
        is_delete,
        replication,
    )
}

pub fn create_item_update_stream_entries_wire_encoded(
    context: StreamEntryContext<'_>,
    item_bytes: &[u8],
    old_item_bytes: Option<&[u8]>,
    stream_item_id: StreamItemId,
    is_delete: bool,
    replication: Option<&ReplicationEventMetadata>,
) -> StorageResult<Vec<(KeyTemplate, Vec<u8>)>> {
    let table_item_stream_name =
        StreamName::table_item_stream(context.table_name, context.item_key)?;

    let created_at = TimestampMillis::now();
    let item_scope = stream_keys::item_stream_scope(context.item_key)?;
    let target_item_stream_version = storage_types::ItemStreamVersion::from(stream_item_id);
    let target_item_stream_id = StreamItemId::from(target_item_stream_version);
    let stored_pointer = if should_embed_stream_items(item_bytes, old_item_bytes) {
        // Keep update/delete old+new images co-located for small payloads so
        // stream reads can satisfy "new and old images" without extra round
        // trips, matching DynamoDB stream view expectations.
        let mut items = Vec::with_capacity(1 + usize::from(old_item_bytes.is_some()));
        items.push(EmbeddedStreamItem {
            data: item_bytes.to_vec(),
            data_type: if is_delete {
                StreamDataType::DeleteMarker
            } else {
                StreamDataType::DynamoDbJson
            },
        });
        if let Some(old) = old_item_bytes {
            items.push(EmbeddedStreamItem {
                data: old.to_vec(),
                data_type: StreamDataType::DynamoDbJson,
            });
        }
        CompactStoredStreamPointer::embedded(
            context.table_identity,
            item_scope.clone(),
            target_item_stream_version,
            items,
            replication.cloned(),
        )
    } else {
        CompactStoredStreamPointer::pointer(
            context.table_identity,
            item_scope.clone(),
            target_item_stream_version,
            replication.cloned(),
        )
    };

    // Persist only StoredStreamItem fields (no id). Stream item id is part of
    // the key and recovered by stream readers from key bytes.
    // This keeps write-side encoding aligned with stream-provider storage rules
    // and avoids serializing duplicate id bytes per entry.
    let pointer_payload = encode_compact_pointer(&stored_pointer)?;
    let pointer_bytes = encode_stored_stream_item_parts(
        None,
        pointer_payload.as_slice(),
        StreamDataType::StreamPointer,
        created_at,
    )?;
    let stream_bytes = encode_stored_stream_item_parts(
        Some(&table_item_stream_name),
        item_bytes,
        if is_delete {
            StreamDataType::DeleteMarker
        } else {
            StreamDataType::DynamoDbJson
        },
        created_at,
    )?;

    let fallback = stream_item_id.as_bytes().to_vec();
    let pointer_binding = PlaceholderBinding::unique(fallback);
    let compact_keys =
        stream_keys::stream_write_keys(context.table_identity, context.item_key, stream_item_id)?;
    let mut item_row_key = compact_keys.item_row;
    item_row_key.extend_from_slice(target_item_stream_id.as_bytes());
    let mut item_pointer_key = compact_keys.item_pointer;
    item_pointer_key.extend_from_slice(target_item_stream_id.as_bytes());

    Ok(vec![
        (
            KeyTemplate::placeholder(compact_keys.system_row, Vec::new(), pointer_binding.clone()),
            pointer_bytes.clone(),
        ),
        (
            KeyTemplate::placeholder(compact_keys.table_row, Vec::new(), pointer_binding.clone()),
            pointer_bytes,
        ),
        (KeyTemplate::literal(item_row_key), stream_bytes),
        (KeyTemplate::literal(item_pointer_key), Vec::new()),
        (
            KeyTemplate::placeholder(compact_keys.table_pointer, Vec::new(), pointer_binding),
            Vec::new(),
        ),
    ])
}
