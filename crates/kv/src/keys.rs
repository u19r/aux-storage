use queue_provider::{MessageId, ReceiptHandle};
use storage_types::{IndexName, StreamName, TableName, TimestampMillis, UserStreamName};

use crate::{
    key_template::{KeyTemplate, PlaceholderBinding},
    newtypes::MessageVisibilityKey,
};

// Key prefixes for organizing data in RocksDB
pub const TABLES_PREFIX: &str = "tables/";
pub const TABLE_DATA_PREFIX: &str = "table/";

#[must_use]
pub fn table_metadata_key(table_name: &TableName) -> Vec<u8> {
    format!("{TABLES_PREFIX}{table_name}").as_bytes().to_vec()
}

#[must_use]
pub fn gsi_backfill_key(table_name: &TableName, index_name: &IndexName) -> Vec<u8> {
    format!("{TABLES_PREFIX}{table_name}/gsi-backfill/{index_name}")
        .as_bytes()
        .to_vec()
}

pub const ITEM_REVISIONS_PREFIX: &str = "sys/sync/item-revisions/";

#[must_use]
pub fn item_revision_prefix() -> Vec<u8> {
    ITEM_REVISIONS_PREFIX.as_bytes().to_vec()
}

pub fn item_revision_key(table_name: &TableName, key_json: &str) -> Vec<u8> {
    format!("{ITEM_REVISIONS_PREFIX}{table_name}/{key_json}")
        .as_bytes()
        .to_vec()
}

#[must_use]
pub fn gsi_tombstone_prefix_from_name(table_name: &TableName, index_name: &IndexName) -> Vec<u8> {
    let mut prefix = table_name.sanitized_name().as_bytes().to_vec();
    prefix.extend(b"/index-tombstone/");
    prefix.extend(index_name.as_ref().as_bytes());
    prefix.extend(b"/data/");
    prefix
}

pub fn gsi_tombstone_key_from_index_key(
    table_name: &TableName,
    index_name: &IndexName,
    index_key: &[u8],
) -> Option<Vec<u8>> {
    let index_prefix = storage_types::ItemKey::index_prefix_from_name(table_name, index_name);
    let index_suffix = index_key.strip_prefix(index_prefix.as_slice())?;
    let mut tombstone_key = gsi_tombstone_prefix_from_name(table_name, index_name);
    tombstone_key.extend_from_slice(index_suffix);
    Some(tombstone_key)
}

// Stream key prefixes
pub const STREAMS_PREFIX: &str = "streams/";
pub const STREAM_CURSORS_PREFIX: &str = "stream-cursors/";

#[must_use]
pub fn stream_metadata_key(user_stream_name: &UserStreamName) -> Vec<u8> {
    let mut key_parts = STREAMS_PREFIX.as_bytes().to_vec();
    key_parts.extend(user_stream_name.as_str().as_bytes().to_vec());
    key_parts
}

#[must_use]
pub fn stream_cursor_key(stream_name: &StreamName, cursor_id: &str) -> Vec<u8> {
    let mut key_parts = stream_cursors_prefix(stream_name);
    key_parts.extend(cursor_id.as_bytes());
    key_parts
}

#[must_use]
pub fn stream_cursors_prefix(stream_name: &StreamName) -> Vec<u8> {
    let mut key_parts = STREAM_CURSORS_PREFIX.as_bytes().to_vec();
    key_parts.extend(stream_name.to_vec());
    key_parts.push(b'/');
    key_parts
}

// Queue-related key functions
#[inline]
#[must_use]
pub fn key_queue_root(queue_url: &str) -> String {
    format!("sys/queues/{queue_url}")
}

#[inline]
#[must_use]
pub fn key_message(queue_url: &str, message_id: &MessageId) -> String {
    format!("sys/queues/{queue_url}/messages/{message_id}")
}

#[must_use]
pub fn queue_message_template(queue_url: &str, message_id: &MessageId) -> KeyTemplate {
    queue_message_template_with_binding(
        queue_url,
        PlaceholderBinding::unique(message_id.as_bytes().to_vec()),
    )
}

#[must_use]
pub fn queue_message_template_with_binding(
    queue_url: &str,
    binding: PlaceholderBinding,
) -> KeyTemplate {
    KeyTemplate::placeholder(queue_message_prefix(queue_url), Vec::new(), binding)
}

#[must_use]
pub fn queue_message_prefix(queue_url: &str) -> Vec<u8> {
    format!("sys/queues/{queue_url}/messages/").into_bytes()
}

#[must_use]
pub fn queue_visibility_template(
    queue_url: &str,
    timestamp: TimestampMillis,
    binding: PlaceholderBinding,
) -> KeyTemplate {
    let prefix = format!("sys/queues/{queue_url}/visibility/{:013}:", *timestamp).into_bytes();
    KeyTemplate::placeholder(prefix, Vec::new(), binding)
}

#[must_use]
pub fn queue_message_storage_key(queue_url: &str, message_id: &MessageId) -> Vec<u8> {
    let mut key = queue_message_prefix(queue_url);
    key.extend_from_slice(message_id.as_bytes());
    key
}

#[must_use]
pub fn queue_visibility_storage_key(
    queue_url: &str,
    timestamp: TimestampMillis,
    message_id: &MessageId,
) -> Vec<u8> {
    let mut key = format!("sys/queues/{queue_url}/visibility/{:013}:", *timestamp).into_bytes();
    key.extend_from_slice(message_id.as_bytes());
    key
}

#[inline]
#[must_use]
pub fn key_receipt_handle(queue_url: &str, receipt_handle: &ReceiptHandle) -> String {
    format!("sys/queues/{queue_url}/receipt_handles/{receipt_handle}")
}

#[inline]
#[must_use]
pub fn key_receipt_checkpoint_uuid(queue_url: &str, receipt_handle: &ReceiptHandle) -> String {
    format!("sys/queues/{queue_url}/receipt_handles/{receipt_handle}/checkpoint")
}

#[inline]
#[must_use]
pub fn key_visibility_pointer(queue_url: &str) -> String {
    format!("sys/queues/{queue_url}/visibility-pointer")
}

#[inline]
#[must_use]
pub fn visibility_key(timestamp: TimestampMillis, message_id: &MessageId) -> String {
    format!("{:013}:{message_id}", *timestamp)
}

#[must_use]
pub fn visibility_index_key_path(queue_url: &str, visibility_key: &MessageVisibilityKey) -> String {
    format!("sys/queues/{queue_url}/visibility/{visibility_key}")
}
