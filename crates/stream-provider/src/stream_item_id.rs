//! Shared ordered IDs for stream rows written by the new SQL and KV stores.

use storage_types::StreamItemId;

#[must_use]
pub fn sequenced_stream_item_id(counter: u64) -> StreamItemId {
    let mut bytes = [0_u8; 12];
    bytes[..4].copy_from_slice(b"SEQ1");
    bytes[4..].copy_from_slice(&counter.to_be_bytes());
    StreamItemId::from(bytes)
}
