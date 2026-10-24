//! Change-index slot routing shared by KV and SQL backends.
//!
//! The change index fans table writes across a fixed ring of slots so
//! ordered-log consumers partition progress without hot spots. Both the
//! slot hash and the slot count live once, here; backends only encode
//! their own marker keys and rows around the assigned slot.

use storage_types::{StreamItemId, TableName};

/// Change-index slot ring size shared by every backend.
pub const CHANGE_INDEX_SLOT_COUNT: u16 = 256;

/// FNV-1a slot for one table. Stable across backends so KV and SQL writers
/// route the same table to the same slot.
#[must_use]
pub fn change_index_slot(table_name: &TableName) -> u16 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET;
    for byte in table_name.as_ref().as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }

    (hash % u64::from(CHANGE_INDEX_SLOT_COUNT)) as u16
}

/// Zero-padded versionstamp sorting lexically in numeric order.
#[must_use]
pub fn sortable_version(stream_item_id: StreamItemId) -> String {
    stream_item_id.to_string()
}
