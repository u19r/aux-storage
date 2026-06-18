use storage_types::{StreamItemId, TableName};

pub(crate) const CHANGE_INDEX_SLOT_COUNT: u16 = 256;

pub(crate) fn slot_for_table(table_name: &TableName) -> u16 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET;
    for byte in table_name.as_ref().as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }

    (hash % u64::from(CHANGE_INDEX_SLOT_COUNT)) as u16
}

pub(crate) fn sortable_version(stream_item_id: StreamItemId) -> String {
    stream_item_id.to_string()
}
