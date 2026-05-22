use storage_types::{IndexName, TableName};

const POSTGRES_IDENTIFIER_LIMIT: usize = 63;
const HASH_HEX_LEN: usize = 16;

pub(super) fn physical_table_name(table: &TableName) -> String {
    shorten_with_hash("table", &table.sanitized_name())
}

pub(super) fn physical_gsi_table_name(table: &TableName, index: &IndexName) -> String {
    shorten_with_hash(
        "gsi",
        &format!("{}_{}", table.sanitized_name(), index.sanitized_name()),
    )
}

pub(super) fn physical_ttl_index_table_name(table: &TableName) -> String {
    shorten_with_hash("ttl_index", &table.sanitized_name())
}

fn shorten_with_hash(prefix: &str, name: &str) -> String {
    let full_name = format!("{prefix}_{name}");
    if full_name.len() <= POSTGRES_IDENTIFIER_LIMIT {
        return full_name;
    }

    let hash = fnv1a64_hex(&full_name);
    let separator_bytes = 2;
    let prefix_bytes = prefix.len() + 1;
    let head_len =
        POSTGRES_IDENTIFIER_LIMIT.saturating_sub(prefix_bytes + separator_bytes + HASH_HEX_LEN);
    let head = ascii_prefix(name, head_len);
    format!("{prefix}_{head}_{hash}")
}

fn ascii_prefix(value: &str, max_len: usize) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii())
        .take(max_len)
        .collect()
}

fn fnv1a64_hex(value: &str) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}
