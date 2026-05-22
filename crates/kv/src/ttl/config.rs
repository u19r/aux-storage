pub use storage_common::ttl::{
    TtlConfigRecord, TtlSweepLock, augment_item_with_ttl_partition, compute_ttl_partition_value,
    is_ttl_index, parse_ttl_index_key, shard_to_string, ttl_gsi_name, ttl_index_key,
    ttl_index_key_for_item, ttl_index_key_map_from_token, ttl_index_key_token_for_item,
    ttl_index_prefix, ttl_index_range_end, ttl_index_range_start, ttl_value_from_item,
};
use storage_types::{AttributeValue, IndexName, ItemKey, StorageResult, TableName};

use crate::keys::TABLES_PREFIX;

const TTL_CONFIG_SUFFIX: &str = "/ttl-config";

#[must_use]
pub fn ttl_config_key(table_name: &TableName) -> Vec<u8> {
    let mut key = TABLES_PREFIX.as_bytes().to_vec();
    key.extend_from_slice(table_name.as_ref().as_bytes());
    key.extend_from_slice(TTL_CONFIG_SUFFIX.as_bytes());
    key
}

pub fn shard_prefix(
    table_name: &TableName,
    gsi_name: &IndexName,
    shard: u8,
) -> StorageResult<Vec<u8>> {
    let mut prefix = ItemKey::index_prefix_from_name(table_name, gsi_name);
    let shard_value = AttributeValue::S(shard_to_string(shard));
    let serialized = ItemKey::serialize_attribute_value_to_bytes(&shard_value)?;
    let length = match u16::try_from(serialized.len()) {
        Ok(value) => value.min(1023),
        Err(_) => 1023,
    };
    let prefix_bytes = (length << 6).to_be_bytes();
    prefix.extend_from_slice(&prefix_bytes);
    prefix.extend_from_slice(&serialized);
    Ok(prefix)
}
