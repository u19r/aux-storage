use std::{sync::Arc, time::Duration};

use storage_types::{StoredTableInfo, TableName, TableStatus, TimestampMillis};

use crate::{constants::TABLE_METADATA_HOT_CACHE_TTL_MILLIS, sorted_kv::TableMetadataHotCache};

fn table_info(table_name: TableName) -> Arc<StoredTableInfo> {
    Arc::new(StoredTableInfo {
        table_name,
        table_status: TableStatus::Active,
        created_at: TimestampMillis::from_timestamp(0),
        attribute_definitions: Vec::new(),
        key_schema: Vec::new(),
        global_secondary_indexes: None,
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
        table_stream_duration: storage_types::StreamRetentionDuration::default(),
        default_item_stream_duration: storage_types::StreamRetentionDuration::default(),
        deletion_protection_enabled: false,
    })
}

#[test]
fn table_metadata_hot_cache_removes_invalidated_entries() {
    let cache = TableMetadataHotCache::new();
    let table_name = TableName::new("hot-table");

    cache.insert(table_name.clone(), table_info(table_name.clone()));
    assert!(cache.get(&table_name).is_some());

    cache.remove(&table_name);
    assert!(cache.get(&table_name).is_none());
}

#[test]
fn table_metadata_hot_cache_expires_entries() {
    let cache = TableMetadataHotCache::new();
    let table_name = TableName::new("expires-table");

    cache.insert(table_name.clone(), table_info(table_name.clone()));
    std::thread::sleep(Duration::from_millis(
        TABLE_METADATA_HOT_CACHE_TTL_MILLIS.saturating_add(1),
    ));

    assert!(cache.get(&table_name).is_none());
}
