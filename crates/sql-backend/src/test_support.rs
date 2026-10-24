//! Catalog fixture shared by SQL statement unit tests.

use std::sync::Arc;

use storage_types::{BoundTableInfo, StoredTableInfo, TableName, TableStatus, TimestampMillis};

pub(crate) fn table() -> BoundTableInfo {
    BoundTableInfo {
        history_id: uuid::Uuid::from_bytes([1; 16]).into(),
        info: Arc::new(StoredTableInfo {
            table_name: TableName::new("table"),
            table_status: TableStatus::Active,
            created_at: TimestampMillis::now(),
            attribute_definitions: vec![],
            key_schema: vec![],
            max_indexers: storage_types::MaxIndexers::ZERO,
            global_secondary_indexes: None,
            table_size_bytes: 0,
            item_count: 0,
            stream_specification: None,
            table_stream_duration: Default::default(),
            default_item_stream_duration: Default::default(),
            deletion_protection_enabled: false,
        }),
    }
}
