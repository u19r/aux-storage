use serde_json::json;

use crate::{StoredTableInfo, StreamRetentionDuration};

#[test]
fn stored_table_info_defaults_stream_duration_metadata_when_omitted() {
    let payload = json!({
        "table_name": "TestTable",
        "table_status": "ACTIVE",
        "created_at": 1_700_000_000_000i64,
        "attribute_definitions": [],
        "key_schema": [],
        "global_secondary_indexes": null,
        "table_size_bytes": 0,
        "item_count": 0,
        "max_indexers": 0,
        "stream_specification": null
    });

    let table_info: StoredTableInfo =
        serde_json::from_value(payload).expect("metadata should deserialize");

    assert_eq!(
        table_info.table_stream_duration,
        StreamRetentionDuration::FiniteHours(72)
    );
    assert_eq!(
        table_info.default_item_stream_duration,
        StreamRetentionDuration::FiniteHours(72)
    );
}

#[test]
fn stored_table_info_serializes_finite_and_forever_stream_duration_metadata() {
    let payload = json!({
        "table_name": "TestTable",
        "table_status": "ACTIVE",
        "created_at": 1_700_000_000_000i64,
        "attribute_definitions": [],
        "key_schema": [],
        "global_secondary_indexes": null,
        "table_size_bytes": 0,
        "item_count": 0,
        "max_indexers": 0,
        "stream_specification": null,
        "table_stream_duration": 24,
        "default_item_stream_duration": -1
    });

    let table_info: StoredTableInfo =
        serde_json::from_value(payload).expect("metadata should deserialize");
    let encoded = serde_json::to_value(&table_info).expect("metadata should serialize");

    assert_eq!(
        table_info.table_stream_duration,
        StreamRetentionDuration::FiniteHours(24)
    );
    assert_eq!(
        table_info.default_item_stream_duration,
        StreamRetentionDuration::Forever
    );
    assert_eq!(encoded["table_stream_duration"], json!(24));
    assert_eq!(encoded["default_item_stream_duration"], json!(-1));
}
