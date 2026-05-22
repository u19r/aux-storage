use storage_types::{ItemStreamVersion, StreamName, TableName};

use crate::{EmbeddedStreamItem, StoredStreamPointer, StreamDataType};

#[test]
fn stored_stream_pointer_serializes_required_item_stream_version() {
    let pointer = StoredStreamPointer::embedded(
        StreamName::new(b"item-stream"),
        TableName::new("test_table"),
        ItemStreamVersion::new(7),
        vec![EmbeddedStreamItem {
            data: b"item".to_vec(),
            data_type: StreamDataType::DynamoDbJson,
        }],
    );

    let value = serde_json::to_value(&pointer).expect("pointer should serialize");

    assert_eq!(value["item_stream_version"], serde_json::json!(7));
    assert_eq!(
        pointer.target_item_stream_version(),
        ItemStreamVersion::new(7)
    );
}

#[test]
fn stored_stream_pointer_rejects_old_format_without_item_stream_version() {
    let pointer = StoredStreamPointer::pointer(
        StreamName::new(b"item-stream"),
        TableName::new("test_table"),
        ItemStreamVersion::new(7),
    );
    let mut old_format = serde_json::to_value(pointer).expect("pointer should serialize");
    old_format
        .as_object_mut()
        .expect("pointer should serialize as object")
        .remove("item_stream_version");

    let err = serde_json::from_value::<StoredStreamPointer>(old_format)
        .expect_err("old-format pointers must be rejected");

    assert!(err.to_string().contains("item_stream_version"));
}
