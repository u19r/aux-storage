use storage_types::{ItemStreamVersion, StreamName, TableName};

use crate::{EmbeddedStreamItem, StoredStreamPointer, StreamDataType};

#[test]
fn stored_stream_pointer_serializes_required_item_stream_version() {
    let pointer = StoredStreamPointer::embedded(
        storage_types::ItemHistoryAddress {
            history_id: 1u32.into(),
            stream_name: StreamName::new(b"item-stream"),
        },
        TableName::new("test_table"),
        storage_types::ItemWriteVersions {
            item_revision: storage_types::ItemRevision::initial(),
            item_stream_version: ItemStreamVersion::new(7),
        },
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
        storage_types::ItemHistoryAddress {
            history_id: 1u32.into(),
            stream_name: StreamName::new(b"item-stream"),
        },
        TableName::new("test_table"),
        storage_types::ItemWriteVersions {
            item_revision: storage_types::ItemRevision::initial(),
            item_stream_version: ItemStreamVersion::new(7),
        },
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

#[test]
fn stored_pointer_requires_a_positive_item_revision_separate_from_stream_version() {
    let pointer = StoredStreamPointer::pointer(
        storage_types::ItemHistoryAddress {
            history_id: 1u32.into(),
            stream_name: StreamName::new(b"item-stream"),
        },
        TableName::new("test_table"),
        storage_types::ItemWriteVersions {
            item_revision: storage_types::ItemRevision::try_from(3_u64).expect("revision"),
            item_stream_version: ItemStreamVersion::new(900_000),
        },
    );
    let mut value = serde_json::to_value(pointer).expect("serialize pointer");
    assert_eq!(value["item_revision"], 3);
    assert_eq!(value["item_stream_version"], 900_000);
    value["item_revision"] = serde_json::json!(0);
    assert!(serde_json::from_value::<StoredStreamPointer>(value.clone()).is_err());
    value
        .as_object_mut()
        .expect("object")
        .remove("item_revision");
    assert!(serde_json::from_value::<StoredStreamPointer>(value).is_err());
}
