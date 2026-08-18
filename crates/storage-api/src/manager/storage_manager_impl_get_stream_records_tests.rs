use std::collections::HashMap;

use storage::DatabaseManager;
use storage_types::{
    AttributeValue, ItemStreamVersion, KeySchemaElement, KeyType, StreamItemId, StreamName,
    TableName, TimestampMillis,
};
use stream_provider::{StreamDataType, StreamItem, StreamPointer};

use crate::manager::storage_manager_impl_get_stream_records::system_stream_record;

#[tokio::test]
async fn given_missing_delete_tombstone_when_system_record_is_built_then_keys_are_preserved() {
    let database = DatabaseManager::new_for_test()
        .await
        .expect("create test database");
    let key_image = HashMap::from([("pk".to_string(), AttributeValue::S("absent".to_string()))]);
    let pointer_id = StreamItemId::from([1; 12]);
    let pointer = StreamPointer {
        indexers: Vec::new(),
        old_indexers: None,
        stream_name: StreamName::new(b"missing-delete/stream-item/absent"),
        table_name: TableName::new("missing-delete"),
        item_stream_version: ItemStreamVersion::new(1),
        stream_item_id: pointer_id,
    };
    let images = [StreamItem {
        id: StreamItemId::from(ItemStreamVersion::new(1)),
        stream_name: None,
        data: storage_types::storage_serde::to_bytes(&key_image).expect("encode key image"),
        data_type: StreamDataType::DeleteMarker,
        created_at: TimestampMillis::now(),
    }];
    let key_schema = [KeySchemaElement {
        attribute_name: "pk".to_string(),
        key_type: KeyType::Hash,
    }];

    let record = system_stream_record(
        database.initialization_stream_provider().as_ref(),
        pointer,
        &images,
        &key_schema,
    )
    .expect("build tombstone record");

    assert_eq!(record.keys, key_image);
    assert!(record.new_image.is_none());
    assert!(record.old_image.is_none());
    assert_eq!(record.sequence_number, pointer_id.to_string());
}
