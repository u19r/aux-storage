use storage_types::{
    IndexerDeclaration, ItemHistoryAddress, ItemHistoryChange, ItemHistoryEntry, ItemHistoryKind,
    ItemHistoryMetadata, ItemHistoryPublication, ItemStreamVersion, MaxIndexers, StreamItemId,
    StreamName, SystemStreamPosition, SystemStreamScope, TimestampMillis, WireItem,
};

use crate::{StreamDataType, StreamItem, StreamRowValue, StreamWriteKind};

#[test]
fn given_history_envelope_when_only_metadata_is_loaded_then_payload_is_unneeded_and_framing_is_exact()
 {
    let history = ItemHistoryAddress {
        history_id: [7; 16].into(),
        stream_name: StreamName::new(b"orders/stream-item/key"),
    };
    let version = ItemStreamVersion::new(2);
    let timestamp = TimestampMillis::from_timestamp(100);
    let metadata = ItemHistoryMetadata::try_new(
        history.clone(),
        version,
        Some(ItemStreamVersion::new(1)),
        ItemHistoryPublication::Event {
            kind: ItemHistoryKind::Image,
            source_scope: SystemStreamScope::try_new("test", 1).unwrap(),
            table_position: SystemStreamPosition::try_new("shard", "lineage", "2").unwrap(),
            created_at: timestamp,
            user_identity: None,
        },
    )
    .unwrap();
    let entry = ItemHistoryEntry::try_new(
        metadata.clone(),
        ItemHistoryChange::Image {
            item: WireItem::dynamo_json(
                serde_json::to_vec(
                    &serde_json::json!({"pk": {"S": "key"}, "body": {"S": "x".repeat(50_000)}}),
                )
                .unwrap(),
            ),
            indexers: IndexerDeclaration::try_new(Vec::new(), MaxIndexers::ZERO).unwrap(),
        },
    )
    .unwrap();
    let row = StreamItem {
        id: StreamItemId::from(version),
        stream_name: Some(history.physical_stream_name().unwrap()),
        data: entry.encode().unwrap(),
        data_type: StreamDataType::DynamoDbJson,
        created_at: timestamp,
    };
    let mut bytes = StreamRowValue::from_planned(StreamWriteKind::Item(row.clone()))
        .unwrap()
        .to_bytes()
        .unwrap();
    let bound = StreamRowValue::history_metadata_prefix_bytes(&history)
        .unwrap()
        .get();
    assert!(bound < bytes.len(), "test must omit payload bytes");
    *bytes.last_mut().unwrap() = b'!';
    assert_eq!(
        StreamRowValue::decode_history_header_prefix(
            &bytes[..bound],
            bytes.len(),
            (&history, version)
        )
        .unwrap()
        .into_metadata(),
        metadata
    );
    assert!(
        StreamRowValue::decode_history_header_prefix(
            &bytes[..bound],
            bytes.len() + 1,
            (&history, version)
        )
        .is_err()
    );
    let wrong = ItemHistoryAddress {
        history_id: [8; 16].into(),
        ..history.clone()
    };
    assert!(
        StreamRowValue::decode_history_header_prefix(
            &bytes[..bound],
            bytes.len(),
            (&wrong, version)
        )
        .is_err()
    );
    for offset in [4, 12, 20] {
        let mut corrupt = bytes[..bound].to_vec();
        corrupt[offset] ^= 1;
        assert!(
            StreamRowValue::decode_history_header_prefix(
                &corrupt,
                bytes.len(),
                (&history, version)
            )
            .is_err()
        );
    }
    let mut short_body = bytes[..bound].to_vec();
    let payload = 21 + history.physical_stream_name().unwrap().as_ref().len();
    short_body[payload + 8] ^= 1;
    assert!(
        StreamRowValue::decode_history_header_prefix(&short_body, bytes.len(), (&history, version))
            .is_err()
    );
    let mut invalid_image = row;
    *invalid_image.data.last_mut().unwrap() = b'!';
    assert!(crate::item_history::decode_item_history(&invalid_image, usize::MAX).is_err());
}

#[test]
fn given_unpublished_anchor_when_envelope_metadata_is_read_then_no_event_time_is_required() {
    let history = ItemHistoryAddress {
        history_id: [7; 16].into(),
        stream_name: StreamName::new(b"orders/stream-item/key"),
    };
    let version = ItemStreamVersion::new(1);
    let metadata = ItemHistoryMetadata::try_new(
        history.clone(),
        version,
        None,
        ItemHistoryPublication::PredecessorAnchor,
    )
    .unwrap();
    let item = WireItem::dynamo_json(br#"{"pk":{"S":"key"}}"#.to_vec());
    let entry = ItemHistoryEntry::try_new(
        metadata.clone(),
        ItemHistoryChange::Image {
            item,
            indexers: IndexerDeclaration::try_new(Vec::new(), MaxIndexers::ZERO).unwrap(),
        },
    )
    .unwrap();
    let row = StreamItem {
        id: StreamItemId::from(version),
        stream_name: Some(history.physical_stream_name().unwrap()),
        data: entry.encode().unwrap(),
        data_type: StreamDataType::DynamoDbJson,
        created_at: TimestampMillis::from_timestamp(0),
    };
    let bytes = StreamRowValue::from_planned(StreamWriteKind::Item(row))
        .unwrap()
        .to_bytes()
        .unwrap();
    assert_eq!(
        StreamRowValue::decode_history_header_prefix(&bytes, bytes.len(), (&history, version))
            .unwrap()
            .into_metadata(),
        metadata
    );
}
