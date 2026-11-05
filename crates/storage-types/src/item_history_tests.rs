use crate::{
    AttributeValue, ItemHistoryAddress, ItemHistoryChange, ItemHistoryEntry, ItemHistoryKind,
    ItemHistoryMetadata, ItemHistoryPublication, ItemStreamVersion, KeyAttributes, StreamName,
    SystemStreamPosition, SystemStreamScope, TableHistoryId, TableName, TimestampMillis,
};

fn address() -> ItemHistoryAddress {
    ItemHistoryAddress {
        history_id: TableHistoryId::from([7; 16]),
        stream_name: StreamName::new(b"orders/stream-item/item-1"),
    }
}

fn publication(kind: ItemHistoryKind) -> ItemHistoryPublication {
    ItemHistoryPublication::Event {
        kind,
        source_scope: SystemStreamScope::try_new("test", 1).unwrap(),
        table_position: SystemStreamPosition::try_new("shard", "lineage", "1").unwrap(),
        created_at: TimestampMillis::from_timestamp(1000),
        user_identity: None,
    }
}

#[test]
fn given_wide_predecessor_when_pointer_is_encoded_then_byte_admission_and_exact_identity_survive() {
    let metadata = ItemHistoryMetadata::try_new(
        address(),
        ItemStreamVersion::new(10),
        Some(ItemStreamVersion::new(3)),
        publication(ItemHistoryKind::Image),
    )
    .unwrap();
    let pointer =
        crate::ItemHistoryPointer::try_new(metadata.clone(), 600, Some(1_200_000)).unwrap();
    let bytes = serde_json::to_vec(&pointer).unwrap();
    let decoded: crate::ItemHistoryPointer = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(decoded, pointer);
    assert_eq!(decoded.metadata(), &metadata);
    assert_eq!(decoded.record_bytes(), 600);
    assert_eq!(
        decoded.predecessor_record_bytes_bound(),
        Some(1_200_000 + crate::ItemHistoryRecord::metadata_length_bound())
    );
    assert!(bytes.len() < 1000, "the pointer carries no image");
    for (record_bytes, previous_bytes) in [
        (0, Some(1)),
        (600, None),
        (600, Some(0)),
        (usize::MAX, Some(1)),
        (600, Some(usize::MAX)),
    ] {
        assert!(
            crate::ItemHistoryPointer::try_new(metadata.clone(), record_bytes, previous_bytes)
                .is_err()
        );
    }
    let mut malformed = serde_json::to_value(&pointer).unwrap();
    malformed["predecessor_payload_bytes"] = serde_json::Value::Null;
    assert!(serde_json::from_value::<crate::ItemHistoryPointer>(malformed).is_err());
}

#[test]
fn given_insert_or_anchor_when_pointer_is_prepared_then_only_published_events_are_indexed() {
    for publication in [
        publication(ItemHistoryKind::Image),
        ItemHistoryPublication::PredecessorAnchor,
    ] {
        let is_event = matches!(publication, ItemHistoryPublication::Event { .. });
        let metadata =
            ItemHistoryMetadata::try_new(address(), ItemStreamVersion::new(1), None, publication)
                .unwrap();
        let pointer = crate::ItemHistoryPointer::try_new(metadata.clone(), 600, None);
        assert_eq!(pointer.is_ok(), is_event);
        if let Ok(pointer) = pointer {
            assert_eq!(pointer.predecessor_record_bytes_bound(), None);
        }
        assert!(crate::ItemHistoryPointer::try_new(metadata, 600, Some(100)).is_err());
    }
}

#[test]
fn given_nonadjacent_predecessor_when_history_is_prepared_then_exact_version_is_kept() {
    let metadata = ItemHistoryMetadata::try_new(
        address(),
        ItemStreamVersion::new(10),
        Some(ItemStreamVersion::new(3)),
        publication(ItemHistoryKind::Image),
    )
    .unwrap();
    assert_eq!(metadata.previous_version(), Some(ItemStreamVersion::new(3)));
    assert_eq!(metadata.version(), ItemStreamVersion::new(10));
}

#[test]
fn given_invalid_versions_when_history_is_prepared_then_metadata_is_rejected() {
    for (version, previous) in [(0, None), (1, Some(0)), (4, Some(4)), (4, Some(5))] {
        assert!(
            ItemHistoryMetadata::try_new(
                address(),
                ItemStreamVersion::new(version),
                previous.map(ItemStreamVersion::new),
                publication(ItemHistoryKind::Image),
            )
            .is_err()
        );
    }
    let table_address = ItemHistoryAddress {
        stream_name: StreamName::table_stream(&TableName::new("orders")),
        ..address()
    };
    assert!(
        ItemHistoryMetadata::try_new(
            table_address,
            ItemStreamVersion::new(1),
            None,
            publication(ItemHistoryKind::Image)
        )
        .is_err()
    );
}

#[test]
fn given_imported_anchor_when_metadata_is_prepared_then_no_event_or_predecessor_is_invented() {
    let metadata = ItemHistoryMetadata::try_new(
        address(),
        ItemStreamVersion::new(5),
        None,
        ItemHistoryPublication::PredecessorAnchor,
    )
    .unwrap();
    assert!(matches!(
        metadata.publication(),
        ItemHistoryPublication::PredecessorAnchor
    ));
    assert!(
        ItemHistoryMetadata::try_new(
            address(),
            ItemStreamVersion::new(5),
            Some(ItemStreamVersion::new(3)),
            ItemHistoryPublication::PredecessorAnchor,
        )
        .is_err()
    );
    let deleted = ItemHistoryChange::Deleted {
        key: KeyAttributes::from([("pk".to_owned(), AttributeValue::S("a".to_owned()))]),
    };
    assert!(ItemHistoryEntry::try_new(metadata, deleted).is_err());
}

#[test]
fn given_delete_marker_when_key_is_absent_then_entry_is_rejected() {
    let metadata = ItemHistoryMetadata::try_new(
        address(),
        ItemStreamVersion::new(2),
        Some(ItemStreamVersion::new(1)),
        publication(ItemHistoryKind::Deleted),
    )
    .unwrap();
    assert!(
        ItemHistoryEntry::try_new(
            metadata,
            ItemHistoryChange::Deleted {
                key: KeyAttributes::default()
            }
        )
        .is_err()
    );
}

#[test]
fn given_materialized_history_when_metadata_is_read_then_image_is_not_deserialized() {
    use crate::{IndexerDeclaration, ItemHistoryRecord, MaxIndexers, WireItem};
    let metadata = ItemHistoryMetadata::try_new(
        address(),
        ItemStreamVersion::new(9),
        Some(ItemStreamVersion::new(2)),
        publication(ItemHistoryKind::Image),
    )
    .unwrap();
    let entry = ItemHistoryEntry::try_new(
        metadata,
        ItemHistoryChange::Image {
            item: WireItem::dynamo_json(br#"{"pk":{"S":"a"},"value":{"S":"snapshot"}}"#.to_vec()),
            indexers: IndexerDeclaration::try_new(Vec::new(), MaxIndexers::ZERO).unwrap(),
        },
    )
    .unwrap();
    let bytes = entry.encode().unwrap();
    let record =
        ItemHistoryRecord::decode(&bytes, (&address(), ItemStreamVersion::new(9)), bytes.len())
            .unwrap();
    let physical = address().physical_stream_name().unwrap();
    let native = ItemHistoryRecord::decode_physical(
        &bytes,
        (&physical, ItemStreamVersion::new(9)),
        bytes.len(),
    )
    .unwrap();
    assert_eq!(native.metadata(), record.metadata());
    let wrong_history = ItemHistoryAddress {
        history_id: [8; 16].into(),
        ..address()
    };
    assert!(
        ItemHistoryRecord::decode_physical(
            &bytes,
            (
                &wrong_history.physical_stream_name().unwrap(),
                ItemStreamVersion::new(9)
            ),
            bytes.len(),
        )
        .is_err()
    );
    assert!(
        ItemHistoryRecord::decode_physical(
            &bytes,
            (&physical, ItemStreamVersion::new(8)),
            bytes.len(),
        )
        .is_err()
    );
    assert_eq!(
        record.metadata().previous_version(),
        Some(ItemStreamVersion::new(2))
    );
    let metadata_bytes = u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize;
    let boundary = crate::constants::ITEM_HISTORY_RECORD_HEADER_BYTES + metadata_bytes;
    let header = ItemHistoryRecord::decode_header_prefix(
        &bytes[..boundary],
        (&address(), ItemStreamVersion::new(9)),
        bytes.len(),
    )
    .unwrap();
    assert_eq!(header.record_bytes(), bytes.len());
    assert_eq!(header.payload_bytes(), bytes.len() - boundary);
    assert_eq!(
        ItemHistoryRecord::decode_header_prefix(
            &bytes[..boundary],
            (&address(), ItemStreamVersion::new(9)),
            bytes.len(),
        )
        .unwrap()
        .into_metadata(),
        *record.metadata(),
    );
    assert!(boundary <= ItemHistoryRecord::metadata_length_bound());
    for length in [0, 20, boundary - 1] {
        assert!(
            ItemHistoryRecord::decode_header_prefix(
                &bytes[..length],
                (&address(), ItemStreamVersion::new(9)),
                bytes.len(),
            )
            .is_err()
        );
    }
    let mut oversized_metadata = bytes.clone();
    oversized_metadata[1..5].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(
        ItemHistoryRecord::decode_header_prefix(
            &oversized_metadata,
            (&address(), ItemStreamVersion::new(9)),
            usize::MAX,
        )
        .is_err()
    );
    let decoded = record.into_entry(MaxIndexers::ZERO).unwrap();
    let ItemHistoryChange::Image { item, .. } = decoded.change() else {
        panic!("snapshot image")
    };
    assert_eq!(
        item.attribute_value("value").unwrap(),
        Some(AttributeValue::S("snapshot".to_owned()))
    );
    let mut damaged = bytes.clone();
    *damaged.last_mut().unwrap() = b'!';
    let metadata_only = ItemHistoryRecord::decode(
        &damaged,
        (&address(), ItemStreamVersion::new(9)),
        damaged.len(),
    )
    .unwrap();
    assert_eq!(
        metadata_only.metadata().version(),
        ItemStreamVersion::new(9)
    );
    assert!(metadata_only.into_entry(MaxIndexers::ZERO).is_err());
    assert!(
        ItemHistoryRecord::decode(&bytes, (&address(), ItemStreamVersion::new(8)), bytes.len())
            .is_err()
    );
    assert!(
        ItemHistoryRecord::decode(
            &bytes,
            (&address(), ItemStreamVersion::new(9)),
            bytes.len() - 1
        )
        .is_err()
    );
    assert!(
        ItemHistoryRecord::decode(
            &bytes[..bytes.len() - 1],
            (&address(), ItemStreamVersion::new(9)),
            bytes.len()
        )
        .is_err()
    );
    let other = ItemHistoryAddress {
        history_id: TableHistoryId::from([8; 16]),
        ..address()
    };
    assert!(
        ItemHistoryRecord::decode(&bytes, (&other, ItemStreamVersion::new(9)), bytes.len())
            .is_err()
    );
}

#[test]
fn given_deleted_history_when_round_tripped_then_marker_has_only_its_key() {
    use crate::{ItemHistoryRecord, MaxIndexers};
    let metadata = ItemHistoryMetadata::try_new(
        address(),
        ItemStreamVersion::new(4),
        Some(ItemStreamVersion::new(3)),
        publication(ItemHistoryKind::Deleted),
    )
    .unwrap();
    let key = KeyAttributes::from([("pk".to_owned(), AttributeValue::S("a".to_owned()))]);
    let bytes =
        ItemHistoryEntry::try_new(metadata, ItemHistoryChange::Deleted { key: key.clone() })
            .unwrap()
            .encode()
            .unwrap();
    let entry =
        ItemHistoryRecord::decode(&bytes, (&address(), ItemStreamVersion::new(4)), bytes.len())
            .unwrap()
            .into_entry(MaxIndexers::ZERO)
            .unwrap();
    let ItemHistoryChange::Deleted { key: decoded } = entry.change() else {
        panic!("delete marker")
    };
    assert_eq!(decoded, &key);
}

#[test]
fn given_pending_native_publication_when_commit_stamp_is_bound_then_metadata_uses_committed_position()
 {
    use crate::{IndexerDeclaration, ItemHistoryRecord, MaxIndexers, WireItem};
    let pending = [255; 12];
    let base = SystemStreamPosition::try_new("shard", "lineage", "1").unwrap();
    let metadata = ItemHistoryMetadata::try_new(
        address(),
        ItemStreamVersion::new(1),
        None,
        ItemHistoryPublication::Event {
            kind: ItemHistoryKind::Image,
            source_scope: SystemStreamScope::try_new("test", 1).unwrap(),
            table_position: base.with_versionstamp(pending).unwrap(),
            created_at: TimestampMillis::from_timestamp(1000),
            user_identity: None,
        },
    )
    .unwrap();
    let entry = ItemHistoryEntry::try_new(
        metadata,
        ItemHistoryChange::Image {
            item: WireItem::dynamo_json(br#"{"pk":{"S":"a"}}"#.to_vec()),
            indexers: IndexerDeclaration::try_new(Vec::new(), MaxIndexers::ZERO).unwrap(),
        },
    )
    .unwrap();
    let mut bytes = entry.encode().unwrap();
    let start = ItemHistoryRecord::VERSIONSTAMP_OFFSET as usize;
    let mut committed = [0; 12];
    committed[7] = 42;
    committed[10..].copy_from_slice(&pending[10..]);
    bytes[start..start + 10].copy_from_slice(&committed[..10]);
    let record =
        ItemHistoryRecord::decode(&bytes, (&address(), ItemStreamVersion::new(1)), bytes.len())
            .unwrap();
    let ItemHistoryPublication::Event { table_position, .. } = record.metadata().publication()
    else {
        panic!("event")
    };
    assert_eq!(table_position.offset().versionstamp(), Some(committed));
    bytes[start + 11] = 0;
    assert!(
        ItemHistoryRecord::decode(&bytes, (&address(), ItemStreamVersion::new(1)), bytes.len())
            .is_err()
    );
}

#[test]
fn given_maximum_history_metadata_when_prefix_is_encoded_then_native_read_bound_admits_it() {
    use crate::{IndexerDeclaration, ItemHistoryRecord, MaxIndexers, WireItem};
    let mut stream = vec![b't'; crate::constants::SYSTEM_STREAM_SOURCE_TABLE_NAME_MAX_BYTES];
    stream.extend_from_slice(StreamName::ITEM_STREAM_SEGMENT);
    stream.extend_from_slice(&vec![255; crate::constants::STREAM_ITEM_KEY_MAX_BYTES]);
    let address = ItemHistoryAddress {
        history_id: TableHistoryId::from([255; 16]),
        stream_name: StreamName::new(&stream),
    };
    let metadata = ItemHistoryMetadata::try_new(
        address.clone(),
        ItemStreamVersion::new(u64::MAX),
        Some(ItemStreamVersion::new(u64::MAX - 1)),
        ItemHistoryPublication::Event {
            kind: ItemHistoryKind::Image,
            source_scope: SystemStreamScope::try_new(
                "\"".repeat(crate::constants::SYSTEM_STREAM_SCOPE_ID_MAX_BYTES),
                u64::MAX,
            )
            .unwrap(),
            table_position: SystemStreamPosition::try_new(
                "\"".repeat(crate::constants::SYSTEM_STREAM_SHARD_ID_MAX_BYTES),
                "\\".repeat(crate::constants::SYSTEM_STREAM_LINEAGE_ID_MAX_BYTES),
                u64::MAX.to_string(),
            )
            .unwrap(),
            created_at: TimestampMillis::from_timestamp(0),
            user_identity: None,
        },
    )
    .unwrap();
    let names = (0..crate::MAX_INDEXERS_CAPACITY)
        .map(|ordinal| format!("{}{}", "\u{0001}".repeat(31), char::from(33 + ordinal)))
        .collect::<Vec<_>>();
    assert_eq!(
        names.iter().map(String::len).sum::<usize>(),
        crate::MAX_INDEXER_DECLARATION_BYTES
    );
    let entry = ItemHistoryEntry::try_new(
        metadata.clone(),
        ItemHistoryChange::Image {
            item: WireItem::dynamo_json(br#"{"pk":{"S":"a"}}"#.to_vec()),
            indexers: IndexerDeclaration::try_new(
                names,
                MaxIndexers::try_new(crate::MAX_INDEXERS_CAPACITY).unwrap(),
            )
            .unwrap(),
        },
    )
    .unwrap();
    let bytes = entry.encode().unwrap();
    let boundary = crate::constants::ITEM_HISTORY_RECORD_HEADER_BYTES
        + u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize;
    assert!(boundary <= ItemHistoryRecord::metadata_length_bound());
    assert_eq!(
        ItemHistoryRecord::decode_header_prefix(
            &bytes[..boundary],
            (&address, ItemStreamVersion::new(u64::MAX)),
            bytes.len(),
        )
        .unwrap()
        .into_metadata(),
        metadata
    );
}

#[test]
fn given_history_reference_when_constructed_or_decoded_then_identity_and_version_are_validated() {
    use crate::ItemHistoryReference;
    let reference = ItemHistoryReference::try_new(address(), ItemStreamVersion::new(1)).unwrap();
    let bytes = serde_json::to_vec(&reference).unwrap();
    let decoded: ItemHistoryReference = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(decoded, reference);
    let mut invalid = serde_json::to_value(&reference).unwrap();
    invalid["version"] = 0.into();
    assert!(serde_json::from_value::<ItemHistoryReference>(invalid).is_err());
    let nil = ItemHistoryAddress {
        history_id: [0; 16].into(),
        ..address()
    };
    assert!(ItemHistoryReference::try_new(nil, ItemStreamVersion::new(1)).is_err());
    let table = ItemHistoryAddress {
        stream_name: StreamName::table_stream(&TableName::new("orders")),
        ..address()
    };
    assert!(ItemHistoryReference::try_new(table, ItemStreamVersion::new(1)).is_err());
}
