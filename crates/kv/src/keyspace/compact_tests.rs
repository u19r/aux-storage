use crate::keyspace::compact::{
    CompactKeyError, CompactKeyMetadata, IndexStorageId, KeyFamily, ParsedCompactKey,
    PartitionControlKind, PubsubRecordKind, QueueMetadataKind, QueueRecordKind, QueueStorageId,
    StreamStorageId, SystemRecordKind, TableStorageId, U48, gsi_backfill_key, gsi_item_key,
    gsi_prefix, gsi_tombstone_key, idempotency_token_key, item_revision_key, parse_compact_key,
    partition_control_key, primary_item_key, primary_item_prefix, pubsub_record_key,
    queue_id_allocator_key, queue_metadata_key, queue_name_lookup_key, queue_ready_prefix,
    queue_record_key, queue_url_lookup_key, stream_high_water_key, stream_trim_due_key,
    stream_trim_state_key, sync_apply_marker_key, sync_last_applied_key, sync_log_entry_key,
    system_stream_key, table_metadata_key, table_name_lookup_key, table_stream_key,
    table_stream_prefix, ttl_config_key, ttl_due_key,
};

const STREAM_ID: [u8; 12] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];

#[test]
fn key_family_registry_round_trips_printable_codes() {
    for (family, _) in KeyFamily::registry() {
        assert_eq!(KeyFamily::from_code(family.code()).unwrap(), *family);
        assert!(family.code().is_ascii_graphic());
    }

    assert_eq!(
        KeyFamily::from_code(b'/'),
        Err(CompactKeyError::UnknownFamily(b'/'))
    );
}

#[test]
fn fixed_width_ids_encode_big_endian() {
    let table = TableStorageId::new(0x0102_0304);
    let index = IndexStorageId::new(0x0506);
    let queue = QueueStorageId::new(0x0000_0a0b_0c0d).unwrap();

    assert_eq!(
        primary_item_key(table, b"pk"),
        b"p\x01\x02\x03\x04pk".to_vec()
    );
    assert_eq!(
        gsi_backfill_key(table, index),
        b"b\x01\x02\x03\x04\x05\x06".to_vec()
    );
    assert_eq!(
        queue_record_key(9, queue, 0x0708, QueueRecordKind::Ready, b"m"),
        b"q\x09\x00\x00\x0a\x0b\x0c\x0d\x07\x08rm".to_vec()
    );
}

#[test]
fn u48_rejects_overflow() {
    assert!(U48::new(0x0000_FFFF_FFFF_FFFF).is_ok());
    assert_eq!(
        U48::new(0x0001_0000_0000_0000),
        Err(CompactKeyError::U48OutOfRange(0x0001_0000_0000_0000))
    );
}

#[test]
fn parser_round_trips_table_and_gsi_keys() {
    let table = TableStorageId::new(42);
    let index = IndexStorageId::new(3);

    assert_eq!(
        parse_compact_key(&table_metadata_key(table)).unwrap(),
        ParsedCompactKey::TableMetadata { table_id: table }
    );
    assert_eq!(
        parse_compact_key(&table_name_lookup_key(b"orders")).unwrap(),
        ParsedCompactKey::TableNameLookup {
            table_name: b"orders"
        }
    );
    assert_eq!(
        parse_compact_key(&primary_item_key(table, b"pk")).unwrap(),
        ParsedCompactKey::PrimaryItem {
            table_id: table,
            key: b"pk"
        }
    );
    assert_eq!(
        parse_compact_key(&gsi_item_key(table, index, b"gpkpk")).unwrap(),
        ParsedCompactKey::GsiItem {
            table_id: table,
            index_id: index,
            suffix: b"gpkpk"
        }
    );
    assert_eq!(
        parse_compact_key(&gsi_tombstone_key(table, index, b"dead")).unwrap(),
        ParsedCompactKey::GsiTombstone {
            table_id: table,
            index_id: index,
            suffix: b"dead"
        }
    );
}

#[test]
fn parser_round_trips_ttl_stream_queue_and_pubsub_keys() {
    let table = TableStorageId::new(42);
    let queue = QueueStorageId::new(7).unwrap();
    let topic = U48::new(8).unwrap();
    let subscription = U48::new(9).unwrap();

    assert_eq!(
        parse_compact_key(&ttl_config_key(table)).unwrap(),
        ParsedCompactKey::TtlConfig { table_id: table }
    );
    assert_eq!(
        parse_compact_key(&ttl_due_key(table, 1_700_000_000, b"pk")).unwrap(),
        ParsedCompactKey::TtlDueIndex {
            table_id: table,
            ttl_seconds: 1_700_000_000,
            key: b"pk"
        }
    );
    assert_eq!(
        parse_compact_key(&system_stream_key(&STREAM_ID)).unwrap(),
        ParsedCompactKey::SystemStreamRow {
            stream_item_id: STREAM_ID.as_slice()
        }
    );
    assert_eq!(
        parse_compact_key(&table_stream_key(table, &STREAM_ID)).unwrap(),
        ParsedCompactKey::TableStreamRow {
            table_id: table,
            stream_item_id: STREAM_ID.as_slice()
        }
    );
    assert_eq!(
        parse_compact_key(&stream_trim_state_key(b"\x74\x00\x00\x00\x2a")).unwrap(),
        ParsedCompactKey::StreamTrimState {
            scope_key: b"\x74\x00\x00\x00\x2a"
        }
    );
    assert_eq!(
        parse_compact_key(&stream_trim_due_key(1_700_000_000_123, b"\x69scope", 9)).unwrap(),
        ParsedCompactKey::StreamTrimDue {
            due_millis: 1_700_000_000_123,
            scope_key: b"\x69scope",
            policy_version: 9
        }
    );
    assert_eq!(
        parse_compact_key(&queue_record_key(
            2,
            queue,
            5,
            QueueRecordKind::Ready,
            b"visible"
        ))
        .unwrap(),
        ParsedCompactKey::PartitionedQueueData {
            bucket: 2,
            queue_id: queue,
            partition_id: 5,
            kind: QueueRecordKind::Ready,
            suffix: b"visible"
        }
    );
    assert_eq!(
        parse_compact_key(&pubsub_record_key(
            PubsubRecordKind::SubscriptionTopic,
            topic,
            Some(subscription),
            b""
        ))
        .unwrap(),
        ParsedCompactKey::PubsubRecord {
            kind: PubsubRecordKind::SubscriptionTopic,
            left_id: topic,
            right_id: Some(subscription),
            suffix: b""
        }
    );
    assert_eq!(
        parse_compact_key(&queue_metadata_key(queue)).unwrap(),
        ParsedCompactKey::QueueMetadata { queue_id: queue }
    );
    assert_eq!(
        parse_compact_key(&queue_url_lookup_key("https://queue.local/orders")).unwrap(),
        ParsedCompactKey::QueueLookup {
            kind: QueueMetadataKind::UrlLookup,
            lookup_key: b"https://queue.local/orders"
        }
    );
    assert_eq!(
        parse_compact_key(&queue_name_lookup_key("orders")).unwrap(),
        ParsedCompactKey::QueueLookup {
            kind: QueueMetadataKind::NameLookup,
            lookup_key: b"orders"
        }
    );
    assert_eq!(
        parse_compact_key(&stream_high_water_key()).unwrap(),
        ParsedCompactKey::SyncRecord {
            kind: SystemRecordKind::StreamHighWater,
            suffix: b""
        }
    );
    let idempotency_key = idempotency_token_key("request-token-000000000001");
    assert!(
        !idempotency_key
            .windows(b"request-token".len())
            .any(|window| window == b"request-token")
    );
    assert_eq!(
        parse_compact_key(&idempotency_key).unwrap(),
        ParsedCompactKey::SyncRecord {
            kind: SystemRecordKind::IdempotencyToken,
            suffix: &idempotency_key[2..]
        }
    );
    let sync_apply_key = sync_apply_marker_key("orders-by-region-2026/000000000001");
    assert!(
        !sync_apply_key
            .windows(b"orders-by-region".len())
            .any(|window| window == b"orders-by-region")
    );
    assert_eq!(
        parse_compact_key(&sync_apply_key).unwrap(),
        ParsedCompactKey::SyncRecord {
            kind: SystemRecordKind::SyncApplyMutation,
            suffix: &sync_apply_key[2..]
        }
    );
    assert_eq!(
        parse_compact_key(&sync_last_applied_key()).unwrap(),
        ParsedCompactKey::SyncRecord {
            kind: SystemRecordKind::SyncLastApplied,
            suffix: b""
        }
    );
    let item_revision = item_revision_key("orders-by-region-2026", r#"{"pk":{"S":"order-1"}}"#);
    assert!(
        !item_revision
            .windows(b"orders-by-region".len())
            .any(|window| window == b"orders-by-region")
    );
    assert!(
        !item_revision
            .windows(b"order-1".len())
            .any(|window| window == b"order-1")
    );
    assert_eq!(
        parse_compact_key(&item_revision).unwrap(),
        ParsedCompactKey::SyncRecord {
            kind: SystemRecordKind::ItemRevision,
            suffix: &item_revision[2..]
        }
    );
    assert_eq!(
        parse_compact_key(&queue_id_allocator_key()).unwrap(),
        ParsedCompactKey::SyncRecord {
            kind: SystemRecordKind::QueueIdAllocator,
            suffix: b""
        }
    );
    let sync_log_key = sync_log_entry_key(3, 9);
    assert_eq!(
        parse_compact_key(&sync_log_key).unwrap(),
        ParsedCompactKey::SyncRecord {
            kind: SystemRecordKind::SyncLogEntry,
            suffix: &sync_log_key[2..]
        }
    );
    let stream = StreamStorageId::new(99).unwrap();
    assert_eq!(
        parse_compact_key(&partition_control_key(
            PartitionControlKind::PartitionInfo,
            stream,
            b"\x00\x07"
        ))
        .unwrap(),
        ParsedCompactKey::PartitionControl {
            kind: PartitionControlKind::PartitionInfo,
            resource_id: stream,
            suffix: b"\x00\x07"
        }
    );
}

#[test]
fn parser_rejects_truncated_and_invalid_kind_keys() {
    assert_eq!(parse_compact_key(b""), Err(CompactKeyError::EmptyKey));
    assert_eq!(
        parse_compact_key(b"p\x00\x00"),
        Err(CompactKeyError::Truncated {
            family: KeyFamily::PrimaryItem,
            expected_at_least: 5,
            actual: 3
        })
    );
    assert_eq!(
        parse_compact_key(b"q\x00\x00\x00\x00\x00\x00\x07\x00\x02z"),
        Err(CompactKeyError::InvalidKind {
            family: KeyFamily::PartitionedQueueData,
            kind: b'z'
        })
    );
    assert_eq!(
        parse_compact_key(b"d\x00\x00\x00"),
        Err(CompactKeyError::Truncated {
            family: KeyFamily::StreamTrimDue,
            expected_at_least: 17,
            actual: 4
        })
    );
    assert_eq!(
        parse_compact_key(b"az"),
        Err(CompactKeyError::InvalidKind {
            family: KeyFamily::SyncRecord,
            kind: b'z'
        })
    );
    assert_eq!(
        parse_compact_key(b"nz\x00\x00\x00\x00\x00\x63"),
        Err(CompactKeyError::InvalidKind {
            family: KeyFamily::PartitionControl,
            kind: b'z'
        })
    );
}

#[test]
fn range_helpers_keep_boundaries_inside_family_and_resource() {
    let table = TableStorageId::new(42);
    let other_table = TableStorageId::new(43);
    let index = IndexStorageId::new(3);
    let queue = QueueStorageId::new(7).unwrap();

    let primary_range = primary_item_prefix(table);
    assert!(primary_item_key(table, b"a") >= primary_range.start);
    assert!(primary_item_key(table, b"a") < primary_range.end);
    assert!(primary_item_key(other_table, b"a") >= primary_range.end);

    let gsi_range = gsi_prefix(table, index);
    assert!(gsi_item_key(table, index, b"a") >= gsi_range.start);
    assert!(gsi_item_key(table, index, b"a") < gsi_range.end);

    let stream_range = table_stream_prefix(table);
    assert!(table_stream_key(table, &STREAM_ID) >= stream_range.start);
    assert!(table_stream_key(table, &STREAM_ID) < stream_range.end);

    let ready_range = queue_ready_prefix(2, queue, 5);
    assert!(queue_record_key(2, queue, 5, QueueRecordKind::Ready, b"a") >= ready_range.start);
    assert!(queue_record_key(2, queue, 5, QueueRecordKind::Ready, b"a") < ready_range.end);
    assert!(queue_record_key(2, queue, 5, QueueRecordKind::Ready, b"\xff") < ready_range.end);
    let body_key = queue_record_key(2, queue, 5, QueueRecordKind::Body, b"a");
    assert!(body_key < ready_range.start || body_key >= ready_range.end);
}

#[test]
fn decoder_formats_keys_without_metadata() {
    let table = TableStorageId::new(42);
    let queue = QueueStorageId::new(7).unwrap();
    let stream = StreamStorageId::new(99).unwrap();

    assert_eq!(
        parse_compact_key(&primary_item_key(table, b"pk"))
            .unwrap()
            .debug_without_metadata()
            .to_string(),
        "p(table=42,key=706b)"
    );
    assert_eq!(
        parse_compact_key(&queue_record_key(2, queue, 5, QueueRecordKind::Ready, b"m"))
            .unwrap()
            .debug_without_metadata()
            .to_string(),
        "q(bucket=2,queue=7,partition=5,kind=ready,suffix=6d)"
    );
    assert_eq!(
        parse_compact_key(&queue_metadata_key(queue))
            .unwrap()
            .debug_without_metadata()
            .to_string(),
        "Q(queue=7)"
    );
    assert_eq!(
        parse_compact_key(&queue_url_lookup_key("https://queue.local/orders"))
            .unwrap()
            .debug_without_metadata()
            .to_string(),
        "Q(kind=url_lookup,lookup=https://queue.local/orders)"
    );
    assert_eq!(
        parse_compact_key(&crate::keyspace::compact::ordered_log_key(
            3, stream, 4, b"id"
        ))
        .unwrap()
        .debug_without_metadata()
        .to_string(),
        "o(bucket=3,stream=99,partition=4,suffix=6964)"
    );
    assert_eq!(
        parse_compact_key(&stream_trim_due_key(123, b"\x74\x00\x00\x00\x2a", 7))
            .unwrap()
            .debug_without_metadata()
            .to_string(),
        "d(due_ms=123,scope=740000002a,policy=7)"
    );
    assert_eq!(
        parse_compact_key(&stream_high_water_key())
            .unwrap()
            .debug_without_metadata()
            .to_string(),
        "a(kind=stream_high_water,suffix=)"
    );
    assert_eq!(
        parse_compact_key(&partition_control_key(
            PartitionControlKind::PartitionInfo,
            stream,
            b"\x00\x05"
        ))
        .unwrap()
        .debug_without_metadata()
        .to_string(),
        "n(kind=partition_info,resource=99,suffix=0005)"
    );
}

#[test]
fn decoder_uses_metadata_when_supplied() {
    let table = TableStorageId::new(42);
    let index = IndexStorageId::new(3);
    let queue = QueueStorageId::new(7).unwrap();
    let metadata = CompactKeyMetadata {
        table_name: Some((table, "orders")),
        index_name: Some((index, "by_status")),
        queue_name: Some((queue, "orders-queue")),
    };

    assert_eq!(
        parse_compact_key(&gsi_item_key(table, index, b"suffix"))
            .unwrap()
            .debug_with_metadata(&metadata)
            .to_string(),
        "g(table=42:orders,index=3:by_status,suffix=737566666978)"
    );
    assert_eq!(
        parse_compact_key(&queue_record_key(2, queue, 5, QueueRecordKind::Ready, b"m"))
            .unwrap()
            .debug_with_metadata(&metadata)
            .to_string(),
        "q(bucket=2,queue=7:orders-queue,partition=5,kind=ready,suffix=6d)"
    );
}
