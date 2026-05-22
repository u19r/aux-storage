use storage_types::{StreamItemId, StreamName, TableName};
use stream_provider::StreamPartitioningMode;
use uuid::Uuid;

use crate::partition_family::{
    OrderedLogSplitBoundary, PartitionInfo, PartitionState, QueueReceiptHandleData,
    apply_ordered_log_split_boundaries, ordered_log_partition_for_key,
    ordered_log_partition_prefix, ordered_log_split_marker_family_prefix,
    parse_ordered_log_split_boundary_from_key, parse_partitioned_stream_item_id,
    parse_queue_partition_marker, parse_stream_partition_marker, queue_partition_marker_bytes,
    stream_partition_marker_bytes, supports_pointer_stream_partitioning,
};

#[test]
fn ordered_log_partition_for_key_is_stable_and_bounded_tests() {
    let first = ordered_log_partition_for_key(b"tenant-a", 16);
    let second = ordered_log_partition_for_key(b"tenant-a", 16);
    let third = ordered_log_partition_for_key(b"tenant-b", 16);

    assert_eq!(first, second);
    assert!(first < 16);
    assert!(third < 16);
}

#[test]
fn ordered_log_partition_prefix_places_slot_near_front_tests() {
    let stream_name = StreamName::from("orders/stream-table");
    let prefix = ordered_log_partition_prefix(&stream_name, 11);
    let prefix_str = String::from_utf8(prefix).expect("partition prefix should be utf8");

    assert!(prefix_str.starts_with("plog/000b/"));
    assert!(prefix_str.ends_with("/000b/"));
}

#[test]
fn partitioned_stream_item_id_reads_versionstamp_suffix_tests() {
    let item_id = StreamItemId::from(Uuid::now_v7());
    let mut key = b"plog/0001/orders/0001/".to_vec();
    key.extend_from_slice(item_id.as_bytes());

    assert_eq!(parse_partitioned_stream_item_id(&key), Some(item_id));
}

#[test]
fn stream_and_queue_partition_markers_round_trip_tests() {
    let stream_marker = parse_stream_partition_marker(
        &stream_partition_marker_bytes(8).expect("encode stream marker"),
    )
    .expect("decode stream marker");
    assert_eq!(
        stream_marker.partitioning_mode,
        StreamPartitioningMode::KeyOrdered
    );
    assert_eq!(stream_marker.partition_count, 8);

    let queue_marker = parse_queue_partition_marker(
        &queue_partition_marker_bytes(12).expect("encode queue marker"),
    )
    .expect("decode queue marker");
    assert_eq!(queue_marker.partition_count, 12);
}

#[test]
fn queue_receipt_handle_round_trip_and_validation_tests() {
    let handle = QueueReceiptHandleData {
        partition_id: 7,
        message_id_hex: "018f4f5b0c7d9aabbccddeef".to_string(),
        visibility_timestamp_ms: 1_700_000_030_000,
        delivery_attempt: 3,
        claim_nonce: Uuid::now_v7().to_string(),
    };

    let encoded = handle.encode().expect("encode receipt handle");
    let decoded = QueueReceiptHandleData::decode(&encoded).expect("decode receipt handle");
    assert_eq!(decoded.partition_id, 7);
    assert_eq!(decoded.message_id_hex, handle.message_id_hex);
    assert_eq!(decoded.visibility_timestamp_ms, 1_700_000_030_000);
    assert_eq!(decoded.delivery_attempt, 3);
    assert_eq!(decoded.claim_nonce, handle.claim_nonce);

    assert!(QueueReceiptHandleData::decode("not-enough-parts").is_err());
    assert!(QueueReceiptHandleData::decode("zzzz.id.1.nonce").is_err());
}

#[test]
fn pointer_stream_partitioning_only_targets_system_and_table_streams_tests() {
    let system_stream = StreamName::system_table_stream();
    let table_stream = StreamName::table_stream(&TableName::new("orders"));
    let item_stream = StreamName::from("orders/stream-item/hash/customer-1");
    let generic_stream = StreamName::from("user-events");

    assert!(supports_pointer_stream_partitioning(&system_stream));
    assert!(supports_pointer_stream_partitioning(&table_stream));
    assert!(!supports_pointer_stream_partitioning(&item_stream));
    assert!(!supports_pointer_stream_partitioning(&generic_stream));
}

#[test]
fn ordered_log_split_boundary_key_round_trip_tests() {
    let family_component = "6f7264657273";
    let parent_partition_id = 7;
    let boundary = StreamItemId::from(Uuid::now_v7());
    let mut key = ordered_log_split_marker_family_prefix(family_component);
    key.extend_from_slice(format!("{parent_partition_id:04x}/").as_bytes());
    key.extend_from_slice(boundary.as_bytes());

    assert_eq!(
        parse_ordered_log_split_boundary_from_key(family_component, &key),
        Some((parent_partition_id, boundary))
    );
}

#[test]
fn ordered_log_split_boundaries_overlay_parent_and_children_tests() {
    let boundary = StreamItemId::from(Uuid::now_v7());
    let mut partitions = vec![
        PartitionInfo {
            partition_id: 1,
            placement_slot: 1,
            state: PartitionState::WriteClosed,
            opened_after_id: None,
            sealed_after_id: None,
            hash_start_inclusive: 0,
            hash_end_exclusive: Some(100),
        },
        PartitionInfo {
            partition_id: 2,
            placement_slot: 2,
            state: PartitionState::Open,
            opened_after_id: None,
            sealed_after_id: None,
            hash_start_inclusive: 0,
            hash_end_exclusive: Some(50),
        },
        PartitionInfo {
            partition_id: 3,
            placement_slot: 3,
            state: PartitionState::Open,
            opened_after_id: None,
            sealed_after_id: None,
            hash_start_inclusive: 50,
            hash_end_exclusive: Some(100),
        },
    ];

    apply_ordered_log_split_boundaries(
        &mut partitions,
        &[OrderedLogSplitBoundary {
            parent_partition_id: 1,
            left_child_partition_id: 2,
            right_child_partition_id: 3,
            boundary,
        }],
    );

    assert_eq!(partitions[0].sealed_after_id, Some(boundary));
    assert_eq!(partitions[1].opened_after_id, Some(boundary));
    assert_eq!(partitions[2].opened_after_id, Some(boundary));
}

#[test]
fn partition_state_only_allows_supported_transitions_tests() {
    assert_eq!(
        PartitionState::Open.transition_to(PartitionState::WriteClosed),
        Ok(PartitionState::WriteClosed)
    );
    assert_eq!(
        PartitionState::Open.transition_to(PartitionState::Draining),
        Ok(PartitionState::Draining)
    );
    assert_eq!(
        PartitionState::Draining.transition_to(PartitionState::Retired),
        Ok(PartitionState::Retired)
    );

    assert!(
        PartitionState::WriteClosed
            .transition_to(PartitionState::Draining)
            .is_err()
    );
    assert!(
        PartitionState::Open
            .transition_to(PartitionState::Retired)
            .is_err()
    );
    assert!(
        PartitionState::Retired
            .transition_to(PartitionState::Open)
            .is_err()
    );
}
