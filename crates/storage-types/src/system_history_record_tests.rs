use crate::{
    ItemHistoryAddress, ItemHistoryKind, ItemHistoryMetadata, ItemHistoryPointer,
    ItemHistoryPublication, ItemStreamVersion, StreamName, SystemHistoryRecord,
    SystemStreamPosition, SystemStreamScope, TimestampMillis,
};

fn record(offset: &str) -> SystemHistoryRecord {
    let metadata = ItemHistoryMetadata::try_new(
        ItemHistoryAddress {
            history_id: 7_u32.into(),
            stream_name: StreamName::new(b"orders/stream-item/private-key"),
        },
        ItemStreamVersion::new(2),
        Some(ItemStreamVersion::new(1)),
        ItemHistoryPublication::Event {
            kind: ItemHistoryKind::Image,
            source_scope: SystemStreamScope::try_new("source", 1).unwrap(),
            table_position: SystemStreamPosition::try_new("shard", "lineage", offset).unwrap(),
            created_at: TimestampMillis::from_timestamp(1000),
        },
    )
    .unwrap();
    ItemHistoryPointer::try_new(metadata, 600, Some(100))
        .unwrap()
        .try_into()
        .unwrap()
}

#[test]
fn given_table_deletion_when_system_payload_is_removed_then_order_survives_without_table_data() {
    let original = record("1");
    let removed = original.clone().into_removed();
    assert_eq!(removed.position(), original.position());
    assert_eq!(removed.created_at(), original.created_at());
    assert_eq!(removed.table_history_id(), None);
    assert!(removed.pointer().is_none());
    let bytes = serde_json::to_vec(&removed).unwrap();
    let decoded: SystemHistoryRecord = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(decoded, removed);
    let text = String::from_utf8(bytes).unwrap();
    assert!(!text.contains("orders") && !text.contains("private-key"));
}

#[test]
fn given_history_reference_when_envelope_is_changed_then_decode_rejects_other_authority() {
    let record = record("1");
    let bytes = serde_json::to_vec(&record).unwrap();
    assert_eq!(
        serde_json::from_slice::<SystemHistoryRecord>(&bytes).unwrap(),
        record
    );
    for (field, value) in [
        ("created_at", serde_json::json!(2000)),
        (
            "position",
            serde_json::to_value(SystemStreamPosition::try_new("other", "other", "1").unwrap())
                .unwrap(),
        ),
        ("old_image", serde_json::json!({"secret": {"S": "payload"}})),
    ] {
        let mut corrupt = serde_json::from_slice::<serde_json::Value>(&bytes).unwrap();
        corrupt[field] = value;
        assert!(serde_json::from_value::<SystemHistoryRecord>(corrupt).is_err());
    }
}

#[test]
fn given_pending_native_record_when_commit_key_is_bound_then_reference_and_position_agree() {
    let pending = record("18446744073709551615-vsffff0001");
    let mut stamp = [0_u8; 12];
    stamp[..8].copy_from_slice(&42_u64.to_be_bytes());
    stamp[11] = 1;
    let committed = pending.clone().with_versionstamp(stamp).unwrap();
    assert_eq!(committed.position().offset().versionstamp(), Some(stamp));
    let ItemHistoryPublication::Event { table_position, .. } =
        committed.pointer().unwrap().metadata().publication()
    else {
        panic!("event")
    };
    assert_eq!(committed.position(), table_position);
    let removed = pending.into_removed().with_versionstamp(stamp).unwrap();
    assert_eq!(removed.position(), committed.position());
    assert!(removed.pointer().is_none());
}
