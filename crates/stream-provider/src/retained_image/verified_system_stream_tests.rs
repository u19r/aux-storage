//! Exercise the shared publication-to-trim boundary independently of native row
//! layouts.

use storage_types::{
    AttributeValue, ItemHistoryAddress, ItemRevision, ItemStreamVersion, ItemWriteVersions,
    KeyAttributes, StreamItemId, StreamName, SystemStreamPosition, SystemStreamSourceTableName,
    TableHistoryId, TableName, TimestampMillis, WireItem, storage_serde,
};

use crate::{
    DynamoStreamEffectRow, EmbeddedStreamItem, LogicalSystemStreamMutation, PointerOwner,
    StoredStreamPointer, StreamDataType, StreamItem, StreamPointerReference,
    SystemStreamStoredRecord, SystemStreamTrimRecord,
};

#[test]
fn given_wire_images_when_published_and_trimmed_then_content_and_pointer_integrity_survive_roundtrips()
 {
    for deleted in [false, true] {
        for embedded in [false, true] {
            let item = WireItem::dynamo_json(br#"{"pk":{"S":"item"},"nested":{"M":{"a":{"N":"2"},"b":{"L":[{"BOOL":true}]}}},"set":{"SS":["blue","red"]}}"#.to_vec());
            let key =
                KeyAttributes::from_iter([("pk".to_owned(), AttributeValue::S("item".into()))]);
            let version = ItemStreamVersion::new(7);
            let history_id = TableHistoryId::from(1u32);
            let position =
                SystemStreamPosition::try_new("shard", "lineage", "1-vs00000000").unwrap();
            let mutation = LogicalSystemStreamMutation {
                history_id,
                stream_item_id: StreamItemId::from(position.offset().versionstamp().unwrap()),
                source_table_name: SystemStreamSourceTableName::try_from("table").unwrap(),
                key,
                item_json: item.dynamo_json_bytes().unwrap().into_owned(),
                old_source: None,
                item: item.clone(),
                old_item: Some(item.clone()),
                item_stream_version: version,
                indexers: Vec::new(),
                old_indexers: None,
                is_deleted: deleted,
                created_at: TimestampMillis::from_timestamp(123),
                routing_key: Vec::new(),
            };
            let (record, receipt) = DynamoStreamEffectRow::for_write(mutation, position).unwrap();
            let encoded_receipt = serde_json::to_value(&receipt).unwrap();
            assert!(encoded_receipt.get("item").is_none());
            assert!(encoded_receipt.get("old_item").is_none());
            assert!(encoded_receipt.get("evidence_blob").is_none());
            let receipt_size = storage_serde::to_bytes(&receipt).unwrap().len();
            let mut large = record.clone();
            let large_image = WireItem::dynamo_json(
                serde_json::to_vec(&serde_json::json!({
                    "pk": { "S": "item" }, "payload": { "S": "x".repeat(16_384) }
                }))
                .unwrap(),
            );
            large.old_image = Some(large_image.clone());
            large.new_image = (!deleted).then_some(large_image);
            let mut large_receipt: DynamoStreamEffectRow =
                serde_json::from_value(encoded_receipt.clone()).unwrap();
            large_receipt.effect_id = large.effect_digest(history_id, version).unwrap();
            assert_eq!(
                storage_serde::to_bytes(&large_receipt).unwrap().len(),
                receipt_size
            );
            let digest = receipt.effect_id.clone();
            let mut stamp = [0u8; 12];
            stamp[7] = 9;
            stamp[11] = 3;
            let receipt = receipt.with_versionstamp(stamp).unwrap();
            assert_eq!(receipt.effect_id, digest);
            let mut record: SystemStreamStoredRecord =
                storage_serde::from_bytes(&storage_serde::to_bytes(&record).unwrap()).unwrap();
            record.position = record.position.with_versionstamp(stamp).unwrap();
            assert_eq!(record.effect_digest(history_id, version).unwrap(), digest);
            let public = record.clone().into_stream_record().unwrap();
            assert_eq!(
                public.old_image.as_ref(),
                Some(&item.to_attribute_map().unwrap())
            );
            assert_eq!(public.new_image.is_none(), deleted);
            let history = ItemHistoryAddress {
                history_id,
                stream_name: StreamName::new(b"history-item"),
            };
            let versions = ItemWriteVersions {
                item_revision: ItemRevision::try_from(7u64).unwrap(),
                item_stream_version: version,
            };
            let pointer = if embedded {
                StoredStreamPointer::embedded(
                    history.clone(),
                    TableName::new("table"),
                    versions,
                    vec![EmbeddedStreamItem {
                        data: storage_serde::compress_json_bytes(
                            &item.dynamo_json_bytes().unwrap(),
                        ),
                        data_type: if deleted {
                            StreamDataType::DeleteMarker
                        } else {
                            StreamDataType::DynamoDbJson
                        },
                    }],
                )
            } else {
                StoredStreamPointer::pointer(history.clone(), TableName::new("table"), versions)
            };
            let pointer = StreamItem {
                id: receipt.pointer_item_id,
                stream_name: Some(StreamName::system_table_stream()),
                data: storage_serde::to_bytes(&pointer).unwrap(),
                data_type: StreamDataType::StreamPointer,
                created_at: receipt.created_at,
            };
            let reference = StreamPointerReference {
                owner: PointerOwner::System,
                owner_stream: StreamName::system_table_stream(),
                owner_item_id: receipt.pointer_item_id,
                item_stream: history.physical_stream_name(),
                item_item_id: (!embedded).then(|| StreamItemId::from(version)),
            };
            let candidate = SystemStreamTrimRecord {
                position: receipt.position.clone(),
                pointer_item_id: receipt.pointer_item_id,
                effect_id: digest,
                created_at: receipt.created_at,
            };
            let receipt_bytes = storage_serde::to_bytes(&receipt).unwrap();
            let decode_receipt =
                || storage_serde::from_bytes::<DynamoStreamEffectRow>(&receipt_bytes).unwrap();
            let trimmed = decode_receipt()
                .accept_trim(
                    &candidate,
                    &verify_record(&record, history_id, version),
                    &pointer,
                    &reference,
                )
                .unwrap();
            assert!(trimmed.trimmed);
            assert!(
                trimmed
                    .accept_trim(
                        &candidate,
                        &verify_record(&record, history_id, version),
                        &pointer,
                        &reference
                    )
                    .is_err()
            );
            let mut changed = record.clone();
            changed.old_image = Some(WireItem::dynamo_json(br#"{"pk":{"S":"other"}}"#.to_vec()));
            assert!(
                decode_receipt()
                    .accept_trim(
                        &candidate,
                        &verify_record(&changed, history_id, version),
                        &pointer,
                        &reference
                    )
                    .is_err()
            );
            changed = record.clone();
            changed.new_image = Some(WireItem::dynamo_json(br#"{"pk":{"S":"other"}}"#.to_vec()));
            assert!(
                decode_receipt()
                    .accept_trim(
                        &candidate,
                        &verify_record(&changed, history_id, version),
                        &pointer,
                        &reference
                    )
                    .is_err()
            );
            changed = record.clone();
            changed.source_table_name = SystemStreamSourceTableName::try_from("other").unwrap();
            assert!(
                decode_receipt()
                    .accept_trim(
                        &candidate,
                        &verify_record(&changed, history_id, version),
                        &pointer,
                        &reference
                    )
                    .is_err()
            );
            let mut changed = decode_receipt();
            changed.history_id = 2u32.into();
            assert!(
                changed
                    .accept_trim(
                        &candidate,
                        &verify_record(&record, history_id, version),
                        &pointer,
                        &reference
                    )
                    .is_err()
            );
            let mut changed = decode_receipt();
            changed.item_stream_version = ItemStreamVersion::new(8);
            assert!(
                changed
                    .accept_trim(
                        &candidate,
                        &verify_record(&record, history_id, version),
                        &pointer,
                        &reference
                    )
                    .is_err()
            );
            let mut changed = pointer.clone();
            changed.id = StreamItemId::from(ItemStreamVersion::new(99));
            assert!(
                decode_receipt()
                    .accept_trim(
                        &candidate,
                        &verify_record(&record, history_id, version),
                        &changed,
                        &reference
                    )
                    .is_err()
            );
            let mut changed = serde_json::to_value(&receipt).unwrap();
            changed["effect_id"] = serde_json::json!("A".repeat(64));
            assert!(serde_json::from_value::<DynamoStreamEffectRow>(changed).is_err());
            let mut changed = serde_json::to_value(&receipt).unwrap();
            changed["unexpected"] = serde_json::json!(true);
            assert!(serde_json::from_value::<DynamoStreamEffectRow>(changed).is_err());
        }
    }
}

// This logical fixture checks receipt identity after content verification.
// Native publication, hydration and custody are exercised by provider
// conformance gates.
fn verify_record(
    record: &SystemStreamStoredRecord,
    history: TableHistoryId,
    version: ItemStreamVersion,
) -> super::verified_system_stream::VerifiedSystemStreamRecord {
    let digest = record.effect_digest(history, version).unwrap();
    super::verified_system_stream::VerifiedSystemStreamRecord::from_verified_content(
        record.clone(),
        history,
        version,
        digest,
    )
}
