use std::sync::Arc;

use storage_types::{
    AttributeMap, AttributeValue, BoundTableInfo, CreateTableRequest, PutItemRequest,
    StreamBinding, StreamViewType, TimestampMillis, WireItem,
};
use stream_provider::{DynamoMutationRoute, PositionedStreamEntry, StreamEntryInput};

use crate::{ItemWriteObservation, PutItemShape};

#[test]
fn given_retained_put_when_bound_and_history_encoded_then_bytes_and_allocations_are_recorded() {
    let request: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "TableName":"orders", "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
        "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
        "StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}
    }))
    .unwrap();
    let table = Arc::new(BoundTableInfo {
        info: Arc::new(
            request
                .stored_table_info_at(TimestampMillis::from_timestamp(0))
                .unwrap(),
        ),
        history_id: 7_u32.into(),
        binding: storage_types::TableBinding {
            stream: Some(StreamBinding {
                view: StreamViewType::NewAndOldImages,
                topology_revision: 0,
                xid_bias: 0,
            }),
            ..Default::default()
        },
    });
    let scope = storage_types::SystemStreamScope::try_new("source", 1).unwrap();
    for (label, size) in [("small", 32), ("near_limit", 390_000)] {
        let make_image = |fill: char| {
            WireItem::try_from(AttributeMap::from_iter([
                ("pk".into(), AttributeValue::S("tenant".into())),
                (
                    "body".into(),
                    AttributeValue::S(std::iter::repeat_n(fill, size).collect()),
                ),
            ]))
            .unwrap()
        };
        let shape = Arc::new(
            PutItemShape::try_from_request(PutItemRequest::new(
                table.table_name.clone(),
                make_image('n'),
            ))
            .unwrap(),
        );
        let old = storage_types::StoredWireItem {
            item: make_image('o'),
            indexers: vec![],
        };
        let old_observations = [old.clone(), old.clone(), old];
        // Fixture construction is excluded. Three binds model fresh decisions
        // after two aborts.
        let guard = alloc_counter::AllocationGuard::start(
            module_path!(),
            "retained_put_history",
            file!(),
            line!(),
            Some(label),
        );
        let mut encoded = Vec::new();
        for old in old_observations {
            let bound = shape.clone().bind_table(Arc::clone(&table)).unwrap();
            let (accepted, _) = bound
                .accept_old(ItemWriteObservation::Absent, Default::default())
                .unwrap()
                .into_result()
                .unwrap();
            let route =
                DynamoMutationRoute::try_new(&table.table_name, &table.key_schema, &accepted.key)
                    .unwrap();
            let old = storage_types::VerifiedWireItem::try_from_stored(old, &accepted.key).unwrap();
            let entry = StreamEntryInput::try_new(
                &table,
                &route,
                Some((&accepted.new_item, &accepted.declaration)),
                Some((&old, &[])),
                TimestampMillis::from_timestamp(123),
                None,
            )
            .unwrap();
            encoded =
                PositionedStreamEntry::encode_input(&table.table_name, &scope, &entry).unwrap();
        }
        let report = guard.finish();
        alloc_counter::emit_report(&report).unwrap();
        use sha2::{Digest, Sha256};
        let expected = match label {
            "small" => "f72f124961ad81f3b7075b57a10ccfbfd86ecff19809322551528425044b5148",
            _ => "9a0950c5dace11ab0db67fb2447aeb0c96cf11bf5de017a0b098b3f6c0263097",
        };
        assert_eq!(hex::encode(Sha256::digest(&encoded)), expected);
        eprintln!(
            "N7 {label}: allocations={} bytes={} encoded={}",
            report.allocation_count,
            report.allocated_bytes,
            encoded.len()
        );
    }
}

#[test]
fn given_checked_maintenance_key_when_absence_accepted_then_no_request_owner_is_allocated() {
    let request: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "TableName":"orders", "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
        "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}]
    }))
    .unwrap();
    let table = Arc::new(BoundTableInfo {
        info: Arc::new(
            request
                .stored_table_info_at(TimestampMillis::from_timestamp(0))
                .unwrap(),
        ),
        history_id: 7_u32.into(),
        binding: Default::default(),
    });
    let mut key = storage_types::KeyAttributes::from_iter([(
        "pk".into(),
        AttributeValue::S("tenant".into()),
    )]);
    storage_types::validate_transact_key(&table.info, &key).unwrap();
    key.normalize_numbers_for_write();
    let key_buffer = match key.get("pk").unwrap() {
        AttributeValue::S(value) => value.as_ptr(),
        _ => unreachable!(),
    };
    let guard = alloc_counter::AllocationGuard::start(
        module_path!(),
        "maintenance_delete_acceptance",
        file!(),
        line!(),
        None,
    );
    let old =
        crate::dynamo_put_item::ObservedWrite::try_new(ItemWriteObservation::Absent, &key).unwrap();
    let accepted =
        crate::AcceptedDeleteItem::try_from_observed(table, key, old, Default::default()).unwrap();
    let report = guard.finish();
    assert_eq!(report.allocation_count, 0);
    assert_eq!(report.allocated_bytes, 0);
    assert!(accepted.old_item.is_none());
    assert!(accepted.response().unwrap().attributes.is_none());
    match accepted.key.get("pk").unwrap() {
        AttributeValue::S(value) => assert_eq!(value.as_ptr(), key_buffer),
        _ => unreachable!(),
    }
}
