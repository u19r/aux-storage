use storage_types::{
    AttributeMap, AttributeValue, BoundTableInfo, CreateTableRequest, IndexerDeclaration,
    KeyAttributes, MaxIndexers, StreamBinding, StreamUserIdentity, StreamViewType, TimestampMillis,
    WireItem,
};

use crate::{DynamoMutationRoute, StreamChange, StreamEntry, StreamEntryInput};

fn table(view: StreamViewType) -> BoundTableInfo {
    let request: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "TableName": "orders",
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"}
        ],
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ]
    }))
    .unwrap();
    let mut info = request
        .stored_table_info_at(TimestampMillis::from_timestamp(0))
        .unwrap();
    info.max_indexers = MaxIndexers::try_new(1).unwrap();
    BoundTableInfo {
        history_id: 7_u32.into(),
        info: std::sync::Arc::new(info),
        binding: storage_types::TableBinding {
            stream: Some(StreamBinding {
                view,
                topology_revision: 0,
                xid_bias: 0,
            }),
            ..Default::default()
        },
    }
}

fn keys() -> KeyAttributes {
    KeyAttributes::from_iter([
        ("pk".into(), AttributeValue::S("tenant".into())),
        ("sk".into(), AttributeValue::S("order".into())),
    ])
}

fn image(body: &str) -> WireItem {
    WireItem::try_from(AttributeMap::from_iter([
        ("pk".into(), AttributeValue::S("tenant".into())),
        ("sk".into(), AttributeValue::S("order".into())),
        ("body".into(), AttributeValue::S(body.into())),
    ]))
    .unwrap()
}

fn entry(table: &BoundTableInfo, has_new: bool, has_old: bool) -> StreamEntry {
    let route =
        DynamoMutationRoute::try_new(&table.table_name, &table.key_schema, &keys()).unwrap();
    let old = route.verify_item(image("before")).unwrap();
    let new = storage_types::VerifiedWireItem::try_from_shared(
        std::sync::Arc::new(image("after")),
        &keys(),
    )
    .unwrap();
    let declaration = IndexerDeclaration::try_new(vec!["body".into()], table.max_indexers).unwrap();
    let old_indexers = ["body".into()];
    let input = StreamEntryInput::try_new(
        table,
        &route,
        has_new.then_some((&new, &declaration)),
        has_old.then_some((&old, &old_indexers)),
        TimestampMillis::from_timestamp(123),
        Some(StreamUserIdentity::service_ttl()),
    )
    .unwrap();
    StreamEntry::decode(&input.encode().unwrap()).unwrap()
}

#[test]
fn given_each_view_when_accepted_changes_are_encoded_then_snapshot_and_old_images_are_complete() {
    for view in [
        StreamViewType::KeysOnly,
        StreamViewType::NewImage,
        StreamViewType::OldImage,
        StreamViewType::NewAndOldImages,
    ] {
        let keeps_old = matches!(
            view,
            StreamViewType::OldImage | StreamViewType::NewAndOldImages
        );
        let table = table(view);
        for (has_new, has_old, change) in [
            (true, false, StreamChange::Insert),
            (true, true, StreamChange::Modify),
            (false, true, StreamChange::Remove),
        ] {
            let accepted = entry(&table, has_new, has_old);
            let decoded = StreamEntry::decode(&accepted.encode().unwrap()).unwrap();
            assert_eq!(decoded.change(), change);
            assert_eq!(decoded.keys(), &keys());
            assert_eq!(decoded.created_at(), TimestampMillis::from_timestamp(123));
            assert_eq!(
                decoded.user_identity(),
                Some(&StreamUserIdentity::service_ttl())
            );
            assert_eq!(decoded.new_image().is_some(), has_new);
            assert_eq!(decoded.old_image().is_some(), keeps_old && has_old);
            for (image, body) in [
                (decoded.new_image(), "after"),
                (decoded.old_image(), "before"),
            ] {
                if let Some(image) = image {
                    assert_eq!(image.indexers, ["body"]);
                    assert_eq!(
                        image.item.to_attribute_map().unwrap().get("body"),
                        Some(&AttributeValue::S(body.into()))
                    );
                }
            }
        }
    }
}

#[test]
fn given_keys_only_first_modify_when_encoded_then_change_and_new_image_survive_without_old_image() {
    let entry = entry(&table(StreamViewType::KeysOnly), true, true);
    let json = storage_types::storage_serde::decompress_bytes(&entry.encode().unwrap()).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&json).unwrap();
    assert_eq!(value["change"], "Modify");
    assert_eq!(value["new_image"]["item"]["body"]["S"], "after");
    assert!(value["old_image"].is_null());
    assert!(value.get("previous_version").is_none());
}

#[test]
fn given_corrupt_entry_when_decoded_then_missing_new_image_and_mismatched_keys_are_rejected() {
    let entry = entry(&table(StreamViewType::NewAndOldImages), true, true);
    let json = storage_types::storage_serde::decompress_bytes(&entry.encode().unwrap()).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&json).unwrap();
    for field in ["new_image", "old_image"] {
        let mut corrupt = value.clone();
        corrupt[field]["item"]["pk"]["S"] = "another-tenant".into();
        assert!(
            StreamEntry::decode(&storage_types::storage_serde::to_bytes(&corrupt).unwrap())
                .is_err()
        );
    }
    let mut corrupt = value;
    corrupt["new_image"] = serde_json::Value::Null;
    assert!(
        StreamEntry::decode(&storage_types::storage_serde::to_bytes(&corrupt).unwrap()).is_err()
    );
}

#[test]
fn given_another_table_when_preparing_entry_then_bound_identity_is_rejected() {
    let table = table(StreamViewType::KeysOnly);
    let route = DynamoMutationRoute::try_new(
        &storage_types::TableName::new("another"),
        &table.key_schema,
        &keys(),
    )
    .unwrap();
    let new = storage_types::VerifiedWireItem::try_from_shared(
        std::sync::Arc::new(image("after")),
        &keys(),
    )
    .unwrap();
    let declaration = IndexerDeclaration::try_new(vec![], table.max_indexers).unwrap();
    assert!(
        StreamEntryInput::try_new(
            &table,
            &route,
            Some((&new, &declaration)),
            None,
            TimestampMillis::from_timestamp(123),
            None
        )
        .is_err()
    );
}

#[test]
fn given_a_native_position_when_encoded_then_only_the_key_owns_the_stamp() {
    let table = table(StreamViewType::KeysOnly);
    let plan = crate::PositionedStreamEntry {
        history_id: table.history_id,
        source_table_name: table.table_name.clone(),
        entry: entry(&table, true, true),
        source_scope: storage_types::SystemStreamScope::try_new("source", 1).unwrap(),
        position: storage_types::SystemStreamPosition::try_new("shard", "lineage", "42").unwrap(),
    };
    let bytes = plan.encode_value().unwrap();
    let borrowed = crate::PositionedStreamEntry::encode_body(
        &plan.source_table_name,
        &plan.source_scope,
        &plan.entry,
    )
    .unwrap();
    assert_eq!(bytes, borrowed);
    let json = storage_types::storage_serde::decompress_bytes(
        &bytes[crate::PositionedStreamEntry::VALUE_PREFIX_BYTES..],
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&json).unwrap();
    assert!(value.get("position").is_none());
    assert!(value.get("history_id").is_none());
    assert_eq!(value["entry"]["change"], "Modify");
    let decoded =
        crate::PositionedStreamEntry::decode_value(table.history_id, plan.position, &bytes)
            .unwrap();
    assert_eq!(decoded.entry.change(), StreamChange::Modify);
    assert_eq!(decoded.source_table_name, table.table_name);
    assert!(decoded.entry.new_image().is_some());
}

#[test]
fn given_canonical_entry_when_prefix_is_corrupt_then_full_decode_rejects_it() {
    let table = table(StreamViewType::KeysOnly);
    let positioned = crate::PositionedStreamEntry {
        history_id: table.history_id,
        source_table_name: table.table_name.clone(),
        entry: entry(&table, true, false),
        source_scope: storage_types::SystemStreamScope::try_new("source", 1).unwrap(),
        position: storage_types::SystemStreamPosition::try_new("shard", "lineage", "42").unwrap(),
    };
    let encoded = positioned.encode_value().unwrap();
    assert_eq!(
        crate::PositionedStreamEntry::decode_created_at(&encoded[..12]).unwrap(),
        positioned.entry.created_at()
    );
    for length in 0..12 {
        assert!(crate::PositionedStreamEntry::decode_created_at(&encoded[..length]).is_err());
        assert!(
            crate::PositionedStreamEntry::decode_value(
                positioned.history_id,
                positioned.position.clone(),
                &encoded[..length]
            )
            .is_err()
        );
    }
    for index in [0, 3, 11] {
        let mut corrupt = encoded.clone();
        corrupt[index] ^= 1;
        assert!(
            crate::PositionedStreamEntry::decode_value(
                positioned.history_id,
                positioned.position.clone(),
                &corrupt
            )
            .is_err()
        );
    }
    assert!(
        crate::PositionedStreamEntry::decode_value(
            positioned.history_id,
            positioned.position,
            &encoded[12..]
        )
        .is_err()
    );
}

#[test]
fn given_compressed_canonical_entry_when_reading_prefix_then_time_and_full_image_roundtrip() {
    let table = table(StreamViewType::NewImage);
    let route =
        DynamoMutationRoute::try_new(&table.table_name, &table.key_schema, &keys()).unwrap();
    let new = storage_types::VerifiedWireItem::try_from_shared(
        std::sync::Arc::new(image(&"large".repeat(20000))),
        &keys(),
    )
    .unwrap();
    let declaration = IndexerDeclaration::try_new(vec![], table.max_indexers).unwrap();
    let input = StreamEntryInput::try_new(
        &table,
        &route,
        Some((&new, &declaration)),
        None,
        TimestampMillis::from_timestamp(-42),
        None,
    )
    .unwrap();
    let accepted = StreamEntry::decode(&input.encode().unwrap()).unwrap();
    let positioned = crate::PositionedStreamEntry {
        history_id: table.history_id,
        source_table_name: table.table_name.clone(),
        source_scope: storage_types::SystemStreamScope::try_new("source", 1).unwrap(),
        position: storage_types::SystemStreamPosition::try_new("shard", "lineage", "42").unwrap(),
        entry: accepted,
    };
    let encoded = positioned.encode_value().unwrap();
    let body = &encoded[crate::PositionedStreamEntry::VALUE_PREFIX_BYTES..];
    assert!(
        storage_types::storage_serde::decompress_bytes(body)
            .unwrap()
            .len()
            > body.len()
    );
    assert_eq!(
        crate::PositionedStreamEntry::decode_created_at(&encoded).unwrap(),
        positioned.entry.created_at()
    );
    let decoded = crate::PositionedStreamEntry::decode_value(
        positioned.history_id,
        positioned.position,
        &encoded,
    )
    .unwrap();
    assert_eq!(
        decoded.entry.encode().unwrap(),
        positioned.entry.encode().unwrap()
    );
    assert_eq!(decoded.entry.created_at(), positioned.entry.created_at());
}

#[test]
fn given_images_when_borrowed_entries_are_encoded_then_owned_roundtrip_bytes_match_for_each_view() {
    use std::sync::Arc;

    use storage_types::VerifiedWireItem;
    let split = |body: &str| {
        WireItem::dynamo_json(
            serde_json::to_vec(&serde_json::json!({
                "pk": {"S": "tenant"},
                "sk": {"S": "order"},
                "body": {"S": body},
            }))
            .unwrap(),
        )
    };
    for size in [32, 390_000] {
        let new =
            VerifiedWireItem::try_from_shared(Arc::new(split(&"n".repeat(size))), &keys()).unwrap();
        let old = VerifiedWireItem::try_new(split(&"o".repeat(size)), &keys()).unwrap();
        for view in [
            StreamViewType::KeysOnly,
            StreamViewType::NewImage,
            StreamViewType::OldImage,
            StreamViewType::NewAndOldImages,
        ] {
            let keeps_old = matches!(
                view,
                StreamViewType::OldImage | StreamViewType::NewAndOldImages
            );
            let table = table(view);
            let route = DynamoMutationRoute::try_new(&table.table_name, &table.key_schema, &keys())
                .unwrap();
            let declaration =
                IndexerDeclaration::try_new(vec!["body".into()], table.max_indexers).unwrap();
            let names = ["body".into()];
            let scope = storage_types::SystemStreamScope::try_new("source", 1).unwrap();
            let input = StreamEntryInput::try_new(
                &table,
                &route,
                Some((&new, &declaration)),
                Some((&old, &names)),
                TimestampMillis::from_timestamp(123),
                None,
            )
            .unwrap();
            let bytes =
                crate::PositionedStreamEntry::encode_input(&table.table_name, &scope, &input)
                    .unwrap();
            let decoded = crate::PositionedStreamEntry::decode_value(
                table.history_id,
                storage_types::SystemStreamPosition::try_new("shard", "lineage", "42").unwrap(),
                &bytes,
            )
            .unwrap();
            assert_eq!(decoded.encode_value().unwrap(), bytes);
            assert_eq!(
                decoded
                    .entry
                    .new_image()
                    .unwrap()
                    .item
                    .to_attribute_map()
                    .unwrap(),
                new.to_attribute_map().unwrap()
            );
            assert_eq!(decoded.entry.old_image().is_some(), keeps_old);
        }
    }
}

fn mark(hidden: bool) -> storage_types::RegionalMark {
    let order = storage_types::RegionalOrder::from_parts(
        1_024_000,
        storage_types::RegionId(std::num::NonZeroU16::new(1).unwrap()),
        false,
    );
    storage_types::RegionalMark {
        hidden,
        ..storage_types::RegionalMark::origin(order)
    }
}

#[test]
fn given_hidden_input_when_encoded_and_decoded_then_entry_is_not_public_and_keeps_mark() {
    let table = table(StreamViewType::NewAndOldImages);
    let route =
        DynamoMutationRoute::try_new(&table.table_name, &table.key_schema, &keys()).unwrap();
    let input = StreamEntryInput::hidden(
        &table,
        &route,
        TimestampMillis::from_timestamp(123),
        mark(false),
    )
    .unwrap();
    let decoded = StreamEntry::decode(&input.encode().unwrap()).unwrap();
    assert!(!decoded.is_public());
    assert_eq!(decoded.regional(), Some(mark(true)));
    assert_eq!(decoded.change(), StreamChange::Remove);
    assert!(decoded.new_image().is_none());
    assert!(decoded.old_image().is_none());
    assert_eq!(decoded.keys(), &keys());
}

#[test]
fn given_hidden_mark_with_image_or_non_remove_change_when_decoded_then_rejected() {
    let table = table(StreamViewType::NewAndOldImages);
    let with_image = entry(&table, true, true);
    let json =
        storage_types::storage_serde::decompress_bytes(&with_image.encode().unwrap()).unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&json).unwrap();
    value["regional"] = serde_json::to_value(mark(true)).unwrap();
    assert!(StreamEntry::decode(&storage_types::storage_serde::to_bytes(&value).unwrap()).is_err());
    // The same mark without `hidden` is an ordinary origin entry.
    value["regional"] = serde_json::to_value(mark(false)).unwrap();
    let origin =
        StreamEntry::decode(&storage_types::storage_serde::to_bytes(&value).unwrap()).unwrap();
    assert!(origin.is_public());
    assert_eq!(origin.regional(), Some(mark(false)));
}

#[test]
fn given_entry_without_mark_when_encoded_then_no_regional_field_and_public() {
    let table = table(StreamViewType::NewAndOldImages);
    let accepted = entry(&table, true, true);
    assert!(accepted.is_public());
    assert!(accepted.regional().is_none());
    let json = storage_types::storage_serde::decompress_bytes(&accepted.encode().unwrap()).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&json).unwrap();
    assert!(value.get("regional").is_none());
    let keys: Vec<_> = value.as_object().unwrap().keys().cloned().collect();
    for key in [
        "change",
        "keys",
        "new_image",
        "old_image",
        "created_at",
        "user_identity",
    ] {
        assert!(keys.iter().any(|k| k == key), "missing {key}");
    }
    assert_eq!(keys.len(), 6);
}

#[test]
fn given_marked_origin_entry_when_with_regional_then_public_with_mark() {
    let table = table(StreamViewType::NewAndOldImages);
    let route =
        DynamoMutationRoute::try_new(&table.table_name, &table.key_schema, &keys()).unwrap();
    let new = storage_types::VerifiedWireItem::try_from_shared(
        std::sync::Arc::new(image("after")),
        &keys(),
    )
    .unwrap();
    let declaration = IndexerDeclaration::try_new(vec![], table.max_indexers).unwrap();
    let input = StreamEntryInput::try_new(
        &table,
        &route,
        Some((&new, &declaration)),
        None,
        TimestampMillis::from_timestamp(123),
        None,
    )
    .unwrap()
    .with_regional(Some(mark(false)));
    let decoded = StreamEntry::decode(&input.encode().unwrap()).unwrap();
    assert!(decoded.is_public());
    assert_eq!(decoded.regional(), Some(mark(false)));
}
