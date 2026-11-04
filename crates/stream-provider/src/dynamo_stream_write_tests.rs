use storage_types::{
    AttributeValue, BoundTableInfo, CreateTableRequest, ItemHistoryChange, ItemHistoryKind,
    ItemHistoryRecord, ItemStreamVersion, KeyAttributes, SystemStreamPosition, SystemStreamScope,
    TimestampMillis, WireItem,
};

use crate::{
    DynamoMutationRoute, DynamoStreamWriteInput, DynamoStreamWritePlan,
    LogicalSystemStreamMutation, LogicalSystemStreamMutationInput, StreamRowValue,
};

fn table() -> BoundTableInfo {
    let request: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "TableName": "orders", "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}]
    })).unwrap();
    BoundTableInfo {
        history_id: 7_u32.into(),
        info: std::sync::Arc::new(
            request
                .stored_table_info_at(TimestampMillis::from_timestamp(0))
                .unwrap(),
        ),
    }
}

#[test]
fn given_wide_current_and_predecessor_when_write_is_planned_then_only_history_contains_payload() {
    let table = table();
    let key = KeyAttributes::from([("pk".into(), AttributeValue::S("key".into()))]);
    let route = DynamoMutationRoute::try_new(&table.table_name, &table.key_schema, &key).unwrap();
    let old = route
        .verify_item(
            WireItem::try_from(storage_types::AttributeMap::from_iter([
                ("pk".into(), AttributeValue::S("key".into())),
                (
                    "body".into(),
                    AttributeValue::S("old-secret".repeat(10_000)),
                ),
            ]))
            .unwrap(),
        )
        .unwrap();
    for current in [Some("new-secret".repeat(10_000)), None] {
        let item = current.as_ref().map(|value| {
            route
                .verify_item(
                    WireItem::try_from(storage_types::AttributeMap::from_iter([
                        ("pk".into(), AttributeValue::S("key".into())),
                        ("body".into(), AttributeValue::S(value.clone())),
                    ]))
                    .unwrap(),
                )
                .unwrap()
        });
        let mutation = LogicalSystemStreamMutation::try_from(LogicalSystemStreamMutationInput {
            history_id: table.history_id,
            route: &route,
            item,
            old_item: Some(&old),
            previous_version: Some(ItemStreamVersion::new(2)),
            indexers: &[],
            item_stream_version: ItemStreamVersion::new(9),
            created_at: TimestampMillis::from_timestamp(1000),
        })
        .unwrap();
        let plan = DynamoStreamWritePlan::try_from(DynamoStreamWriteInput {
            table: &table,
            mutation,
            source_scope: SystemStreamScope::try_new("source", 1).unwrap(),
            position: SystemStreamPosition::try_new("shard", "lineage", "42").unwrap(),
        })
        .unwrap();
        let [image, pointer] = plan.rows;
        let reference = plan.record.pointer().unwrap();
        assert_eq!(
            reference.metadata().previous_version(),
            Some(ItemStreamVersion::new(2))
        );
        assert_eq!(
            reference.predecessor_record_bytes_bound(),
            Some(
                old.dynamo_json_bytes().unwrap().len() + ItemHistoryRecord::metadata_length_bound()
            )
        );
        let index = StreamRowValue::from_planned(pointer)
            .unwrap()
            .to_bytes()
            .unwrap();
        assert!(index.len() < 2000);
        let system = serde_json::to_vec(&plan.record).unwrap();
        assert!(system.len() < 2000);
        let record = ItemHistoryRecord::decode(
            &image.value().data,
            (
                reference.metadata().address(),
                reference.metadata().version(),
            ),
            ItemHistoryRecord::encoded_length_bound(),
        )
        .unwrap()
        .into_entry(table.max_indexers)
        .unwrap();
        assert_eq!(
            record.metadata().kind(),
            if current.is_some() {
                ItemHistoryKind::Image
            } else {
                ItemHistoryKind::Deleted
            }
        );
        match record.change() {
            ItemHistoryChange::Image { item, .. } => assert_eq!(
                item.to_attribute_map().unwrap().get("body"),
                Some(&AttributeValue::S(current.unwrap()))
            ),
            ItemHistoryChange::Deleted { key: marker } => assert_eq!(marker, &key),
        }
        assert!(!String::from_utf8(system).unwrap().contains("secret"));
    }
}

#[test]
fn given_another_table_when_write_is_planned_then_identity_is_rejected() {
    let table = table();
    let key = KeyAttributes::from([("pk".into(), AttributeValue::S("key".into()))]);
    let route = DynamoMutationRoute::try_new(&table.table_name, &table.key_schema, &key).unwrap();
    let mutation = LogicalSystemStreamMutation::try_from(LogicalSystemStreamMutationInput {
        history_id: 8_u32.into(),
        route: &route,
        item: Some(
            route
                .verify_item(
                    WireItem::try_from(storage_types::AttributeMap::from_iter([(
                        "pk".into(),
                        AttributeValue::S("key".into()),
                    )]))
                    .unwrap(),
                )
                .unwrap(),
        ),
        old_item: None,
        previous_version: None,
        indexers: &[],
        item_stream_version: ItemStreamVersion::new(1),
        created_at: TimestampMillis::from_timestamp(1000),
    })
    .unwrap();
    assert!(
        DynamoStreamWritePlan::try_from(DynamoStreamWriteInput {
            table: &table,
            mutation,
            source_scope: SystemStreamScope::try_new("source", 1).unwrap(),
            position: SystemStreamPosition::try_new("shard", "lineage", "1").unwrap(),
        })
        .is_err()
    );
}
