use std::sync::Arc;

use regional_replication::message::PageChange;
use storage_types::{
    AttributeMap, AttributeValue, BoundTableInfo, CreateTableRequest, IndexerDeclaration,
    KeyAttributes, MaxIndexers, RegionId, RegionalMark, RegionalOrder, RegionalRole, StreamBinding,
    StreamViewType, TimestampMillis, VerifiedWireItem, WireItem,
};
use stream_provider::{DynamoMutationRoute, PositionedStreamEntry, StreamEntry, StreamEntryInput};

use super::regional_send::page_entry;

fn table() -> BoundTableInfo {
    let request: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "TableName": "orders",
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}]
    }))
    .unwrap();
    let mut info = request
        .stored_table_info_at(TimestampMillis::from_timestamp(0))
        .unwrap();
    info.max_indexers = MaxIndexers::try_new(1).unwrap();
    BoundTableInfo {
        history_id: 7_u32.into(),
        info: Arc::new(info),
        binding: storage_types::TableBinding {
            stream: Some(StreamBinding {
                view: StreamViewType::NewAndOldImages,
                topology_revision: 0,
                xid_bias: 0,
            }),
            ..Default::default()
        },
    }
}

fn keys() -> KeyAttributes {
    KeyAttributes::from_iter([("pk".into(), AttributeValue::S("a".into()))])
}

fn image(body: &str) -> WireItem {
    WireItem::try_from(AttributeMap::from_iter([
        ("pk".into(), AttributeValue::S("a".into())),
        ("body".into(), AttributeValue::S(body.into())),
    ]))
    .unwrap()
}

fn mark(role: RegionalRole) -> RegionalMark {
    let order = RegionalOrder::from_parts(
        1_024_000,
        RegionId(std::num::NonZeroU16::new(1).unwrap()),
        false,
    );
    RegionalMark {
        order,
        role,
        hidden: false,
    }
}

/// A positioned entry of the given shape; `mark` None leaves it unpaired.
fn positioned(has_new: bool, has_old: bool, mark: Option<RegionalMark>) -> PositionedStreamEntry {
    let table = table();
    let route =
        DynamoMutationRoute::try_new(&table.table_name, &table.key_schema, &keys()).unwrap();
    let old = route.verify_item(image("before")).unwrap();
    let new = VerifiedWireItem::try_from_shared(Arc::new(image("after")), &keys()).unwrap();
    let declaration = IndexerDeclaration::try_new(vec![], table.max_indexers).unwrap();
    let no_indexers: [String; 0] = [];
    let input = StreamEntryInput::try_new(
        &table,
        &route,
        has_new.then_some((&new, &declaration)),
        has_old.then_some((&old, &no_indexers[..])),
        TimestampMillis::from_timestamp(123),
        None,
    )
    .unwrap()
    .with_regional(mark);
    wrap(
        &table,
        StreamEntry::decode(&input.encode().unwrap()).unwrap(),
    )
}

fn hidden(role: RegionalRole) -> PositionedStreamEntry {
    let table = table();
    let route =
        DynamoMutationRoute::try_new(&table.table_name, &table.key_schema, &keys()).unwrap();
    let input = StreamEntryInput::hidden(
        &table,
        &route,
        TimestampMillis::from_timestamp(123),
        mark(role),
    )
    .unwrap();
    wrap(
        &table,
        StreamEntry::decode(&input.encode().unwrap()).unwrap(),
    )
}

fn wrap(table: &BoundTableInfo, entry: StreamEntry) -> PositionedStreamEntry {
    PositionedStreamEntry {
        history_id: table.history_id,
        source_table_name: table.table_name.clone(),
        entry,
        source_scope: storage_types::SystemStreamScope::try_new("source", 1).unwrap(),
        position: storage_types::SystemStreamPosition::try_new("shard", "lineage", "42").unwrap(),
    }
}

#[test]
fn given_marks_when_page_entry_then_selection() {
    let origin_insert = page_entry(&positioned(true, false, Some(mark(RegionalRole::Origin))))
        .unwrap()
        .unwrap();
    assert!(matches!(origin_insert.change, PageChange::Put { .. }));
    assert_eq!(origin_insert.keys, keys());
    assert_eq!(origin_insert.order, mark(RegionalRole::Origin).order);
    assert_eq!(
        origin_insert.position,
        *positioned(true, false, None).position.offset()
    );

    let origin_modify = page_entry(&positioned(true, true, Some(mark(RegionalRole::Origin))))
        .unwrap()
        .unwrap();
    assert!(matches!(origin_modify.change, PageChange::Put { .. }));

    let origin_remove = page_entry(&positioned(false, true, Some(mark(RegionalRole::Origin))))
        .unwrap()
        .unwrap();
    assert!(matches!(origin_remove.change, PageChange::Delete));

    let origin_hidden = page_entry(&hidden(RegionalRole::Origin)).unwrap().unwrap();
    assert!(matches!(origin_hidden.change, PageChange::Delete));

    let echo_hidden = page_entry(&hidden(RegionalRole::Echo)).unwrap().unwrap();
    assert!(matches!(echo_hidden.change, PageChange::Echo));
    let echo_public = page_entry(&positioned(false, true, Some(mark(RegionalRole::Echo))))
        .unwrap()
        .unwrap();
    assert!(matches!(echo_public.change, PageChange::Echo));

    assert!(
        page_entry(&positioned(true, true, Some(mark(RegionalRole::Applied))))
            .unwrap()
            .is_none()
    );
    assert!(page_entry(&positioned(true, true, None)).unwrap().is_none());
}
