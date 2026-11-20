use storage_types::{
    AttributeValue, KeyAttributes, PairId, RegionId, RegionalOrder, StoredWireItem,
    SystemStreamPosition, TableName, WireItem,
};

use crate::{
    message::{
        LineageHead, LineageRef, PageChange, PageEntry, RegionalRejection, RegionalRequest,
        RegionalResponse, ReplicationPage,
    },
    transport::codec,
};

fn lineage(name: &str) -> LineageRef {
    let position = SystemStreamPosition::try_new("shard", name, "0").unwrap();
    LineageRef {
        shard: position.shard_id().clone(),
        lineage: position.lineage_id().clone(),
    }
}

fn offset(value: &str) -> storage_types::SystemStreamOffset {
    SystemStreamPosition::try_new("shard", "lineage", value)
        .unwrap()
        .offset()
        .clone()
}

fn order(stamp: u64) -> RegionalOrder {
    RegionalOrder::from_parts(
        stamp,
        RegionId(std::num::NonZeroU16::new(1).unwrap()),
        false,
    )
}

fn keys(key: &str) -> KeyAttributes {
    KeyAttributes::from_iter([("pk".into(), AttributeValue::S(key.into()))])
}

fn round_trip<T: serde::Serialize + serde::de::DeserializeOwned>(value: &T) -> T {
    let frame = codec::encode(value, 1 << 20).unwrap();
    codec::decode(frame.packed, frame.raw_bytes).unwrap()
}

fn json<T: serde::Serialize>(value: &T) -> serde_json::Value {
    serde_json::to_value(value).unwrap()
}

#[test]
fn given_each_message_when_json_round_trip_then_equal_fields() {
    let item = StoredWireItem {
        item: WireItem::dynamo_json(br#"{"pk":{"S":"a"},"v":{"S":"x"}}"#.to_vec()),
        indexers: Vec::new(),
    };
    let page = ReplicationPage {
        pair: PairId::from_bytes([9; 16]),
        table: TableName::new("orders"),
        lineage: lineage("l1"),
        parents: vec![lineage("l0")],
        after: None,
        through: Some(offset("12")),
        sealed: false,
        entries: vec![
            PageEntry {
                position: offset("10"),
                keys: keys("a"),
                order: order(100),
                change: PageChange::Put {
                    item: Box::new(item),
                },
            },
            PageEntry {
                position: offset("11"),
                keys: keys("b"),
                order: order(101),
                change: PageChange::Delete,
            },
            PageEntry {
                position: offset("12"),
                keys: keys("c"),
                order: order(102),
                change: PageChange::Echo,
            },
        ],
    };
    let request = RegionalRequest::Page(page);
    let decoded = round_trip(&request);
    assert_eq!(json(&decoded), json(&request));
    let RegionalRequest::Page(decoded) = decoded else {
        panic!("page expected")
    };
    assert_eq!(decoded.entries.len(), 3);
    assert_eq!(decoded.after, None);
    assert_eq!(decoded.through, Some(offset("12")));
    assert!(matches!(decoded.entries[0].change, PageChange::Put { .. }));
    assert!(matches!(decoded.entries[1].change, PageChange::Delete));
    assert!(matches!(decoded.entries[2].change, PageChange::Echo));

    let gap = RegionalResponse::Gap { expected: None };
    assert_eq!(json(&round_trip(&gap)), json(&gap));

    let heads = vec![
        LineageHead {
            lineage: lineage("l0"),
            through: Some(offset("5")),
            sealed: true,
        },
        LineageHead {
            lineage: lineage("l1"),
            through: None,
            sealed: false,
        },
    ];
    let left = RegionalResponse::Left {
        last: heads.clone(),
    };
    let RegionalResponse::Left { last } = round_trip(&left) else {
        panic!("left expected")
    };
    assert_eq!(last, heads);

    let rejected = RegionalResponse::Rejected(RegionalRejection::ParentNotSealed);
    assert!(matches!(
        round_trip(&rejected),
        RegionalResponse::Rejected(RegionalRejection::ParentNotSealed)
    ));
}
