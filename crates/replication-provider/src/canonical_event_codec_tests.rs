use std::num::NonZeroUsize;

use replication_types::{
    error::ReplicationProtocolError,
    event::{
        content::{
            AcceptedItemEvent, CanonicalEventCodec, CanonicalPayload, MutationCause, SourceImages,
        },
        identity::{CanonicalEventId, OriginEventId, VersionIdentity},
        order::{EffectRank, HybridLogicalClock, WinnerOrder},
    },
    identity::{origin::OriginId, resource::TableIdentity},
    protocol::{CanonicalBytes, PayloadBounds},
};
use storage_types::{AttributeValue, KeyAttributes, WireItem, WireItemKeyAttributes};

use super::canonical_event_codec::CanonicalJsonCodec;

fn codec() -> CanonicalJsonCodec {
    CanonicalJsonCodec::new(bounds())
}

#[test]
fn given_conflicting_or_incomplete_images_when_encoded_then_no_original_event_is_accepted() {
    for image in conflicting_images().into_iter().chain(incomplete_images()) {
        assert_images_rejected(Some(image.clone()), Some(matching_image()));
        assert_images_rejected(Some(matching_image()), Some(image));
    }
}

#[test]
fn given_equivalent_numeric_images_when_original_events_roundtrip_then_bytes_and_values_survive() {
    let old = WireItem::dynamo_json(
        br#"{ "pk": {"N":"1e2"}, "sk":{"S":"sort"}, "payload":{"S":"old"} }"#.to_vec(),
    );
    let new = WireItem::local_split(
        WireItemKeyAttributes::new(
            "pk".into(),
            AttributeValue::N("100.0".into()),
            Some("sk".into()),
            Some(AttributeValue::S("sort".into())),
        ),
        None,
        Some(br#"{ "payload": {"S":"new"} }"#.to_vec()),
    );
    let candidate = candidate(Some(old), Some(new)).unwrap();
    let expected = candidate.as_ref().to_vec();
    let event = codec().decode_event(candidate.as_ref()).unwrap();
    assert_eq!(event.as_ref().bytes.as_ref(), expected);
    let spelling = std::str::from_utf8(event.as_ref().bytes.as_ref()).unwrap();
    assert!(spelling.contains("1e2") && spelling.contains("100.0"));
    let CanonicalPayload::Item(item) = &event.as_ref().payload else {
        panic!("expected an accepted item event");
    };
    assert!(matches!(
        &item.images.old,
        Some(WireItem::DynamoJson { .. })
    ));
    assert!(matches!(
        &item.images.new,
        Some(WireItem::DynamoJson { .. })
    ));
}

fn assert_images_rejected(old: Option<WireItem>, new: Option<WireItem>) {
    let result = candidate(old, new);
    assert!(matches!(
        result,
        Err(ReplicationProtocolError::IdentityContentMismatch {
            context: "canonical source images"
        })
    ));
}

fn matching_image() -> WireItem {
    WireItem::dynamo_json(br#"{"pk":{"N":"100"},"sk":{"S":"sort"}}"#.to_vec())
}

fn conflicting_images() -> [WireItem; 6] {
    [
        WireItem::dynamo_json(br#"{"pk":{"N":"101"},"sk":{"S":"sort"}}"#.to_vec()),
        WireItem::dynamo_json(br#"{"pk":{"N":"100"},"sk":{"S":"other"}}"#.to_vec()),
        WireItem::local_split(
            WireItemKeyAttributes::new(
                "pk".into(),
                AttributeValue::N("101".into()),
                Some("sk".into()),
                Some(AttributeValue::S("sort".into())),
            ),
            None,
            None,
        ),
        WireItem::local_split(
            WireItemKeyAttributes::new(
                "pk".into(),
                AttributeValue::N("100".into()),
                Some("sk".into()),
                Some(AttributeValue::S("sort".into())),
            ),
            Some(WireItemKeyAttributes::new(
                "pk".into(),
                AttributeValue::N("101".into()),
                None,
                None,
            )),
            None,
        ),
        WireItem::local_split(
            WireItemKeyAttributes::new(
                "pk".into(),
                AttributeValue::N("100".into()),
                Some("sk".into()),
                Some(AttributeValue::S("sort".into())),
            ),
            None,
            Some(br#"{"pk":{"N":"101"}}"#.to_vec()),
        ),
        WireItem::local_split(
            WireItemKeyAttributes::new(
                "pk".into(),
                AttributeValue::N("100".into()),
                Some("pk".into()),
                Some(AttributeValue::N("101".into())),
            ),
            Some(WireItemKeyAttributes::new(
                "sk".into(),
                AttributeValue::S("sort".into()),
                None,
                None,
            )),
            None,
        ),
    ]
}

fn incomplete_images() -> [WireItem; 4] {
    [
        WireItem::dynamo_json(b"{}".to_vec()),
        WireItem::dynamo_json(br#"{"pk":{"N":"100"}}"#.to_vec()),
        WireItem::dynamo_json(br#"{"sk":{"S":"sort"}}"#.to_vec()),
        WireItem::local_split(
            WireItemKeyAttributes::new("pk".into(), AttributeValue::N("100".into()), None, None),
            None,
            None,
        ),
    ]
}

fn candidate(
    old: Option<WireItem>,
    new: Option<WireItem>,
) -> Result<CanonicalBytes, ReplicationProtocolError> {
    let origin = OriginId {
        region: "test-region".parse().unwrap(),
        database_group: [3; 16].try_into().unwrap(),
        journal_partition: 0.into(),
        journal_generation: 1.into(),
    };
    let id = OriginEventId {
        lineage: [2; 16].try_into().unwrap(),
        origin: origin.clone(),
        position: 1.into(),
        ordinal: 0.into(),
    };
    let key = KeyAttributes::from_iter([
        ("pk".into(), AttributeValue::N("100".into())),
        ("sk".into(), AttributeValue::S("sort".into())),
    ]);
    let payload = CanonicalPayload::Item(Box::new(AcceptedItemEvent {
        source_table: TableIdentity::try_from([1; 16]).unwrap(),
        lineage: id.lineage,
        schema: 1.into(),
        canonical_key: codec().encode_key(&key).unwrap(),
        key,
        images: SourceImages {
            old,
            new,
            old_indexers: None,
            new_indexers: None,
        },
        order: WinnerOrder::Mutation {
            clock: HybridLogicalClock {
                physical_ms: 1,
                logical: 0,
            },
            origin,
            position: id.position,
            ordinal: id.ordinal,
            rank: EffectRank::Mutation,
        },
        version: VersionIdentity::Mutation(id.clone()),
        cause: MutationCause::Update,
        predecessor: None,
        victim_order: None,
        parents: Vec::new(),
        transaction: None,
        observations: Vec::new(),
    }));
    let id = CanonicalEventId::Origin(id);
    codec().encode_event(
        crate::constants::CANONICAL_EVENT_SEMANTIC_VERSION.into(),
        &id,
        &payload,
    )
}

fn bounds() -> PayloadBounds {
    let limit = NonZeroUsize::new(16_384).unwrap();
    PayloadBounds {
        event_bytes: limit,
        manifest_bytes: limit,
    }
}
