use super::{WireItem, WireItemKeyAttributes};
use crate::{AttributeMap, AttributeValue, KeyAttributes};

#[test]
fn given_incomplete_image_when_keys_are_verified_then_missing_keys_remain_missing() {
    let key = KeyAttributes::from_iter([
        ("pk".into(), AttributeValue::N("100".into())),
        ("sk".into(), AttributeValue::S("sort".into())),
    ]);
    let image = WireItem::dynamo_json(br#"{"pk":{"N":"1e2"},"payload":{"S":"value"}}"#.to_vec());
    let before = serde_json::to_vec(&image).unwrap();

    assert!(!image.has_verified_keys(&key).unwrap());
    assert_eq!(serde_json::to_vec(&image).unwrap(), before);
    assert!(image.attribute_value("sk").unwrap().is_none());
}

#[test]
fn given_conflicting_or_invalid_numeric_image_when_keys_are_verified_then_verification_fails() {
    let key = KeyAttributes::from_iter([("pk".into(), AttributeValue::N("100".into()))]);
    for data in [
        br#"{"pk":{"N":"101"}}"#.as_slice(),
        br#"{"pk":{"N":"invalid"}}"#.as_slice(),
        br#"{"pk":{"S":"100"}}"#.as_slice(),
    ] {
        let image = WireItem::dynamo_json(data.to_vec());
        assert!(image.has_verified_keys(&key).is_err());
    }
}

#[test]
fn given_equivalent_split_key_when_keys_are_verified_then_numeric_bytes_and_representation_survive()
{
    let key = KeyAttributes::from_iter([("pk".into(), AttributeValue::N("100".into()))]);
    let image = WireItem::local_split(
        WireItemKeyAttributes::new("pk".into(), AttributeValue::N("1e2".into()), None, None),
        Some(WireItemKeyAttributes::new(
            "pk".into(),
            AttributeValue::N("100.0".into()),
            None,
            None,
        )),
        Some(br#"{ "pk":{"N":"100E0"}, "payload": {"S":"value"} }"#.to_vec()),
    );
    let before = serde_json::to_vec(&image).unwrap();

    assert!(image.has_verified_keys(&key).unwrap());
    assert_eq!(serde_json::to_vec(&image).unwrap(), before);
    assert!(matches!(image, WireItem::LocalSplit { .. }));
}

#[test]
fn given_duplicate_json_key_when_keys_are_verified_then_ambiguous_images_are_rejected() {
    let key = KeyAttributes::from_iter([("pk".into(), AttributeValue::N("100".into()))]);
    for data in [
        br#"{"pk":{"N":"101"},"pk":{"N":"100"}}"#.as_slice(),
        br#"{"pk":{"N":"100"},"pk":{"N":"101"}}"#.as_slice(),
    ] {
        let image = WireItem::dynamo_json(data.to_vec());
        assert!(AttributeMap::try_from(&image).is_err());
        assert!(image.has_verified_keys(&key).is_err());
    }
}

#[test]
fn given_only_an_entity_alias_when_keys_are_verified_then_the_addressed_component_is_missing() {
    let key = KeyAttributes::from_iter([("created_at".into(), AttributeValue::N("100".into()))]);
    let image = WireItem::dynamo_json(br#"{"c_at":{"N":"100"}}"#.to_vec());

    assert!(!image.has_verified_keys(&key).unwrap());
}

#[test]
fn given_trailing_json_data_when_keys_are_verified_then_the_image_is_rejected() {
    let key = KeyAttributes::from_iter([("pk".into(), AttributeValue::N("100".into()))]);
    let image = WireItem::dynamo_json(br#"{"pk":{"N":"100"}} trailing"#.to_vec());

    assert!(image.has_verified_keys(&key).is_err());
}
