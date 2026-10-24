use crate::{AttributeValue, KeyAttributes, VerifiedWireItem, WireItem};

#[test]
fn given_addressed_item_when_keys_are_verified_then_missing_keys_are_completed_and_conflicts_rejected()
 {
    let key = KeyAttributes::from_iter([("pk".into(), AttributeValue::S("expected".into()))]);
    let missing = WireItem::dynamo_json(br#"{"payload":{"S":"value"}}"#.to_vec());
    let verified = VerifiedWireItem::try_new(missing, &key).unwrap();
    assert_eq!(
        verified.attribute_value("pk").unwrap(),
        Some(AttributeValue::S("expected".into()))
    );
    let item = WireItem::from(verified);
    assert_eq!(
        item.attribute_value("payload").unwrap(),
        Some(AttributeValue::S("value".into()))
    );
    let conflicting = WireItem::dynamo_json(br#"{"pk":{"S":"different"}}"#.to_vec());
    assert!(VerifiedWireItem::try_new(conflicting, &key).is_err());
}
