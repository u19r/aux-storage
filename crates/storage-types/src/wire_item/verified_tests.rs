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

#[test]
fn given_shared_images_when_verified_then_write_completion_and_stored_checks_keep_their_rules() {
    use std::sync::Arc;

    use crate::StoredWireItem;
    let key = KeyAttributes::from_iter([("pk".into(), AttributeValue::S("expected".into()))]);
    let images = [
        WireItem::dynamo_json(br#"{"pk":{"S":"expected"},"payload":{"S":"v"}}"#.to_vec()),
        WireItem::dynamo_json(br#"{"payload":{"S":"v"}}"#.to_vec()),
        WireItem::dynamo_json(br#"{"pk":{"S":"different"}}"#.to_vec()),
    ];
    for item in images {
        let owned = VerifiedWireItem::try_new(item.clone(), &key);
        let shared = Arc::new(item.clone());
        let result = VerifiedWireItem::try_from_shared(Arc::clone(&shared), &key);
        match (owned, result) {
            (Ok(owned), Ok(verified)) => {
                assert_eq!(
                    serde_json::to_vec(&*owned).unwrap(),
                    serde_json::to_vec(&*verified).unwrap()
                );
                if item.has_complete_write_keys(&key).unwrap() {
                    assert!(std::ptr::eq(&*verified, &*shared));
                }
            }
            (Err(_), Err(_)) => {}
            _ => panic!("shared ownership changed write verification"),
        }
        let expected = item.has_verified_keys(&key).unwrap_or(false);
        assert_eq!(
            VerifiedWireItem::try_from_stored(
                StoredWireItem {
                    item,
                    indexers: vec![]
                },
                &key
            )
            .is_ok(),
            expected
        );
    }
    let unique = Arc::new(WireItem::dynamo_json(br#"{"payload":{"S":"v"}}"#.to_vec()));
    assert!(
        VerifiedWireItem::try_from_shared(unique, &key)
            .unwrap()
            .has_verified_keys(&key)
            .unwrap()
    );
}
