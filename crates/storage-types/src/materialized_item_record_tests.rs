use crate::{
    AttributeValue, IndexerDeclaration, KeyAttributes, MaterializedItemRecord, MaxIndexers,
    TableHistoryId, VerifiedWireItem, WireItem,
};

fn history() -> TableHistoryId {
    TableHistoryId::from([3; 16])
}

#[test]
fn given_full_image_when_stored_then_exact_spelling_and_declarations_are_preserved() {
    let raw = br#"{ "pk" : {"S":"a"}, "number":{"N":"1.00e+0"}, "category":{"S":"x"} }"#;
    let item = WireItem::dynamo_json(raw.to_vec());
    let capacity = MaxIndexers::try_new(2).unwrap();
    let declaration =
        IndexerDeclaration::try_new(vec!["category".into(), "absent".into()], capacity).unwrap();
    let bytes = MaterializedItemRecord::encode(history(), &item, &declaration).unwrap();
    let record = MaterializedItemRecord::decode(&bytes, history(), bytes.len()).unwrap();
    assert!(record.decoded_bytes() > raw.len());
    let stored = record.into_stored(capacity).unwrap();
    assert_eq!(stored.indexers, declaration.names());
    assert_eq!(stored.item.dynamo_json_bytes().unwrap().as_ref(), raw);
    let key = KeyAttributes::from([("pk".to_owned(), AttributeValue::S("a".into()))]);
    let verified = VerifiedWireItem::try_from_stored(stored, &key).unwrap();
    assert_eq!(verified.dynamo_json_bytes().unwrap().as_ref(), raw);
}

#[test]
fn given_wrong_identity_or_frame_when_native_value_is_decoded_then_rejected_before_item_parsing() {
    let item = WireItem::dynamo_json(br#"{"pk":{"S":"a"}}"#.to_vec());
    let declaration = IndexerDeclaration::try_new(Vec::new(), MaxIndexers::ZERO).unwrap();
    let bytes = MaterializedItemRecord::encode(history(), &item, &declaration).unwrap();
    assert!(
        MaterializedItemRecord::decode(&bytes, TableHistoryId::from([4; 16]), bytes.len()).is_err()
    );
    assert!(MaterializedItemRecord::decode(&bytes, history(), bytes.len() - 1).is_err());
    for length in [0, 1, 20, bytes.len() - 1] {
        assert!(MaterializedItemRecord::decode(&bytes[..length], history(), bytes.len()).is_err());
    }
    let mut changed = bytes.clone();
    changed[0] = 2;
    assert!(MaterializedItemRecord::decode(&changed, history(), changed.len()).is_err());
    let mut changed = bytes;
    changed[20] ^= 1;
    assert!(MaterializedItemRecord::decode(&changed, history(), changed.len()).is_err());
}

#[test]
fn given_invalid_declaration_or_value_when_materialized_image_is_read_then_rejected() {
    let capacity = MaxIndexers::try_new(2).unwrap();
    for names in [vec!["x", "x"], vec![""], vec!["x"]] {
        let item = WireItem::dynamo_json(br#"{"pk":{"S":"a"},"x":{"N":"1"}}"#.to_vec());
        let payload = serde_json::to_vec(&(names, crate::dynamo_json::ItemRef(&item))).unwrap();
        let mut bytes = vec![1];
        bytes.extend_from_slice(history().as_ref());
        bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&payload);
        assert!(
            MaterializedItemRecord::decode(&bytes, history(), bytes.len())
                .unwrap()
                .into_stored(capacity)
                .is_err()
        );
    }
}

#[test]
fn given_missing_stored_key_when_verified_then_no_legacy_key_completion_is_allowed() {
    let stored = crate::StoredWireItem {
        item: WireItem::dynamo_json(br#"{"value":{"S":"x"}}"#.to_vec()),
        indexers: Vec::new(),
    };
    let key = KeyAttributes::from([("pk".to_owned(), AttributeValue::S("a".into()))]);
    assert!(VerifiedWireItem::try_from_stored(stored, &key).is_err());
}
