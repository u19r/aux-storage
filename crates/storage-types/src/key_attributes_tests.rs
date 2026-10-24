use std::collections::HashMap;

use crate::{AttributeValue, KeyAttributes, canonicalize_top_level_number_value};

#[test]
fn given_number_set_when_canonicalized_then_valid_members_change_and_invalid_ones_survive() {
    let mut value = AttributeValue::NS(vec![
        "1E2".to_owned(),
        "2.00".to_owned(),
        "not-a-number".to_owned(),
    ]);

    canonicalize_top_level_number_value(&mut value);

    assert_eq!(
        value,
        AttributeValue::NS(vec![
            "100".to_owned(),
            "2".to_owned(),
            "not-a-number".to_owned(),
        ])
    );
}

#[test]
fn given_same_keys_inserted_in_reverse_order_when_compared_then_keys_are_equal() {
    let forward = KeyAttributes::from([
        ("pk".to_owned(), AttributeValue::S("partition".to_owned())),
        ("sk".to_owned(), AttributeValue::S("sort".to_owned())),
    ]);
    let reverse = KeyAttributes::from([
        ("sk".to_owned(), AttributeValue::S("sort".to_owned())),
        ("pk".to_owned(), AttributeValue::S("partition".to_owned())),
    ]);

    assert_eq!(forward, reverse);
}

#[test]
fn given_hash_map_keys_when_canonically_serialized_and_deserialized_then_keys_are_equal() {
    let from_hash_map = KeyAttributes::from(HashMap::from([
        ("pk".to_owned(), AttributeValue::S("partition".to_owned())),
        ("sk".to_owned(), AttributeValue::S("sort".to_owned())),
    ]));
    let canonical = from_hash_map
        .canonical_dynamo_json()
        .expect("canonical key serialization");
    let round_trip: KeyAttributes =
        serde_json::from_str(&canonical).expect("canonical key deserialization");

    assert_eq!(from_hash_map, round_trip);
}

#[test]
fn given_equivalent_numeric_key_forms_when_canonically_serialized_then_json_is_identical() {
    let scientific =
        KeyAttributes::from([("pk".to_owned(), AttributeValue::N("1E-130".to_owned()))]);
    let expanded = KeyAttributes::from([(
        "pk".to_owned(),
        AttributeValue::N(
            "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001".to_owned(),
        ),
    )]);

    assert_eq!(
        scientific.canonical_dynamo_json().unwrap(),
        expanded.canonical_dynamo_json().unwrap()
    );
}

#[test]
fn given_mixed_keys_when_canonically_serialized_then_bytes_and_errors_are_preserved() {
    let keys = KeyAttributes::from([
        ("sk".to_owned(), AttributeValue::N("1.0".to_owned())),
        ("pk".to_owned(), AttributeValue::S("a\"b".to_owned())),
    ]);
    assert_eq!(
        keys.canonical_dynamo_json().expect("canonical key"),
        r#"{"pk":{"S":"a\"b"},"sk":{"N":"1"}}"#
    );
    let invalid = KeyAttributes::from([("pk".to_owned(), AttributeValue::N("NaN".to_owned()))]);
    assert!(invalid.canonical_dynamo_json().is_err());
}

#[test]
fn given_different_key_name_value_or_cardinality_when_compared_then_keys_are_not_equal() {
    let base = KeyAttributes::from([("pk".to_owned(), AttributeValue::S("partition".to_owned()))]);
    let different_name = KeyAttributes::from([(
        "other".to_owned(),
        AttributeValue::S("partition".to_owned()),
    )]);
    let different_value =
        KeyAttributes::from([("pk".to_owned(), AttributeValue::S("different".to_owned()))]);
    let different_cardinality = KeyAttributes::from([
        ("pk".to_owned(), AttributeValue::S("partition".to_owned())),
        ("sk".to_owned(), AttributeValue::S("sort".to_owned())),
    ]);

    assert_ne!(base, different_name);
    assert_ne!(base, different_value);
    assert_ne!(base, different_cardinality);
}

#[test]
fn given_duplicate_key_name_when_inserted_then_value_is_replaced_without_growth() {
    let mut keys = KeyAttributes::new();
    keys.insert("pk", AttributeValue::S("old".to_owned()));
    keys.insert("pk", AttributeValue::S("new".to_owned()));

    assert_eq!(keys.len(), 1);
    assert_eq!(keys.get("pk"), Some(&AttributeValue::S("new".to_owned())));
}
