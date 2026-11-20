use std::collections::HashMap;

use crate::{AttributeValue, KeyAttributes, WireItem};

fn key(value: AttributeValue) -> KeyAttributes {
    let mut key = KeyAttributes::new();
    key.insert("pk", value);
    key
}

#[test]
fn complete_wire_item_keeps_its_owned_representation_for_equivalent_numeric_key() {
    let original = WireItem::try_from(&HashMap::from([
        ("pk".to_owned(), AttributeValue::N("1E2".to_owned())),
        ("data".to_owned(), AttributeValue::S("value".to_owned())),
    ]))
    .unwrap();
    let WireItem::DynamoJson { data } = &original;
    let original_data = data.as_ptr();

    let verified = original
        .with_verified_keys(&key(AttributeValue::N("100".to_owned())))
        .unwrap();

    let WireItem::DynamoJson { data } = verified;
    assert_eq!(data.as_ptr(), original_data);
}

#[test]
fn missing_wire_key_is_completed_and_numeric_key_is_write_normalized() {
    let original = WireItem::try_from(&HashMap::from([(
        "data".to_owned(),
        AttributeValue::S("value".to_owned()),
    )]))
    .unwrap();

    let completed = original
        .with_verified_keys(&key(AttributeValue::N("1E2".to_owned())))
        .unwrap();

    assert_eq!(
        completed.attribute_value("pk").unwrap(),
        Some(AttributeValue::N("100".to_owned()))
    );
    assert_eq!(
        completed.attribute_value("data").unwrap(),
        Some(AttributeValue::S("value".to_owned()))
    );
}

#[test]
fn invalid_stored_numeric_key_is_not_treated_as_equal_to_invalid_request_key() {
    let item = WireItem::try_from(&HashMap::from([(
        "pk".to_owned(),
        AttributeValue::N("invalid".to_owned()),
    )]))
    .unwrap();

    let error = item
        .with_verified_keys(&key(AttributeValue::N("also-invalid".to_owned())))
        .unwrap_err();

    assert!(error.to_string().contains("Invalid numeric key"));
}
