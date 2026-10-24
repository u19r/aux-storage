use std::collections::HashMap;

use crate::{
    AttributeValue, KeyAttributes, StorageEnum, WireItem, WireItemKeyAttributes,
    context::WrappedError as _,
};

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
    let original_data = match &original {
        WireItem::DynamoJson { data } => data.as_ptr(),
        WireItem::LocalSplit { .. } => panic!("expected Dynamo JSON"),
    };

    let verified = original
        .with_verified_keys(&key(AttributeValue::N("100".to_owned())))
        .unwrap();

    match verified {
        WireItem::DynamoJson { data } => assert_eq!(data.as_ptr(), original_data),
        WireItem::LocalSplit { .. } => panic!("complete item changed representation"),
    }
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
fn mismatched_local_split_key_is_rejected_even_if_blob_disagrees_in_other_direction() {
    let blob = serde_json::to_vec(&HashMap::from([(
        "pk".to_owned(),
        AttributeValue::S("requested".to_owned()),
    )]))
    .unwrap();
    let item = WireItem::local_split(
        WireItemKeyAttributes::new(
            "pk".to_owned(),
            AttributeValue::S("stored".to_owned()),
            None,
            None,
        ),
        None,
        Some(blob),
    );

    let error = item
        .with_verified_keys(&key(AttributeValue::S("requested".to_owned())))
        .unwrap_err();

    assert!(matches!(
        error.to_enum(),
        StorageEnum::InternalServerError { message }
            if message == "Key attribute mismatch for pk"
    ));
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
