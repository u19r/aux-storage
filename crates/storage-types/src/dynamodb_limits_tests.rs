use std::collections::HashMap;

use crate::{
    AttributeValue, KeySchemaElement, KeyType, StorageEnum,
    context::WrappedError,
    dynamodb_limits::{
        attribute_map_numbers_need_write_normalization, dynamodb_number_size,
        normalize_attribute_map_numbers_for_write, normalize_dynamodb_number_for_write,
        validate_item_key_attributes_for_schema, validate_key_attributes_for_schema,
    },
};

#[test]
fn dynamodb_number_size_matches_decimal_and_exponent_boundaries() {
    let cases = [
        ("1.2300", 3),
        ("0.000001", 2),
        ("1E+37", 2),
        ("1E-37", 2),
        ("12300", 3),
        ("-1.2300", 4),
        ("-0.000001", 3),
        ("-1E+37", 3),
        ("+1.2300", 3),
        ("+1E+37", 2),
    ];

    for (value, expected_size) in cases {
        assert_eq!(
            dynamodb_number_size(value),
            expected_size,
            "unexpected DynamoDB item-size bytes for {value}"
        );
    }
}

#[test]
fn normalize_dynamodb_number_for_write_expands_scientific_notation() {
    let normalized = normalize_dynamodb_number_for_write("1E-130");
    assert_eq!(
        normalized,
        "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001"
    );

    assert_eq!(normalize_dynamodb_number_for_write("-12.3E2"), "-1230");
    assert_eq!(normalize_dynamodb_number_for_write("12.3"), "12.3");
}

#[test]
fn normalize_attribute_map_numbers_for_write_walks_nested_values() {
    let mut item = HashMap::from([
        ("pk".to_string(), AttributeValue::N("1E2".to_string())),
        (
            "nested".to_string(),
            AttributeValue::M(HashMap::from([(
                "numbers".to_string(),
                AttributeValue::NS(vec!["2E2".to_string(), "3".to_string()]),
            )])),
        ),
        (
            "list".to_string(),
            AttributeValue::L(vec![AttributeValue::N("-4E-2".to_string())]),
        ),
    ]);

    assert!(attribute_map_numbers_need_write_normalization(&item));
    assert!(normalize_attribute_map_numbers_for_write(&mut item));
    assert_eq!(item.get("pk"), Some(&AttributeValue::N("100".to_string())));
    assert_eq!(
        item.get("nested"),
        Some(&AttributeValue::M(HashMap::from([(
            "numbers".to_string(),
            AttributeValue::NS(vec!["200".to_string(), "3".to_string()])
        )])))
    );
    assert_eq!(
        item.get("list"),
        Some(&AttributeValue::L(vec![AttributeValue::N(
            "-0.04".to_string()
        )]))
    );

    assert!(!attribute_map_numbers_need_write_normalization(&item));
    assert!(!normalize_attribute_map_numbers_for_write(&mut item));
}

#[test]
fn validate_key_attributes_rejects_invalid_dynamodb_numbers() {
    let schema = vec![KeySchemaElement {
        attribute_name: "pk".to_string(),
        key_type: KeyType::Hash,
    }];

    for (value, expected) in [
        ("", "The parameter cannot be converted to a numeric value"),
        (
            "999999999999999999999999999999999999999",
            "Attempting to store more than 38 significant digits in a Number",
        ),
        (
            "1E-131",
            "Number underflow. Attempting to store a number with magnitude smaller than supported \
             range",
        ),
    ] {
        let mut key = crate::KeyAttributes::new();
        key.insert("pk", AttributeValue::N(value.to_string()));

        let err = validate_key_attributes_for_schema(&schema, &key)
            .expect_err("invalid number key should fail");
        assert_validation_message(&err, expected);
    }
}

#[test]
fn validate_item_key_attributes_rejects_invalid_dynamodb_numbers() {
    let schema = vec![KeySchemaElement {
        attribute_name: "pk".to_string(),
        key_type: KeyType::Hash,
    }];
    let mut item = HashMap::new();
    item.insert(
        "pk".to_string(),
        AttributeValue::N("999999999999999999999999999999999999999".to_string()),
    );

    let err = validate_item_key_attributes_for_schema(&schema, &item)
        .expect_err("invalid number key should fail");

    assert_validation_message(
        &err,
        "Attempting to store more than 38 significant digits in a Number",
    );
}

fn assert_validation_message(err: &crate::StorageError, expected: &str) {
    let StorageEnum::Validation { message } = err.to_enum() else {
        panic!("expected validation error, got {err:?}");
    };
    assert_eq!(message, expected);
}
