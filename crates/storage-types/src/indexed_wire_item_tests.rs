use crate::{
    AttributeMap, AttributeValue, IndexerDeclaration, MaxIndexers, StorageValidationKind,
    validate_logical_item, validate_logical_item_names,
};

fn attributes<const N: usize>(entries: [(String, AttributeValue); N]) -> AttributeMap {
    entries.into_iter().collect()
}

fn declaration(names: &[&str]) -> IndexerDeclaration {
    IndexerDeclaration::try_new(
        names.iter().map(|name| (*name).to_string()).collect(),
        MaxIndexers::try_new(32).unwrap(),
    )
    .unwrap()
}

#[test]
fn given_non_string_indexed_value_when_validated_then_validation_fails() {
    let item = attributes([(
        "customer_id".to_string(),
        AttributeValue::N("1".to_string()),
    )]);

    let error = validate_logical_item(&item, &declaration(&["customer_id"])).unwrap_err();

    assert_eq!(
        error.validation_kind(),
        StorageValidationKind::InvalidIndexerType
    );
    assert!(
        error
            .to_string()
            .contains("Indexers:attribute_must_be_string")
    );
}

#[test]
fn given_each_non_string_attribute_family_when_indexed_then_validation_fails() {
    let values = [
        AttributeValue::N("1".to_string()),
        AttributeValue::B("AA==".to_string()),
        AttributeValue::BOOL(true),
        AttributeValue::NULL(true),
        AttributeValue::M(AttributeMap::new()),
        AttributeValue::L(Vec::new()),
        AttributeValue::SS(vec!["x".to_string()]),
        AttributeValue::NS(vec!["1".to_string()]),
        AttributeValue::BS(vec!["AA==".to_string()]),
    ];

    for value in values {
        let item = attributes([("indexed".to_string(), value)]);
        let error = validate_logical_item(&item, &declaration(&["indexed"])).unwrap_err();
        assert_eq!(
            error.validation_kind(),
            StorageValidationKind::InvalidIndexerType
        );
    }
}

#[test]
fn given_empty_indexed_string_when_validated_then_validation_fails() {
    let item = attributes([("customer_id".to_string(), AttributeValue::S(String::new()))]);

    let error = validate_logical_item(&item, &declaration(&["customer_id"])).unwrap_err();

    assert_eq!(error.validation_kind(), StorageValidationKind::EmptyIndexer);
    assert!(error.to_string().contains("Indexers:empty_string"));
}

#[test]
fn given_present_and_missing_string_values_when_validated_then_item_is_accepted() {
    let item = attributes([
        ("pk".to_string(), AttributeValue::S("item".to_string())),
        (
            "customer_id".to_string(),
            AttributeValue::S("customer".to_string()),
        ),
    ]);

    validate_logical_item(&item, &declaration(&["customer_id", "absent"])).unwrap();
    validate_logical_item_names(&item, &["customer_id".to_string(), "absent".to_string()]).unwrap();
}

#[test]
fn given_non_string_value_in_unindexed_attribute_when_validated_then_item_is_accepted() {
    let item = attributes([("count".to_string(), AttributeValue::N("1".to_string()))]);

    validate_logical_item_names(&item, &["customer_id".to_string()]).unwrap();
}
