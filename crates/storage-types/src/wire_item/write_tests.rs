use crate::{AttributeValue, WireItem};

#[test]
fn given_ordinary_numbers_when_normalized_then_original_wire_buffer_is_retained() {
    let bytes = br#"{ "pk": {"S":"x"}, "n":{"N":"123"}, "escaped":{"N":"\u0031"} }"#.to_vec();
    let pointer = bytes.as_ptr();
    let item = WireItem::dynamo_json(bytes);
    let expected_size = item.validate_for_write("Item").unwrap();
    let (item, size) = item.normalized_for_write().unwrap();
    let WireItem::DynamoJson { data } = item;
    assert_eq!(data.as_ptr(), pointer);
    assert_eq!(size, expected_size);
}

#[test]
fn given_scientific_and_nested_numbers_when_normalized_then_only_numeric_values_change() {
    let item = WireItem::dynamo_json(br#"{"pk":{"S":"x"},"n":{"N":"1e2"},"nested":{"M":{"values":{"NS":["2e1","3"]}}},"text":{"S":"1e2"},"zero":{"N":"0e125"}}"#.to_vec());
    let expected_size = item.validate_for_write("Item").unwrap();
    let (item, size) = item.normalized_for_write().unwrap();
    let actual = item.into_attribute_map().unwrap();
    assert_eq!(actual.get("n"), Some(&AttributeValue::N("100".into())));
    assert_eq!(actual.get("text"), Some(&AttributeValue::S("1e2".into())));
    assert_eq!(actual.get("zero"), Some(&AttributeValue::N("0".into())));
    let AttributeValue::M(nested) = actual.get("nested").unwrap() else {
        panic!("expected map")
    };
    assert_eq!(
        nested.get("values"),
        Some(&AttributeValue::NS(vec!["20".into(), "3".into()]))
    );
    assert_eq!(size, expected_size);
}

#[test]
fn given_invalid_wire_attributes_when_validated_or_normalized_then_both_reject_them() {
    for bytes in [
        r#"{}"#,
        r#"{"n":{"N":"1e126"}}"#,
        r#"{"n":{"N":"invalid"}}"#,
        r#"{"n":{"N":"1","S":"x"}}"#,
        r#"{"n":{"N":"1"},"n":{"N":"2"}}"#,
        r#"{"n":{"NS":["1","1e0"]}}"#,
        r#"{"nested":{"L":[{"N":"invalid"}]}}"#,
    ] {
        let item = WireItem::dynamo_json(bytes.as_bytes().to_vec());
        assert!(item.validate_for_write("Item").is_err(), "{bytes}");
        assert!(item.normalized_for_write().is_err(), "{bytes}");
    }
}

#[test]
fn given_ordered_numeric_keys_when_normalized_then_lookup_order_is_preserved() {
    let bytes = br#"{"pk":{"N":"1e2"},"sk":{"S":"sort"},"gsi1pk":{"N":"2e2"},"gsi1sk":{"S":"index"},"ttl":{"N":"3e2"},"indexer":{"S":"ref"},"payload":{"S":"value"}}"#;
    let item = WireItem::dynamo_json(bytes.to_vec());
    let (normalized, _) = item.normalized_for_write().unwrap();
    let attributes = normalized.to_attribute_map().unwrap();
    assert_eq!(
        attributes.keys().collect::<Vec<_>>(),
        ["pk", "sk", "gsi1pk", "gsi1sk", "ttl", "indexer", "payload"]
    );
    assert_eq!(attributes.get("pk"), Some(&AttributeValue::N("100".into())));
    assert_eq!(
        attributes.get("gsi1pk"),
        Some(&AttributeValue::N("200".into()))
    );
    assert_eq!(
        attributes.get("ttl"),
        Some(&AttributeValue::N("300".into()))
    );
}
