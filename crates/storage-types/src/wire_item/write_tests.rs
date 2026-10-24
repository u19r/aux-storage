use crate::{AttributeValue, WireItem, WireItemKeyAttributes};

#[test]
fn given_ordinary_numbers_when_normalized_then_original_wire_buffer_is_retained() {
    let bytes = br#"{ "pk": {"S":"x"}, "n":{"N":"123.00"}, "escaped":{"N":"\u0031"} }"#.to_vec();
    let pointer = bytes.as_ptr();
    let item = WireItem::dynamo_json(bytes);
    let expected_size = item.validate_for_write("Item").unwrap();
    let (item, size) = item.normalized_for_write().unwrap();
    let WireItem::DynamoJson { data } = item else {
        panic!("expected wire buffer")
    };
    assert_eq!(data.as_ptr(), pointer);
    assert_eq!(size, expected_size);
}

#[test]
fn given_scientific_and_nested_numbers_when_normalized_then_only_numeric_values_change() {
    let item = WireItem::dynamo_json(br#"{"pk":{"S":"x"},"n":{"N":"1e2"},"nested":{"M":{"values":{"NS":["2e1","3"]}}},"text":{"S":"1e2"},"zero":{"N":"0e2147483647"}}"#.to_vec());
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
fn given_split_item_when_normalized_then_canonical_keys_and_residual_have_logical_size() {
    let primary =
        WireItemKeyAttributes::new("pk".into(), AttributeValue::N("1e2".into()), None, None);
    let secondary =
        WireItemKeyAttributes::new("gsi".into(), AttributeValue::N("2e1".into()), None, None);
    let item = WireItem::local_split(
        primary,
        Some(secondary),
        Some(br#"{"n":{"N":"3e1"},"nested":{"L":[{"N":"4e1"}]},"pk":{"S":"shadowed"}}"#.to_vec()),
    );
    let expected =
        serde_json::from_slice::<crate::AttributeMap>(&item.dynamo_json_bytes().unwrap()).unwrap();
    let expected_size = crate::LogicalItemSize::try_from_entries(expected.iter()).unwrap();
    let (item, size) = item.normalized_for_write().unwrap();
    assert!(matches!(item, WireItem::DynamoJson { .. }));
    assert_eq!(
        item.attribute_value("pk").unwrap(),
        Some(AttributeValue::N("100".into()))
    );
    assert_eq!(
        item.attribute_value("gsi").unwrap(),
        Some(AttributeValue::N("20".into()))
    );
    assert_eq!(
        item.attribute_value("n").unwrap(),
        Some(AttributeValue::N("30".into()))
    );
    assert_eq!(size, expected_size);
    assert_eq!(size, item.logical_size().unwrap());
}

#[test]
fn given_split_item_without_residual_when_normalized_then_typed_keys_are_validated() {
    for blob in [None, Some(vec![]), Some(b"{}".to_vec())] {
        let primary =
            WireItemKeyAttributes::new("pk".into(), AttributeValue::N("2e1".into()), None, None);
        let (item, size) = WireItem::local_split(primary, None, blob)
            .normalized_for_write()
            .unwrap();
        assert_eq!(
            item.attribute_value("pk").unwrap(),
            Some(AttributeValue::N("20".into()))
        );
        assert_eq!(size, item.logical_size().unwrap());
    }
    let primary =
        WireItemKeyAttributes::new("pk".into(), AttributeValue::N("invalid".into()), None, None);
    assert!(
        WireItem::local_split(primary, None, None)
            .normalized_for_write()
            .is_err()
    );
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
fn given_overlapping_split_keys_when_normalized_then_last_key_wins_and_shadowed_residual_is_ignored()
 {
    let primary =
        WireItemKeyAttributes::new("pk".into(), AttributeValue::N("invalid".into()), None, None);
    let secondary =
        WireItemKeyAttributes::new("pk".into(), AttributeValue::N("1e2".into()), None, None);
    let item = WireItem::local_split(
        primary,
        Some(secondary),
        Some(br#"{"pk":{"unknown":"shadowed"}}"#.to_vec()),
    );
    let (item, size) = item.normalized_for_write().unwrap();
    assert_eq!(
        item.attribute_value("pk").unwrap(),
        Some(AttributeValue::N("100".into()))
    );
    assert_eq!(u64::from(size), 4);
}

#[test]
fn given_split_and_json_items_when_normalized_then_all_attribute_types_and_errors_are_equivalent() {
    for blob in [
        r#"{"n":{"N":"-1e-2"},"ns":{"NS":["2e1","3"]},"ss":{"SS":["a","b"]},"bs":{"BS":["YQ==","Yg=="]},"map":{"M":{"nested":{"L":[{"N":"1e2"},{"BOOL":true},{"NULL":true}]}}}}"#,
        r#"{"n":{"N":"1e126"}}"#,
        r#"{"nested":{"L":[{"N":"invalid"}]}}"#,
        r#"{"n":{"NS":["1","1e0"]}}"#,
        r#"{"n":{"N":"1","S":"bad"}}"#,
    ] {
        let primary =
            WireItemKeyAttributes::new("pk".into(), AttributeValue::S("x".into()), None, None);
        let split = WireItem::local_split(primary, None, Some(blob.as_bytes().to_vec()));
        let json = WireItem::dynamo_json(split.dynamo_json_bytes().unwrap().into_owned());
        match (split.normalized_for_write(), json.normalized_for_write()) {
            (Ok((split, split_size)), Ok((json, json_size))) => {
                assert_eq!(split_size, json_size);
                assert_eq!(
                    split.into_attribute_map().unwrap(),
                    json.into_attribute_map().unwrap()
                );
            }
            (Err(split), Err(json)) if blob == r#"{"n":{"N":"1","S":"bad"}}"# => {
                assert_eq!(
                    split.to_string(),
                    "AttributeValue must have exactly one field at line 1 column 17"
                );
                assert_eq!(
                    json.to_string(),
                    "AttributeValue must have exactly one field at line 1 column 32"
                );
            }
            (Err(split), Err(json)) => assert_eq!(split.to_string(), json.to_string()),
            other => panic!("different write results: {other:?}"),
        }
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
