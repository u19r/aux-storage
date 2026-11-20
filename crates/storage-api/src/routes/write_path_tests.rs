use serde_json::{Value, json};
use storage_types::{
    WireItem,
    condition::{evaluate_wire_condition, parse_condition_expression_opt},
};

fn build_write_item(extra: usize) -> Value {
    let mut item =
        json!({"pk":{"S":"tenant"},"sk":{"S":"item"},"ttl":{"N":"4102444800"},"version":{"N":"1"}});
    for index in 1..=5 {
        item[format!("gsi{index}pk")] = json!({"S":format!("group{index}")});
        item[format!("gsi{index}sk")] = json!({"S":"sort"});
        item[format!("indexer{index}")] = json!({"S":"indexed"});
    }
    for index in 0..extra {
        item[format!("filler{index:03}")] = json!({"S":"x".repeat(128)});
    }
    item
}

#[test]
fn given_raw_attributes_when_validating_and_normalizing_then_match_typed_policy() {
    use storage_types::{AttributeMap, LogicalItemSize};
    let item = json!({
        "pk":{"S":"tenant"}, "escaped\"name":{"S":"line\nbreak"},
        "number":{"N":"1e2"}, "binary":{"B":"YWJj"},
        "strings":{"SS":["one","two"]}, "numbers":{"NS":["1e1","2e1"]},
        "binaries":{"BS":["YQ==","Yg=="]}, "bool":{"BOOL":true}, "null":{"NULL":true},
        "nested":{"M":{"child":{"L":[{"N":"3e2"},{"N":"4e2"},{"S":"hello"}]}}}
    });
    let bytes = serde_json::to_vec(&item).unwrap();
    let mut typed: AttributeMap = serde_json::from_slice(&bytes).unwrap();
    let expected_size = LogicalItemSize::try_from_entries(typed.iter()).unwrap();
    let wire = WireItem::dynamo_json(bytes);
    assert_eq!(wire.validate_for_write("Item").unwrap(), expected_size);
    typed.normalize_numbers_for_write();
    let (normalized, _) = wire.normalized_for_write().unwrap();
    assert_eq!(AttributeMap::try_from(&normalized).unwrap(), typed);
    let value =
        serde_json::from_str::<Value>(&normalized.to_dynamo_json_string().unwrap()).unwrap();
    assert_eq!(value["nested"]["M"]["child"]["L"][1]["N"], "400");
    assert_eq!(
        normalized.attribute_value("escaped\"name").unwrap(),
        Some(storage_types::AttributeValue::S("line\nbreak".to_owned()))
    );
}

#[test]
fn given_invalid_unselected_attributes_when_validating_write_then_reject_them() {
    let invalid = [
        json!({"N":"not-a-number"}),
        json!({"NS":["1","1.0"]}),
        json!({"SS":[]}),
        json!({"SS":["duplicate","duplicate"]}),
        json!({"B":"not base64!"}),
        json!({"BS":["YQ==","YQ=="]}),
        json!({"L":[{"M":{"nested":{"N":"invalid"}}}]}),
        json!({"S":"text","N":"1"}),
        json!({"UNKNOWN":"text"}),
        json!({}),
    ];
    for attribute in invalid {
        let wire = WireItem::dynamo_json(
            serde_json::to_vec(&json!({"pk":{"S":"valid"},"unused":attribute})).unwrap(),
        );
        assert!(wire.validate_for_write("Item").is_err(), "{attribute}");
        assert!(wire.normalized_for_write().is_err(), "{attribute}");
    }
    for bytes in [
        br#"{}"#.as_slice(),
        br#"{"pk":{"S":"a"},"pk":{"S":"b"}}"#,
        br#"{"pk":{"S":"a"}} true"#,
    ] {
        assert!(
            WireItem::dynamo_json(bytes.to_vec())
                .validate_for_write("Item")
                .is_err()
        );
    }
    let oversized = json!({"pk":{"S":"x".repeat(400*1024)}});
    assert!(
        WireItem::dynamo_json(serde_json::to_vec(&oversized).unwrap())
            .validate_for_write("Item")
            .is_err()
    );
}

#[test]
fn given_raw_item_when_replacing_timestamp_then_preserve_other_attributes() {
    use storage_types::{AttributeMap, AttributeValue};
    let wire = WireItem::dynamo_json(br#"{"pk":{"S":"tenant"},"updated_at":{"N":"1"},"u_at":{"N":"2"},"escaped\"name": {"L":[{"N":"3"},{"S":"large"}]}}"#.to_vec());
    let replacements =
        AttributeMap::from_iter([("u_at".to_owned(), AttributeValue::N("4".to_owned()))]);
    let updated = wire
        .with_attribute_updates(&["updated_at"], &replacements)
        .unwrap();
    assert_eq!(
        updated.attribute_value("u_at").unwrap(),
        Some(AttributeValue::N("4".to_owned()))
    );
    let raw: Value = serde_json::from_str(&updated.to_dynamo_json_string().unwrap()).unwrap();
    assert!(raw.get("updated_at").is_none());
    assert_eq!(
        updated.attribute_value("escaped\"name").unwrap(),
        wire.attribute_value("escaped\"name").unwrap()
    );
    assert_eq!(
        updated
            .to_dynamo_json_string()
            .unwrap()
            .matches("\"u_at\"")
            .count(),
        1
    );
}

#[test]
fn given_nested_condition_roots_when_selecting_once_then_match_full_evaluation() {
    use storage_types::AttributeMap;
    let item = json!({"pk":{"S":"tenant"},"nested":{"M":{"children":{"L":[{"N":"7"},{"S":"hello"}]}}},"unused":{"S":"x".repeat(4096)}});
    let bytes = serde_json::to_vec(&item).unwrap();
    let full: AttributeMap = serde_json::from_slice(&bytes).unwrap();
    let wire = WireItem::dynamo_json(bytes);
    let expressions = [
        "#root.children[0] = :seven AND #root.children[1] = :hello",
        "#root.children[0] > :seven OR attribute_not_exists(missing)",
        "NOT (#root.children[0] = :seven)",
        "attribute_exists(#root.children[1]) AND begins_with(#root.children[1], :hello)",
        "contains(#root.children, :seven)",
        "size(#root.children) = :two",
    ];
    for expression in expressions {
        let condition = parse_condition_expression_opt(
            Some(expression.to_owned()),
            Some(
                std::collections::HashMap::from([("#root".to_owned(), "nested".to_owned())]).into(),
            ),
            Some(
                serde_json::from_value(
                    json!({":seven":{"N":"7"},":hello":{"S":"hello"},":two":{"N":"2"}}),
                )
                .unwrap(),
            ),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            evaluate_wire_condition(Some(&wire), &condition).unwrap(),
            storage_types::condition::evaluate_condition(&full, &condition),
            "{expression}"
        );
    }
}

#[test]
fn given_wide_item_with_duplicate_name_when_validating_then_reject_after_hash_transition() {
    let mut bytes = serde_json::to_vec(&build_write_item(64)).unwrap();
    bytes.pop();
    bytes.extend_from_slice(br#", "pk":{"S":"duplicate"}}"#);
    assert!(
        WireItem::dynamo_json(bytes)
            .validate_for_write("Item")
            .is_err()
    );
}
