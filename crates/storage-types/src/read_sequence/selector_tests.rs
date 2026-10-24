use std::collections::HashMap;

use crate::{AttributeMap, AttributeValue, ParsedReadSequenceSelector, ReadSequenceSelector};

#[test]
fn given_supported_item_paths_when_evaluating_borrowed_then_owned_semantics_match() {
    let item = selector_item();
    let root = AttributeValue::M(item.clone().into());

    for raw in [
        "$",
        "$.pk",
        "$.nested.values[1].S",
        "$.tags[*].S",
        "$.missing",
    ] {
        let selector = ParsedReadSequenceSelector::parse(&ReadSequenceSelector(raw.to_string()))
            .expect("parse selector");
        assert_eq!(
            selector.evaluate_item_values(&item).expect("borrowed path"),
            selector.evaluate_values(&root).expect("owned path"),
            "selector {raw}"
        );
    }
}

#[test]
fn given_type_mismatch_when_evaluating_borrowed_then_error_semantics_match() {
    let item = selector_item();
    let root = AttributeValue::M(item.clone().into());
    let selector =
        ParsedReadSequenceSelector::parse(&ReadSequenceSelector("$.pk.values".to_string()))
            .expect("parse selector");

    let borrowed = selector
        .evaluate_item_values(&item)
        .expect_err("borrowed error");
    let owned = selector.evaluate_values(&root).expect_err("owned error");

    assert_eq!(borrowed.to_string(), owned.to_string());
}

fn selector_item() -> AttributeMap {
    HashMap::from([
        ("pk".to_string(), AttributeValue::S("tenant#1".to_string())),
        (
            "nested".to_string(),
            AttributeValue::M(HashMap::from([(
                "values".to_string(),
                AttributeValue::L(vec![
                    AttributeValue::S("first".to_string()),
                    AttributeValue::S("second".to_string()),
                ]),
            )])),
        ),
        (
            "tags".to_string(),
            AttributeValue::SS(vec!["one".to_string(), "two".to_string()]),
        ),
    ])
    .into()
}
