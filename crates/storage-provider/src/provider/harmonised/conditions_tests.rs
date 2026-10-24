use std::collections::HashMap;

use storage_types::{AttributeValue, WireItem};

use super::conditions::{CheckedCondition, UncheckedCondition};

fn item(pk: &str) -> WireItem {
    WireItem::try_from(&HashMap::from([(
        "pk".to_owned(),
        AttributeValue::S(pk.to_owned()),
    )]))
    .unwrap()
}

#[test]
fn given_absent_expression_when_parse_then_none() {
    let parsed = UncheckedCondition::parse(None, None, None).unwrap();
    assert!(parsed.is_none());
}

#[test]
fn given_invalid_expression_when_parse_then_error() {
    UncheckedCondition::parse(Some("this is not a condition ((( ".to_owned()), None, None)
        .unwrap_err();
}

#[test]
fn given_existing_item_when_check_exists_then_pass() {
    let unchecked = UncheckedCondition::parse(Some("attribute_exists(pk)".to_owned()), None, None)
        .unwrap()
        .unwrap();
    match unchecked.with_old_item(Some(item("pk-1"))).check().unwrap() {
        CheckedCondition::Pass(_) => {}
        CheckedCondition::Fail(_) => panic!("existing item passes attribute_exists"),
    }
}

#[test]
fn given_missing_item_when_check_exists_then_fail_without_old() {
    let unchecked = UncheckedCondition::parse(Some("attribute_exists(pk)".to_owned()), None, None)
        .unwrap()
        .unwrap();
    match unchecked.with_old_item(None).check().unwrap() {
        CheckedCondition::Pass(_) => panic!("missing item fails attribute_exists"),
        CheckedCondition::Fail(receipt) => assert!(receipt.old_item.is_none()),
    }
}

#[test]
fn given_present_item_when_check_not_exists_then_fail_with_old() {
    let unchecked =
        UncheckedCondition::parse(Some("attribute_not_exists(pk)".to_owned()), None, None)
            .unwrap()
            .unwrap();
    match unchecked.with_old_item(Some(item("pk-1"))).check().unwrap() {
        CheckedCondition::Pass(_) => panic!("present item fails attribute_not_exists"),
        CheckedCondition::Fail(receipt) => assert!(receipt.old_item.is_some()),
    }
}
