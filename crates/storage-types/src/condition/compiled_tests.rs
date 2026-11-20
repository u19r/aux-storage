use std::collections::HashMap;

use super::compiled::*;
use crate::{AttributeMap, AttributeValue, WireItem};

fn item(pk: &str) -> WireItem {
    WireItem::try_from(&HashMap::from([(
        "pk".to_owned(),
        AttributeValue::S(pk.to_owned()),
    )]))
    .unwrap()
}

#[test]
fn given_absent_expression_when_parse_then_none() {
    let parsed = CompiledCondition::compile_write(None, None, None).unwrap();
    assert!(parsed.is_none());
}

#[test]
fn given_invalid_expression_when_parse_then_error() {
    CompiledCondition::compile_write(Some("this is not a condition ((( ".to_owned()), None, None)
        .unwrap_err();
}

#[test]
fn given_existing_item_when_check_exists_then_pass() {
    let unchecked =
        CompiledCondition::compile_write(Some("attribute_exists(pk)".to_owned()), None, None)
            .unwrap()
            .unwrap();
    assert!(unchecked.check_borrowed(Some(&item("pk-1"))).unwrap());
}

#[test]
fn given_missing_item_when_check_exists_then_fail() {
    let unchecked =
        CompiledCondition::compile_write(Some("attribute_exists(pk)".to_owned()), None, None)
            .unwrap()
            .unwrap();
    assert!(!unchecked.check_borrowed(None).unwrap());
}

#[test]
fn given_present_item_when_check_not_exists_then_fail() {
    let unchecked =
        CompiledCondition::compile_write(Some("attribute_not_exists(pk)".to_owned()), None, None)
            .unwrap()
            .unwrap();
    assert!(!unchecked.check_borrowed(Some(&item("pk-1"))).unwrap());
}

const REVERSED_BETWEEN: &str = "a BETWEEN :lo AND :hi";

fn reversed_between_values() -> AttributeMap {
    AttributeMap::from_iter([
        (":lo".to_string(), AttributeValue::N("5".to_string())),
        (":hi".to_string(), AttributeValue::N("1".to_string())),
    ])
}

#[test]
fn given_reversed_between_when_compile_filter_then_filter_expression_text() {
    let values = reversed_between_values();
    let message = CompiledCondition::compile_filter(REVERSED_BETWEEN, None, Some(&values))
        .unwrap_err()
        .to_string();
    assert!(
        message.contains(
            "Invalid FilterExpression: The BETWEEN operator requires upper bound to be greater \
             than or equal to lower bound; lower bound operand: AttributeValue: {N:5}, upper \
             bound operand: AttributeValue: {N:1}"
        ),
        "{message}"
    );
    assert!(!message.contains("ConditionExpression"), "{message}");
}

#[test]
fn given_reversed_between_when_compile_write_then_invalid_condition_string() {
    let message = CompiledCondition::compile_write(
        Some(REVERSED_BETWEEN.to_owned()),
        None,
        Some(reversed_between_values()),
    )
    .unwrap_err()
    .to_string();
    assert!(message.contains("Invalid condition string"), "{message}");
}

#[test]
fn given_reversed_between_when_compile_key_condition_then_key_condition_prefix() {
    let values = reversed_between_values();
    let message = CompiledCondition::compile_key_condition(REVERSED_BETWEEN, None, Some(&values))
        .unwrap_err()
        .to_string();
    assert!(
        message.contains(
            "Invalid key condition expression: Invalid ConditionExpression: The BETWEEN operator \
             requires upper bound"
        ),
        "{message}"
    );
}

#[test]
fn given_filter_when_matches_then_evaluates_item() {
    let values = AttributeMap::from_iter([(":v".to_string(), AttributeValue::S("x".to_string()))]);
    let filter = CompiledCondition::compile_filter("a = :v", None, Some(&values)).unwrap();
    let item =
        |value: &str| HashMap::from([("a".to_string(), AttributeValue::S(value.to_string()))]);
    assert!(filter.matches(&item("x")));
    assert!(!filter.matches(&item("y")));
}
