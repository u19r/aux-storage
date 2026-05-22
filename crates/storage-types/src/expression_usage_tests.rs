use std::collections::{HashMap, HashSet};

use crate::{
    AttributeValue, extract_expression_attribute_placeholders,
    subset_expression_attribute_values_for_expression, validate_expression_attribute_usage,
};

#[test]
fn extract_expression_attribute_placeholders_ignores_quoted_placeholders() {
    let expression = "#field = ':ignored' AND #name = :value";
    let (names, values) = extract_expression_attribute_placeholders(expression);

    assert_eq!(
        names,
        HashSet::from(["#field".to_string(), "#name".to_string()])
    );
    assert_eq!(values, HashSet::from([":value".to_string()]));
}

#[test]
fn validate_expression_attribute_usage_reports_unused_values() {
    let values = HashMap::from([(":pk".to_string(), AttributeValue::S("ignored".to_string()))]);
    let error = validate_expression_attribute_usage(None, Some(&values), ["pk = :pk_val"])
        .expect_err("expected validation error for unused values");

    assert_eq!(
        error.to_string(),
        "Value provided in ExpressionAttributeValues unused in expressions: keys: {:pk}"
    );
}

#[test]
fn validate_expression_attribute_usage_reports_unused_names() {
    let names = HashMap::from([("#field".to_string(), "field".to_string())]);
    let error = validate_expression_attribute_usage(Some(&names), None, ["pk = :pk_val"])
        .expect_err("expected validation error for unused names");

    assert_eq!(
        error.to_string(),
        "Value provided in ExpressionAttributeNames unused in expressions: keys: {#field}"
    );
}

#[test]
fn validate_expression_attribute_usage_allows_usage_across_multiple_expressions() {
    let names = HashMap::from([("#status".to_string(), "status".to_string())]);
    let values = HashMap::from([
        (":pk".to_string(), AttributeValue::S("tenant#1".to_string())),
        (
            ":status".to_string(),
            AttributeValue::S("active".to_string()),
        ),
    ]);

    let result = validate_expression_attribute_usage(
        Some(&names),
        Some(&values),
        ["pk = :pk", "#status = :status"],
    );

    assert!(result.is_ok());
}

#[test]
fn subset_expression_attribute_values_for_expression_filters_unused_values() {
    let values = HashMap::from([
        (":pk".to_string(), AttributeValue::S("tenant#1".to_string())),
        (":sk".to_string(), AttributeValue::S("meta".to_string())),
        (
            ":unused".to_string(),
            AttributeValue::S("ignored".to_string()),
        ),
    ]);

    let subset =
        subset_expression_attribute_values_for_expression("pk = :pk AND sk = :sk", Some(&values))
            .expect("expected subset map");

    assert_eq!(subset.len(), 2);
    assert!(subset.contains_key(":pk"));
    assert!(subset.contains_key(":sk"));
    assert!(!subset.contains_key(":unused"));
}
