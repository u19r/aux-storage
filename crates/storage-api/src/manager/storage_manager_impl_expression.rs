use std::collections::HashMap;

use http_error::HttpApiError;
use storage_condition::{Condition, evaluate_condition, parse_condition_expression};
use storage_types::{AttributeProjection, AttributeValue};

pub(crate) fn apply_filter_expression_refs<'a>(
    items: &'a [HashMap<String, AttributeValue>],
    filter_expr: &str,
    attribute_names: Option<&HashMap<String, String>>,
    attribute_values: Option<&HashMap<String, AttributeValue>>,
) -> Result<Vec<&'a HashMap<String, AttributeValue>>, HttpApiError> {
    let condition = parse_filter_expression(filter_expr, attribute_names, attribute_values)?;
    Ok(apply_filter_condition_refs(items, &condition))
}

pub(crate) fn parse_filter_expression(
    filter_expr: &str,
    attribute_names: Option<&HashMap<String, String>>,
    attribute_values: Option<&HashMap<String, AttributeValue>>,
) -> Result<Condition, HttpApiError> {
    parse_condition_expression(filter_expr, attribute_names, attribute_values)
        .map_err(filter_expression_error)
}

pub(crate) fn apply_filter_condition_refs<'a>(
    items: &'a [HashMap<String, AttributeValue>],
    condition: &Condition,
) -> Vec<&'a HashMap<String, AttributeValue>> {
    items
        .iter()
        .filter(|item| evaluate_condition(item, condition))
        .collect()
}

pub(crate) fn apply_projection_expression_refs(
    items: &[&HashMap<String, AttributeValue>],
    projection_expr: &str,
    attribute_names: Option<&HashMap<String, String>>,
) -> Vec<HashMap<String, AttributeValue>> {
    let projection = AttributeProjection::from_expression(projection_expr, attribute_names);
    items
        .iter()
        .map(|item| projection.project(item).into_hashmap())
        .collect()
}

fn filter_expression_error(message: String) -> HttpApiError {
    HttpApiError::validation_error(message.replace("ConditionExpression", "FilterExpression"))
}
