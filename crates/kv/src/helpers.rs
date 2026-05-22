use std::collections::HashMap;

use storage_types::{AttributeValue, StorageError, StorageResult, StorageValidationKind};

#[must_use]
#[expect(clippy::implicit_hasher)]
pub fn parse_hash_key_query<'a>(
    key_condition_expression: &str,
    expression_attribute_values: &'a Option<HashMap<String, AttributeValue>>,
) -> Option<&'a AttributeValue> {
    // Parse simple expressions like "pk = :pk_val" or "hashKey = :value"
    let expression_values = expression_attribute_values.as_ref()?;

    // Look for pattern: "attribute_name = :variable_name"
    let (_, variable_expr) = key_condition_expression.split_once('=')?;
    if variable_expr.contains('=') {
        return None;
    }

    let variable_name = variable_expr.split_whitespace().next()?;
    if !variable_name.starts_with(':') {
        return None;
    }

    expression_values.get(variable_name)
}

#[must_use]
#[expect(clippy::implicit_hasher)]
pub fn parse_hash_range_key_query<'a>(
    key_condition_expression: &str,
    expression_attribute_values: &'a Option<HashMap<String, AttributeValue>>,
) -> Option<(&'a AttributeValue, &'a AttributeValue)> {
    // Parse expressions like "pk = :pk_val AND sk = :sk_val"
    let expression_values = expression_attribute_values.as_ref()?;
    if !key_condition_expression.contains(" AND ") {
        return None;
    }

    let (hash_condition, range_condition) = key_condition_expression.split_once(" AND ")?;
    if range_condition.contains(" AND ") {
        return None;
    }

    let hash_value = parse_single_condition(hash_condition, expression_values)?;
    let range_value = parse_single_condition(range_condition, expression_values)?;
    Some((hash_value, range_value))
}

#[must_use]
#[expect(clippy::implicit_hasher)]
pub fn parse_single_condition<'a>(
    condition: &str,
    expression_values: &'a HashMap<String, AttributeValue>,
) -> Option<&'a AttributeValue> {
    // Parse "attribute = :variable" format
    let (_, variable_expr) = condition.split_once(" = ")?;
    if variable_expr.contains(" = ") {
        return None;
    }
    let variable_name = variable_expr.trim();
    if !variable_name.starts_with(':') {
        return None;
    }
    expression_values.get(variable_name)
}

#[expect(clippy::implicit_hasher)]
pub fn parse_hash_begins_with_query<'a>(
    key_condition_expression: &str,
    expression_attribute_values: &'a Option<HashMap<String, AttributeValue>>,
) -> StorageResult<Option<(&'a AttributeValue, &'a AttributeValue)>> {
    // Parse expressions like "pk = :pk_val AND begins_with(sk, :sk_prefix)"
    let expression_values = expression_attribute_values
        .as_ref()
        .ok_or_else(|| StorageError::validation(StorageValidationKind::BeginsWithRequiresString))?;
    if !key_condition_expression.contains(" AND ") {
        return Ok(None);
    }
    if !key_condition_expression.contains("begins_with(") {
        return Ok(None);
    }

    let Some((hash_condition, begins_with_condition)) =
        key_condition_expression.split_once(" AND ")
    else {
        return Ok(None);
    };
    if begins_with_condition.contains(" AND ") {
        return Ok(None);
    }

    // First part should be hash key equality
    let Some(hash_value) = parse_single_condition(hash_condition, expression_values) else {
        return Ok(None);
    };

    // Second part should be begins_with condition
    let begins_with_part = begins_with_condition.trim();
    let Some(rest) = begins_with_part.strip_prefix("begins_with(") else {
        return Ok(None);
    };
    let Some(inner) = rest.strip_suffix(')') else {
        return Ok(None);
    };

    let Some((_, variable_part)) = inner.split_once(',') else {
        return Ok(None);
    };
    if variable_part.contains(',') {
        return Ok(None);
    }
    let variable_name = variable_part.trim();
    if !variable_name.starts_with(':') {
        return Ok(None);
    }
    let prefix_value = expression_values.get(variable_name).ok_or_else(|| {
        StorageError::validation(format!("Missing expression value: {variable_name}"))
    })?;

    // Validate that prefix_value is a string type
    if !matches!(prefix_value, AttributeValue::S(_)) {
        return Err(StorageError::validation(
            "begins_with is only valid for string types",
        ));
    }

    Ok(Some((hash_value, prefix_value)))
}

#[must_use]
#[expect(clippy::implicit_hasher)]
pub fn parse_hash_between_query<'a>(
    key_condition_expression: &str,
    expression_attribute_values: &'a Option<HashMap<String, AttributeValue>>,
) -> Option<(&'a AttributeValue, &'a AttributeValue, &'a AttributeValue)> {
    // Parse expressions like "pk = :pk_val AND timestamp BETWEEN :start AND :end"
    // or "pk = :pk_val AND #ts BETWEEN :start AND :end"
    let Some(expression_values) = expression_attribute_values else {
        return None;
    };
    if !key_condition_expression.contains(" AND ")
        || !key_condition_expression.contains(" BETWEEN ")
    {
        return None;
    }

    // Find the position of " BETWEEN " to split the expression properly
    let between_pos = key_condition_expression.find(" BETWEEN ")?;
    // Split at the word before BETWEEN (which should be the range key attribute)
    let before_between = key_condition_expression.get(..between_pos)?.trim();
    let between_and_after = key_condition_expression.get(between_pos..)?.trim();

    // Find the last " AND " before BETWEEN to separate hash key condition
    let last_and_pos = before_between.rfind(" AND ")?;
    let hash_part = before_between.get(..last_and_pos)?.trim();

    // Parse hash key condition
    let hash_value = parse_single_condition(hash_part, expression_values)?;

    // Parse BETWEEN values
    // between_and_after should be like "BETWEEN :start AND :end"
    let between_values = between_and_after.strip_prefix("BETWEEN ")?;
    let and_pos = between_values.find(" AND ")?;
    let start_var = between_values.get(..and_pos)?.trim();
    let end_var = between_values.get(and_pos + 5..)?.trim(); // Skip " AND "

    if !start_var.starts_with(':') || !end_var.starts_with(':') {
        return None;
    }
    let (Some(start_value), Some(end_value)) = (
        expression_values.get(start_var),
        expression_values.get(end_var),
    ) else {
        return None;
    };
    Some((hash_value, start_value, end_value))
}

#[must_use]
#[expect(clippy::implicit_hasher)]
pub fn parse_hash_comparison_query<'a>(
    key_condition_expression: &str,
    expression_attribute_values: &'a Option<HashMap<String, AttributeValue>>,
) -> Option<(&'a AttributeValue, &'static str, &'a AttributeValue)> {
    // Parse expressions like "pk = :pk_val AND timestamp < :max_ts"
    // or "pk = :pk_val AND #ts > :min_ts", etc.
    let Some(expression_values) = expression_attribute_values else {
        return None;
    };
    if !key_condition_expression.contains(" AND ") {
        return None;
    }

    let (hash_condition, comparison_part) = key_condition_expression.split_once(" AND ")?;
    if comparison_part.contains(" AND ") {
        return None;
    }

    // First part should be hash key equality
    let hash_value = parse_single_condition(hash_condition, expression_values)?;

    // Second part should be comparison condition
    let comparison_part = comparison_part.trim();

    // Look for comparison operators: <, >, <=, >=
    for op in &["<=", ">=", "<", ">"] {
        let Some(op_pos) = comparison_part.find(&format!(" {op} ")) else {
            continue;
        };

        let value_var = comparison_part.get(op_pos + op.len() + 2..)?.trim(); // Skip " op "
        if !value_var.starts_with(':') {
            continue;
        }
        let Some(comparison_value) = expression_values.get(value_var) else {
            continue;
        };

        return Some((hash_value, *op, comparison_value));
    }
    // If no valid comparison operator found, return None
    None
}

#[must_use]
#[expect(clippy::implicit_hasher)]
pub fn parse_hash_bounded_comparison_query<'a>(
    key_condition_expression: &str,
    expression_attribute_values: &'a Option<HashMap<String, AttributeValue>>,
) -> Option<(
    &'a AttributeValue,
    &'static str,
    &'a AttributeValue,
    &'static str,
    &'a AttributeValue,
)> {
    let expression_values = expression_attribute_values.as_ref()?;
    let mut parts = key_condition_expression.split(" AND ");
    let hash_condition = parts.next()?;
    let first_range_condition = parts.next()?;
    let second_range_condition = parts.next()?;
    if parts.next().is_some() {
        return None;
    }

    let hash_value = parse_single_condition(hash_condition, expression_values)?;
    let (first_operator, first_value) =
        parse_range_comparison(first_range_condition, expression_values)?;
    let (second_operator, second_value) =
        parse_range_comparison(second_range_condition, expression_values)?;

    match (first_operator, second_operator) {
        (">" | ">=", "<" | "<=") => Some((
            hash_value,
            first_operator,
            first_value,
            second_operator,
            second_value,
        )),
        ("<" | "<=", ">" | ">=") => Some((
            hash_value,
            second_operator,
            second_value,
            first_operator,
            first_value,
        )),
        _ => None,
    }
}

fn parse_range_comparison<'a>(
    comparison_part: &str,
    expression_values: &'a HashMap<String, AttributeValue>,
) -> Option<(&'static str, &'a AttributeValue)> {
    for op in &["<=", ">=", "<", ">"] {
        let Some(op_pos) = comparison_part.find(&format!(" {op} ")) else {
            continue;
        };

        let value_var = comparison_part.get(op_pos + op.len() + 2..)?.trim();
        if !value_var.starts_with(':') {
            return None;
        }
        return expression_values.get(value_var).map(|value| (*op, value));
    }
    None
}

/// Deserialize an item from bytes.
///
/// # Errors
/// Returns an error if deserialization fails.
pub fn deserialize_item_from_bytes(data: &[u8]) -> StorageResult<HashMap<String, AttributeValue>> {
    storage_types::storage_serde::from_bytes(data)
}

#[must_use]
pub fn increment_bytes(mut bytes: Vec<u8>) -> Vec<u8> {
    for byte in bytes.iter_mut().rev() {
        if *byte < 0xFF {
            *byte += 1;
            return bytes;
        }
        *byte = 0; // Reset to 0 and carry over
    }

    bytes
}

#[must_use]
pub fn decrement_bytes(mut bytes: Vec<u8>) -> Vec<u8> {
    for byte in bytes.iter_mut().rev() {
        if *byte > 0x00 {
            *byte -= 1;
            return bytes;
        }
        *byte = 0xFF;
    }

    bytes
}
