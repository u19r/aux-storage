use std::collections::HashMap;

use storage_types::{AttributeValue, StorageError, StorageResult};

#[derive(Debug, Clone)]
pub(crate) struct CompiledKeyCondition {
    condition: String,
    values: Vec<String>,
}

impl CompiledKeyCondition {
    pub fn into_parts(self) -> (String, Vec<String>) {
        (self.condition, self.values)
    }
}

pub fn parse_key_condition_expression(
    expression: &str,
    attribute_names: Option<&HashMap<String, String>>,
    attribute_values: Option<&HashMap<String, AttributeValue>>,
) -> StorageResult<CompiledKeyCondition> {
    let expr = expression.trim();

    // Special case: handle equality AND BETWEEN where the first AND precedes
    // BETWEEN
    if let (Some(first_and), Some(between_pos)) = (expr.find(" AND "), expr.find(" BETWEEN "))
        && first_and < between_pos
    {
        let first_condition = expr.get(..first_and).unwrap_or("").trim();
        let between_condition = expr.get(first_and + 5..).unwrap_or("").trim();

        let mut conditions = Vec::new();
        let mut values = Vec::new();

        // First condition: expect equality (hash key)
        if let Some(equals_pos) = first_condition.find(" = ") {
            let left = first_condition.get(..equals_pos).unwrap_or("").trim();
            let right = first_condition.get(equals_pos + 3..).unwrap_or("").trim();

            let attr_name = resolve_attribute_name_simple(left, attribute_names);
            let attr_value = resolve_attribute_value_simple(right, attribute_values)?;

            conditions.push(format!("{attr_name} = ?1"));
            values.push(scalar_value(&attr_value)?);
        }

        // BETWEEN condition for range key
        if let Some(between_pos) = between_condition.find(" BETWEEN ") {
            let left = between_condition.get(..between_pos).unwrap_or("").trim();
            let right = between_condition
                .get(between_pos + 9..)
                .unwrap_or("")
                .trim();

            if let Some(and_pos) = right.find(" AND ") {
                let start_val = right.get(..and_pos).unwrap_or("").trim();
                let end_val = right.get(and_pos + 5..).unwrap_or("").trim();

                let attr_name = resolve_attribute_name_simple(left, attribute_names);
                let start_attr = resolve_attribute_value_simple(start_val, attribute_values)?;
                let end_attr = resolve_attribute_value_simple(end_val, attribute_values)?;

                conditions.push(format!(
                    "{} BETWEEN ?{} AND ?{}",
                    attr_name,
                    values.len() + 1,
                    values.len() + 2
                ));
                values.push(scalar_value(&start_attr)?);
                values.push(scalar_value(&end_attr)?);
            }
        }

        return Ok(CompiledKeyCondition {
            condition: conditions.join(" AND "),
            values,
        });
    }

    // Compound conditions without BETWEEN
    if expr.contains(" AND ") && !expr.contains(" BETWEEN ") {
        let parts: Vec<&str> = expr.split(" AND ").collect();
        let mut conditions = Vec::new();
        let mut values = Vec::new();

        for (i, part) in parts.iter().enumerate() {
            let part = part.trim();

            // Equality
            if let Some(equals_pos) = part.find(" = ") {
                let left = part.get(..equals_pos).unwrap_or("").trim();
                let right = part.get(equals_pos + 3..).unwrap_or("").trim();

                let attr_name = resolve_attribute_name_simple(left, attribute_names);
                let attr_value = resolve_attribute_value_simple(right, attribute_values)?;

                conditions.push(format!("{} = ?{}", attr_name, i + 1));
                values.push(scalar_value(&attr_value)?);
                continue;
            }

            // begins_with(attr, :prefix)
            if let Some(start) = part.find("begins_with(") {
                let func_content = part.get(start + 12..).unwrap_or("");
                if let Some(end) = func_content.find(')') {
                    let func_args = func_content.get(..end).unwrap_or("");
                    let args: Vec<&str> = func_args.split(',').map(str::trim).collect();
                    if args.len() == 2 {
                        let attr_name = resolve_attribute_name_simple(args[0], attribute_names);
                        let attr_value = resolve_attribute_value_simple(args[1], attribute_values)?;

                        // Validate that prefix_value is a string type
                        if !matches!(attr_value, AttributeValue::S(_)) {
                            return Err(StorageError::validation(
                                "begins_with is only valid for string types",
                            ));
                        }

                        let prefix = scalar_value(&attr_value)?;

                        conditions.push(format!("{} LIKE ?{}", attr_name, i + 1));
                        values.push(format!("{prefix}%"));
                        continue;
                    }
                }
            }

            // Comparisons
            for (op, sql_op) in [
                (" < ", " < "),
                (" <= ", " <= "),
                (" > ", " > "),
                (" >= ", " >= "),
            ] {
                if let Some(pos) = part.find(op) {
                    let left = part.get(..pos).unwrap_or("").trim();
                    let right = part.get(pos + op.len()..).unwrap_or("").trim();

                    let attr_name = resolve_attribute_name_simple(left, attribute_names);
                    let attr_value = resolve_attribute_value_simple(right, attribute_values)?;

                    conditions.push(format!("{}{}?{}", attr_name, sql_op, i + 1));
                    values.push(scalar_value(&attr_value)?);
                    break;
                }
            }
        }

        return Ok(CompiledKeyCondition {
            condition: conditions.join(" AND "),
            values,
        });
    }

    // Simple equality
    if let Some(equals_pos) = expr.find(" = ") {
        let left = expr.get(..equals_pos).unwrap_or("").trim();
        let right = expr.get(equals_pos + 3..).unwrap_or("").trim();

        let attr_name = resolve_attribute_name_simple(left, attribute_names);
        let attr_value = resolve_attribute_value_simple(right, attribute_values)?;

        return Ok(CompiledKeyCondition {
            condition: format!("{attr_name} = ?1"),
            values: vec![scalar_value(&attr_value)?],
        });
    }

    // begins_with(attr, :prefix)
    if let Some(start) = expr.find("begins_with(") {
        let func_content = expr.get(start + 12..).unwrap_or("");
        if let Some(end) = func_content.find(')') {
            let func_args = func_content.get(..end).unwrap_or("");
            let args: Vec<&str> = func_args.split(',').map(str::trim).collect();
            if args.len() == 2 {
                let attr_name = resolve_attribute_name_simple(args[0], attribute_names);
                let attr_value = resolve_attribute_value_simple(args[1], attribute_values)?;

                // Validate that prefix_value is a string type
                if !matches!(attr_value, AttributeValue::S(_)) {
                    return Err(StorageError::validation(
                        "begins_with is only valid for string types",
                    ));
                }

                let prefix = scalar_value(&attr_value)?;

                return Ok(CompiledKeyCondition {
                    condition: format!("{attr_name} LIKE ?1"),
                    values: vec![format!("{}%", prefix)],
                });
            }
        }
    }

    // BETWEEN
    if let Some(between_pos) = expr.find(" BETWEEN ") {
        let left = expr.get(..between_pos).unwrap_or("").trim();
        let right = expr.get(between_pos + 9..).unwrap_or("").trim();

        if let Some(and_pos) = right.find(" AND ") {
            let start_val = right.get(..and_pos).unwrap_or("").trim();
            let end_val = right.get(and_pos + 5..).unwrap_or("").trim();

            let attr_name = resolve_attribute_name_simple(left, attribute_names);
            let start_attr = resolve_attribute_value_simple(start_val, attribute_values)?;
            let end_attr = resolve_attribute_value_simple(end_val, attribute_values)?;

            return Ok(CompiledKeyCondition {
                condition: format!("{attr_name} BETWEEN ?1 AND ?2"),
                values: vec![scalar_value(&start_attr)?, scalar_value(&end_attr)?],
            });
        }
    }

    // Single comparison
    for (op, sql_op) in [
        (" < ", " < "),
        (" <= ", " <= "),
        (" > ", " > "),
        (" >= ", " >= "),
    ] {
        if let Some(pos) = expr.find(op) {
            let left = expr.get(..pos).unwrap_or("").trim();
            let right = expr.get(pos + op.len()..).unwrap_or("").trim();

            let attr_name = resolve_attribute_name_simple(left, attribute_names);
            let attr_value = resolve_attribute_value_simple(right, attribute_values)?;

            return Ok(CompiledKeyCondition {
                condition: format!("{attr_name}{sql_op}?1"),
                values: vec![scalar_value(&attr_value)?],
            });
        }
    }

    Err(unsupported_key_condition_expression_error(expression))
}

fn scalar_value(attr_value: &AttributeValue) -> StorageResult<String> {
    attr_value
        .inner_str()
        .map(str::to_owned)
        .map_err(|error| expected_scalar_attribute_value_error(&error))
}

fn resolve_attribute_name_simple(
    name: &str,
    attribute_names: Option<&HashMap<String, String>>,
) -> String {
    if let Some(stripped) = name.strip_prefix('#') {
        if let Some(map) = attribute_names
            && let Some(resolved) = map.get(name)
        {
            return resolved.clone();
        }
        return stripped.to_string();
    }
    name.to_string()
}

pub fn resolve_attribute_value_simple(
    value: &str,
    attribute_values: Option<&HashMap<String, AttributeValue>>,
) -> StorageResult<AttributeValue> {
    if value.starts_with(':') {
        if let Some(map) = attribute_values
            && let Some(resolved) = map.get(value)
        {
            return Ok(resolved.clone());
        }
        return Err(missing_expression_attribute_value_error(value));
    }
    Ok(AttributeValue::S(value.to_string()))
}

#[cold]
#[inline(never)]
fn unsupported_key_condition_expression_error(expression: &str) -> StorageError {
    StorageError::validation(format!(
        "Unsupported key condition expression: {expression}"
    ))
}

#[cold]
#[inline(never)]
fn missing_expression_attribute_value_error(value: &str) -> StorageError {
    StorageError::validation(format!("ExpressionAttributeValues missing key: {value}"))
}

#[cold]
#[inline(never)]
fn expected_scalar_attribute_value_error(error: &storage_types::ConversionError) -> StorageError {
    StorageError::validation(format!("expected scalar attribute value: {error}"))
}
