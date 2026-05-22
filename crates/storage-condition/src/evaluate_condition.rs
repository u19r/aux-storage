use std::collections::HashMap;

use storage_types::AttributeValue;

use crate::{Condition, SizeComparison, helpers::str_to_f64};

#[must_use]
pub fn evaluate_condition_bytes(item: Option<&[u8]>, condition: &Condition) -> bool {
    let item: HashMap<String, AttributeValue> = item
        .and_then(|c| storage_types::storage_serde::from_bytes(c).ok())
        .unwrap_or_default();
    evaluate_condition(&item, condition)
}

#[must_use]
#[expect(clippy::too_many_lines)]
#[expect(clippy::implicit_hasher)]
pub fn evaluate_condition(item: &HashMap<String, AttributeValue>, condition: &Condition) -> bool {
    match condition {
        Condition::Exists { field } => get_path_value(item, field).is_some(),
        Condition::NotExists { field } => get_path_value(item, field).is_none(),
        Condition::LessThan { field, value } => {
            if let Some(field_value) = get_path_value(item, field) {
                match field_value {
                    AttributeValue::N(n) => str_to_f64(n) < str_to_f64(value),
                    AttributeValue::S(s) => s < value,
                    AttributeValue::B(b) => b < value,
                    _ => false,
                }
            } else {
                false
            }
        }
        Condition::LessThanEqual { field, value } => {
            if let Some(field_value) = get_path_value(item, field) {
                match field_value {
                    AttributeValue::N(n) => str_to_f64(n) <= str_to_f64(value),
                    AttributeValue::S(s) => s <= value,
                    AttributeValue::B(b) => b <= value,
                    _ => false,
                }
            } else {
                false
            }
        }
        Condition::GreaterThan { field, value } => {
            if let Some(field_value) = get_path_value(item, field) {
                match field_value {
                    AttributeValue::N(n) => str_to_f64(n) > str_to_f64(value),
                    AttributeValue::S(s) => s > value,
                    AttributeValue::B(b) => b > value,
                    _ => false,
                }
            } else {
                false
            }
        }
        Condition::GreaterThanEqual { field, value } => {
            if let Some(field_value) = get_path_value(item, field) {
                match field_value {
                    AttributeValue::N(n) => str_to_f64(n) >= str_to_f64(value),
                    AttributeValue::S(s) => s >= value,
                    AttributeValue::B(b) => b >= value,
                    _ => false,
                }
            } else {
                false
            }
        }
        Condition::Equal { field, value } => {
            if let Some(field_value) = get_path_value(item, field) {
                match field_value {
                    AttributeValue::S(s) => s == value,
                    AttributeValue::N(n) => n == value,
                    AttributeValue::B(b) => b == value,
                    AttributeValue::BOOL(b) => b.to_string() == *value,
                    AttributeValue::NULL(_) => value == "null",
                    _ => false,
                }
            } else {
                false
            }
        }
        Condition::NotEqual { field, value } => {
            if let Some(field_value) = get_path_value(item, field) {
                match field_value {
                    AttributeValue::S(s) => s != value,
                    AttributeValue::N(n) => n != value,
                    AttributeValue::B(b) => b != value,
                    AttributeValue::BOOL(b) => b.to_string() != *value,
                    AttributeValue::NULL(_) => value != "null",
                    _ => true, // Different types are considered not equal
                }
            } else {
                // Field doesn't exist, so it's not equal to any specific value
                true
            }
        }
        Condition::Between { field, min, max } => {
            if let Some(field_value) = get_path_value(item, field) {
                match field_value {
                    AttributeValue::N(n) => {
                        let min = str_to_f64(min);
                        let max = str_to_f64(max);
                        let val = str_to_f64(n);
                        min <= val && val <= max
                    }
                    AttributeValue::S(s) => s >= min && s <= max,
                    AttributeValue::B(b) => b >= min && b <= max,
                    _ => false,
                }
            } else {
                false
            }
        }
        Condition::In { field, values } => {
            if let Some(field_value) = get_path_value(item, field) {
                match field_value {
                    AttributeValue::S(s) => values.contains(s),
                    AttributeValue::N(n) => {
                        let n = str_to_f64(n);
                        values.iter().any(|v| str_to_f64(v).total_cmp(&n).is_eq())
                    }
                    AttributeValue::B(b) => values.contains(b),
                    _ => false,
                }
            } else {
                false
            }
        }
        Condition::Contains { field, value } => {
            let Some(field_value) = get_path_value(item, field) else {
                return false;
            };
            match field_value {
                AttributeValue::S(s) => matches!(value, AttributeValue::S(v) if s.contains(v)),
                AttributeValue::B(b) => {
                    let AttributeValue::B(value) = value else {
                        return false;
                    };
                    decoded_binary_contains(b, value)
                }
                AttributeValue::SS(ss) => matches!(value, AttributeValue::S(v) if ss.contains(v)),
                AttributeValue::NS(ns) => matches!(value, AttributeValue::N(v) if ns.contains(v)),
                AttributeValue::BS(bs) => matches!(value, AttributeValue::B(v) if bs.contains(v)),
                AttributeValue::L(l) => l.iter().any(|elem| elem == value),
                _ => false,
            }
        }
        Condition::BeginsWith { field, prefix } => {
            if let Some(field_value) = get_path_value(item, field) {
                match (field_value, prefix) {
                    (AttributeValue::S(s), AttributeValue::S(prefix)) => s.starts_with(prefix),
                    (AttributeValue::B(b), AttributeValue::B(prefix)) => {
                        decoded_binary_starts_with(b, prefix)
                    }
                    _ => false,
                }
            } else {
                false
            }
        }
        Condition::AttributeType {
            field,
            attribute_type,
        } => get_path_value(item, field).is_some_and(|value| {
            attribute_type_code(value).is_some_and(|actual| actual == attribute_type)
        }),
        Condition::Size { field, size } => {
            get_path_value(item, field).and_then(attribute_size) == Some(*size)
        }
        Condition::SizeCompare {
            field,
            operator,
            size,
        } => get_path_value(item, field)
            .and_then(attribute_size)
            .is_some_and(|actual_size| match operator {
                SizeComparison::Equal => actual_size == *size,
                SizeComparison::NotEqual => actual_size != *size,
                SizeComparison::LessThan => actual_size < *size,
                SizeComparison::LessThanEqual => actual_size <= *size,
                SizeComparison::GreaterThan => actual_size > *size,
                SizeComparison::GreaterThanEqual => actual_size >= *size,
            }),
        Condition::And { conditions } => {
            // All conditions must be true
            conditions.iter().all(|cond| evaluate_condition(item, cond))
        }
        Condition::Or { conditions } => {
            // At least one condition must be true
            conditions.iter().any(|cond| evaluate_condition(item, cond))
        }
        Condition::Not { condition } => !evaluate_condition(item, condition),
    }
}

fn attribute_size(value: &AttributeValue) -> Option<usize> {
    match value {
        AttributeValue::S(s) => Some(s.encode_utf16().count()),
        AttributeValue::B(b) => storage_types::dynamodb_binary::decode_base64_string(b)
            .ok()
            .map(|bytes| bytes.len()),
        AttributeValue::SS(ss) => Some(ss.len()),
        AttributeValue::NS(ns) => Some(ns.len()),
        AttributeValue::BS(bs) => Some(bs.len()),
        AttributeValue::L(l) => Some(l.len()),
        AttributeValue::M(m) => Some(m.len()),
        _ => None,
    }
}

fn attribute_type_code(value: &AttributeValue) -> Option<&'static str> {
    match value {
        AttributeValue::S(_) => Some("S"),
        AttributeValue::N(_) => Some("N"),
        AttributeValue::B(_) => Some("B"),
        AttributeValue::SS(_) => Some("SS"),
        AttributeValue::NS(_) => Some("NS"),
        AttributeValue::BS(_) => Some("BS"),
        AttributeValue::BOOL(_) => Some("BOOL"),
        AttributeValue::NULL(true) => Some("NULL"),
        AttributeValue::NULL(false) => None,
        AttributeValue::L(_) => Some("L"),
        AttributeValue::M(_) => Some("M"),
    }
}

fn decoded_binary_contains(value: &str, operand: &str) -> bool {
    let Ok(value) = storage_types::dynamodb_binary::decode_base64_string(value) else {
        return false;
    };
    let Ok(operand) = storage_types::dynamodb_binary::decode_base64_string(operand) else {
        return false;
    };
    !operand.is_empty() && value.windows(operand.len()).any(|window| window == operand)
}

fn decoded_binary_starts_with(value: &str, operand: &str) -> bool {
    let Ok(value) = storage_types::dynamodb_binary::decode_base64_string(value) else {
        return false;
    };
    let Ok(operand) = storage_types::dynamodb_binary::decode_base64_string(operand) else {
        return false;
    };
    value.starts_with(&operand)
}

fn get_path_value<'a>(
    item: &'a HashMap<String, AttributeValue>,
    path: &str,
) -> Option<&'a AttributeValue> {
    let mut cursor = 0usize;
    let root_end = next_path_separator(path, cursor).unwrap_or(path.len());
    let mut current = item.get(path.get(cursor..root_end)?)?;
    cursor = root_end;

    while cursor < path.len() {
        match path.as_bytes().get(cursor).copied()? {
            b'.' => {
                cursor += 1;
                let end = next_path_separator(path, cursor).unwrap_or(path.len());
                let key = path.get(cursor..end)?;
                let AttributeValue::M(map) = current else {
                    return None;
                };
                current = map.get(key)?;
                cursor = end;
            }
            b'[' => {
                cursor += 1;
                let end = path.get(cursor..)?.find(']')? + cursor;
                let index = path.get(cursor..end)?.parse::<usize>().ok()?;
                let AttributeValue::L(list) = current else {
                    return None;
                };
                current = list.get(index)?;
                cursor = end + 1;
            }
            _ => return None,
        }
    }

    Some(current)
}

fn next_path_separator(path: &str, start: usize) -> Option<usize> {
    path.get(start..)?
        .char_indices()
        .find(|(_, ch)| *ch == '.' || *ch == '[')
        .map(|(index, _)| start + index)
}
