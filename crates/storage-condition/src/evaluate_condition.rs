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
                dynamodb_attribute_values_equal(field_value, value)
            } else {
                false
            }
        }
        Condition::NotEqual { field, value } => {
            if let Some(field_value) = get_path_value(item, field) {
                !dynamodb_attribute_values_equal(field_value, value)
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
                values
                    .iter()
                    .any(|value| dynamodb_attribute_values_equal(field_value, value))
            } else {
                false
            }
        }
        Condition::ValueEqual { left, right } => dynamodb_attribute_values_equal(left, right),
        Condition::ValueNotEqual { left, right } => !dynamodb_attribute_values_equal(left, right),
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

pub fn try_evaluate_condition_with_root<E>(
    condition: &Condition,
    root_value: &mut impl FnMut(&str) -> Result<Option<AttributeValue>, E>,
) -> Result<bool, E> {
    match condition {
        Condition::Exists { field } => Ok(try_get_path_value(root_value, field)?.is_some()),
        Condition::NotExists { field } => Ok(try_get_path_value(root_value, field)?.is_none()),
        Condition::LessThan { field, value } => {
            let Some(field_value) = try_get_path_value(root_value, field)? else {
                return Ok(false);
            };
            Ok(match field_value {
                AttributeValue::N(n) => str_to_f64(&n) < str_to_f64(value),
                AttributeValue::S(s) => &s < value,
                AttributeValue::B(b) => &b < value,
                _ => false,
            })
        }
        Condition::LessThanEqual { field, value } => {
            let Some(field_value) = try_get_path_value(root_value, field)? else {
                return Ok(false);
            };
            Ok(match field_value {
                AttributeValue::N(n) => str_to_f64(&n) <= str_to_f64(value),
                AttributeValue::S(s) => &s <= value,
                AttributeValue::B(b) => &b <= value,
                _ => false,
            })
        }
        Condition::GreaterThan { field, value } => {
            let Some(field_value) = try_get_path_value(root_value, field)? else {
                return Ok(false);
            };
            Ok(match field_value {
                AttributeValue::N(n) => str_to_f64(&n) > str_to_f64(value),
                AttributeValue::S(s) => &s > value,
                AttributeValue::B(b) => &b > value,
                _ => false,
            })
        }
        Condition::GreaterThanEqual { field, value } => {
            let Some(field_value) = try_get_path_value(root_value, field)? else {
                return Ok(false);
            };
            Ok(match field_value {
                AttributeValue::N(n) => str_to_f64(&n) >= str_to_f64(value),
                AttributeValue::S(s) => &s >= value,
                AttributeValue::B(b) => &b >= value,
                _ => false,
            })
        }
        Condition::Equal { field, value } => {
            let Some(field_value) = try_get_path_value(root_value, field)? else {
                return Ok(false);
            };
            Ok(dynamodb_attribute_values_equal(&field_value, value))
        }
        Condition::NotEqual { field, value } => {
            let Some(field_value) = try_get_path_value(root_value, field)? else {
                return Ok(true);
            };
            Ok(!dynamodb_attribute_values_equal(&field_value, value))
        }
        Condition::Between { field, min, max } => {
            let Some(field_value) = try_get_path_value(root_value, field)? else {
                return Ok(false);
            };
            Ok(match field_value {
                AttributeValue::N(n) => {
                    let min = str_to_f64(min);
                    let max = str_to_f64(max);
                    let val = str_to_f64(&n);
                    min <= val && val <= max
                }
                AttributeValue::S(s) => &s >= min && &s <= max,
                AttributeValue::B(b) => &b >= min && &b <= max,
                _ => false,
            })
        }
        Condition::In { field, values } => {
            let Some(field_value) = try_get_path_value(root_value, field)? else {
                return Ok(false);
            };
            Ok(values
                .iter()
                .any(|value| dynamodb_attribute_values_equal(&field_value, value)))
        }
        Condition::ValueEqual { left, right } => Ok(dynamodb_attribute_values_equal(left, right)),
        Condition::ValueNotEqual { left, right } => {
            Ok(!dynamodb_attribute_values_equal(left, right))
        }
        Condition::Contains { field, value } => {
            let Some(field_value) = try_get_path_value(root_value, field)? else {
                return Ok(false);
            };
            Ok(match field_value {
                AttributeValue::S(s) => matches!(value, AttributeValue::S(v) if s.contains(v)),
                AttributeValue::B(b) => {
                    let AttributeValue::B(value) = value else {
                        return Ok(false);
                    };
                    decoded_binary_contains(&b, value)
                }
                AttributeValue::SS(ss) => matches!(value, AttributeValue::S(v) if ss.contains(v)),
                AttributeValue::NS(ns) => matches!(value, AttributeValue::N(v) if ns.contains(v)),
                AttributeValue::BS(bs) => matches!(value, AttributeValue::B(v) if bs.contains(v)),
                AttributeValue::L(l) => l.iter().any(|elem| elem == value),
                _ => false,
            })
        }
        Condition::BeginsWith { field, prefix } => {
            let Some(field_value) = try_get_path_value(root_value, field)? else {
                return Ok(false);
            };
            Ok(match (field_value, prefix) {
                (AttributeValue::S(s), AttributeValue::S(prefix)) => s.starts_with(prefix),
                (AttributeValue::B(b), AttributeValue::B(prefix)) => {
                    decoded_binary_starts_with(&b, prefix)
                }
                _ => false,
            })
        }
        Condition::AttributeType {
            field,
            attribute_type,
        } => Ok(try_get_path_value(root_value, field)?.is_some_and(|value| {
            attribute_type_code(&value).is_some_and(|actual| actual == attribute_type)
        })),
        Condition::Size { field, size } => Ok(try_get_path_value(root_value, field)?
            .as_ref()
            .and_then(attribute_size)
            == Some(*size)),
        Condition::SizeCompare {
            field,
            operator,
            size,
        } => Ok(try_get_path_value(root_value, field)?
            .as_ref()
            .and_then(attribute_size)
            .is_some_and(|actual_size| match operator {
                SizeComparison::Equal => actual_size == *size,
                SizeComparison::NotEqual => actual_size != *size,
                SizeComparison::LessThan => actual_size < *size,
                SizeComparison::LessThanEqual => actual_size <= *size,
                SizeComparison::GreaterThan => actual_size > *size,
                SizeComparison::GreaterThanEqual => actual_size >= *size,
            })),
        Condition::And { conditions } => {
            for condition in conditions {
                if !try_evaluate_condition_with_root(condition, root_value)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Condition::Or { conditions } => {
            for condition in conditions {
                if try_evaluate_condition_with_root(condition, root_value)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Condition::Not { condition } => {
            Ok(!try_evaluate_condition_with_root(condition, root_value)?)
        }
    }
}

pub fn try_evaluate_condition_with_cached_roots<E>(
    condition: &Condition,
    root_value: &mut impl FnMut(&str) -> Result<Option<AttributeValue>, E>,
) -> Result<bool, E> {
    let mut cache = Vec::new();
    try_evaluate_condition_with_cached_roots_inner(condition, root_value, &mut cache)
}

#[expect(clippy::too_many_lines)]
fn try_evaluate_condition_with_cached_roots_inner<'c, E>(
    condition: &'c Condition,
    root_value: &mut impl FnMut(&str) -> Result<Option<AttributeValue>, E>,
    cache: &mut Vec<(&'c str, Option<AttributeValue>)>,
) -> Result<bool, E> {
    match condition {
        Condition::Exists { field } => {
            Ok(try_get_path_value_cached(root_value, cache, field)?.is_some())
        }
        Condition::NotExists { field } => {
            Ok(try_get_path_value_cached(root_value, cache, field)?.is_none())
        }
        Condition::LessThan { field, value } => {
            let Some(field_value) = try_get_path_value_cached(root_value, cache, field)? else {
                return Ok(false);
            };
            Ok(match field_value {
                AttributeValue::N(n) => str_to_f64(n) < str_to_f64(value),
                AttributeValue::S(s) => s < value,
                AttributeValue::B(b) => b < value,
                _ => false,
            })
        }
        Condition::LessThanEqual { field, value } => {
            let Some(field_value) = try_get_path_value_cached(root_value, cache, field)? else {
                return Ok(false);
            };
            Ok(match field_value {
                AttributeValue::N(n) => str_to_f64(n) <= str_to_f64(value),
                AttributeValue::S(s) => s <= value,
                AttributeValue::B(b) => b <= value,
                _ => false,
            })
        }
        Condition::GreaterThan { field, value } => {
            let Some(field_value) = try_get_path_value_cached(root_value, cache, field)? else {
                return Ok(false);
            };
            Ok(match field_value {
                AttributeValue::N(n) => str_to_f64(n) > str_to_f64(value),
                AttributeValue::S(s) => s > value,
                AttributeValue::B(b) => b > value,
                _ => false,
            })
        }
        Condition::GreaterThanEqual { field, value } => {
            let Some(field_value) = try_get_path_value_cached(root_value, cache, field)? else {
                return Ok(false);
            };
            Ok(match field_value {
                AttributeValue::N(n) => str_to_f64(n) >= str_to_f64(value),
                AttributeValue::S(s) => s >= value,
                AttributeValue::B(b) => b >= value,
                _ => false,
            })
        }
        Condition::Equal { field, value } => {
            let Some(field_value) = try_get_path_value_cached(root_value, cache, field)? else {
                return Ok(false);
            };
            Ok(dynamodb_attribute_values_equal(field_value, value))
        }
        Condition::NotEqual { field, value } => {
            let Some(field_value) = try_get_path_value_cached(root_value, cache, field)? else {
                return Ok(true);
            };
            Ok(!dynamodb_attribute_values_equal(field_value, value))
        }
        Condition::Between { field, min, max } => {
            let Some(field_value) = try_get_path_value_cached(root_value, cache, field)? else {
                return Ok(false);
            };
            Ok(match field_value {
                AttributeValue::N(n) => {
                    let min = str_to_f64(min);
                    let max = str_to_f64(max);
                    let val = str_to_f64(n);
                    min <= val && val <= max
                }
                AttributeValue::S(s) => s >= min && s <= max,
                AttributeValue::B(b) => b >= min && b <= max,
                _ => false,
            })
        }
        Condition::In { field, values } => {
            let Some(field_value) = try_get_path_value_cached(root_value, cache, field)? else {
                return Ok(false);
            };
            Ok(values
                .iter()
                .any(|value| dynamodb_attribute_values_equal(field_value, value)))
        }
        Condition::ValueEqual { left, right } => Ok(dynamodb_attribute_values_equal(left, right)),
        Condition::ValueNotEqual { left, right } => {
            Ok(!dynamodb_attribute_values_equal(left, right))
        }
        Condition::Contains { field, value } => {
            let Some(field_value) = try_get_path_value_cached(root_value, cache, field)? else {
                return Ok(false);
            };
            Ok(match field_value {
                AttributeValue::S(s) => matches!(value, AttributeValue::S(v) if s.contains(v)),
                AttributeValue::B(b) => {
                    let AttributeValue::B(value) = value else {
                        return Ok(false);
                    };
                    decoded_binary_contains(b, value)
                }
                AttributeValue::SS(ss) => matches!(value, AttributeValue::S(v) if ss.contains(v)),
                AttributeValue::NS(ns) => matches!(value, AttributeValue::N(v) if ns.contains(v)),
                AttributeValue::BS(bs) => matches!(value, AttributeValue::B(v) if bs.contains(v)),
                AttributeValue::L(l) => l.iter().any(|elem| elem == value),
                _ => false,
            })
        }
        Condition::BeginsWith { field, prefix } => {
            let Some(field_value) = try_get_path_value_cached(root_value, cache, field)? else {
                return Ok(false);
            };
            Ok(match (field_value, prefix) {
                (AttributeValue::S(s), AttributeValue::S(prefix)) => s.starts_with(prefix),
                (AttributeValue::B(b), AttributeValue::B(prefix)) => {
                    decoded_binary_starts_with(b, prefix)
                }
                _ => false,
            })
        }
        Condition::AttributeType {
            field,
            attribute_type,
        } => Ok(
            try_get_path_value_cached(root_value, cache, field)?.is_some_and(|value| {
                attribute_type_code(value).is_some_and(|actual| actual == attribute_type)
            }),
        ),
        Condition::Size { field, size } => Ok(try_get_path_value_cached(root_value, cache, field)?
            .and_then(attribute_size)
            == Some(*size)),
        Condition::SizeCompare {
            field,
            operator,
            size,
        } => Ok(try_get_path_value_cached(root_value, cache, field)?
            .and_then(attribute_size)
            .is_some_and(|actual_size| match operator {
                SizeComparison::Equal => actual_size == *size,
                SizeComparison::NotEqual => actual_size != *size,
                SizeComparison::LessThan => actual_size < *size,
                SizeComparison::LessThanEqual => actual_size <= *size,
                SizeComparison::GreaterThan => actual_size > *size,
                SizeComparison::GreaterThanEqual => actual_size >= *size,
            })),
        Condition::And { conditions } => {
            for condition in conditions {
                if !try_evaluate_condition_with_cached_roots_inner(condition, root_value, cache)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Condition::Or { conditions } => {
            for condition in conditions {
                if try_evaluate_condition_with_cached_roots_inner(condition, root_value, cache)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Condition::Not { condition } => Ok(!try_evaluate_condition_with_cached_roots_inner(
            condition, root_value, cache,
        )?),
    }
}

#[must_use]
pub fn condition_has_repeated_root_field(condition: &Condition) -> bool {
    let mut roots = [const { None }; 8];
    let mut root_count = 0usize;
    condition_has_repeated_root_field_inner(condition, &mut roots, &mut root_count)
}

fn condition_has_repeated_root_field_inner<'a>(
    condition: &'a Condition,
    roots: &mut [Option<&'a str>; 8],
    root_count: &mut usize,
) -> bool {
    match condition {
        Condition::Exists { field }
        | Condition::NotExists { field }
        | Condition::LessThan { field, .. }
        | Condition::LessThanEqual { field, .. }
        | Condition::GreaterThan { field, .. }
        | Condition::GreaterThanEqual { field, .. }
        | Condition::Equal { field, .. }
        | Condition::NotEqual { field, .. }
        | Condition::Between { field, .. }
        | Condition::In { field, .. }
        | Condition::Contains { field, .. }
        | Condition::BeginsWith { field, .. }
        | Condition::AttributeType { field, .. }
        | Condition::Size { field, .. }
        | Condition::SizeCompare { field, .. } => {
            record_root_field(root_field(field), roots, root_count)
        }
        Condition::ValueEqual { .. } | Condition::ValueNotEqual { .. } => false,
        Condition::And { conditions } | Condition::Or { conditions } => conditions
            .iter()
            .any(|condition| condition_has_repeated_root_field_inner(condition, roots, root_count)),
        Condition::Not { condition } => {
            condition_has_repeated_root_field_inner(condition, roots, root_count)
        }
    }
}

fn record_root_field<'a>(
    root: &'a str,
    roots: &mut [Option<&'a str>; 8],
    root_count: &mut usize,
) -> bool {
    if roots.iter().flatten().any(|seen| *seen == root) {
        return true;
    }
    let Some(slot) = roots.get_mut(*root_count) else {
        return true;
    };
    *slot = Some(root);
    *root_count += 1;
    false
}

fn root_field(path: &str) -> &str {
    let root_end = next_path_separator(path, 0).unwrap_or(path.len());
    path.get(..root_end).unwrap_or_default()
}

fn dynamodb_attribute_values_equal(left: &AttributeValue, right: &AttributeValue) -> bool {
    match (left, right) {
        (AttributeValue::N(left), AttributeValue::N(right)) => {
            str_to_f64(left).total_cmp(&str_to_f64(right)).is_eq()
        }
        (AttributeValue::SS(left), AttributeValue::SS(right))
        | (AttributeValue::NS(left), AttributeValue::NS(right))
        | (AttributeValue::BS(left), AttributeValue::BS(right)) => string_sets_equal(left, right),
        _ => left == right,
    }
}

fn string_sets_equal(left: &[String], right: &[String]) -> bool {
    left.len() == right.len() && left.iter().all(|value| right.contains(value))
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

fn try_get_path_value<E>(
    root_value: &mut impl FnMut(&str) -> Result<Option<AttributeValue>, E>,
    path: &str,
) -> Result<Option<AttributeValue>, E> {
    let mut cursor = 0usize;
    let root_end = next_path_separator(path, cursor).unwrap_or(path.len());
    let Some(mut current) = root_value(path.get(cursor..root_end).unwrap_or_default())? else {
        return Ok(None);
    };
    cursor = root_end;

    while cursor < path.len() {
        match path.as_bytes().get(cursor).copied() {
            Some(b'.') => {
                cursor += 1;
                let end = next_path_separator(path, cursor).unwrap_or(path.len());
                let Some(key) = path.get(cursor..end) else {
                    return Ok(None);
                };
                let AttributeValue::M(map) = current else {
                    return Ok(None);
                };
                let Some(value) = map.get(key) else {
                    return Ok(None);
                };
                current = value.clone();
                cursor = end;
            }
            Some(b'[') => {
                cursor += 1;
                let Some(end) = path.get(cursor..).and_then(|tail| tail.find(']')) else {
                    return Ok(None);
                };
                let end = end + cursor;
                let Some(index) = path
                    .get(cursor..end)
                    .and_then(|value| value.parse::<usize>().ok())
                else {
                    return Ok(None);
                };
                let AttributeValue::L(list) = current else {
                    return Ok(None);
                };
                let Some(value) = list.get(index) else {
                    return Ok(None);
                };
                current = value.clone();
                cursor = end + 1;
            }
            _ => return Ok(None),
        }
    }

    Ok(Some(current))
}

fn try_get_path_value_cached<'b, 'c, E>(
    root_value: &mut impl FnMut(&str) -> Result<Option<AttributeValue>, E>,
    cache: &'b mut Vec<(&'c str, Option<AttributeValue>)>,
    path: &'c str,
) -> Result<Option<&'b AttributeValue>, E> {
    let mut cursor = 0usize;
    let root_end = next_path_separator(path, cursor).unwrap_or(path.len());
    let root = path.get(cursor..root_end).unwrap_or_default();
    let Some(mut current) = cached_root_value(root_value, cache, root)? else {
        return Ok(None);
    };
    cursor = root_end;

    while cursor < path.len() {
        match path.as_bytes().get(cursor).copied() {
            Some(b'.') => {
                cursor += 1;
                let end = next_path_separator(path, cursor).unwrap_or(path.len());
                let Some(key) = path.get(cursor..end) else {
                    return Ok(None);
                };
                let AttributeValue::M(map) = current else {
                    return Ok(None);
                };
                let Some(value) = map.get(key) else {
                    return Ok(None);
                };
                current = value;
                cursor = end;
            }
            Some(b'[') => {
                cursor += 1;
                let Some(end) = path.get(cursor..).and_then(|tail| tail.find(']')) else {
                    return Ok(None);
                };
                let end = end + cursor;
                let Some(index) = path
                    .get(cursor..end)
                    .and_then(|value| value.parse::<usize>().ok())
                else {
                    return Ok(None);
                };
                let AttributeValue::L(list) = current else {
                    return Ok(None);
                };
                let Some(value) = list.get(index) else {
                    return Ok(None);
                };
                current = value;
                cursor = end + 1;
            }
            _ => return Ok(None),
        }
    }

    Ok(Some(current))
}

fn cached_root_value<'b, 'c, E>(
    root_value: &mut impl FnMut(&str) -> Result<Option<AttributeValue>, E>,
    cache: &'b mut Vec<(&'c str, Option<AttributeValue>)>,
    root: &'c str,
) -> Result<Option<&'b AttributeValue>, E> {
    if let Some(index) = cache.iter().position(|(cached, _)| *cached == root) {
        return Ok(cache[index].1.as_ref());
    }
    let value = root_value(root)?;
    cache.push((root, value));
    let index = cache.len() - 1;
    Ok(cache[index].1.as_ref())
}

fn next_path_separator(path: &str, start: usize) -> Option<usize> {
    path.get(start..)?
        .char_indices()
        .find(|(_, ch)| *ch == '.' || *ch == '[')
        .map(|(index, _)| start + index)
}
