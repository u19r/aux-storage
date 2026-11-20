use std::cmp::Ordering;

use smallvec::SmallVec;

use crate::{
    AttributeValue, AttributeValueLookup, StorageResult, WireItem, compare_dynamodb_numbers,
    condition::{Condition, SizeComparison},
};

#[must_use]
#[expect(clippy::too_many_lines)]
pub fn evaluate_condition(item: &impl AttributeValueLookup, condition: &Condition) -> bool {
    match condition {
        Condition::Exists { field } => field.lookup(item).is_some(),
        Condition::NotExists { field } => field.lookup(item).is_none(),
        Condition::LessThan { field, value } => {
            if let Some(field_value) = field.lookup(item) {
                match field_value {
                    AttributeValue::N(n) => {
                        compare_dynamodb_numbers(n, value).is_ok_and(Ordering::is_lt)
                    }
                    AttributeValue::S(s) => s < value,
                    AttributeValue::B(b) => compare_binary(b, value).is_lt(),
                    _ => false,
                }
            } else {
                false
            }
        }
        Condition::LessThanEqual { field, value } => {
            if let Some(field_value) = field.lookup(item) {
                match field_value {
                    AttributeValue::N(n) => {
                        compare_dynamodb_numbers(n, value).is_ok_and(Ordering::is_le)
                    }
                    AttributeValue::S(s) => s <= value,
                    AttributeValue::B(b) => compare_binary(b, value).is_le(),
                    _ => false,
                }
            } else {
                false
            }
        }
        Condition::GreaterThan { field, value } => {
            if let Some(field_value) = field.lookup(item) {
                match field_value {
                    AttributeValue::N(n) => {
                        compare_dynamodb_numbers(n, value).is_ok_and(Ordering::is_gt)
                    }
                    AttributeValue::S(s) => s > value,
                    AttributeValue::B(b) => compare_binary(b, value).is_gt(),
                    _ => false,
                }
            } else {
                false
            }
        }
        Condition::GreaterThanEqual { field, value } => {
            if let Some(field_value) = field.lookup(item) {
                match field_value {
                    AttributeValue::N(n) => {
                        compare_dynamodb_numbers(n, value).is_ok_and(Ordering::is_ge)
                    }
                    AttributeValue::S(s) => s >= value,
                    AttributeValue::B(b) => compare_binary(b, value).is_ge(),
                    _ => false,
                }
            } else {
                false
            }
        }
        Condition::Equal { field, value } => {
            if let Some(field_value) = field.lookup(item) {
                dynamodb_attribute_values_equal(field_value, value)
            } else {
                false
            }
        }
        Condition::NotEqual { field, value } => {
            if let Some(field_value) = field.lookup(item) {
                !dynamodb_attribute_values_equal(field_value, value)
            } else {
                // Field doesn't exist, so it's not equal to any specific value
                true
            }
        }
        Condition::Between { field, min, max } => {
            if let Some(field_value) = field.lookup(item) {
                match field_value {
                    AttributeValue::N(n) => {
                        compare_dynamodb_numbers(n, min).is_ok_and(Ordering::is_ge)
                            && compare_dynamodb_numbers(n, max).is_ok_and(Ordering::is_le)
                    }
                    AttributeValue::S(s) => s >= min && s <= max,
                    AttributeValue::B(b) => {
                        compare_binary(b, min).is_ge() && compare_binary(b, max).is_le()
                    }
                    _ => false,
                }
            } else {
                false
            }
        }
        Condition::In { field, values } => {
            if let Some(field_value) = field.lookup(item) {
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
            let Some(field_value) = field.lookup(item) else {
                return false;
            };
            contains_attribute(field_value, value)
        }
        Condition::BeginsWith { field, prefix } => {
            if let Some(field_value) = field.lookup(item) {
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
        } => field.lookup(item).is_some_and(|value| {
            attribute_type_code(value).is_some_and(|actual| actual == attribute_type)
        }),
        Condition::Size { field, size } => {
            field
                .lookup(item)
                .and_then(attribute_size)
                .is_some_and(|actual| {
                    compare_size(actual, size.as_deref()).is_some_and(Ordering::is_eq)
                })
        }
        Condition::SizeCompare {
            field,
            operator,
            size,
        } => field
            .lookup(item)
            .and_then(attribute_size)
            .is_some_and(|actual| {
                let ordering = compare_size(actual, size.as_deref());
                match operator {
                    // A type mismatch is unequal, as for any `<>` across types.
                    SizeComparison::NotEqual => ordering.is_none_or(Ordering::is_ne),
                    SizeComparison::Equal => ordering.is_some_and(Ordering::is_eq),
                    SizeComparison::LessThan => ordering.is_some_and(Ordering::is_lt),
                    SizeComparison::LessThanEqual => ordering.is_some_and(Ordering::is_le),
                    SizeComparison::GreaterThan => ordering.is_some_and(Ordering::is_gt),
                    SizeComparison::GreaterThanEqual => ordering.is_some_and(Ordering::is_ge),
                }
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

pub fn evaluate_wire_condition(
    item: Option<&WireItem>,
    condition: &Condition,
) -> StorageResult<bool> {
    let Some(item) = item else {
        return Ok(evaluate_condition(
            &SelectedConditionItem {
                fields: &[],
                values: &[],
            },
            condition,
        ));
    };
    let mut fields = SmallVec::<[&str; 8]>::new();
    collect_condition_roots(condition, &mut fields);
    let values = item.select_attribute_values(&fields)?;
    Ok(evaluate_condition(
        &SelectedConditionItem {
            fields: &fields,
            values: &values,
        },
        condition,
    ))
}

struct SelectedConditionItem<'a> {
    fields: &'a [&'a str],
    values: &'a [Option<AttributeValue>],
}

impl AttributeValueLookup for SelectedConditionItem<'_> {
    fn attribute_count(&self) -> usize {
        self.values.iter().filter(|value| value.is_some()).count()
    }

    fn get_attribute_value(&self, name: &str) -> Option<&AttributeValue> {
        self.fields
            .iter()
            .zip(self.values)
            .find_map(|(field, value)| (*field == name).then_some(value.as_ref()).flatten())
    }
}

fn collect_condition_roots<'a>(condition: &'a Condition, roots: &mut SmallVec<[&'a str; 8]>) {
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
            let root = field.root();
            if !roots.contains(&root) {
                roots.push(root);
            }
        }
        Condition::And { conditions } | Condition::Or { conditions } => {
            for condition in conditions {
                collect_condition_roots(condition, roots);
            }
        }
        Condition::Not { condition } => collect_condition_roots(condition, roots),
        Condition::ValueEqual { .. } | Condition::ValueNotEqual { .. } => {}
    }
}

impl Condition {
    /// Top-level attribute names this condition reads; a name can repeat.
    pub fn visit_root_names<'a>(&'a self, visit: &mut impl FnMut(&'a str)) {
        self.visit_attribute_paths(&mut |path| visit(path.root()));
    }
}

/// Binary values are stored as base64 text; DynamoDB orders them by unsigned
/// bytes. Undecodable text falls back to text order.
fn compare_binary(left: &str, right: &str) -> Ordering {
    match (
        crate::dynamodb_binary::decode_base64_string(left),
        crate::dynamodb_binary::decode_base64_string(right),
    ) {
        (Ok(left), Ok(right)) => left.cmp(&right),
        _ => left.cmp(right),
    }
}

fn contains_attribute(field_value: &AttributeValue, value: &AttributeValue) -> bool {
    match field_value {
        AttributeValue::S(s) => matches!(value, AttributeValue::S(v) if s.contains(v)),
        AttributeValue::B(b) => {
            let AttributeValue::B(value) = value else {
                return false;
            };
            decoded_binary_contains(b, value)
        }
        AttributeValue::SS(ss) => matches!(value, AttributeValue::S(v) if ss.contains(v)),
        AttributeValue::NS(ns) => matches!(value, AttributeValue::N(value)
            if ns.iter().any(|member| compare_dynamodb_numbers(member, value).is_ok_and(Ordering::is_eq))),
        AttributeValue::BS(bs) => matches!(value, AttributeValue::B(v) if bs.contains(v)),
        AttributeValue::L(l) => l
            .iter()
            .any(|elem| dynamodb_attribute_values_equal(elem, value)),
        _ => false,
    }
}

fn dynamodb_attribute_values_equal(left: &AttributeValue, right: &AttributeValue) -> bool {
    match (left, right) {
        (AttributeValue::N(left), AttributeValue::N(right)) => {
            compare_dynamodb_numbers(left, right).is_ok_and(Ordering::is_eq)
        }
        (AttributeValue::SS(left), AttributeValue::SS(right))
        | (AttributeValue::BS(left), AttributeValue::BS(right)) => string_sets_equal(left, right),
        (AttributeValue::NS(left), AttributeValue::NS(right)) => number_sets_equal(left, right),
        (AttributeValue::L(left), AttributeValue::L(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| dynamodb_attribute_values_equal(left, right))
        }
        (AttributeValue::M(left), AttributeValue::M(right)) => {
            left.len() == right.len()
                && left.iter().all(|(name, value)| {
                    right
                        .get(name)
                        .is_some_and(|other| dynamodb_attribute_values_equal(value, other))
                })
        }
        _ => left == right,
    }
}

fn string_sets_equal(left: &[String], right: &[String]) -> bool {
    left.len() == right.len() && left.iter().all(|value| right.contains(value))
}

fn number_sets_equal(left: &[String], right: &[String]) -> bool {
    left.len() == right.len()
        && left.iter().all(|value| {
            right.iter().any(|candidate| {
                compare_dynamodb_numbers(value, candidate).is_ok_and(Ordering::is_eq)
            })
        })
}

/// Orders an item's size against a DynamoDB number operand without allocating.
fn compare_size(size: usize, operand: Option<&str>) -> Option<Ordering> {
    let mut digits = [0u8; 20];
    let mut start = digits.len();
    let mut rest = size;
    loop {
        start -= 1;
        digits[start] = b'0' + u8::try_from(rest % 10).unwrap_or(0);
        rest /= 10;
        if rest == 0 {
            break;
        }
    }
    let size = std::str::from_utf8(&digits[start..]).ok()?;
    compare_dynamodb_numbers(size, operand?).ok()
}

fn attribute_size(value: &AttributeValue) -> Option<usize> {
    match value {
        AttributeValue::S(s) => Some(s.encode_utf16().count()),
        AttributeValue::B(b) => crate::dynamodb_binary::decode_base64_string(b)
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
    let Ok(value) = crate::dynamodb_binary::decode_base64_string(value) else {
        return false;
    };
    let Ok(operand) = crate::dynamodb_binary::decode_base64_string(operand) else {
        return false;
    };
    !operand.is_empty() && value.windows(operand.len()).any(|window| window == operand)
}

fn decoded_binary_starts_with(value: &str, operand: &str) -> bool {
    let Ok(value) = crate::dynamodb_binary::decode_base64_string(value) else {
        return false;
    };
    let Ok(operand) = crate::dynamodb_binary::decode_base64_string(operand) else {
        return false;
    };
    value.starts_with(&operand)
}
