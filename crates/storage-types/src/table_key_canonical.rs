use std::cmp::Ordering;

use crate::{AttributeMap, AttributeValue, TableKey};

impl PartialEq for TableKey {
    fn eq(&self, other: &Self) -> bool {
        self.canonical_cmp(other).is_eq()
    }
}

impl Eq for TableKey {}

impl TableKey {
    #[must_use]
    pub fn canonical_cmp(&self, other: &Self) -> Ordering {
        compare_length_prefixed(self.table_name.as_ref(), other.table_name.as_ref())
            .then_with(|| compare_attribute_values(&self.hash_key, &other.hash_key))
            .then_with(|| match (&self.range_key, &other.range_key) {
                (Some(left), Some(right)) => compare_attribute_values(left, right),
                (Some(_), None) => Ordering::Greater,
                (None, Some(_)) => Ordering::Less,
                (None, None) => Ordering::Equal,
            })
    }
}

fn compare_length_prefixed(left: &str, right: &str) -> Ordering {
    let left_bytes = left.as_bytes();
    let right_bytes = right.as_bytes();
    compare_encoded_length(left_bytes.len(), right_bytes.len())
        .then_with(|| left_bytes.cmp(right_bytes))
}

fn compare_encoded_length(left: usize, right: usize) -> Ordering {
    u32::try_from(left)
        .unwrap_or(u32::MAX)
        .cmp(&u32::try_from(right).unwrap_or(u32::MAX))
}

fn compare_attribute_values(left: &AttributeValue, right: &AttributeValue) -> Ordering {
    let tag_order = attribute_tag(left).cmp(&attribute_tag(right));
    if tag_order != Ordering::Equal {
        return tag_order;
    }

    match (left, right) {
        (AttributeValue::S(left), AttributeValue::S(right))
        | (AttributeValue::N(left), AttributeValue::N(right))
        | (AttributeValue::B(left), AttributeValue::B(right)) => {
            compare_length_prefixed(left, right)
        }
        (AttributeValue::SS(left), AttributeValue::SS(right))
        | (AttributeValue::NS(left), AttributeValue::NS(right))
        | (AttributeValue::BS(left), AttributeValue::BS(right)) => {
            compare_encoded_length(left.len(), right.len()).then_with(|| {
                left.iter()
                    .zip(right)
                    .map(|(left, right)| compare_length_prefixed(left, right))
                    .find(|order| *order != Ordering::Equal)
                    .unwrap_or(Ordering::Equal)
            })
        }
        (AttributeValue::BOOL(left), AttributeValue::BOOL(right))
        | (AttributeValue::NULL(left), AttributeValue::NULL(right)) => left.cmp(right),
        (AttributeValue::L(left), AttributeValue::L(right)) => {
            compare_encoded_length(left.len(), right.len()).then_with(|| {
                left.iter()
                    .zip(right)
                    .map(|(left, right)| compare_attribute_values(left, right))
                    .find(|order| *order != Ordering::Equal)
                    .unwrap_or(Ordering::Equal)
            })
        }
        (AttributeValue::M(left), AttributeValue::M(right)) => compare_maps(left, right),
        _ => Ordering::Equal,
    }
}

fn attribute_tag(value: &AttributeValue) -> u8 {
    match value {
        AttributeValue::S(_) => b'S',
        AttributeValue::N(_) => b'N',
        AttributeValue::B(_) => b'B',
        AttributeValue::SS(_) => b's',
        AttributeValue::NS(_) => b'n',
        AttributeValue::BS(_) => b'b',
        AttributeValue::BOOL(_) => b'1',
        AttributeValue::NULL(_) => b'0',
        AttributeValue::L(_) => b'l',
        AttributeValue::M(_) => b'm',
    }
}

fn compare_maps(left: &AttributeMap, right: &AttributeMap) -> Ordering {
    let count_order = compare_encoded_length(left.len(), right.len());
    if count_order != Ordering::Equal {
        return count_order;
    }

    let mut left_previous = None;
    let mut right_previous = None;
    loop {
        let left_entry = next_map_entry(left, left_previous);
        let right_entry = next_map_entry(right, right_previous);
        match (left_entry, right_entry) {
            (Some((left_key, left_value)), Some((right_key, right_value))) => {
                let key_order = compare_length_prefixed(left_key, right_key);
                if key_order != Ordering::Equal {
                    return key_order;
                }
                let value_order = compare_attribute_values(left_value, right_value);
                if value_order != Ordering::Equal {
                    return value_order;
                }
                left_previous = Some(left_key);
                right_previous = Some(right_key);
            }
            (None, None) => return Ordering::Equal,
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
        }
    }
}

fn next_map_entry<'a>(
    values: &'a AttributeMap,
    previous: Option<&str>,
) -> Option<(&'a str, &'a AttributeValue)> {
    values
        .iter()
        .filter(|(key, _)| previous.is_none_or(|previous| *key > previous))
        .min_by_key(|(key, _)| *key)
}
