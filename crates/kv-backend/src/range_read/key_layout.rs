//! Ordered KV spans for table and GSI Query and Scan pages.

use storage_provider::{BoundRangeReadPage, BoundRangeReadPlan};
use storage_types::{
    AttributeValue, BoundTableInfo, IndexName, ItemKey, KeyAttributeType, KeySchemaElement,
    StorageError, StorageResult, condition::Condition,
};

use crate::{
    catalog::table_id_from_history,
    constants::GSI_PROJECTION_KEY_PREFIX_BYTES,
    gsi::ordered_projection_key,
    item_read::primary_item_key,
    range::prefix_upper_bound,
    table_key_layout::{TableSubFamily, table_prefix},
};

fn index_id(table: &BoundTableInfo, name: &IndexName) -> StorageResult<u16> {
    table
        .binding
        .gsi(name)
        .map(|index| index.index_id)
        .ok_or_else(|| StorageError::internal("KV index binding is absent"))
}

pub(super) struct PhysicalSpan {
    pub(super) begin: Vec<u8>,
    pub(super) end: Vec<u8>,
}

pub(super) fn physical_span(
    table: &BoundTableInfo,
    bound: &BoundRangeReadPlan<'_>,
    page: &BoundRangeReadPage<'_>,
) -> StorageResult<PhysicalSpan> {
    let index_id = bound
        .index_name
        .as_ref()
        .map(|name| index_id(table, name))
        .transpose()?;
    let mut span = condition_span(table, index_id, bound, page.condition)?;
    if let Some(start) = page.start.as_ref() {
        let physical = physical_item_key(table, bound, start)?;
        if page.forward {
            span.begin = span.begin.max(after_key(physical));
        } else {
            span.end = span.end.min(physical);
        }
    }
    Ok(span)
}

fn condition_span(
    table: &BoundTableInfo,
    index_id: Option<u16>,
    bound: &BoundRangeReadPlan<'_>,
    condition: Option<&Condition>,
) -> StorageResult<PhysicalSpan> {
    let prefix = range_prefix(table, index_id, None)?;
    let mut span = PhysicalSpan {
        begin: prefix.clone(),
        end: prefix_upper_bound(&prefix)
            .ok_or_else(|| StorageError::internal("KV range prefix has no upper bound"))?,
    };
    if let Some(condition) = condition {
        let schema = bound
            .index
            .as_ref()
            .map_or(bound.table.key_schema.as_slice(), |index| {
                index.key_schema.as_slice()
            });
        let hash_field = KeySchemaElement::ordered(schema)
            .next()
            .ok_or_else(|| StorageError::internal("range schema lacks a hash key"))?;
        let hash = hash_equality(condition, &hash_field.attribute_name)
            .ok_or_else(|| StorageError::internal("validated query lacks a hash equality"))?;
        let partition = range_prefix(table, index_id, Some((hash, None)))?;
        span.end = prefix_upper_bound(&partition)
            .ok_or_else(|| StorageError::internal("KV range prefix has no upper bound"))?;
        span.begin = partition;
        if let Some(sort_field) = KeySchemaElement::ordered(schema).nth(1)
            && let Some(sort) = find_sort_condition(condition, &sort_field.attribute_name)
        {
            let kind = bound
                .table
                .attribute_definitions
                .iter()
                .find(|definition| definition.attribute_name == sort_field.attribute_name)
                .ok_or_else(|| {
                    StorageError::internal("range sort key has no attribute definition")
                })?
                .attribute_type;
            span.constrain_sort(table, index_id, (hash, kind), sort)?;
        }
    }
    Ok(span)
}

fn hash_equality<'a>(condition: &'a Condition, field: &str) -> Option<&'a AttributeValue> {
    match condition {
        Condition::Equal { field: name, value } if name == field => Some(value),
        Condition::And { conditions } => conditions
            .iter()
            .find_map(|part| hash_equality(part, field)),
        _ => None,
    }
}

fn find_sort_condition<'a>(condition: &'a Condition, field: &str) -> Option<&'a Condition> {
    match condition {
        Condition::And { conditions } => conditions
            .iter()
            .find_map(|part| find_sort_condition(part, field)),
        Condition::Equal { field: name, .. }
        | Condition::LessThan { field: name, .. }
        | Condition::LessThanEqual { field: name, .. }
        | Condition::GreaterThan { field: name, .. }
        | Condition::GreaterThanEqual { field: name, .. }
        | Condition::Between { field: name, .. }
        | Condition::BeginsWith { field: name, .. }
            if name == field =>
        {
            Some(condition)
        }
        _ => None,
    }
}

impl PhysicalSpan {
    fn constrain_sort(
        &mut self,
        table: &BoundTableInfo,
        index_id: Option<u16>,
        key: (&AttributeValue, KeyAttributeType),
        condition: &Condition,
    ) -> StorageResult<()> {
        let [begin, end] = encode_condition_bounds(table, index_id, key, condition)?;
        if let Some(begin) = begin
            && begin > self.begin
        {
            self.begin = begin;
        }
        if let Some(end) = end
            && end < self.end
        {
            self.end = end;
        }
        Ok(())
    }
}

fn encode_condition_bounds(
    table: &BoundTableInfo,
    index_id: Option<u16>,
    key: (&AttributeValue, KeyAttributeType),
    condition: &Condition,
) -> StorageResult<[Option<Vec<u8>>; 2]> {
    let (hash, kind) = key;
    let exact = |value: &AttributeValue| encode_exact_sort_key(table, index_id, hash, value);
    let raw = |value: &str| bind_sort_value(kind, value);
    match condition {
        Condition::Equal { value, .. } => {
            let key = exact(value)?;
            Ok([
                Some(key.clone()),
                Some(advance_past_exact_sort_key(key, index_id)?),
            ])
        }
        Condition::LessThan { value, .. } => Ok([None, Some(exact(&raw(value))?)]),
        Condition::LessThanEqual { value, .. } => {
            let key = exact(&raw(value))?;
            Ok([None, Some(advance_past_exact_sort_key(key, index_id)?)])
        }
        Condition::GreaterThan { value, .. } => {
            let key = exact(&raw(value))?;
            Ok([Some(advance_past_exact_sort_key(key, index_id)?), None])
        }
        Condition::GreaterThanEqual { value, .. } => Ok([Some(exact(&raw(value))?), None]),
        Condition::Between { min, max, .. } => {
            let low = exact(&raw(min))?;
            let high = exact(&raw(max))?;
            Ok([
                Some(low),
                Some(advance_past_exact_sort_key(high, index_id)?),
            ])
        }
        Condition::BeginsWith { prefix, .. } => {
            let low = range_prefix(table, index_id, Some((hash, Some(prefix))))?;
            let high = prefix_upper_bound(&low);
            Ok([Some(low), high])
        }
        _ => Err(StorageError::internal(
            "validated query has an unsupported sort condition",
        )),
    }
}

fn bind_sort_value(kind: KeyAttributeType, raw: &str) -> AttributeValue {
    match kind {
        KeyAttributeType::S => AttributeValue::S(raw.to_owned()),
        KeyAttributeType::N => AttributeValue::N(raw.to_owned()),
        KeyAttributeType::B => AttributeValue::B(raw.to_owned()),
    }
}

fn encode_exact_sort_key(
    table: &BoundTableInfo,
    index_id: Option<u16>,
    hash: &AttributeValue,
    sort: &AttributeValue,
) -> StorageResult<Vec<u8>> {
    let mut key = range_prefix(table, index_id, Some((hash, Some(sort))))?;
    if index_id.is_some() {
        // The prefix codec omits this terminator for begins_with. Exact GSI
        // bounds need it before the table-key tie suffix.
        key.push(0);
    }
    Ok(key)
}

fn advance_past_exact_sort_key(key: Vec<u8>, index_id: Option<u16>) -> StorageResult<Vec<u8>> {
    if index_id.is_some() {
        // One GSI sort value may have many table-key ties.
        prefix_upper_bound(&key)
            .ok_or_else(|| StorageError::internal("KV sort key has no upper bound"))
    } else {
        Ok(after_key(key))
    }
}

fn range_prefix(
    table: &BoundTableInfo,
    index_id: Option<u16>,
    key: Option<(&AttributeValue, Option<&AttributeValue>)>,
) -> StorageResult<Vec<u8>> {
    let family = if index_id.is_some() {
        TableSubFamily::GsiEntry
    } else {
        TableSubFamily::Item
    };
    let mut prefix = Vec::with_capacity(GSI_PROJECTION_KEY_PREFIX_BYTES + key.map_or(0, |_| 64));
    prefix.extend_from_slice(&table_prefix(
        table_id_from_history(table.history_id)?,
        family,
    ));
    if let Some(index_id) = index_id {
        prefix.extend_from_slice(&index_id.to_be_bytes());
    }
    if let Some((hash, sort_prefix)) = key {
        let key = if let Some(index_id) = index_id {
            let name = table
                .binding
                .gsis
                .iter()
                .find(|index| index.index_id == index_id)
                .ok_or_else(|| StorageError::internal("KV range index binding is absent"))?
                .name
                .clone();
            ItemKey::index_prefix(
                table.table_name.clone(),
                name,
                hash.clone(),
                sort_prefix.cloned(),
            )
        } else {
            ItemKey::table_key(table.table_name.clone(), hash.clone(), sort_prefix.cloned())
        };
        let suffix = match (index_id, sort_prefix) {
            (Some(_), Some(_)) => crate::gsi::ordered_projection_prefix(&key)?,
            (Some(_), None) => crate::gsi::ordered_projection_suffix(&key)?,
            (None, _) => key.sorted_storage_suffix()?,
        };
        prefix.extend_from_slice(&suffix);
    }
    Ok(prefix)
}

pub(super) fn after_key(mut key: Vec<u8>) -> Vec<u8> {
    key.push(0);
    key
}

pub(super) fn physical_item_key(
    table: &BoundTableInfo,
    bound: &BoundRangeReadPlan<'_>,
    key: &ItemKey,
) -> StorageResult<Vec<u8>> {
    if let Some(name) = &bound.index_name {
        ordered_projection_key(
            table_id_from_history(table.history_id)?,
            index_id(table, name)?,
            key,
        )
    } else {
        primary_item_key(
            table_id_from_history(table.history_id)?,
            table,
            &key.key_attributes(&table.info)?,
        )
    }
}
