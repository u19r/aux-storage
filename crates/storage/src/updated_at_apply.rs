use std::collections::HashMap;

use storage_types::{
    AttributeValue, BatchWriteItemEncodeRequest, BatchWriteItemRequest, StorageError,
    StorageResult, TimestampMillis, TransactEncodeItem, TransactWriteItem, WireItem,
};

use crate::constants::{UPDATED_AT_NAME_PLACEHOLDER_BASE, UPDATED_AT_VALUE_PLACEHOLDER_BASE};

pub(crate) fn stamp_item_map_now(item: &mut HashMap<String, AttributeValue>) -> StorageResult<()> {
    stamp_item_map(item, current_updated_at_ms());
    Ok(())
}

pub(crate) fn stamp_wire_item_now(item: &mut WireItem) -> StorageResult<()> {
    stamp_wire_item(item, current_updated_at_ms())
}

pub(crate) fn refresh_existing_item_map_timestamp_now(
    item: &mut HashMap<String, AttributeValue>,
) -> StorageResult<()> {
    refresh_existing_item_map_timestamp(item, current_updated_at_ms());
    Ok(())
}

pub(crate) fn refresh_existing_wire_item_timestamp_now(item: &mut WireItem) -> StorageResult<()> {
    refresh_existing_wire_item_timestamp(item, current_updated_at_ms())
}

pub(crate) fn stamp_batch_write_request(request: &mut BatchWriteItemRequest) -> StorageResult<()> {
    let updated_at_ms = current_updated_at_ms();
    for write_requests in request.request_items.values_mut() {
        for write_request in write_requests {
            if let Some(put_request) = write_request.put_request.as_mut() {
                stamp_item_map(&mut put_request.item, updated_at_ms);
            }
        }
    }
    Ok(())
}

pub(crate) fn stamp_batch_write_encode_request(
    request: &mut BatchWriteItemEncodeRequest,
) -> StorageResult<()> {
    let updated_at_ms = current_updated_at_ms();
    for write_requests in request.request_items.values_mut() {
        for write_request in write_requests {
            if let Some(put_request) = write_request.put_request.as_mut() {
                stamp_wire_item(put_request.item.item_mut(), updated_at_ms)?;
            }
        }
    }
    Ok(())
}

pub(crate) fn refresh_existing_batch_write_timestamps(
    request: &mut BatchWriteItemRequest,
) -> StorageResult<()> {
    let updated_at_ms = current_updated_at_ms();
    for write_requests in request.request_items.values_mut() {
        for write_request in write_requests {
            if let Some(put_request) = write_request.put_request.as_mut() {
                refresh_existing_item_map_timestamp(&mut put_request.item, updated_at_ms);
            }
        }
    }
    Ok(())
}

pub(crate) fn refresh_existing_batch_write_encode_timestamps(
    request: &mut BatchWriteItemEncodeRequest,
) -> StorageResult<()> {
    let updated_at_ms = current_updated_at_ms();
    for write_requests in request.request_items.values_mut() {
        for write_request in write_requests {
            if let Some(put_request) = write_request.put_request.as_mut() {
                refresh_existing_wire_item_timestamp(put_request.item.item_mut(), updated_at_ms)?;
            }
        }
    }
    Ok(())
}

pub(crate) fn stamp_transact_write_item(item: &mut TransactWriteItem) -> StorageResult<()> {
    let updated_at_ms = current_updated_at_ms();
    if let Some(put) = item.put.as_mut() {
        stamp_item_map(&mut put.item, updated_at_ms);
    }
    if let Some(update) = item.update.as_mut() {
        inject_updated_at_into_update_expression_with_ms(
            &mut update.update_expression,
            &mut update.expression_attribute_names,
            &mut update.expression_attribute_values,
            updated_at_ms,
        )?;
    }
    Ok(())
}

pub(crate) fn stamp_transact_encode_item(item: &mut TransactEncodeItem) -> StorageResult<()> {
    let updated_at_ms = current_updated_at_ms();
    if let Some(put) = item.put.as_mut() {
        stamp_wire_item(put.item.item_mut(), updated_at_ms)?;
    }
    if let Some(update) = item.update.as_mut() {
        inject_updated_at_into_update_expression_with_ms(
            &mut update.update_expression,
            &mut update.expression_attribute_names,
            &mut update.expression_attribute_values,
            updated_at_ms,
        )?;
    }
    Ok(())
}

pub(crate) fn refresh_existing_transact_write_item_timestamp(
    item: &mut TransactWriteItem,
) -> StorageResult<()> {
    let updated_at_ms = current_updated_at_ms();
    if let Some(put) = item.put.as_mut() {
        refresh_existing_item_map_timestamp(&mut put.item, updated_at_ms);
    }
    Ok(())
}

pub(crate) fn refresh_existing_transact_encode_item_timestamp(
    item: &mut TransactEncodeItem,
) -> StorageResult<()> {
    let updated_at_ms = current_updated_at_ms();
    if let Some(put) = item.put.as_mut() {
        refresh_existing_wire_item_timestamp(put.item.item_mut(), updated_at_ms)?;
    }
    Ok(())
}

pub(crate) fn inject_updated_at_into_update_expression(
    update_expression: &mut String,
    expression_attribute_names: &mut Option<HashMap<String, String>>,
    expression_attribute_values: &mut Option<HashMap<String, AttributeValue>>,
) -> StorageResult<()> {
    normalize_timestamp_attribute_names_in_update_expression(
        update_expression,
        expression_attribute_names,
    );
    inject_updated_at_into_update_expression_with_ms(
        update_expression,
        expression_attribute_names,
        expression_attribute_values,
        current_updated_at_ms(),
    )
}

fn inject_updated_at_into_update_expression_with_ms(
    update_expression: &mut String,
    expression_attribute_names: &mut Option<HashMap<String, String>>,
    expression_attribute_values: &mut Option<HashMap<String, AttributeValue>>,
    updated_at_ms: i64,
) -> StorageResult<()> {
    normalize_timestamp_attribute_names_in_update_expression(
        update_expression,
        expression_attribute_names,
    );
    let existing_placeholder =
        updated_at_assignment_placeholder(update_expression, expression_attribute_names.as_ref())?;
    let name_placeholder = reserve_placeholder(
        expression_attribute_names.as_ref(),
        UPDATED_AT_NAME_PLACEHOLDER_BASE,
    );
    let value_placeholder = reserve_placeholder(
        expression_attribute_values.as_ref(),
        UPDATED_AT_VALUE_PLACEHOLDER_BASE,
    );
    let values = expression_attribute_values.get_or_insert_with(HashMap::new);
    if let Some(placeholder) = existing_placeholder {
        values.insert(placeholder, updated_at_attr_value(updated_at_ms));
        return Ok(());
    }

    expression_attribute_names
        .get_or_insert_with(HashMap::new)
        .insert(
            name_placeholder.clone(),
            storage_types::single_table_entity::UPDATED_AT_ALIAS_ATTR.to_string(),
        );
    values.insert(
        value_placeholder.clone(),
        updated_at_attr_value(updated_at_ms),
    );
    *update_expression =
        append_update_set_assignment(update_expression, &name_placeholder, &value_placeholder);
    Ok(())
}

fn append_update_set_assignment(
    update_expression: &str,
    name_placeholder: &str,
    value_placeholder: &str,
) -> String {
    let assignment = format!("{name_placeholder} = {value_placeholder}");
    if let Some((set_start, set_end)) = update_set_section_bounds(update_expression) {
        let set_keyword_start = set_start.saturating_sub(4);
        let prefix = update_expression
            .get(..set_keyword_start)
            .map(str::trim)
            .unwrap_or_default();
        let existing_set = update_expression
            .get(set_start..set_end)
            .map(str::trim)
            .unwrap_or_default();
        let merged_set = if existing_set.is_empty() {
            assignment
        } else {
            format!("{existing_set}, {assignment}")
        };
        let tail = update_expression
            .get(set_end..)
            .map(str::trim)
            .unwrap_or_default();
        if prefix.is_empty() && tail.is_empty() {
            return format!("SET {merged_set}");
        }

        let mut rebuilt = String::new();
        if !prefix.is_empty() {
            rebuilt.push_str(prefix);
            rebuilt.push(' ');
        }
        rebuilt.push_str("SET ");
        rebuilt.push_str(&merged_set);
        if !tail.is_empty() {
            rebuilt.push(' ');
            rebuilt.push_str(tail);
        }
        return rebuilt;
    }

    let trimmed = update_expression.trim();
    if trimmed.is_empty() {
        return format!("SET {assignment}");
    }
    format!("SET {assignment} {trimmed}")
}

fn update_set_section_bounds(update_expression: &str) -> Option<(usize, usize)> {
    let upper = update_expression.to_ascii_uppercase();
    let set_pos = upper.find("SET ")?;
    let set_start = set_pos + 4;
    let upper_tail = upper.get(set_start..)?;
    let mut set_end = update_expression.len();
    for marker in [" REMOVE ", " ADD ", " DELETE "] {
        if let Some(marker_pos) = upper_tail.find(marker) {
            set_end = set_end.min(set_start + marker_pos);
        }
    }
    Some((set_start, set_end))
}

fn updated_at_assignment_placeholder(
    update_expression: &str,
    expression_attribute_names: Option<&HashMap<String, String>>,
) -> StorageResult<Option<String>> {
    let Some((set_start, set_end)) = update_set_section_bounds(update_expression) else {
        return Ok(None);
    };
    let Some(set_section) = update_expression.get(set_start..set_end) else {
        return Ok(None);
    };
    for assignment in set_section
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        let Some((lhs, rhs)) = assignment.split_once('=') else {
            continue;
        };
        let attribute_name = resolve_update_attribute_name(lhs.trim(), expression_attribute_names)?;
        if !is_updated_at_attribute_name(attribute_name.as_str()) {
            continue;
        }
        let value_placeholder = rhs.trim();
        if !value_placeholder.starts_with(':') {
            return Err(StorageError::validation(
                "updated_at assignment must use an expression value placeholder",
            ));
        }
        return Ok(Some(value_placeholder.to_string()));
    }
    Ok(None)
}

fn normalize_timestamp_attribute_names_in_update_expression(
    update_expression: &mut String,
    expression_attribute_names: &mut Option<HashMap<String, String>>,
) {
    if let Some(names) = expression_attribute_names.as_mut() {
        for name in names.values_mut() {
            if let Some(alias) = timestamp_attribute_alias(name.as_str()) {
                *name = alias.to_string();
            }
        }
    }

    let Some((set_start, set_end)) = update_set_section_bounds(update_expression) else {
        return;
    };
    let Some(set_section) = update_expression.get(set_start..set_end) else {
        return;
    };

    let normalized_set = set_section
        .split(',')
        .map(str::trim)
        .filter(|assignment| !assignment.is_empty())
        .map(normalize_set_assignment_timestamp_aliases)
        .collect::<Vec<_>>()
        .join(", ");

    let set_keyword_start = set_start.saturating_sub(4);
    let prefix = update_expression
        .get(..set_keyword_start)
        .map(str::trim)
        .unwrap_or_default();
    let tail = update_expression
        .get(set_end..)
        .map(str::trim)
        .unwrap_or_default();

    let mut rebuilt = String::new();
    if !prefix.is_empty() {
        rebuilt.push_str(prefix);
        rebuilt.push(' ');
    }
    rebuilt.push_str("SET ");
    rebuilt.push_str(&normalized_set);
    if !tail.is_empty() {
        rebuilt.push(' ');
        rebuilt.push_str(tail);
    }
    *update_expression = rebuilt;
}

fn normalize_set_assignment_timestamp_aliases(assignment: &str) -> String {
    let Some((lhs, rhs)) = assignment.split_once('=') else {
        return assignment.to_string();
    };
    let lhs = lhs.trim();
    let rhs = rhs.trim();
    let normalized_lhs = timestamp_attribute_alias(lhs).unwrap_or(lhs);
    format!("{normalized_lhs} = {rhs}")
}

fn timestamp_attribute_alias(name: &str) -> Option<&'static str> {
    match name {
        storage_types::single_table_entity::CREATED_AT_ATTR => {
            Some(storage_types::single_table_entity::CREATED_AT_ALIAS_ATTR)
        }
        storage_types::single_table_entity::UPDATED_AT_ATTR => {
            Some(storage_types::single_table_entity::UPDATED_AT_ALIAS_ATTR)
        }
        storage_types::single_table_entity::EXPIRES_AT_ATTR => {
            Some(storage_types::single_table_entity::EXPIRES_AT_ALIAS_ATTR)
        }
        _ => None,
    }
}

fn is_updated_at_attribute_name(name: &str) -> bool {
    name == storage_types::single_table_entity::UPDATED_AT_ATTR
        || name == storage_types::single_table_entity::UPDATED_AT_ALIAS_ATTR
}

pub(crate) fn resolve_update_attribute_name(
    token: &str,
    expression_attribute_names: Option<&HashMap<String, String>>,
) -> StorageResult<String> {
    if !token.contains('#') {
        return Ok(token.to_string());
    }
    let names = expression_attribute_names.ok_or_else(|| {
        StorageError::validation(
            "update expression uses attribute name placeholders but expression_attribute_names \
             was missing",
        )
    })?;

    let mut resolved = String::with_capacity(token.len());
    let mut chars = token.char_indices().peekable();
    while let Some((start, ch)) = chars.next() {
        if ch != '#' {
            resolved.push(ch);
            continue;
        }

        let mut end = token.len();
        while let Some((index, next)) = chars.peek().copied() {
            if matches!(next, '.' | '[') {
                end = index;
                break;
            }
            chars.next();
        }
        let Some(placeholder) = token.get(start..end) else {
            return Err(StorageError::validation("Invalid update document path"));
        };
        let attribute_name = names.get(placeholder).ok_or_else(|| {
            StorageError::validation(format!(
                "update expression placeholder '{placeholder}' was not found in \
                 expression_attribute_names"
            ))
        })?;
        resolved.push_str(attribute_name);
    }
    Ok(resolved)
}

fn reserve_placeholder<T>(existing: Option<&HashMap<String, T>>, base: &str) -> String {
    if existing.is_none_or(|map| !map.contains_key(base)) {
        return base.to_string();
    }
    let mut suffix = 1usize;
    loop {
        let candidate = format!("{base}_{suffix}");
        if existing.is_none_or(|map| !map.contains_key(candidate.as_str())) {
            return candidate;
        }
        suffix = suffix.saturating_add(1);
    }
}

fn stamp_item_map(item: &mut HashMap<String, AttributeValue>, updated_at_ms: i64) {
    let updated_at_ms = item
        .get(storage_types::single_table_entity::CREATED_AT_ALIAS_ATTR)
        .or_else(|| item.get(storage_types::single_table_entity::CREATED_AT_ATTR))
        .and_then(|value| match value {
            AttributeValue::N(value) => value.parse::<i64>().ok(),
            _ => None,
        })
        .map_or(updated_at_ms, |created_at_ms| {
            updated_at_ms.max(created_at_ms)
        });
    item.remove(storage_types::single_table_entity::UPDATED_AT_ATTR);
    item.insert(
        storage_types::single_table_entity::UPDATED_AT_ALIAS_ATTR.to_string(),
        updated_at_attr_value(updated_at_ms),
    );
}

fn stamp_wire_item(item: &mut WireItem, updated_at_ms: i64) -> StorageResult<()> {
    let mut map = item.clone().into_attribute_map()?;
    stamp_item_map(&mut map, updated_at_ms);
    *item = WireItem::from_attribute_map(&map)?;
    Ok(())
}

fn refresh_existing_item_map_timestamp(
    item: &mut HashMap<String, AttributeValue>,
    updated_at_ms: i64,
) {
    if has_updated_at_attribute(item) {
        stamp_item_map(item, updated_at_ms);
    }
}

fn refresh_existing_wire_item_timestamp(
    item: &mut WireItem,
    updated_at_ms: i64,
) -> StorageResult<()> {
    let mut map = item.clone().into_attribute_map()?;
    if !has_updated_at_attribute(&map) {
        return Ok(());
    }
    stamp_item_map(&mut map, updated_at_ms);
    *item = WireItem::from_attribute_map(&map)?;
    Ok(())
}

fn has_updated_at_attribute(item: &HashMap<String, AttributeValue>) -> bool {
    item.contains_key(storage_types::single_table_entity::UPDATED_AT_ATTR)
        || item.contains_key(storage_types::single_table_entity::UPDATED_AT_ALIAS_ATTR)
}

fn updated_at_attr_value(updated_at_ms: i64) -> AttributeValue {
    AttributeValue::N(updated_at_ms.to_string())
}

fn current_updated_at_ms() -> i64 {
    TimestampMillis::now().timestamp_millis()
}
