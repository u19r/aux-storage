use storage_types::{
    AttributeMap, AttributeValue, BatchWriteItemRequest, StorageError, StorageResult,
    TimestampMillis, TransactWriteItem, WireItem,
};

use crate::constants::{UPDATED_AT_NAME_PLACEHOLDER_BASE, UPDATED_AT_VALUE_PLACEHOLDER_BASE};

pub(crate) fn stamp_wire_item_now(item: &mut WireItem) -> StorageResult<()> {
    stamp_wire_item(item, current_updated_at_ms())
}

pub(crate) fn stamp_batch_write_request(request: &mut BatchWriteItemRequest) -> StorageResult<()> {
    let updated_at_ms = current_updated_at_ms();
    for write_requests in request.request_items.values_mut() {
        for write_request in write_requests {
            if let Some(put_request) = write_request.put_request.as_mut() {
                stamp_wire_item(&mut put_request.item, updated_at_ms)?;
            }
        }
    }
    Ok(())
}

pub(crate) fn stamp_transact_write_item(item: &mut TransactWriteItem) -> StorageResult<()> {
    let updated_at_ms = current_updated_at_ms();
    if let Some(put) = item.put.as_mut() {
        stamp_wire_item(&mut put.item, updated_at_ms)?;
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

pub(crate) fn inject_updated_at_into_update_expression(
    update_expression: &mut String,
    expression_attribute_names: &mut Option<storage_types::VecMap<String, String>>,
    expression_attribute_values: &mut Option<storage_types::AttributeMap>,
) -> StorageResult<()> {
    inject_updated_at_into_update_expression_with_ms(
        update_expression,
        expression_attribute_names,
        expression_attribute_values,
        current_updated_at_ms(),
    )
}

fn inject_updated_at_into_update_expression_with_ms(
    update_expression: &mut String,
    expression_attribute_names: &mut Option<storage_types::VecMap<String, String>>,
    expression_attribute_values: &mut Option<storage_types::AttributeMap>,
    updated_at_ms: i64,
) -> StorageResult<()> {
    normalize_timestamp_attribute_names_in_update_expression(
        update_expression,
        expression_attribute_names,
    );
    if has_updated_at_assignment(update_expression, expression_attribute_names.as_ref())? {
        return Ok(());
    }
    let values = expression_attribute_values.get_or_insert_with(storage_types::AttributeMap::new);

    let name_placeholder = reserve_placeholder(UPDATED_AT_NAME_PLACEHOLDER_BASE, |candidate| {
        expression_attribute_names
            .as_ref()
            .is_some_and(|names| names.contains_key(candidate))
    });
    let value_placeholder = reserve_placeholder(UPDATED_AT_VALUE_PLACEHOLDER_BASE, |candidate| {
        values.contains_key(candidate)
    });

    expression_attribute_names
        .get_or_insert_with(storage_types::VecMap::new)
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

fn has_updated_at_assignment(
    update_expression: &str,
    expression_attribute_names: Option<&storage_types::VecMap<String, String>>,
) -> StorageResult<bool> {
    let Some((set_start, set_end)) = update_set_section_bounds(update_expression) else {
        return Ok(false);
    };
    let Some(set_section) = update_expression.get(set_start..set_end) else {
        return Ok(false);
    };
    for assignment in set_section
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        let Some((lhs, _)) = assignment.split_once('=') else {
            continue;
        };
        let attribute_name = resolve_update_attribute_name(lhs.trim(), expression_attribute_names)?;
        if !is_updated_at_attribute_name(attribute_name.as_str()) {
            continue;
        }
        return Ok(true);
    }
    Ok(false)
}

fn normalize_timestamp_attribute_names_in_update_expression(
    update_expression: &mut String,
    expression_attribute_names: &mut Option<storage_types::VecMap<String, String>>,
) {
    let Some((set_start, set_end)) = update_set_section_bounds(update_expression) else {
        return;
    };
    let Some(set_section) = update_expression.get(set_start..set_end) else {
        return;
    };

    if let Some(names) = expression_attribute_names.as_mut() {
        for (lhs, _) in set_section
            .split(',')
            .filter_map(|assignment| assignment.split_once('='))
        {
            if let Some(name) = names.get_mut(lhs.trim())
                && let Some(alias) = timestamp_attribute_alias(name.as_str())
            {
                *name = alias.to_string();
            }
        }
    }

    let needs_expression_rebuild = set_section
        .split(',')
        .map(str::trim)
        .filter(|assignment| !assignment.is_empty())
        .any(|assignment| {
            assignment
                .split_once('=')
                .is_some_and(|(lhs, _)| timestamp_attribute_alias(lhs.trim()).is_some())
        });
    if !needs_expression_rebuild {
        return;
    }

    let normalized_set = set_section
        .split(',')
        .map(str::trim)
        .filter(|assignment| !assignment.is_empty())
        .map(normalize_set_assignment_timestamp_aliases)
        .collect::<Vec<_>>()
        .join(", ");

    update_expression.replace_range(set_start..set_end, &normalized_set);
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
    expression_attribute_names: Option<&storage_types::VecMap<String, String>>,
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

fn reserve_placeholder(base: &str, mut exists: impl FnMut(&str) -> bool) -> String {
    if !exists(base) {
        return base.to_string();
    }
    let mut suffix = 1usize;
    loop {
        let candidate = format!("{base}_{suffix}");
        if !exists(&candidate) {
            return candidate;
        }
        suffix = suffix.saturating_add(1);
    }
}

fn stamp_wire_item(item: &mut WireItem, updated_at_ms: i64) -> StorageResult<()> {
    use storage_types::single_table_entity::{
        CREATED_AT_ATTR, UPDATED_AT_ALIAS_ATTR, UPDATED_AT_ATTR,
    };
    if item.attribute_value(UPDATED_AT_ATTR)?.is_some() {
        return Ok(());
    }
    let updated_at_ms = updated_at_ms.max(
        item.number_attribute_i64(CREATED_AT_ATTR)?
            .unwrap_or(updated_at_ms),
    );
    let replacements = AttributeMap::from_iter([(
        UPDATED_AT_ALIAS_ATTR.to_string(),
        updated_at_attr_value(updated_at_ms),
    )]);
    *item = item.with_attribute_updates(&[], &replacements)?;
    Ok(())
}

fn updated_at_attr_value(updated_at_ms: i64) -> AttributeValue {
    AttributeValue::N(updated_at_ms.to_string())
}

fn current_updated_at_ms() -> i64 {
    TimestampMillis::now().timestamp_millis()
}
