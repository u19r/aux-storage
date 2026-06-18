use std::collections::HashMap;

use storage_types::{AttributeValue, StorageError, StorageResult};

pub(super) fn resolve_document_path(
    path: &str,
    expression_attribute_names: Option<&HashMap<String, String>>,
) -> StorageResult<String> {
    if !path.contains('#') {
        return Ok(path.to_string());
    }
    if expression_attribute_names.is_none() {
        Err(StorageError::validation(format!(
            "Attribute name {path} requires ExpressionAttributeNames"
        )))
    } else {
        resolve_path_aliases(path, expression_attribute_names)
    }
}

fn resolve_path_aliases(
    path: &str,
    expression_attribute_names: Option<&HashMap<String, String>>,
) -> StorageResult<String> {
    let Some(names) = expression_attribute_names else {
        return Ok(path.to_string());
    };

    let mut resolved = String::with_capacity(path.len());
    let mut chars = path.char_indices().peekable();
    while let Some((idx, ch)) = chars.next() {
        if ch != '#' {
            resolved.push(ch);
            continue;
        }

        let mut end = path.len();
        while let Some((idx, next)) = chars.peek().copied() {
            if next == '.' || next == '[' {
                end = idx;
                break;
            }
            chars.next();
        }
        let Some(placeholder) = path.get(idx..end) else {
            return Err(StorageError::validation("Invalid document path"));
        };
        let value = names.get(placeholder).ok_or_else(|| {
            StorageError::validation(format!(
                "Attribute name {placeholder} not found in ExpressionAttributeNames"
            ))
        })?;
        resolved.push_str(value);
    }
    Ok(resolved)
}

pub(super) fn set_attribute_value(
    item: &mut HashMap<String, AttributeValue>,
    field: &str,
    value: AttributeValue,
) -> StorageResult<()> {
    if is_top_level_path(field) {
        item.insert(field.to_string(), value);
        return Ok(());
    }
    let path = parse_update_path(field)?;
    set_path_value(item, &path, value)
}

pub(super) fn get_attribute_value<'a>(
    item: &'a HashMap<String, AttributeValue>,
    field: &str,
) -> Option<&'a AttributeValue> {
    if is_top_level_path(field) {
        return item.get(field);
    }
    let path = parse_update_path(field).ok()?;
    get_path_value(item, &path)
}

pub(super) fn remove_attribute_value(
    item: &mut HashMap<String, AttributeValue>,
    field: &str,
) -> StorageResult<()> {
    if is_top_level_path(field) {
        item.remove(field);
        return Ok(());
    }
    let path = parse_update_path(field)?;
    remove_path_value(item, &path)
}

fn is_top_level_path(field: &str) -> bool {
    !field
        .as_bytes()
        .iter()
        .any(|byte| matches!(byte, b'.' | b'['))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum UpdatePathSegment {
    Name(String),
    Index(usize),
}

fn parse_update_path(path: &str) -> StorageResult<Vec<UpdatePathSegment>> {
    let mut segments = Vec::with_capacity(2);
    let mut current = String::new();
    let mut after_index = false;
    let mut chars = path.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '.' => {
                if current.is_empty() && !after_index {
                    return Err(StorageError::validation(format!(
                        "Invalid document path: {path}"
                    )));
                }
                if !current.is_empty() {
                    segments.push(UpdatePathSegment::Name(std::mem::take(&mut current)));
                }
                after_index = false;
            }
            '[' => {
                if !current.is_empty() {
                    segments.push(UpdatePathSegment::Name(std::mem::take(&mut current)));
                }
                let mut index = String::new();
                for next in chars.by_ref() {
                    if next == ']' {
                        break;
                    }
                    index.push(next);
                }
                let index = index.parse::<usize>().map_err(|_| {
                    StorageError::validation(format!("Invalid document path: {path}"))
                })?;
                segments.push(UpdatePathSegment::Index(index));
                after_index = true;
            }
            _ => {
                current.push(ch);
                after_index = false;
            }
        }
    }
    if !current.is_empty() {
        segments.push(UpdatePathSegment::Name(current));
    }
    if segments.is_empty() {
        return Err(StorageError::validation(format!(
            "Invalid document path: {path}"
        )));
    }
    Ok(segments)
}

fn get_path_value<'a>(
    item: &'a HashMap<String, AttributeValue>,
    path: &[UpdatePathSegment],
) -> Option<&'a AttributeValue> {
    let (first, rest) = path.split_first()?;
    let UpdatePathSegment::Name(name) = first else {
        return None;
    };
    let mut value = item.get(name)?;
    for segment in rest {
        match (segment, value) {
            (UpdatePathSegment::Name(name), AttributeValue::M(map)) => {
                value = map.get(name)?;
            }
            (UpdatePathSegment::Index(index), AttributeValue::L(list)) => {
                value = list.get(*index)?;
            }
            _ => return None,
        }
    }
    Some(value)
}

fn set_path_value(
    item: &mut HashMap<String, AttributeValue>,
    path: &[UpdatePathSegment],
    value: AttributeValue,
) -> StorageResult<()> {
    let (first, rest) = path
        .split_first()
        .ok_or_else(|| StorageError::validation("Invalid document path for SET operation"))?;
    let UpdatePathSegment::Name(name) = first else {
        return Err(StorageError::validation(
            "SET operation requires an attribute path",
        ));
    };
    if rest.is_empty() {
        item.insert(name.clone(), value);
        return Ok(());
    }
    let root = item.get_mut(name).ok_or_else(|| {
        StorageError::validation(
            "The document path provided in the update expression is invalid for update",
        )
    })?;
    set_nested_path_value(root, rest, value)
}

fn set_nested_path_value(
    current: &mut AttributeValue,
    path: &[UpdatePathSegment],
    value: AttributeValue,
) -> StorageResult<()> {
    let (segment, rest) = path
        .split_first()
        .ok_or_else(|| StorageError::validation("Invalid document path for SET operation"))?;
    if rest.is_empty() {
        match (segment, current) {
            (UpdatePathSegment::Name(name), AttributeValue::M(map)) => {
                map.insert(name.clone(), value);
                Ok(())
            }
            (UpdatePathSegment::Index(index), AttributeValue::L(list)) => {
                if *index >= list.len() {
                    list.push(value);
                } else {
                    list[*index] = value;
                }
                Ok(())
            }
            _ => Err(StorageError::validation(
                "The document path provided in the update expression is invalid for update",
            )),
        }
    } else {
        match (segment, current) {
            (UpdatePathSegment::Name(name), AttributeValue::M(map)) => {
                let next = map.get_mut(name).ok_or_else(|| {
                    StorageError::validation(
                        "The document path provided in the update expression is invalid for update",
                    )
                })?;
                set_nested_path_value(next, rest, value)
            }
            (UpdatePathSegment::Index(index), AttributeValue::L(list)) => {
                let next = list.get_mut(*index).ok_or_else(|| {
                    StorageError::validation(
                        "The document path provided in the update expression is invalid for update",
                    )
                })?;
                set_nested_path_value(next, rest, value)
            }
            _ => Err(StorageError::validation(
                "The document path provided in the update expression is invalid for update",
            )),
        }
    }
}

fn remove_path_value(
    item: &mut HashMap<String, AttributeValue>,
    path: &[UpdatePathSegment],
) -> StorageResult<()> {
    let Some((first, rest)) = path.split_first() else {
        return Ok(());
    };
    let UpdatePathSegment::Name(name) = first else {
        return Ok(());
    };
    if rest.is_empty() {
        item.remove(name);
        return Ok(());
    }
    let Some(root) = item.get_mut(name) else {
        return remove_missing_path_result(rest);
    };
    remove_nested_path_value(root, rest)
}

fn remove_nested_path_value(
    current: &mut AttributeValue,
    path: &[UpdatePathSegment],
) -> StorageResult<()> {
    let Some((segment, rest)) = path.split_first() else {
        return Ok(());
    };
    if rest.is_empty() {
        match (segment, current) {
            (UpdatePathSegment::Name(name), AttributeValue::M(map)) => {
                map.remove(name);
            }
            (UpdatePathSegment::Index(index), AttributeValue::L(list)) if *index < list.len() => {
                list.remove(*index);
            }
            _ => {}
        }
        return Ok(());
    }
    match (segment, current) {
        (UpdatePathSegment::Name(name), AttributeValue::M(map)) => {
            let Some(next) = map.get_mut(name) else {
                return remove_missing_path_result(rest);
            };
            remove_nested_path_value(next, rest)
        }
        (UpdatePathSegment::Index(index), AttributeValue::L(list)) => {
            let Some(next) = list.get_mut(*index) else {
                return remove_missing_path_result(rest);
            };
            remove_nested_path_value(next, rest)
        }
        _ => Err(invalid_update_document_path()),
    }
}

fn remove_missing_path_result(rest: &[UpdatePathSegment]) -> StorageResult<()> {
    if rest.is_empty() {
        Ok(())
    } else {
        Err(invalid_update_document_path())
    }
}

fn invalid_update_document_path() -> StorageError {
    StorageError::validation(
        "The document path provided in the update expression is invalid for update",
    )
}
