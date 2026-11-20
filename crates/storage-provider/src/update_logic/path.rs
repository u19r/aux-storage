use storage_types::{
    AttributeMap, AttributeValue, AttributeValueLookup, StorageError, StorageResult,
    condition::{AttributePath, PathElement},
};

pub(super) fn resolve_document_path(
    path: &str,
    expression_attribute_names: Option<&storage_types::VecMap<String, String>>,
) -> StorageResult<AttributePath> {
    if path.contains('#') && expression_attribute_names.is_none() {
        return Err(StorageError::validation(format!(
            "Attribute name {path} requires ExpressionAttributeNames"
        )));
    }
    let invalid = || StorageError::validation(format!("Invalid document path: {path}"));
    let name_element = |raw: &str| -> StorageResult<PathElement> {
        if raw.is_empty() {
            return Err(invalid());
        }
        if raw.starts_with('#') {
            let value = expression_attribute_names
                .and_then(|names| names.get(raw))
                .ok_or_else(|| {
                    StorageError::validation(format!(
                        "Attribute name {raw} not found in ExpressionAttributeNames"
                    ))
                })?;
            return Ok(PathElement::Name(value.clone()));
        }
        Ok(PathElement::Name(raw.to_owned()))
    };
    let bytes = path.as_bytes();
    let first_end = bytes
        .iter()
        .position(|byte| matches!(byte, b'.' | b'['))
        .unwrap_or(bytes.len());
    let PathElement::Name(root) = name_element(path.get(..first_end).ok_or_else(invalid)?)? else {
        return Err(invalid());
    };
    let mut resolved = AttributePath::top(root);
    let mut at = first_end;
    while at < bytes.len() {
        if bytes[at] == b'[' {
            let close = bytes[at..]
                .iter()
                .position(|byte| *byte == b']')
                .ok_or_else(invalid)?
                + at;
            let index = path
                .get(at + 1..close)
                .ok_or_else(invalid)?
                .parse::<usize>()
                .map_err(|_| invalid())?;
            resolved.push(PathElement::Index(index));
            at = close + 1;
        } else {
            at += 1; // '.'
            let end = bytes[at..]
                .iter()
                .position(|byte| matches!(byte, b'.' | b'['))
                .map_or(bytes.len(), |offset| at + offset);
            resolved.push(name_element(path.get(at..end).ok_or_else(invalid)?)?);
            at = end;
        }
    }
    Ok(resolved)
}

pub(super) fn set_attribute_value(
    item: &mut AttributeMap,
    path: &AttributePath,
    value: AttributeValue,
) -> StorageResult<()> {
    if let Some(name) = path.top_level() {
        item.insert(name.to_string(), value);
        return Ok(());
    }
    set_path_value(item, path.elements(), value)
}

pub(super) fn get_attribute_value<'a>(
    item: &'a impl AttributeValueLookup,
    path: &AttributePath,
) -> Option<&'a AttributeValue> {
    if let Some(name) = path.top_level() {
        return item.get_attribute_value(name);
    }
    get_path_value(item, path.elements())
}

pub(super) fn remove_attribute_value(
    item: &mut AttributeMap,
    path: &AttributePath,
) -> StorageResult<()> {
    if let Some(name) = path.top_level() {
        item.remove(name);
        return Ok(());
    }
    remove_path_value(item, path.elements())
}

fn get_path_value<'a>(
    item: &'a impl AttributeValueLookup,
    path: &[PathElement],
) -> Option<&'a AttributeValue> {
    let (first, rest) = path.split_first()?;
    let PathElement::Name(name) = first else {
        return None;
    };
    let mut value = item.get_attribute_value(name)?;
    for segment in rest {
        match (segment, value) {
            (PathElement::Name(name), AttributeValue::M(map)) => {
                value = map.get(name)?;
            }
            (PathElement::Index(index), AttributeValue::L(list)) => {
                value = list.get(*index)?;
            }
            _ => return None,
        }
    }
    Some(value)
}

fn set_path_value(
    item: &mut AttributeMap,
    path: &[PathElement],
    value: AttributeValue,
) -> StorageResult<()> {
    let (first, rest) = path
        .split_first()
        .ok_or_else(|| StorageError::validation("Invalid document path for SET operation"))?;
    let PathElement::Name(name) = first else {
        return Err(StorageError::validation(
            "SET operation requires an attribute path",
        ));
    };
    if rest.is_empty() {
        item.insert(name.clone(), value);
        return Ok(());
    }
    let mut root = item.remove(name).ok_or_else(|| {
        StorageError::validation(
            "The document path provided in the update expression is invalid for update",
        )
    })?;
    let result = set_nested_path_value(&mut root, rest, value);
    item.insert(name.clone(), root);
    result
}

fn set_nested_path_value(
    current: &mut AttributeValue,
    path: &[PathElement],
    value: AttributeValue,
) -> StorageResult<()> {
    let (segment, rest) = path
        .split_first()
        .ok_or_else(|| StorageError::validation("Invalid document path for SET operation"))?;
    if rest.is_empty() {
        match (segment, current) {
            (PathElement::Name(name), AttributeValue::M(map)) => {
                map.insert(name.clone(), value);
                Ok(())
            }
            (PathElement::Index(index), AttributeValue::L(list)) => {
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
            (PathElement::Name(name), AttributeValue::M(map)) => {
                let next = map.get_mut(name).ok_or_else(|| {
                    StorageError::validation(
                        "The document path provided in the update expression is invalid for update",
                    )
                })?;
                set_nested_path_value(next, rest, value)
            }
            (PathElement::Index(index), AttributeValue::L(list)) => {
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

fn remove_path_value(item: &mut AttributeMap, path: &[PathElement]) -> StorageResult<()> {
    let Some((first, rest)) = path.split_first() else {
        return Ok(());
    };
    let PathElement::Name(name) = first else {
        return Ok(());
    };
    if rest.is_empty() {
        item.remove(name);
        return Ok(());
    }
    let Some(mut root) = item.remove(name) else {
        return remove_missing_path_result(rest);
    };
    let result = remove_nested_path_value(&mut root, rest);
    item.insert(name.clone(), root);
    result
}

fn remove_nested_path_value(
    current: &mut AttributeValue,
    path: &[PathElement],
) -> StorageResult<()> {
    let Some((segment, rest)) = path.split_first() else {
        return Ok(());
    };
    if rest.is_empty() {
        match (segment, current) {
            (PathElement::Name(name), AttributeValue::M(map)) => {
                map.remove(name);
            }
            (PathElement::Index(index), AttributeValue::L(list)) => {
                if *index < list.len() {
                    list.remove(*index);
                }
            }
            _ => return Err(invalid_update_document_path()),
        }
        return Ok(());
    }
    match (segment, current) {
        (PathElement::Name(name), AttributeValue::M(map)) => {
            let Some(next) = map.get_mut(name) else {
                return remove_missing_path_result(rest);
            };
            remove_nested_path_value(next, rest)
        }
        (PathElement::Index(index), AttributeValue::L(list)) => {
            let Some(next) = list.get_mut(*index) else {
                return remove_missing_path_result(rest);
            };
            remove_nested_path_value(next, rest)
        }
        _ => Err(invalid_update_document_path()),
    }
}

fn remove_missing_path_result(rest: &[PathElement]) -> StorageResult<()> {
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

/// Fails unless the parent of `path` exists and can hold the path's last
/// element (a map for a name, a list for an index).
pub(super) fn require_update_parent(
    item: &AttributeMap,
    path: &AttributePath,
) -> StorageResult<()> {
    let elements = path.elements();
    let Some((last, parents)) = elements.split_last() else {
        return Ok(());
    };
    let Some((PathElement::Name(root), nested)) = parents.split_first() else {
        return Err(invalid_update_document_path());
    };
    let Some(mut value) = item.get_attribute_value(root) else {
        return Err(invalid_update_document_path());
    };
    for segment in nested {
        value = match (segment, value) {
            (PathElement::Name(name), AttributeValue::M(map)) => map.get(name),
            (PathElement::Index(index), AttributeValue::L(list)) => list.get(*index),
            _ => None,
        }
        .ok_or_else(invalid_update_document_path)?;
    }
    match (last, value) {
        (PathElement::Name(_), AttributeValue::M(_))
        | (PathElement::Index(_), AttributeValue::L(_)) => Ok(()),
        _ => Err(invalid_update_document_path()),
    }
}
