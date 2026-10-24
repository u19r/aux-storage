use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use storage_types::{
    AttributeMap, AttributeValue, ReturnValuesOldNewUpdated, StorageResult, UpdateItemRequest,
    UpdateItemResponse,
};

use crate::update_logic::{
    logic::{BoundUpdateOperation, UpdateOperation},
    path::get_attribute_value,
};

pub trait UpdateFieldName {
    fn field_name(&self) -> &str;
}

/// Bind the updated-field names used by `ReturnValues=UPDATED_*` responses.
///
/// This runs the canonical update-expression parser so every provider shapes
/// the response from the same bound paths and reports the same parse errors.
pub fn update_item_response_fields(request: &UpdateItemRequest) -> StorageResult<Vec<Arc<str>>> {
    let (operations, _) = super::logic::before_update_item_optional(
        request.update_expression.as_deref(),
        None,
        request.expression_attribute_names.as_ref(),
        request.expression_attribute_values.as_ref(),
    )?;
    Ok(operations
        .iter()
        .map(|operation| operation.field_name_arc())
        .collect())
}

impl UpdateFieldName for UpdateOperation {
    fn field_name(&self) -> &str {
        UpdateOperation::field_name(self)
    }
}

impl UpdateFieldName for BoundUpdateOperation<'_> {
    fn field_name(&self) -> &str {
        BoundUpdateOperation::field_name(self)
    }
}

impl UpdateFieldName for String {
    fn field_name(&self) -> &str {
        self.as_str()
    }
}

impl UpdateFieldName for Arc<str> {
    fn field_name(&self) -> &str {
        self.as_ref()
    }
}

pub fn update_item_response<T: UpdateFieldName>(
    operations: &[T],
    old_item: Option<HashMap<String, AttributeValue>>,
    new_item: Option<HashMap<String, AttributeValue>>,
    return_values: Option<&ReturnValuesOldNewUpdated>,
) -> StorageResult<UpdateItemResponse> {
    let attributes = match return_values {
        None => None,
        Some(rv) => match rv {
            ReturnValuesOldNewUpdated::None => None,
            ReturnValuesOldNewUpdated::AllOld => old_item,
            ReturnValuesOldNewUpdated::AllNew => new_item,
            ReturnValuesOldNewUpdated::UpdatedOld => {
                old_item.and_then(|item| non_empty_updated_attributes_owned(operations, item))
            }
            ReturnValuesOldNewUpdated::UpdatedNew => {
                new_item.and_then(|item| non_empty_updated_attributes_owned(operations, item))
            }
        },
    };

    Ok(UpdateItemResponse {
        attributes: attributes.map(Into::into),
    })
}

/// Shape the new-path response without materializing a HashMap for flat paths.
pub fn update_item_response_attributes<T: UpdateFieldName>(
    operations: &[T],
    old_item: Option<AttributeMap>,
    new_item: Option<AttributeMap>,
    return_values: Option<&ReturnValuesOldNewUpdated>,
) -> StorageResult<UpdateItemResponse> {
    let attributes = match return_values {
        None | Some(ReturnValuesOldNewUpdated::None) => None,
        Some(ReturnValuesOldNewUpdated::AllOld) => old_item,
        Some(ReturnValuesOldNewUpdated::AllNew) => new_item,
        Some(ReturnValuesOldNewUpdated::UpdatedOld) => {
            old_item.and_then(|item| updated_attribute_map(operations, item))
        }
        Some(ReturnValuesOldNewUpdated::UpdatedNew) => {
            new_item.and_then(|item| updated_attribute_map(operations, item))
        }
    };
    Ok(UpdateItemResponse { attributes })
}

fn updated_attribute_map<T: UpdateFieldName>(
    operations: &[T],
    mut item: AttributeMap,
) -> Option<AttributeMap> {
    if operations
        .iter()
        .all(|op| is_top_level_response_path(op.field_name()))
    {
        let mut attributes = AttributeMap::with_capacity(operations.len());
        for operation in operations {
            if let Some(value) = item.remove(operation.field_name()) {
                attributes.insert(operation.field_name(), value);
            }
        }
        return (!attributes.is_empty()).then_some(attributes);
    }
    let attributes = updated_attributes_for_response(operations, &item.into_hashmap());
    (!attributes.is_empty()).then(|| attributes.into())
}

#[must_use]
pub const fn return_values_need_old_item(
    return_values: Option<&ReturnValuesOldNewUpdated>,
) -> bool {
    matches!(
        return_values,
        Some(ReturnValuesOldNewUpdated::AllOld | ReturnValuesOldNewUpdated::UpdatedOld)
    )
}

#[must_use]
pub const fn return_values_need_updated_fields(
    return_values: Option<&ReturnValuesOldNewUpdated>,
) -> bool {
    matches!(
        return_values,
        Some(ReturnValuesOldNewUpdated::UpdatedOld | ReturnValuesOldNewUpdated::UpdatedNew)
    )
}

fn non_empty_updated_attributes<T: UpdateFieldName>(
    operations: &[T],
    item: &HashMap<String, AttributeValue>,
) -> Option<HashMap<String, AttributeValue>> {
    let attributes = updated_attributes_for_response(operations, item);
    (!attributes.is_empty()).then_some(attributes)
}

fn non_empty_updated_attributes_owned<T: UpdateFieldName>(
    operations: &[T],
    mut item: HashMap<String, AttributeValue>,
) -> Option<HashMap<String, AttributeValue>> {
    if !operations
        .iter()
        .all(|operation| is_top_level_response_path(operation.field_name()))
    {
        return non_empty_updated_attributes(operations, &item);
    }

    let mut attributes = HashMap::with_capacity(operations.len());
    for operation in operations {
        if let Some(value) = item.remove(operation.field_name()) {
            attributes.insert(operation.field_name().to_string(), value);
        }
    }
    (!attributes.is_empty()).then_some(attributes)
}

pub fn updated_attributes_for_response<T: UpdateFieldName>(
    operations: &[T],
    item: &HashMap<String, AttributeValue>,
) -> HashMap<String, AttributeValue> {
    // The common form is a flat SET/ADD/REMOVE response.  Avoid building a
    // temporary projection tree when no operation addresses a document path;
    // nested paths still use the merge logic below because a root operation
    // and a child operation must retain their order-dependent shape.
    if operations
        .iter()
        .all(|operation| is_top_level_response_path(operation.field_name()))
    {
        let mut attributes = HashMap::with_capacity(operations.len());
        for operation in operations {
            if let Some(value) = item.get(operation.field_name()) {
                attributes.insert(operation.field_name().to_string(), value.clone());
            }
        }
        return attributes;
    }

    let mut projection = UpdatedAttributeProjection::default();
    for op in operations {
        let field_name = op.field_name();
        if let Some(field_value) = get_attribute_value(item, field_name) {
            projection.insert(field_name, field_value.clone());
        }
    }
    projection.into_attributes()
}

fn is_top_level_response_path(path: &str) -> bool {
    !path.is_empty()
        && !path
            .as_bytes()
            .iter()
            .any(|byte| matches!(byte, b'.' | b'['))
}

#[derive(Default)]
struct UpdatedAttributeProjection {
    roots: HashMap<String, ProjectedAttributeValue>,
}

impl UpdatedAttributeProjection {
    fn insert(&mut self, field_name: &str, value: AttributeValue) {
        let Ok(path) = parse_response_path(field_name) else {
            return;
        };
        let Some((ResponsePathSegment::Name(root), rest)) = path.split_first() else {
            return;
        };
        insert_projected_path(
            self.roots.entry((*root).to_string()).or_default(),
            rest,
            value,
        );
    }

    fn into_attributes(self) -> HashMap<String, AttributeValue> {
        self.roots
            .into_iter()
            .map(|(name, value)| (name, value.into_attribute_value()))
            .collect()
    }
}

#[derive(Default)]
enum ProjectedAttributeValue {
    #[default]
    Empty,
    Leaf(AttributeValue),
    Map(HashMap<String, ProjectedAttributeValue>),
    List(BTreeMap<usize, ProjectedAttributeValue>),
}

impl ProjectedAttributeValue {
    fn into_attribute_value(self) -> AttributeValue {
        match self {
            ProjectedAttributeValue::Empty => AttributeValue::NULL(true),
            ProjectedAttributeValue::Leaf(value) => value,
            ProjectedAttributeValue::Map(map) => AttributeValue::M(
                map.into_iter()
                    .map(|(name, value)| (name, value.into_attribute_value()))
                    .collect(),
            ),
            ProjectedAttributeValue::List(list) => AttributeValue::L(
                list.into_values()
                    .map(ProjectedAttributeValue::into_attribute_value)
                    .collect(),
            ),
        }
    }
}

fn insert_projected_path(
    target: &mut ProjectedAttributeValue,
    path: &[ResponsePathSegment<'_>],
    value: AttributeValue,
) {
    let Some((segment, rest)) = path.split_first() else {
        *target = ProjectedAttributeValue::Leaf(value);
        return;
    };
    match segment {
        ResponsePathSegment::Name(name) => {
            if !matches!(target, ProjectedAttributeValue::Map(_)) {
                *target = ProjectedAttributeValue::Map(HashMap::new());
            }
            let ProjectedAttributeValue::Map(map) = target else {
                unreachable!();
            };
            insert_projected_path(map.entry((*name).to_string()).or_default(), rest, value);
        }
        ResponsePathSegment::Index(index) => {
            if !matches!(target, ProjectedAttributeValue::List(_)) {
                *target = ProjectedAttributeValue::List(BTreeMap::new());
            }
            let ProjectedAttributeValue::List(list) = target else {
                unreachable!();
            };
            insert_projected_path(list.entry(*index).or_default(), rest, value);
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ResponsePathSegment<'a> {
    Name(&'a str),
    Index(usize),
}

fn parse_response_path(path: &str) -> StorageResult<Vec<ResponsePathSegment<'_>>> {
    let mut segments = Vec::with_capacity(2);
    let mut name_start = 0;
    let mut chars = path.char_indices().peekable();

    while let Some((index, ch)) = chars.next() {
        match ch {
            '.' => {
                if name_start == index {
                    return Err(storage_types::StorageError::validation(
                        "Invalid document path",
                    ));
                }
                let name = path.get(name_start..index).ok_or_else(|| {
                    storage_types::StorageError::validation("Invalid document path")
                })?;
                segments.push(ResponsePathSegment::Name(name));
                name_start = index + ch.len_utf8();
            }
            '[' => {
                if name_start < index {
                    let name = path.get(name_start..index).ok_or_else(|| {
                        storage_types::StorageError::validation("Invalid document path")
                    })?;
                    segments.push(ResponsePathSegment::Name(name));
                }
                let index_start = index + ch.len_utf8();
                let mut close = None;
                for (next_index, next_ch) in chars.by_ref() {
                    if next_ch == ']' {
                        close = Some(next_index);
                        break;
                    }
                }
                let close = close.ok_or_else(|| {
                    storage_types::StorageError::validation("Invalid document path")
                })?;
                let list_index = path
                    .get(index_start..close)
                    .ok_or_else(|| {
                        storage_types::StorageError::validation("Invalid document path")
                    })?
                    .parse::<usize>()
                    .map_err(|_| {
                        storage_types::StorageError::validation("Invalid document path")
                    })?;
                segments.push(ResponsePathSegment::Index(list_index));
                name_start = close + ']'.len_utf8();
                if let Some((dot_index, '.')) = chars.peek().copied() {
                    let _ = chars.next();
                    name_start = dot_index + '.'.len_utf8();
                }
            }
            _ => {}
        }
    }

    if name_start < path.len() {
        let name = path
            .get(name_start..)
            .ok_or_else(|| storage_types::StorageError::validation("Invalid document path"))?;
        segments.push(ResponsePathSegment::Name(name));
    }
    if segments.is_empty() {
        return Err(storage_types::StorageError::validation(
            "Invalid document path",
        ));
    }
    Ok(segments)
}
