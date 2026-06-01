use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use storage_types::{AttributeValue, ReturnValuesOldNewUpdated, StorageResult, UpdateItemResponse};

use crate::update_logic::{
    logic::{BoundUpdateOperation, UpdateOperation},
    path::get_attribute_value,
};

pub trait UpdateFieldName {
    fn field_name(&self) -> &str;
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
                old_item.and_then(|item| non_empty_updated_attributes(operations, &item))
            }
            ReturnValuesOldNewUpdated::UpdatedNew => {
                new_item.and_then(|item| non_empty_updated_attributes(operations, &item))
            }
        },
    };

    Ok(UpdateItemResponse {
        attributes: attributes.map(Into::into),
    })
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

pub fn updated_attributes_for_response<T: UpdateFieldName>(
    operations: &[T],
    item: &HashMap<String, AttributeValue>,
) -> HashMap<String, AttributeValue> {
    let mut projection = UpdatedAttributeProjection::default();
    for op in operations {
        let field_name = op.field_name();
        if let Some(field_value) = get_attribute_value(item, field_name) {
            projection.insert(field_name, field_value.clone());
        }
    }
    projection.into_attributes()
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
        let projected_value = projected_value_from_path(rest, value);
        merge_projected_value(
            self.roots.entry((*root).to_string()).or_default(),
            projected_value,
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

fn projected_value_from_path(
    path: &[ResponsePathSegment<'_>],
    value: AttributeValue,
) -> ProjectedAttributeValue {
    let Some((segment, rest)) = path.split_first() else {
        return ProjectedAttributeValue::Leaf(value);
    };
    match segment {
        ResponsePathSegment::Name(name) => {
            let mut map = HashMap::with_capacity(1);
            map.insert((*name).to_string(), projected_value_from_path(rest, value));
            ProjectedAttributeValue::Map(map)
        }
        ResponsePathSegment::Index(index) => {
            let mut list = BTreeMap::new();
            list.insert(*index, projected_value_from_path(rest, value));
            ProjectedAttributeValue::List(list)
        }
    }
}

fn merge_projected_value(target: &mut ProjectedAttributeValue, incoming: ProjectedAttributeValue) {
    match (target, incoming) {
        (target @ ProjectedAttributeValue::Empty, incoming) => {
            *target = incoming;
        }
        (target @ ProjectedAttributeValue::Leaf(_), incoming) => {
            *target = incoming;
        }
        (ProjectedAttributeValue::Map(target_map), ProjectedAttributeValue::Map(incoming_map)) => {
            for (name, incoming_value) in incoming_map {
                merge_projected_value(target_map.entry(name).or_default(), incoming_value);
            }
        }
        (
            ProjectedAttributeValue::List(target_list),
            ProjectedAttributeValue::List(incoming_list),
        ) => {
            for (index, incoming_value) in incoming_list {
                merge_projected_value(target_list.entry(index).or_default(), incoming_value);
            }
        }
        (target, incoming) => {
            *target = incoming;
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
