use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use storage_types::{
    AttributeMap, AttributeValue, AttributeValueLookup, ReturnValuesOldNewUpdated, StorageResult,
    UpdateItemRequest,
    condition::{AttributePath, PathElement},
};

use crate::update_logic::{
    logic::{BoundUpdateOperation, UpdateOperation},
    path::get_attribute_value,
};

pub trait UpdateFieldName {
    fn field_path(&self) -> &AttributePath;
}

/// Bind the updated-field paths used by `ReturnValues=UPDATED_*` responses.
///
/// This runs the canonical update-expression parser so every provider shapes
/// the response from the same bound paths and reports the same parse errors.
pub fn update_item_response_fields(
    request: &UpdateItemRequest,
) -> StorageResult<Vec<Arc<AttributePath>>> {
    let (operations, _) = super::logic::before_update_item_optional(
        request.update_expression.as_deref(),
        None,
        request.expression_attribute_names.as_ref(),
        request.expression_attribute_values.as_ref(),
    )?;
    Ok(operations
        .iter()
        .map(BoundUpdateOperation::field_path_arc)
        .collect())
}

impl UpdateFieldName for UpdateOperation {
    fn field_path(&self) -> &AttributePath {
        UpdateOperation::field_path(self)
    }
}

impl UpdateFieldName for BoundUpdateOperation<'_> {
    fn field_path(&self) -> &AttributePath {
        BoundUpdateOperation::field_path(self)
    }
}

impl UpdateFieldName for Arc<AttributePath> {
    fn field_path(&self) -> &AttributePath {
        self
    }
}

pub(crate) fn updated_attribute_map<T: UpdateFieldName>(
    operations: &[T],
    mut item: AttributeMap,
) -> Option<AttributeMap> {
    if operations
        .iter()
        .all(|op| op.field_path().top_level().is_some())
    {
        let mut attributes = AttributeMap::with_capacity(operations.len());
        for operation in operations {
            let name = operation.field_path().root();
            if let Some(value) = item.remove(name) {
                attributes.insert(name, value);
            }
        }
        return (!attributes.is_empty()).then_some(attributes);
    }
    let attributes = updated_attributes_for_response(operations, &item);
    (!attributes.is_empty()).then(|| attributes.into())
}

/// `UPDATED_OLD`: clone only the updated paths of the old image.
pub(crate) fn updated_attribute_map_borrowed<T: UpdateFieldName>(
    operations: &[T],
    item: &AttributeMap,
) -> Option<AttributeMap> {
    if operations
        .iter()
        .all(|op| op.field_path().top_level().is_some())
    {
        let mut attributes = AttributeMap::with_capacity(operations.len());
        for operation in operations {
            let name = operation.field_path().root();
            if let Some(value) = item.get(name) {
                attributes.insert(name, value.clone());
            }
        }
        return (!attributes.is_empty()).then_some(attributes);
    }
    let attributes = updated_attributes_for_response(operations, item);
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

pub fn updated_attributes_for_response<T: UpdateFieldName>(
    operations: &[T],
    item: &impl AttributeValueLookup,
) -> HashMap<String, AttributeValue> {
    // The common form is a flat SET/ADD/REMOVE response.  Avoid building a
    // temporary projection tree when no operation addresses a document path;
    // nested paths still use the merge logic below because a root operation
    // and a child operation must retain their order-dependent shape.
    if operations
        .iter()
        .all(|operation| operation.field_path().top_level().is_some())
    {
        let mut attributes = HashMap::with_capacity(operations.len());
        for operation in operations {
            let name = operation.field_path().root();
            if let Some(value) = item.get_attribute_value(name) {
                attributes.insert(name.to_string(), value.clone());
            }
        }
        return attributes;
    }

    let mut projection = UpdatedAttributeProjection::default();
    for op in operations {
        let path = op.field_path();
        if let Some(field_value) = get_attribute_value(item, path) {
            projection.insert(path, field_value.clone());
        }
    }
    projection.into_attributes()
}

#[derive(Default)]
struct UpdatedAttributeProjection {
    roots: HashMap<String, ProjectedAttributeValue>,
}

impl UpdatedAttributeProjection {
    fn insert(&mut self, path: &AttributePath, value: AttributeValue) {
        let Some((PathElement::Name(root), rest)) = path.elements().split_first() else {
            return;
        };
        insert_projected_path(self.roots.entry(root.clone()).or_default(), rest, value);
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
            ProjectedAttributeValue::Map(map) => {
                AttributeValue::M(storage_types::AttributeMap::from_distinct(
                    map.into_iter()
                        .map(|(name, value)| (name, value.into_attribute_value())),
                ))
            }
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
    path: &[PathElement],
    value: AttributeValue,
) {
    let Some((segment, rest)) = path.split_first() else {
        *target = ProjectedAttributeValue::Leaf(value);
        return;
    };
    match segment {
        PathElement::Name(name) => {
            if !matches!(target, ProjectedAttributeValue::Map(_)) {
                *target = ProjectedAttributeValue::Map(HashMap::new());
            }
            let ProjectedAttributeValue::Map(map) = target else {
                unreachable!();
            };
            insert_projected_path(map.entry(name.clone()).or_default(), rest, value);
        }
        PathElement::Index(index) => {
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
