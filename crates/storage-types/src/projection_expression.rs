use std::{
    collections::{BTreeMap, HashMap},
    ops::Deref,
};

use smallvec::SmallVec;

use crate::{
    AttributeMap, AttributeValue, AttributeValueLookup, IndexName, ProjectionType, StorageError,
    StorageResult, StoredTableInfo, WireItem,
};

pub fn validate_gsi_projection(
    table_info: &StoredTableInfo,
    index_name: Option<&IndexName>,
    projection_expression: Option<&str>,
    attributes_to_get: Option<&[String]>,
    expression_attribute_names: Option<&crate::VecMap<String, String>>,
) -> StorageResult<()> {
    if !gsi_requires_projection_validation(table_info, index_name) {
        return Ok(());
    }
    let requested = requested_top_level_attributes(
        projection_expression,
        attributes_to_get,
        expression_attribute_names,
    );
    validate_gsi_required_attributes(table_info, index_name, requested.iter().map(String::as_str))
}

fn gsi_requires_projection_validation(
    table_info: &StoredTableInfo,
    index_name: Option<&IndexName>,
) -> bool {
    let Some(index_name) = index_name else {
        return false;
    };
    let Some(index) = table_info
        .global_secondary_indexes
        .as_ref()
        .and_then(|indexes| indexes.iter().find(|index| index.index_name == *index_name))
    else {
        return false;
    };
    !index
        .projection
        .projection_type
        .as_ref()
        .is_none_or(|projection_type| *projection_type == ProjectionType::All)
}

pub fn validate_gsi_required_attributes<'a>(
    table_info: &StoredTableInfo,
    index_name: Option<&IndexName>,
    requested: impl IntoIterator<Item = &'a str>,
) -> StorageResult<()> {
    let Some(index_name) = index_name else {
        return Ok(());
    };
    let Some(index) = table_info
        .global_secondary_indexes
        .as_ref()
        .and_then(|indexes| indexes.iter().find(|index| index.index_name == *index_name))
    else {
        return Ok(());
    };
    if index
        .projection
        .projection_type
        .as_ref()
        .is_none_or(|projection_type| *projection_type == ProjectionType::All)
    {
        return Ok(());
    }

    let mut unprojected = Vec::new();
    for attribute_name in requested {
        if !gsi_projects_attribute(table_info, index, attribute_name)
            && !unprojected.iter().any(|name| name == attribute_name)
        {
            unprojected.push(attribute_name.to_string());
        }
    }

    if unprojected.is_empty() {
        return Ok(());
    }
    Err(StorageError::validation(format!(
        "One or more parameter values were invalid: Global secondary index {index_name} does not \
         project [{}]",
        unprojected.join(", ")
    )))
}

fn requested_top_level_attributes(
    projection_expression: Option<&str>,
    attributes_to_get: Option<&[String]>,
    expression_attribute_names: Option<&crate::VecMap<String, String>>,
) -> Vec<String> {
    if let Some(expression) = projection_expression {
        return expression
            .split(',')
            .map(str::trim)
            .filter(|path| !path.is_empty())
            .filter_map(|path| parse_projection_path(path, expression_attribute_names))
            .filter_map(|path| match path.first() {
                Some(ProjectionSegment::Key(name)) => Some((*name).to_owned()),
                _ => None,
            })
            .collect();
    }
    attributes_to_get.map_or_else(Vec::new, <[String]>::to_vec)
}

fn gsi_projects_attribute(
    table_info: &StoredTableInfo,
    index: &crate::GlobalSecondaryIndex,
    attribute_name: &str,
) -> bool {
    table_info
        .key_schema
        .iter()
        .chain(index.key_schema.iter())
        .any(|key| key.attribute_name == attribute_name)
        || (index.projection.projection_type == Some(ProjectionType::Include)
            && index
                .projection
                .non_key_attributes
                .as_ref()
                .is_some_and(|attributes| attributes.iter().any(|name| name == attribute_name)))
}

pub fn project_wire_items(
    items: Vec<WireItem>,
    projection_expression: Option<&str>,
    expression_attribute_names: Option<&crate::VecMap<String, String>>,
) -> StorageResult<Vec<WireItem>> {
    let Some(projection_expression) = projection_expression else {
        return Ok(items);
    };
    let projection =
        AttributeProjection::from_expression(projection_expression, expression_attribute_names);
    let paths = projection.paths.as_deref().unwrap_or_default();

    items
        .into_iter()
        .map(|item| {
            let item = item.into_attribute_map()?;
            WireItem::try_from(&AttributeProjection::project_paths(&item, paths))
        })
        .collect()
}

#[must_use]
pub fn project_attribute_map(
    item: AttributeMap,
    projection_expression: Option<&str>,
    attributes_to_get: Option<&[String]>,
    expression_attribute_names: Option<&crate::VecMap<String, String>>,
) -> AttributeMap {
    let Some(projection) = AttributeProjection::new(
        projection_expression,
        attributes_to_get,
        expression_attribute_names,
    ) else {
        return item;
    };
    projection.project(&item)
}

pub struct AttributeProjection<'a> {
    paths: Option<Vec<ProjectionPath<'a>>>,
    attributes: Option<&'a [String]>,
}

impl<'a> AttributeProjection<'a> {
    #[must_use]
    pub fn from_expression(
        expression: &'a str,
        expression_attribute_names: Option<&'a crate::VecMap<String, String>>,
    ) -> Self {
        let paths = expression
            .split(',')
            .map(str::trim)
            .filter(|path| !path.is_empty())
            .filter_map(|path| parse_projection_path(path, expression_attribute_names))
            .collect();
        Self {
            paths: Some(paths),
            attributes: None,
        }
    }

    #[must_use]
    pub const fn from_attributes(attributes: &'a [String]) -> Self {
        Self {
            paths: None,
            attributes: Some(attributes),
        }
    }

    #[must_use]
    pub fn new(
        projection_expression: Option<&'a str>,
        attributes_to_get: Option<&'a [String]>,
        expression_attribute_names: Option<&'a crate::VecMap<String, String>>,
    ) -> Option<Self> {
        if let Some(expression) = projection_expression {
            return Some(Self::from_expression(
                expression,
                expression_attribute_names,
            ));
        }
        attributes_to_get.map(Self::from_attributes)
    }

    #[must_use]
    pub fn project<Item>(&self, item: &Item) -> AttributeMap
    where Item: AttributeValueLookup + ?Sized {
        if let Some(paths) = self.paths.as_deref() {
            return Self::project_paths(item, paths);
        }
        let attributes = self.attributes.unwrap_or_default();
        let mut projected = AttributeMap::with_capacity(attributes.len());
        for name in attributes {
            if let Some(value) = item.get_attribute_value(name) {
                projected.insert(name.clone(), value.clone());
            }
        }
        projected
    }

    fn project_paths(
        item: &(impl AttributeValueLookup + ?Sized),
        paths: &[ProjectionPath<'_>],
    ) -> AttributeMap {
        let mut root = ProjectedValue::Map(HashMap::new());
        for path in paths {
            if let Some(value) = get_path_value(item, path) {
                insert_projected_value(&mut root, path, value.clone());
            }
        }
        root.into_attribute_map().unwrap_or_default()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProjectionSegment<'a> {
    Key(&'a str),
    Index(usize),
}

/// A parsed document path that borrows names from its expression and aliases.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectionPath<'a>(SmallVec<[ProjectionSegment<'a>; 2]>);

impl<'a> Deref for ProjectionPath<'a> {
    type Target = [ProjectionSegment<'a>];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub fn parse_projection_path<'a>(
    path: &'a str,
    attribute_names: Option<&'a crate::VecMap<String, String>>,
) -> Option<ProjectionPath<'a>> {
    let mut segments = SmallVec::new();
    let bytes = path.as_bytes();
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        match bytes[cursor] {
            b'.' => cursor += 1,
            b'[' => {
                cursor += 1;
                let end = bytes.get(cursor..)?.iter().position(|byte| *byte == b']')? + cursor;
                let index = path.get(cursor..end)?.parse().ok()?;
                segments.push(ProjectionSegment::Index(index));
                cursor = end + 1;
            }
            _ => {
                let end = bytes[cursor..]
                    .iter()
                    .position(|byte| matches!(*byte, b'.' | b'['))
                    .map_or(path.len(), |offset| cursor + offset);
                let raw = path.get(cursor..end)?;
                let key = attribute_names
                    .and_then(|names| names.get(raw))
                    .map_or(raw, String::as_str);
                segments.push(ProjectionSegment::Key(key));
                cursor = end;
            }
        }
    }
    Some(ProjectionPath(segments))
}

fn get_path_value<'a>(
    item: &'a (impl AttributeValueLookup + ?Sized),
    path: &[ProjectionSegment<'_>],
) -> Option<&'a AttributeValue> {
    let (first, rest) = path.split_first()?;
    let ProjectionSegment::Key(key) = first else {
        return None;
    };
    let mut current = item.get_attribute_value(key)?;
    for segment in rest {
        match (segment, current) {
            (ProjectionSegment::Key(key), AttributeValue::M(map)) => current = map.get(*key)?,
            (ProjectionSegment::Index(index), AttributeValue::L(list)) => {
                current = list.get(*index)?;
            }
            _ => return None,
        }
    }
    Some(current)
}

enum ProjectedValue {
    Leaf(AttributeValue),
    Map(HashMap<String, ProjectedValue>),
    List(BTreeMap<usize, ProjectedValue>),
}

impl ProjectedValue {
    fn into_attribute_value(self) -> AttributeValue {
        match self {
            Self::Leaf(value) => value,
            Self::Map(map) => AttributeValue::M(
                map.into_iter()
                    .map(|(key, value)| (key, value.into_attribute_value()))
                    .collect(),
            ),
            Self::List(values) => AttributeValue::L(
                values
                    .into_values()
                    .map(ProjectedValue::into_attribute_value)
                    .collect(),
            ),
        }
    }

    fn into_attribute_map(self) -> Option<AttributeMap> {
        let Self::Map(map) = self else {
            return None;
        };
        Some(
            map.into_iter()
                .map(|(key, value)| (key, value.into_attribute_value()))
                .collect(),
        )
    }
}

fn insert_projected_value(
    target: &mut ProjectedValue,
    path: &[ProjectionSegment<'_>],
    value: AttributeValue,
) {
    let Some((segment, rest)) = path.split_first() else {
        *target = ProjectedValue::Leaf(value);
        return;
    };

    match segment {
        ProjectionSegment::Key(key) => {
            if !matches!(target, ProjectedValue::Map(_)) {
                *target = ProjectedValue::Map(HashMap::new());
            }
            let ProjectedValue::Map(map) = target else {
                return;
            };
            let child = map
                .entry((*key).to_owned())
                .or_insert_with(|| next_projected_value(rest));
            insert_projected_value(child, rest, value);
        }
        ProjectionSegment::Index(index) => {
            if !matches!(target, ProjectedValue::List(_)) {
                *target = ProjectedValue::List(BTreeMap::new());
            }
            let ProjectedValue::List(list) = target else {
                return;
            };
            let child = list
                .entry(*index)
                .or_insert_with(|| next_projected_value(rest));
            insert_projected_value(child, rest, value);
        }
    }
}

fn next_projected_value(rest: &[ProjectionSegment<'_>]) -> ProjectedValue {
    match rest.first() {
        Some(ProjectionSegment::Index(_)) => ProjectedValue::List(BTreeMap::new()),
        _ => ProjectedValue::Map(HashMap::new()),
    }
}
