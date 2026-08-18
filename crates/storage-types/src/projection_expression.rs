use std::collections::{BTreeMap, HashMap};

use crate::{
    AttributeMap, AttributeValue, IndexName, ProjectionType, StorageError, StorageResult,
    StoredTableInfo, WireItem,
};

pub fn validate_gsi_projection(
    table_info: &StoredTableInfo,
    index_name: Option<&IndexName>,
    projection_expression: Option<&str>,
    attributes_to_get: Option<&[String]>,
    expression_attribute_names: Option<&HashMap<String, String>>,
) -> StorageResult<()> {
    let requested = requested_top_level_attributes(
        projection_expression,
        attributes_to_get,
        expression_attribute_names,
    );
    validate_gsi_required_attributes(table_info, index_name, requested.iter().map(String::as_str))
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
    expression_attribute_names: Option<&HashMap<String, String>>,
) -> Vec<String> {
    if let Some(expression) = projection_expression {
        return expression
            .split(',')
            .map(str::trim)
            .filter(|path| !path.is_empty())
            .filter_map(|path| parse_projection_path(path, expression_attribute_names))
            .filter_map(|path| match path.into_iter().next() {
                Some(ProjectionSegment::Key(name)) => Some(name),
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
    expression_attribute_names: Option<&HashMap<String, String>>,
) -> StorageResult<Vec<WireItem>> {
    let Some(projection_expression) = projection_expression else {
        return Ok(items);
    };
    let paths = projection_expression
        .split(',')
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .filter_map(|path| parse_projection_path(path, expression_attribute_names))
        .collect::<Vec<_>>();

    items
        .into_iter()
        .map(|item| {
            let item = item.into_attribute_map()?;
            WireItem::from_attribute_map(&project_hash_map(&item, &paths))
        })
        .collect()
}

#[must_use]
pub fn project_attribute_map(
    item: AttributeMap,
    projection_expression: Option<&str>,
    attributes_to_get: Option<&[String]>,
    expression_attribute_names: Option<&HashMap<String, String>>,
) -> AttributeMap {
    let Some(projection) = AttributeProjection::new(
        projection_expression,
        attributes_to_get,
        expression_attribute_names,
    ) else {
        return item;
    };
    projection.project(&item.into_hashmap())
}

#[must_use]
pub fn project_attribute_map_ref(
    item: &HashMap<String, AttributeValue>,
    projection_expression: Option<&str>,
    attributes_to_get: Option<&[String]>,
    expression_attribute_names: Option<&HashMap<String, String>>,
) -> AttributeMap {
    AttributeProjection::new(
        projection_expression,
        attributes_to_get,
        expression_attribute_names,
    )
    .map_or_else(
        || item.clone().into(),
        |projection| projection.project(item),
    )
}

pub struct AttributeProjection<'a> {
    paths: Option<Vec<Vec<ProjectionSegment>>>,
    attributes: Option<&'a [String]>,
}

impl<'a> AttributeProjection<'a> {
    #[must_use]
    pub fn from_expression(
        expression: &str,
        expression_attribute_names: Option<&HashMap<String, String>>,
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
        projection_expression: Option<&str>,
        attributes_to_get: Option<&'a [String]>,
        expression_attribute_names: Option<&HashMap<String, String>>,
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
    pub fn project(&self, item: &HashMap<String, AttributeValue>) -> AttributeMap {
        if let Some(paths) = self.paths.as_deref() {
            return project_hash_map(item, paths).into();
        }
        let attributes = self.attributes.unwrap_or_default();
        let mut projected = HashMap::with_capacity(attributes.len());
        for name in attributes {
            if let Some(value) = item.get(name) {
                projected.insert(name.clone(), value.clone());
            }
        }
        projected.into()
    }
}

fn project_hash_map(
    item: &HashMap<String, AttributeValue>,
    paths: &[Vec<ProjectionSegment>],
) -> HashMap<String, AttributeValue> {
    let mut root = ProjectedValue::Map(HashMap::new());
    for path in paths {
        if let Some(value) = get_path_value(item, path) {
            insert_projected_value(&mut root, path, value.clone());
        }
    }
    root.into_attribute_map().unwrap_or_default()
}

#[derive(Clone)]
enum ProjectionSegment {
    Key(String),
    Index(usize),
}

fn parse_projection_path(
    path: &str,
    attribute_names: Option<&HashMap<String, String>>,
) -> Option<Vec<ProjectionSegment>> {
    let mut segments = Vec::new();
    let mut cursor = 0usize;
    while cursor < path.len() {
        let bytes = path.as_bytes();
        match bytes.get(cursor).copied()? {
            b'.' => cursor += 1,
            b'[' => {
                cursor += 1;
                let end = path.get(cursor..)?.find(']')? + cursor;
                let index = path.get(cursor..end)?.parse().ok()?;
                segments.push(ProjectionSegment::Index(index));
                cursor = end + 1;
            }
            _ => {
                let end = path
                    .get(cursor..)?
                    .find(['.', '['])
                    .map_or(path.len(), |offset| cursor + offset);
                let raw = path.get(cursor..end)?;
                let key = attribute_names
                    .and_then(|names| names.get(raw))
                    .map_or_else(|| raw.to_string(), Clone::clone);
                segments.push(ProjectionSegment::Key(key));
                cursor = end;
            }
        }
    }
    Some(segments)
}

fn get_path_value<'a>(
    item: &'a HashMap<String, AttributeValue>,
    path: &[ProjectionSegment],
) -> Option<&'a AttributeValue> {
    let (first, rest) = path.split_first()?;
    let ProjectionSegment::Key(key) = first else {
        return None;
    };
    let mut current = item.get(key)?;
    for segment in rest {
        match (segment, current) {
            (ProjectionSegment::Key(key), AttributeValue::M(map)) => current = map.get(key)?,
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

    fn into_attribute_map(self) -> Option<HashMap<String, AttributeValue>> {
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
    path: &[ProjectionSegment],
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
                .entry(key.clone())
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

fn next_projected_value(rest: &[ProjectionSegment]) -> ProjectedValue {
    match rest.first() {
        Some(ProjectionSegment::Index(_)) => ProjectedValue::List(BTreeMap::new()),
        _ => ProjectedValue::Map(HashMap::new()),
    }
}
