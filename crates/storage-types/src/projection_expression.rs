use std::collections::{BTreeMap, HashMap};

use crate::{
    AttributeValue, IndexName, ProjectionType, StorageError, StorageResult, StoredTableInfo,
    WireItem,
};

pub fn validate_gsi_projection_expression(
    table_info: &StoredTableInfo,
    index_name: Option<&IndexName>,
    projection_expression: Option<&str>,
    expression_attribute_names: Option<&HashMap<String, String>>,
) -> StorageResult<()> {
    let (Some(index_name), Some(projection_expression)) = (index_name, projection_expression)
    else {
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
    for path in projection_expression
        .split(',')
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .filter_map(|path| parse_projection_path(path, expression_attribute_names))
    {
        let Some(ProjectionSegment::Key(attribute_name)) = path.first() else {
            continue;
        };
        let is_key = table_info
            .key_schema
            .iter()
            .chain(index.key_schema.iter())
            .any(|key| key.attribute_name == *attribute_name);
        let is_included = index.projection.projection_type == Some(ProjectionType::Include)
            && index
                .projection
                .non_key_attributes
                .as_ref()
                .is_some_and(|attributes| attributes.contains(attribute_name));
        if !is_key && !is_included && !unprojected.contains(attribute_name) {
            unprojected.push(attribute_name.clone());
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
            let mut root = ProjectedValue::Map(HashMap::new());
            for path in &paths {
                if let Some(value) = get_path_value(&item, path) {
                    insert_projected_value(&mut root, path, value.clone());
                }
            }
            WireItem::from_attribute_map(&root.into_attribute_map().unwrap_or_default())
        })
        .collect()
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
