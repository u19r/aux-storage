use std::collections::{BTreeMap, HashMap};

use http_error::HttpApiError;
use storage_condition::{evaluate_condition, parse_condition_expression};
use storage_types::{AttributeMap, AttributeValue};

pub(crate) fn apply_filter_expression_refs<'a>(
    items: &'a [HashMap<String, AttributeValue>],
    filter_expr: &str,
    attribute_names: Option<&HashMap<String, String>>,
    attribute_values: Option<&HashMap<String, AttributeValue>>,
) -> Result<Vec<&'a HashMap<String, AttributeValue>>, HttpApiError> {
    let condition = parse_condition_expression(filter_expr, attribute_names, attribute_values)
        .map_err(filter_expression_error)?;
    Ok(items
        .iter()
        .filter(|item| evaluate_condition(item, &condition))
        .collect())
}

pub(crate) fn apply_projection_expression_refs(
    items: &[&HashMap<String, AttributeValue>],
    projection_expr: &str,
    attribute_names: Option<&HashMap<String, String>>,
) -> Vec<HashMap<String, AttributeValue>> {
    let paths = projection_expr
        .split(',')
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .filter_map(|path| parse_projection_path(path, attribute_names))
        .collect::<Vec<_>>();

    let mut projected_items = Vec::with_capacity(items.len());
    for item in items {
        let mut root = ProjectedValue::Map(HashMap::new());
        for path in &paths {
            if let Some(value) = get_path_value(item, path) {
                insert_projected_value(&mut root, path, value.clone());
            }
        }
        projected_items.push(root.into_attribute_map().unwrap_or_default());
    }
    projected_items
}

pub(crate) fn project_attribute_map(
    item: AttributeMap,
    projection_expr: Option<&str>,
    attributes_to_get: Option<&[String]>,
    attribute_names: Option<&HashMap<String, String>>,
) -> AttributeMap {
    let Some(projection_expr) = projection_expr
        .map(str::to_string)
        .or_else(|| attributes_to_get.map(|attributes| attributes.join(", ")))
    else {
        return item;
    };
    let item = item.into_hashmap();
    let projected = apply_projection_expression_refs(&[&item], &projection_expr, attribute_names);
    projected.into_iter().next().unwrap_or_default().into()
}

fn filter_expression_error(message: String) -> HttpApiError {
    HttpApiError::validation_error(message.replace("ConditionExpression", "FilterExpression"))
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
            b'.' => {
                cursor += 1;
            }
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
                current = list.get(*index)?
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
