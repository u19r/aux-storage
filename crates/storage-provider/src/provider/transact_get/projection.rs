//! Single-item TransactGet projection with borrowed request paths.

use std::collections::HashMap;

use storage_types::{
    AttributeMap, AttributeValue, ProjectionPath, ProjectionSegment, VecMap, parse_projection_path,
};

pub(super) struct TransactGetProjection;

impl TransactGetProjection {
    pub(super) fn project_item(
        item: &AttributeMap,
        projection_expr: &str,
        expression_attribute_names: Option<&storage_types::VecMap<String, String>>,
    ) -> AttributeMap {
        let path_count = projection_expr
            .as_bytes()
            .iter()
            .filter(|byte| **byte == b',')
            .count()
            + 1;
        let mut paths = Vec::with_capacity(path_count);
        for path in projection_expr.split(',').map(str::trim) {
            if path.is_empty() {
                continue;
            }
            if let Some(path) = parse_projection_path(path, expression_attribute_names) {
                paths.push(path);
            }
        }
        Self::project_item_with_paths(item, &paths)
    }

    fn project_item_with_paths(item: &AttributeMap, paths: &[ProjectionPath<'_>]) -> AttributeMap {
        let mut projected = VecMap::with_capacity(paths.len());
        for path in paths {
            let Some((ProjectionSegment::Key(key), tail)) = path.split_first() else {
                continue;
            };
            let Some(value) = Self::get_projection_path_value(item, path) else {
                continue;
            };
            if tail.is_empty() {
                projected.insert(*key, value.clone());
                continue;
            }
            let child = projected.get_or_insert_with(*key, || match tail.first() {
                Some(ProjectionSegment::Index(_)) => AttributeValue::L(Vec::new()),
                _ => AttributeValue::M(HashMap::new()),
            });
            Self::insert_projected_attribute_value(child, tail, value.clone());
        }
        projected.into()
    }

    fn get_projection_path_value<'a>(
        item: &'a AttributeMap,
        path: &[ProjectionSegment<'_>],
    ) -> Option<&'a AttributeValue> {
        let (first, rest) = path.split_first()?;
        let ProjectionSegment::Key(first_key) = first else {
            return None;
        };
        let mut current = item.get(first_key)?;
        for segment in rest {
            match (segment, current) {
                (ProjectionSegment::Key(key), AttributeValue::M(map)) => current = map.get(*key)?,
                (ProjectionSegment::Index(index), AttributeValue::L(list)) => {
                    current = list.get(*index)?
                }
                _ => return None,
            }
        }
        Some(current)
    }

    fn insert_projected_value(
        target: &mut ProjectedValue,
        path: &[ProjectionSegment<'_>],
        value: AttributeValue,
    ) {
        let Some((head, tail)) = path.split_first() else {
            return;
        };
        match (target, head) {
            (ProjectedValue::Map(map), ProjectionSegment::Key(key)) if tail.is_empty() => {
                map.insert((*key).to_owned(), value);
            }
            (ProjectedValue::Map(map), ProjectionSegment::Key(key)) => {
                let child = map
                    .entry((*key).to_owned())
                    .or_insert_with(|| match tail.first() {
                        Some(ProjectionSegment::Index(_)) => AttributeValue::L(Vec::new()),
                        _ => AttributeValue::M(HashMap::new()),
                    });
                Self::insert_projected_attribute_value(child, tail, value);
            }
            (ProjectedValue::List(list), ProjectionSegment::Index(index)) => {
                if list.len() <= *index {
                    list.resize_with(index + 1, || None);
                }
                if tail.is_empty() {
                    list[*index] = Some(value);
                } else {
                    let child = list[*index].get_or_insert_with(|| match tail.first() {
                        Some(ProjectionSegment::Index(_)) => AttributeValue::L(Vec::new()),
                        _ => AttributeValue::M(HashMap::new()),
                    });
                    Self::insert_projected_attribute_value(child, tail, value);
                }
            }
            _ => {}
        }
    }

    fn insert_projected_attribute_value(
        target: &mut AttributeValue,
        path: &[ProjectionSegment<'_>],
        value: AttributeValue,
    ) {
        match target {
            AttributeValue::M(map) => {
                let mut projected = ProjectedValue::Map(std::mem::take(map));
                Self::insert_projected_value(&mut projected, path, value);
                if let ProjectedValue::Map(updated) = projected {
                    *map = updated;
                }
            }
            AttributeValue::L(list) => {
                let mut projected = ProjectedValue::List(
                    std::mem::take(list)
                        .into_iter()
                        .map(Some)
                        .collect::<Vec<_>>(),
                );
                Self::insert_projected_value(&mut projected, path, value);
                if let ProjectedValue::List(updated) = projected {
                    *list = updated.into_iter().flatten().collect();
                }
            }
            _ => {}
        }
    }
}

enum ProjectedValue {
    // Nested values retain the AttributeValue::M representation.
    Map(HashMap<String, AttributeValue>),
    List(Vec<Option<AttributeValue>>),
}
