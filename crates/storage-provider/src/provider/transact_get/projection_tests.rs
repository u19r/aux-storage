//! Moved TransactGet single-item projection allocation witness.
//!
//! Relocated from `storage::database_manager::read_ops_projection_perf_tests`
//! with the projection implementation: the optimized entry below must keep
//! beating the baseline on allocation count and bytes. The runtime probe
//! stays explicitly invoked, never an ordinary gate.

use std::{
    collections::HashMap,
    process::Command,
    time::{Duration, Instant},
};

use alloc_counter::AllocationGuard;
use storage_types::AttributeValue;

use super::projection::TransactGetProjection;

const ITEM_COUNT: usize = 40;
const ITERATIONS: usize = 1_000;

type SingleItemProjectionFn = fn(
    &storage_types::AttributeMap,
    &str,
    Option<&storage_types::VecMap<String, String>>,
) -> storage_types::AttributeMap;

fn realistic_items() -> Vec<storage_types::AttributeMap> {
    (0..ITEM_COUNT)
        .map(|index| {
            let mut item = storage_types::AttributeMap::with_capacity(14);
            item.insert(
                "pk".to_string(),
                AttributeValue::S(realistic_key("tenant", index)),
            );
            item.insert(
                "sk".to_string(),
                AttributeValue::S(realistic_key("item", index)),
            );
            item.insert(
                "gsi1pk".to_string(),
                AttributeValue::S(realistic_key("account", index)),
            );
            item.insert(
                "gsi1sk".to_string(),
                AttributeValue::S(realistic_key("created", index)),
            );
            item.insert(
                "gsi2pk".to_string(),
                AttributeValue::S(realistic_key("status", index)),
            );
            item.insert(
                "gsi2sk".to_string(),
                AttributeValue::S(realistic_key("bucket", index)),
            );
            item.insert(
                "ttl".to_string(),
                AttributeValue::N((1_900_000_000 + index as u64).to_string()),
            );
            for attr_index in 0..7 {
                item.insert(
                    format!("attr_{attr_index}"),
                    AttributeValue::S(realistic_value(index, attr_index)),
                );
            }
            item.insert("meta".to_string(), realistic_nested_value(index));
            item
        })
        .collect()
}

fn realistic_key(prefix: &str, index: usize) -> String {
    format!("{prefix}#{index:04}#{}", "k".repeat(90))
}

fn realistic_value(item_index: usize, attr_index: usize) -> String {
    let target_len = 800 + ((item_index + attr_index) % 8) * 100;
    format!(
        "value#{item_index:04}#{attr_index:02}#{}",
        "v".repeat(target_len)
    )
}

fn realistic_nested_value(index: usize) -> AttributeValue {
    let mut map = HashMap::with_capacity(3);
    map.insert(
        "child".to_string(),
        AttributeValue::S(format!("nested#{index}#{}", "n".repeat(900))),
    );
    map.insert("count".to_string(), AttributeValue::N(index.to_string()));
    map.insert(
        "events".to_string(),
        AttributeValue::L(vec![
            event_value(index, "created"),
            event_value(index, "updated"),
            event_value(index, "expired"),
        ]),
    );
    AttributeValue::M(map)
}

fn event_value(index: usize, event_type: &str) -> AttributeValue {
    let mut map = HashMap::with_capacity(2);
    map.insert(
        "name".to_string(),
        AttributeValue::S(event_type.to_string()),
    );
    map.insert(
        "payload".to_string(),
        AttributeValue::S(realistic_value(index, 0)),
    );
    AttributeValue::M(map)
}

fn projection_expr() -> &'static str {
    "pk, sk, ttl, gsi1pk, gsi1sk, gsi2pk, gsi2sk, #meta.child, #meta.count, #meta.events[1].name"
}

fn expression_attribute_names() -> storage_types::VecMap<String, String> {
    storage_types::VecMap::from_iter([("#meta".to_string(), "meta".to_string())])
}

fn measure_single_item_projection_allocations(
    project: SingleItemProjectionFn,
) -> alloc_counter::AllocationReport<'static> {
    let items = realistic_items();
    let names = expression_attribute_names();
    let guard = AllocationGuard::start(
        module_path!(),
        "transact_get_single_item_projection_allocation_profile_tests",
        file!(),
        line!(),
        None,
    );

    for _ in 0..ITERATIONS {
        for item in &items[..25] {
            let projected = project(item, projection_expr(), Some(&names));
            assert!(!projected.is_empty());
        }
    }

    guard.finish()
}

fn measure_single_item_projection_runtime(project: SingleItemProjectionFn) -> Duration {
    let items = realistic_items();
    let names = expression_attribute_names();
    let started = Instant::now();
    for _ in 0..ITERATIONS {
        for item in &items[..25] {
            let projected = project(item, projection_expr(), Some(&names));
            assert!(!projected.is_empty());
        }
    }
    started.elapsed()
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn transact_get_single_item_projection_allocation_profile_tests() {
    const ISOLATED_ENV: &str = "AUX_STORAGE_TRANSACT_GET_PROJECTION_ISOLATED";
    if std::env::var_os(ISOLATED_ENV).is_none() {
        let status = Command::new(
            std::env::current_exe()
                .unwrap(),
        )
        .arg("--exact")
        .arg(
            "provider::transact_get::projection_tests::transact_get_single_item_projection_allocation_profile_tests",
        )
        .arg("--nocapture")
        .arg("--ignored")
        .env(ISOLATED_ENV, "1")
        .status()
        .unwrap();
        assert!(
            status.success(),
            "isolated projection allocation test failed"
        );
        return;
    }

    let baseline = measure_single_item_projection_allocations(baseline_single_item_projection);
    let optimized = measure_single_item_projection_allocations(TransactGetProjection::project_item);
    let _ = alloc_counter::emit_report(&baseline);
    let _ = alloc_counter::emit_report(&optimized);
    assert!(optimized.allocation_count < baseline.allocation_count);
    assert!(optimized.allocated_bytes < baseline.allocated_bytes);
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn transact_get_single_item_projection_runtime_perf_probe() {
    let baseline = measure_single_item_projection_runtime(baseline_single_item_projection);
    let optimized = measure_single_item_projection_runtime(TransactGetProjection::project_item);
    println!(
        "baseline_single_item_projection iterations={ITERATIONS} items_per_iter=25 \
         elapsed_ms={:.3} ns_per_iter={:.2}",
        baseline.as_secs_f64() * 1_000.0,
        baseline.as_nanos() as f64 / ITERATIONS as f64
    );
    println!(
        "optimized_single_item_projection iterations={ITERATIONS} items_per_iter=25 \
         elapsed_ms={:.3} ns_per_iter={:.2}",
        optimized.as_secs_f64() * 1_000.0,
        optimized.as_nanos() as f64 / ITERATIONS as f64
    );
    assert!(optimized.as_nanos() > 0);
}

fn baseline_storage_api_projection(
    items: &[storage_types::AttributeMap],
    projection_expr: &str,
    expression_attribute_names: Option<&storage_types::VecMap<String, String>>,
) -> Vec<HashMap<String, AttributeValue>> {
    let paths = projection_expr
        .split(',')
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .filter_map(|path| baseline_parse_projection_path(path, expression_attribute_names))
        .collect::<Vec<_>>();

    let mut projected_items = Vec::with_capacity(items.len());
    for item in items {
        let mut projected_item = BaselineProjectedValue::Map(HashMap::new());
        for path in &paths {
            if let Some(value) = baseline_get_projection_path_value(item, path) {
                baseline_insert_projected_value(&mut projected_item, path, value.clone());
            }
        }
        projected_items.push(projected_item.into_attribute_map().unwrap_or_default());
    }
    projected_items
}

fn baseline_single_item_projection(
    item: &storage_types::AttributeMap,
    projection_expr: &str,
    expression_attribute_names: Option<&storage_types::VecMap<String, String>>,
) -> storage_types::AttributeMap {
    baseline_storage_api_projection(
        std::slice::from_ref(item),
        projection_expr,
        expression_attribute_names,
    )
    .into_iter()
    .next()
    .unwrap_or_default()
    .into()
}

#[derive(Clone)]
enum BaselineProjectionSegment {
    Key(String),
    Index(usize),
}

enum BaselineProjectedValue {
    Map(HashMap<String, AttributeValue>),
    List(Vec<Option<AttributeValue>>),
}

fn baseline_parse_projection_path(
    path: &str,
    attribute_names: Option<&storage_types::VecMap<String, String>>,
) -> Option<Vec<BaselineProjectionSegment>> {
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
                segments.push(BaselineProjectionSegment::Index(index));
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
                segments.push(BaselineProjectionSegment::Key(key));
                cursor = end;
            }
        }
    }
    Some(segments)
}

fn baseline_get_projection_path_value<'a>(
    item: &'a storage_types::AttributeMap,
    path: &[BaselineProjectionSegment],
) -> Option<&'a AttributeValue> {
    let (first, rest) = path.split_first()?;
    let BaselineProjectionSegment::Key(first_key) = first else {
        return None;
    };
    let mut current = item.get(first_key)?;
    for segment in rest {
        match (segment, current) {
            (BaselineProjectionSegment::Key(key), AttributeValue::M(map)) => {
                current = map.get(key)?;
            }
            (BaselineProjectionSegment::Index(index), AttributeValue::L(list)) => {
                current = list.get(*index)?;
            }
            _ => return None,
        }
    }
    Some(current)
}

fn baseline_insert_projected_value(
    target: &mut BaselineProjectedValue,
    path: &[BaselineProjectionSegment],
    value: AttributeValue,
) {
    let Some((head, tail)) = path.split_first() else {
        return;
    };
    match (target, head) {
        (BaselineProjectedValue::Map(map), BaselineProjectionSegment::Key(key))
            if tail.is_empty() =>
        {
            map.insert(key.clone(), value);
        }
        (BaselineProjectedValue::Map(map), BaselineProjectionSegment::Key(key)) => {
            let child = map
                .entry(key.clone())
                .or_insert_with(|| match tail.first() {
                    Some(BaselineProjectionSegment::Index(_)) => AttributeValue::L(Vec::new()),
                    _ => AttributeValue::M(HashMap::new()),
                });
            baseline_insert_projected_attribute_value(child, tail, value);
        }
        (BaselineProjectedValue::List(list), BaselineProjectionSegment::Index(index)) => {
            if list.len() <= *index {
                list.resize_with(index + 1, || None);
            }
            if tail.is_empty() {
                list[*index] = Some(value);
            } else {
                let child = list[*index].get_or_insert_with(|| match tail.first() {
                    Some(BaselineProjectionSegment::Index(_)) => AttributeValue::L(Vec::new()),
                    _ => AttributeValue::M(HashMap::new()),
                });
                baseline_insert_projected_attribute_value(child, tail, value);
            }
        }
        _ => {}
    }
}

fn baseline_insert_projected_attribute_value(
    target: &mut AttributeValue,
    path: &[BaselineProjectionSegment],
    value: AttributeValue,
) {
    match target {
        AttributeValue::M(map) => {
            let mut projected = BaselineProjectedValue::Map(std::mem::take(map));
            baseline_insert_projected_value(&mut projected, path, value);
            if let BaselineProjectedValue::Map(updated) = projected {
                *map = updated;
            }
        }
        AttributeValue::L(list) => {
            let mut projected = BaselineProjectedValue::List(
                std::mem::take(list)
                    .into_iter()
                    .map(Some)
                    .collect::<Vec<_>>(),
            );
            baseline_insert_projected_value(&mut projected, path, value);
            if let BaselineProjectedValue::List(updated) = projected {
                *list = updated.into_iter().flatten().collect();
            }
        }
        _ => {}
    }
}

impl BaselineProjectedValue {
    fn into_attribute_map(self) -> Option<HashMap<String, AttributeValue>> {
        match self {
            Self::Map(map) => Some(map),
            Self::List(_) => None,
        }
    }
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_transact_projection_paths_when_repeated_then_report_cpu_and_allocations() {
    use std::hint::black_box;

    use storage_types::{AttributeProjection, WireItem};

    let items = realistic_items();
    let names = expression_attribute_names();
    for (label, expression) in [
        ("flat", "pk, sk, ttl, gsi1pk, gsi1sk"),
        ("nested", projection_expr()),
        ("missing", "absent, unknown_attribute.child[1]"),
    ] {
        measure_transact_projection_cost(&format!("current_{label}"), || {
            let projected =
                TransactGetProjection::project_item(black_box(&items[0]), expression, Some(&names));
            black_box(WireItem::try_from(projected).unwrap());
        });
        measure_transact_projection_cost(&format!("shared_{label}"), || {
            let projected = AttributeProjection::from_expression(expression, Some(&names))
                .project(black_box(&items[0]));
            black_box(WireItem::try_from(projected).unwrap());
        });
        let mapped_expression = expression
            .replace("ttl", "#ttl")
            .replace(".child", ".#child")
            .replace(".count", ".#count")
            .replace(".events", ".#events")
            .replace(".name", ".#name");
        let request_names = storage_types::VecMap::from_iter(
            [
                ("#meta", "meta"),
                ("#ttl", "ttl"),
                ("#child", "child"),
                ("#count", "count"),
                ("#events", "events"),
                ("#name", "name"),
            ]
            .into_iter()
            .filter(|(alias, _)| mapped_expression.contains(alias))
            .map(|(alias, name)| (alias.to_owned(), name.to_owned())),
        );
        let request = storage_types::TransactGetItemsRequest {
            transact_items: items
                .iter()
                .take(10)
                .map(|item| storage_types::TransactGetItem {
                    get: storage_types::TransactGetRequest {
                        table_name: storage_types::TableName::new("projection-table"),
                        key: storage_types::KeyAttributes::from_iter([
                            ("pk".to_owned(), item.get("pk").unwrap().clone()),
                            ("sk".to_owned(), item.get("sk").unwrap().clone()),
                        ]),
                        projection_expression: Some(mapped_expression.clone()),
                        expression_attribute_names: (!request_names.is_empty())
                            .then(|| request_names.clone()),
                    },
                })
                .collect(),
            return_consumed_capacity: None,
        };
        let plan = super::plan::TransactGetPlan::try_from(request).unwrap();
        let observations = items
            .iter()
            .take(10)
            .map(|item| Some(WireItem::try_from(item).unwrap()))
            .collect::<Vec<_>>();
        let table: storage_types::StoredTableInfo = serde_json::from_value(serde_json::json!({
            "table_name": "projection-table", "table_status": "ACTIVE", "created_at": 0,
            "attribute_definitions": [{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "sk", "AttributeType": "S"}],
            "key_schema": [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}], "max_indexers": 0,
            "global_secondary_indexes": null, "table_size_bytes": 0, "item_count": 0,
            "stream_specification": null
        })).unwrap();
        measure_transact_projection_cost(&format!("finish_{label}"), || {
            let prepared = super::prepared::PreparedTransactGet::bound(&plan, vec![table.clone()]);
            let response = prepared.finish(black_box(observations.clone())).unwrap();
            black_box(serde_json::to_vec(&response).unwrap());
        });
    }
}

fn measure_transact_projection_cost(label: &str, mut operation: impl FnMut()) {
    const ITERATIONS: usize = 2_000;
    for _ in 0..100 {
        operation();
    }
    let guard = AllocationGuard::start(
        module_path!(),
        "transact_projection",
        file!(),
        line!(),
        Some(label),
    );
    let started = cpu_time::ProcessTime::try_now().unwrap();
    for _ in 0..ITERATIONS {
        operation();
    }
    let cpu_ns = started.try_elapsed().unwrap().as_nanos();
    alloc_counter::emit_report(&guard.finish()).unwrap();
    eprintln!("transact_projection label={label} iterations={ITERATIONS} cpu_ns={cpu_ns}");
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_transact_projection_widths_when_repeated_then_report_cpu_and_allocations() {
    use std::hint::black_box;
    for width in [1, 8, 16, 24, 32, 128, 256] {
        let item = storage_types::AttributeMap::from_iter((0..width).map(|index| {
            (
                format!("field_{index}"),
                AttributeValue::S("scalar-value".to_owned()),
            )
        }));
        let expression = (0..width)
            .map(|index| format!("field_{index}"))
            .collect::<Vec<_>>()
            .join(", ");
        measure_transact_projection_cost(&format!("width_{width}"), || {
            let projected =
                TransactGetProjection::project_item(black_box(&item), black_box(&expression), None);
            black_box(storage_types::WireItem::try_from(projected).unwrap());
        });
    }
}

#[test]
fn given_borrowed_projection_names_when_returned_then_output_owns_names_and_matches_selection() {
    let item = storage_types::AttributeMap::from_iter([
        (
            "étiquette".to_owned(),
            AttributeValue::S("value".to_owned()),
        ),
        (
            "tags".to_owned(),
            AttributeValue::L(vec![
                AttributeValue::S("first".to_owned()),
                AttributeValue::S("second".to_owned()),
            ]),
        ),
    ]);
    let projected = {
        let names = storage_types::VecMap::from_iter([("#é".to_owned(), "étiquette".to_owned())]);
        let expression = "#é, tags[1], absent, tags[bad]".to_owned();
        TransactGetProjection::project_item(&item, &expression, Some(&names))
    };
    assert_eq!(projected.len(), 2);
    assert_eq!(projected.get("étiquette"), item.get("étiquette"));
    assert_eq!(
        projected.get("tags"),
        Some(&AttributeValue::L(vec![AttributeValue::S(
            "second".to_owned()
        )]))
    );
    for expression in ["", " , ", "tags[99]", "tags[", "[0]", "absent"] {
        assert!(TransactGetProjection::project_item(&item, expression, None).is_empty());
    }
    let duplicate = TransactGetProjection::project_item(&item, "étiquette, étiquette", None);
    assert_eq!(duplicate.len(), 1);
}
