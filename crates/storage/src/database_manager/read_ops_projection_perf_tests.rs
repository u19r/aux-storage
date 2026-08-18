use std::{
    collections::HashMap,
    process::Command,
    time::{Duration, Instant},
};

use alloc_counter::AllocationGuard;
use storage_types::AttributeValue;

use crate::database_manager::read_ops::{storage_api_project_item, storage_api_projection};

const ITEM_COUNT: usize = 40;
const ITERATIONS: usize = 1_000;

type ProjectionFn = fn(
    &[HashMap<String, AttributeValue>],
    &str,
    Option<&HashMap<String, String>>,
) -> Vec<HashMap<String, AttributeValue>>;
type SingleItemProjectionFn = fn(
    &HashMap<String, AttributeValue>,
    &str,
    Option<&HashMap<String, String>>,
) -> HashMap<String, AttributeValue>;

fn realistic_items() -> Vec<HashMap<String, AttributeValue>> {
    (0..ITEM_COUNT)
        .map(|index| {
            let mut item = HashMap::with_capacity(14);
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

fn expression_attribute_names() -> HashMap<String, String> {
    HashMap::from([("#meta".to_string(), "meta".to_string())])
}

fn measure_projection_allocations(
    project: ProjectionFn,
) -> alloc_counter::AllocationReport<'static> {
    let items = realistic_items();
    let names = expression_attribute_names();
    let guard = AllocationGuard::start(
        module_path!(),
        "storage_api_projection_allocation_profile_tests",
        file!(),
        line!(),
        None,
    );

    for _ in 0..ITERATIONS {
        let projected = project(&items, projection_expr(), Some(&names));
        assert_eq!(projected.len(), ITEM_COUNT);
    }

    guard.finish()
}

fn measure_single_item_projection_allocations(
    project: SingleItemProjectionFn,
) -> alloc_counter::AllocationReport<'static> {
    let items = realistic_items();
    let names = expression_attribute_names();
    let guard = AllocationGuard::start(
        module_path!(),
        "storage_api_single_item_projection_allocation_profile_tests",
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

fn measure_projection_runtime(project: ProjectionFn) -> Duration {
    let items = realistic_items();
    let names = expression_attribute_names();
    let started = Instant::now();
    for _ in 0..ITERATIONS {
        let projected = project(&items, projection_expr(), Some(&names));
        assert_eq!(projected.len(), ITEM_COUNT);
    }
    started.elapsed()
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
fn storage_api_projection_allocation_profile_tests() {
    const ISOLATED_ENV: &str = "AUX_STORAGE_PROJECTION_BATCH_ALLOCATION_ISOLATED";
    if std::env::var_os(ISOLATED_ENV).is_none() {
        let status = Command::new(
            std::env::current_exe()
                .expect("projection allocation test executable should be available"),
        )
        .arg("--exact")
        .arg("database_manager::read_ops_projection_perf_tests::storage_api_projection_allocation_profile_tests")
        .arg("--nocapture")
        .env(ISOLATED_ENV, "1")
        .status()
        .expect("isolated projection allocation test child should start");
        assert!(
            status.success(),
            "isolated projection allocation test failed"
        );
        return;
    }

    let baseline = measure_projection_allocations(baseline_storage_api_projection);
    let optimized = measure_projection_allocations(storage_api_projection);
    alloc_counter::emit_report(&baseline);
    alloc_counter::emit_report(&optimized);
    assert!(optimized.allocation_count <= baseline.allocation_count);
    assert!(optimized.allocated_bytes < baseline.allocated_bytes);
}

#[test]
fn storage_api_single_item_projection_allocation_profile_tests() {
    const ISOLATED_ENV: &str = "AUX_STORAGE_PROJECTION_ALLOCATION_ISOLATED";
    if std::env::var_os(ISOLATED_ENV).is_none() {
        let status = Command::new(
            std::env::current_exe()
                .expect("projection allocation test executable should be available"),
        )
        .arg("--exact")
        .arg(
            "database_manager::read_ops_projection_perf_tests::storage_api_single_item_projection_allocation_profile_tests",
        )
        .arg("--nocapture")
        .env(ISOLATED_ENV, "1")
        .status()
        .expect("isolated projection allocation test child should start");
        assert!(
            status.success(),
            "isolated projection allocation test failed"
        );
        return;
    }

    let baseline = measure_single_item_projection_allocations(baseline_single_item_projection);
    let optimized = measure_single_item_projection_allocations(storage_api_project_item);
    alloc_counter::emit_report(&baseline);
    alloc_counter::emit_report(&optimized);
    assert!(optimized.allocation_count < baseline.allocation_count);
    assert!(optimized.allocated_bytes < baseline.allocated_bytes);
}

#[test]
#[ignore = "manual runtime perf probe; run with --ignored --nocapture --test-threads=1"]
fn storage_api_projection_runtime_perf_probe() {
    let baseline = measure_projection_runtime(baseline_storage_api_projection);
    let optimized = measure_projection_runtime(storage_api_projection);
    println!(
        "baseline_storage_api_projection iterations={ITERATIONS} items_per_iter={ITEM_COUNT} \
         elapsed_ms={:.3} ns_per_iter={:.2}",
        baseline.as_secs_f64() * 1_000.0,
        baseline.as_nanos() as f64 / ITERATIONS as f64
    );
    println!(
        "optimized_storage_api_projection iterations={ITERATIONS} items_per_iter={ITEM_COUNT} \
         elapsed_ms={:.3} ns_per_iter={:.2}",
        optimized.as_secs_f64() * 1_000.0,
        optimized.as_nanos() as f64 / ITERATIONS as f64
    );
    assert!(optimized.as_nanos() > 0);
}

#[test]
#[ignore = "manual runtime perf probe; run with --ignored --nocapture --test-threads=1"]
fn storage_api_single_item_projection_runtime_perf_probe() {
    let baseline = measure_single_item_projection_runtime(baseline_single_item_projection);
    let optimized = measure_single_item_projection_runtime(storage_api_project_item);
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
    items: &[HashMap<String, AttributeValue>],
    projection_expr: &str,
    expression_attribute_names: Option<&HashMap<String, String>>,
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
    item: &HashMap<String, AttributeValue>,
    projection_expr: &str,
    expression_attribute_names: Option<&HashMap<String, String>>,
) -> HashMap<String, AttributeValue> {
    baseline_storage_api_projection(
        std::slice::from_ref(item),
        projection_expr,
        expression_attribute_names,
    )
    .into_iter()
    .next()
    .unwrap_or_default()
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
    attribute_names: Option<&HashMap<String, String>>,
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
    item: &'a HashMap<String, AttributeValue>,
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
