use std::{collections::HashMap, hint::black_box};

use alloc_counter::AllocationGuard;
use serde::{Deserialize, Serialize};

use crate::{AttributeMap, AttributeValue, WireItem};

const ITERATIONS: usize = 10_000;

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_attribute_shapes_when_serialized_then_report_cpu_and_allocations() {
    for (label, value) in [
        ("scalar", AttributeValue::N("12345.67".to_owned())),
        (
            "escaped",
            AttributeValue::S("quote\"slash\\newline\n".repeat(8)),
        ),
        (
            "list",
            AttributeValue::L((0..32).map(|n| AttributeValue::N(n.to_string())).collect()),
        ),
        ("nested", AttributeValue::M(nested_fields(16))),
    ] {
        let encoded = serde_json::to_vec(&value).unwrap();
        assert_eq!(
            serde_json::from_slice::<AttributeValue>(&encoded).unwrap(),
            value
        );
        measure_operation(label, || serde_json::to_vec(black_box(&value)).unwrap());
    }
    for size in [4, 32] {
        let attributes = AttributeMap::from_iter(nested_fields(size));
        let label = format!("outer_wire_{size}");
        measure_operation(&label, || {
            WireItem::try_from(black_box(&attributes)).unwrap()
        });
    }
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_nested_map_sizes_when_operations_run_then_compare_map_representations() {
    for size in [1, 4, 16, 32, 128] {
        let hash = scalar_fields(size);
        let ordered = AttributeMap::from_iter(hash.clone());
        let data = serde_json::to_vec(&hash).unwrap();
        assert_eq!(
            serde_json::to_value(&hash).unwrap(),
            serde_json::to_value(&ordered).unwrap()
        );
        measure_map_operations(size, &hash, &data, "hash");
        measure_map_operations(size, &ordered, &data, "attribute");
        let keys = [
            "field_0".to_owned(),
            format!("field_{}", size / 2),
            format!("field_{}", size - 1),
            "missing".to_owned(),
        ];
        measure_operation(&format!("hash_{size}_lookup"), || {
            keys.iter()
                .filter(|name| black_box(&hash).get(black_box(name.as_str())).is_some())
                .count()
        });
        measure_operation(&format!("attribute_{size}_lookup"), || {
            keys.iter()
                .filter(|name| black_box(&ordered).get(black_box(name.as_str())).is_some())
                .count()
        });
    }
}

fn measure_map_operations<T>(size: usize, map: &T, data: &[u8], representation: &str)
where T: Clone + Serialize + for<'de> Deserialize<'de> {
    measure_operation(&format!("{representation}_{size}_clone"), || {
        black_box(map).clone()
    });
    measure_operation(&format!("{representation}_{size}_decode"), || {
        serde_json::from_slice::<T>(black_box(data)).unwrap()
    });
    measure_operation(&format!("{representation}_{size}_serialize"), || {
        serde_json::to_vec(black_box(map)).unwrap()
    });
}

fn scalar_fields(size: usize) -> HashMap<String, AttributeValue> {
    (0..size)
        .map(|n| {
            (
                format!("field_{n}"),
                AttributeValue::S(format!("value_{n}")),
            )
        })
        .collect()
}

fn nested_fields(size: usize) -> HashMap<String, AttributeValue> {
    (0..size)
        .map(|n| (format!("field_{n}"), AttributeValue::M(scalar_fields(4))))
        .collect()
}

fn measure_operation<T>(label: &str, operation: impl Fn() -> T) {
    for _ in 0..100 {
        black_box(operation());
    }
    let guard = AllocationGuard::start(
        module_path!(),
        "attribute_serialization",
        file!(),
        line!(),
        Some(label),
    );
    let started = cpu_time::ProcessTime::now();
    for _ in 0..ITERATIONS {
        black_box(operation());
    }
    let cpu = started.elapsed();
    alloc_counter::emit_report(&guard.finish()).unwrap();
    eprintln!(
        "attribute_serialization label={label} iterations={ITERATIONS} cpu_ns={}",
        cpu.as_nanos()
    );
}
