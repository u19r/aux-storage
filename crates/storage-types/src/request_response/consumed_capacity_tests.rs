use std::hint::black_box;

use alloc_counter::AllocationGuard;

use crate::{ConsumedCapacity, ConsumedCapacityMetrics, TableName};

fn capacity(size: usize) -> ConsumedCapacity {
    ConsumedCapacity {
        table_name: TableName::new("items"),
        capacity_units: 0.5,
        global_secondary_indexes: Some(
            (0..size)
                .map(|index| {
                    (
                        format!("index_{index}"),
                        ConsumedCapacityMetrics {
                            capacity_units: index as f64 + 0.5,
                        },
                    )
                })
                .collect(),
        ),
    }
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_capacity_map_sizes_when_constructed_cloned_or_serialized_then_report_cpu_and_allocations()
{
    for size in [0, 1, 4, 16, 32, 128] {
        let fixture = capacity(size);
        let bytes = serde_json::to_vec(&fixture).unwrap();
        assert_eq!(
            serde_json::from_slice::<ConsumedCapacity>(&bytes).unwrap(),
            fixture
        );
        report(&format!("capacity_{size}_construct"), || {
            capacity(black_box(size))
        });
        report(&format!("capacity_{size}_clone"), || {
            black_box(&fixture).clone()
        });
        report(&format!("capacity_{size}_serialize"), || {
            serde_json::to_vec(black_box(&fixture)).unwrap()
        });
        report(&format!("capacity_{size}_deserialize"), || {
            serde_json::from_slice::<ConsumedCapacity>(black_box(&bytes)).unwrap()
        });
    }
}

fn report<T>(label: &str, operation: impl Fn() -> T) {
    for _ in 0..100 {
        drop(black_box(operation()));
    }
    let guard = AllocationGuard::start(
        module_path!(),
        "consumed_capacity",
        file!(),
        line!(),
        Some(label),
    );
    let started = cpu_time::ProcessTime::now();
    for _ in 0..10_000 {
        drop(black_box(operation()));
    }
    let cpu = started.elapsed();
    alloc_counter::emit_report(&guard.finish()).unwrap();
    eprintln!(
        "consumed_capacity label={label} iterations=10000 cpu_ns={}",
        cpu.as_nanos()
    );
}

#[test]
fn given_capacity_wire_objects_when_decoded_then_preserve_names_values_and_duplicates() {
    for indexes in [
        None,
        Some(serde_json::json!({})),
        Some(
            serde_json::json!({"index_1": {"CapacityUnits": 1.5}, "quoted\"index": {"CapacityUnits": 2.5}}),
        ),
    ] {
        let mut expected = serde_json::json!({"TableName": "items", "CapacityUnits": 3.5});
        if let Some(indexes) = indexes {
            expected["GlobalSecondaryIndexes"] = indexes;
        }
        let decoded: ConsumedCapacity = serde_json::from_value(expected.clone()).unwrap();
        assert_eq!(serde_json::to_value(&decoded).unwrap(), expected);
    }
    let decoded: ConsumedCapacity = serde_json::from_str(r#"{"TableName":"items","CapacityUnits":0.5,"GlobalSecondaryIndexes":{"same":{"CapacityUnits":1.0},"same":{"CapacityUnits":2.0}}}"#).unwrap();
    let indexes = decoded.global_secondary_indexes.unwrap();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes.get("same").unwrap().capacity_units, 2.0);
}

#[test]
fn given_consumed_capacity_schema_when_generated_then_preserve_index_map_shape() {
    let schema =
        serde_json::to_value(<ConsumedCapacity as utoipa::PartialSchema>::schema()).unwrap();
    let field = &schema["properties"]["GlobalSecondaryIndexes"];
    assert!(field.is_object());
    assert_eq!(
        field,
        &serde_json::json!({"additionalProperties":{"$ref":"#/components/schemas/ConsumedCapacityMetrics"},"propertyNames":{"type":"string"},"type":["object","null"]})
    );
}
