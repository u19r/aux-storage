use std::hint::black_box;

use alloc_counter::AllocationGuard;
use storage_types::{ConsumedCapacity, ConsumedCapacityMetrics, QueryRequest, TableName, WireItem};

use crate::{DynamoRangeReadShape, RangeReadProgress, dynamo_range_read::DynamoRangeOutput};

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_query_page_shapes_when_response_finished_and_serialized_then_report_cpu_and_allocations() {
    for size in [0, 1, 4, 32] {
        for width in [64, 1024] {
            let item = WireItem::dynamo_json(
                format!(
                    r#"{{"id":{{"S":"item"}},"payload":{{"S":"{}"}}}}"#,
                    "x".repeat(width)
                )
                .into_bytes(),
            );
            let items = vec![item; size];
            for has_index in [false, true] {
                let label = format!("query_{size}_{width}_index_{has_index}");
                let output = finish_response(&items, has_index);
                let value: serde_json::Value = serde_json::from_slice(&output).unwrap();
                assert_eq!(value["Count"], size);
                assert_eq!(value["ScannedCount"], size);
                assert_eq!(value["Items"].as_array().unwrap().len(), size);
                assert_eq!(
                    value["ConsumedCapacity"]
                        .get("GlobalSecondaryIndexes")
                        .is_some(),
                    has_index
                );
                for _ in 0..100 {
                    drop(black_box(finish_response(&items, has_index)));
                }
                let guard = AllocationGuard::start(
                    module_path!(),
                    "query_capacity_response",
                    file!(),
                    line!(),
                    Some(&label),
                );
                let started = cpu_time::ProcessTime::now();
                for _ in 0..10_000 {
                    drop(black_box(finish_response(
                        black_box(&items),
                        black_box(has_index),
                    )));
                }
                let cpu = started.elapsed();
                alloc_counter::emit_report(&guard.finish()).unwrap();
                eprintln!(
                    "query_capacity_response label={label} iterations=10000 cpu_ns={}",
                    cpu.as_nanos()
                );
            }
        }
    }
}

fn finish_response(items: &[WireItem], has_index: bool) -> Vec<u8> {
    let units = (items.len() as f64).max(0.5);
    let capacity = ConsumedCapacity {
        table_name: TableName::new("items"),
        capacity_units: units,
        global_secondary_indexes: has_index.then(|| {
            [(
                "index".to_owned(),
                ConsumedCapacityMetrics {
                    capacity_units: units,
                },
            )]
            .into_iter()
            .collect()
        }),
    };
    let response = QueryRequest::finish(DynamoRangeOutput {
        items: Some(
            items
                .iter()
                .map(|item| crate::RangeReadItem {
                    key: storage_types::ItemKey::table_key(
                        TableName::new("items"),
                        storage_types::AttributeValue::S("item".into()),
                        None,
                    ),
                    item: item.clone(),
                })
                .collect(),
        ),
        count: items.len() as u32,
        progress: RangeReadProgress {
            evaluated_count: items.len() as u32,
            ..Default::default()
        },
        continuation: None,
        last_evaluated_key: None,
        consumed_capacity: Some(capacity),
        item_cursors: None,
    });
    serde_json::to_vec(&response).unwrap()
}
