use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use alloc_counter::AllocationGuard;
use storage_types::{AttributeValue, BatchGetWireItemResponse, TableName, WireItem};

use crate::batch_get::merge_cached_batch_get_response;

const TABLE_COUNT: usize = 3;
const ITEM_COUNT: usize = 4;
const ITERATIONS: usize = 10_000;

fn item(table: usize, index: usize) -> WireItem {
    let attributes = HashMap::from([
        (
            "pk".to_string(),
            AttributeValue::S(format!("table#{table}#item#{index}")),
        ),
        ("payload".to_string(), AttributeValue::S("x".repeat(256))),
    ]);
    WireItem::from_attribute_map(&attributes).expect("wire item")
}

fn cached_responses() -> HashMap<TableName, Vec<WireItem>> {
    (0..TABLE_COUNT)
        .map(|table| {
            (
                TableName::new(&format!("table_{table}")),
                (0..ITEM_COUNT).map(|index| item(table, index)).collect(),
            )
        })
        .collect()
}

fn db_response() -> BatchGetWireItemResponse {
    BatchGetWireItemResponse {
        responses: Some(HashMap::from([(
            TableName::new("table_0"),
            vec![item(0, ITEM_COUNT)],
        )])),
        unprocessed_keys: None,
        consumed_capacity: None,
    }
}

fn measure_allocations(
    label: &'static str,
    mut make_response: impl FnMut() -> BatchGetWireItemResponse,
) -> alloc_counter::AllocationReport<'static> {
    let cached = cached_responses();
    let guard = AllocationGuard::start(
        module_path!(),
        "merge_cached_batch_get_response_allocation_profile_tests",
        file!(),
        line!(),
        Some(label),
    );

    for _ in 0..ITERATIONS {
        let mut response = make_response();
        merge_cached_batch_get_response(&mut response, cached.clone());
        std::hint::black_box(response.responses.as_ref().map(HashMap::len));
    }

    guard.finish()
}

fn measure_runtime(
    label: &str,
    mut make_response: impl FnMut() -> BatchGetWireItemResponse,
) -> f64 {
    let cached = cached_responses();
    let started = Instant::now();
    let mut checksum = 0usize;

    for _ in 0..ITERATIONS {
        let mut response = make_response();
        merge_cached_batch_get_response(&mut response, cached.clone());
        checksum ^= response.responses.as_ref().map(HashMap::len).unwrap_or(0);
    }

    let elapsed = started.elapsed();
    std::hint::black_box(checksum);
    print_runtime(label, elapsed);
    elapsed.as_nanos() as f64 / ITERATIONS as f64
}

fn print_runtime(label: &str, elapsed: Duration) {
    println!(
        "{label} iterations={ITERATIONS} elapsed_ms={:.3} ns_per_iter={:.2}",
        elapsed.as_secs_f64() * 1_000.0,
        elapsed.as_nanos() as f64 / ITERATIONS as f64
    );
}

#[test]
fn merge_cached_batch_get_response_allocation_profile_tests() {
    let empty_response = measure_allocations("empty_response", BatchGetWireItemResponse::default);
    let existing_response = measure_allocations("existing_response", db_response);

    alloc_counter::emit_report(&empty_response);
    alloc_counter::emit_report(&existing_response);

    assert!(empty_response.allocation_count > 0);
    assert!(existing_response.allocation_count > 0);
}

#[test]
#[ignore = "manual runtime perf probe; run with --ignored --nocapture before/after merge changes"]
fn merge_cached_batch_get_response_runtime_perf_probe() {
    let empty_response = measure_runtime(
        "merge_cached_batch_get_response_empty",
        BatchGetWireItemResponse::default,
    );
    let existing_response =
        measure_runtime("merge_cached_batch_get_response_existing", db_response);

    assert!(empty_response > 0.0);
    assert!(existing_response > 0.0);
}
