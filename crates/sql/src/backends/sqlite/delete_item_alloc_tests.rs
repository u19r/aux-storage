use std::collections::HashMap;

use alloc_counter::AllocationGuard;
use storage_types::{AttributeValue, KeyAttributes};

use crate::delete_item_impl::key_values_borrowed_for_tests;

const ITERATIONS: usize = 1_024;

fn sample_key() -> KeyAttributes {
    KeyAttributes::from(HashMap::from([
        ("pk".to_string(), AttributeValue::S("P1".to_string())),
        ("sk".to_string(), AttributeValue::S("S1".to_string())),
    ]))
}

fn measure_borrowed_conversion_baseline() -> alloc_counter::AllocationReport<'static> {
    let key = sample_key();
    let guard = AllocationGuard::start(
        module_path!(),
        "borrowed_key_scalar_conversion",
        file!(),
        line!(),
        Some("baseline"),
    );
    for _ in 0..ITERATIONS {
        let values = match key_values_borrowed_for_tests(&key) {
            Ok(values) => values,
            Err(err) => panic!("borrowed conversion should succeed: {err}"),
        };
        assert_eq!(values.len(), 2);
    }
    guard.finish()
}

#[test]
fn borrowed_key_scalar_conversion_allocation_baseline_tests() {
    // Snapshot (2026-02-18, `cargo test -p sqlite delete_item_alloc_tests --
    // --nocapture`): allocation_count=1024, allocated_bytes=65536.
    let report = measure_borrowed_conversion_baseline();
    alloc_counter::emit_report(&report);
    assert!(report.allocation_count > 0);
    assert!(report.allocated_bytes > 0);
}
