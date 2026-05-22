use alloc_counter::AllocationGuard;
use storage_types::{IndexName, TableName};

use crate::manager::storage_manager_impl_consumed_capacity::{
    calculate_consumed_capacity_from_inputs, calculate_consumed_capacity_json_baseline_for_tests,
};

const ITERATIONS: usize = 1_024;

fn measure_json_baseline() -> alloc_counter::AllocationReport<'static> {
    let table_name = TableName::new(&"TestTable");
    let index_name = IndexName::new(&"gsi1");
    let guard = AllocationGuard::start(
        module_path!(),
        "consumed_capacity_json_baseline",
        file!(),
        line!(),
        Some("baseline"),
    );
    for _ in 0..ITERATIONS {
        let value = calculate_consumed_capacity_json_baseline_for_tests(
            Some("INDEXES"),
            &table_name,
            Some(&index_name),
            3,
        );
        assert!(value.is_some());
    }
    guard.finish()
}

fn measure_typed_experiment() -> alloc_counter::AllocationReport<'static> {
    let table_name = TableName::new(&"TestTable");
    let index_name = IndexName::new(&"gsi1");
    let guard = AllocationGuard::start(
        module_path!(),
        "consumed_capacity_typed_experiment",
        file!(),
        line!(),
        Some("experiment"),
    );
    for _ in 0..ITERATIONS {
        let value = calculate_consumed_capacity_from_inputs(
            Some("INDEXES"),
            &table_name,
            Some(&index_name),
            3,
        );
        assert!(value.is_some());
    }
    guard.finish()
}

#[test]
fn consumed_capacity_typed_builder_reduces_allocations_tests() {
    let baseline = measure_json_baseline();
    let experiment = measure_typed_experiment();

    alloc_counter::emit_report(&baseline);
    alloc_counter::emit_report(&experiment);

    assert!(
        experiment.allocation_count < baseline.allocation_count,
        "expected fewer allocations, baseline={} experiment={}",
        baseline.allocation_count,
        experiment.allocation_count
    );
    assert!(
        experiment.allocated_bytes < baseline.allocated_bytes,
        "expected fewer allocated bytes, baseline={} experiment={}",
        baseline.allocated_bytes,
        experiment.allocated_bytes
    );
}
