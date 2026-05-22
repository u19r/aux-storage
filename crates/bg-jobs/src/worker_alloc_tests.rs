use alloc_counter::AllocationGuard;

use crate::LeaseUpdateBuilder;

const ITERATIONS: usize = 1_024;

fn measure_map_construction() -> alloc_counter::AllocationReport<'static> {
    let builder = LeaseUpdateBuilder::new();
    let guard = AllocationGuard::start(
        module_path!(),
        "lease_expression_map_construction",
        file!(),
        line!(),
        Some("baseline"),
    );
    for _ in 0..ITERATIONS {
        let statement = builder.build_update_statement("worker-1", 10_000, 9_000);
        assert_eq!(
            statement.expression_attribute_names.get("#status"),
            Some(&"status".to_string())
        );
        assert!(
            statement
                .expression_attribute_values
                .contains_key(":worker")
        );
    }
    guard.finish()
}

fn measure_custom_status_construction() -> alloc_counter::AllocationReport<'static> {
    let builder = LeaseUpdateBuilder::new();
    let guard = AllocationGuard::start(
        module_path!(),
        "lease_expression_ref_construction",
        file!(),
        line!(),
        Some("experiment"),
    );
    for _ in 0..ITERATIONS {
        let statement = builder.build_update_statement("worker-1", 10_000, 9_000);
        assert!(statement.condition_expression.contains(":status0"));
    }
    guard.finish()
}

#[test]
fn lease_update_statement_allocation_report_tests() {
    let baseline = measure_map_construction();
    let experiment = measure_custom_status_construction();

    alloc_counter::emit_report(&baseline);
    alloc_counter::emit_report(&experiment);
}
