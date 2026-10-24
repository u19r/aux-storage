use alloc_counter::AllocationGuard;

use super::read_sequence_metrics::*;

#[test]
fn snapshot_reports_exact_provider_call_dimensions() {
    record_read_sequence_provider_call(ReadSequenceProviderCall {
        backend: "postgres",
        operation: "query_table",
        item_count: 3,
        byte_count: 1200,
        serialized: true,
        blocking: true,
        serial_depth: 2,
        has_continuation: Some(true),
    });
    let snapshot = read_sequence_provider_metrics_snapshot();
    assert!(snapshot.contains(
        "storage_read_sequence_provider_requests_total{backend=\"postgres\",operation=\"\
         query_table\"} 1"
    ));
    assert!(snapshot.contains(concat!(
        "storage_read_sequence_provider_serial_requests_total{backend=\"postgres\",",
        "operation=\"query_table\"} 1"
    )));
    assert!(snapshot.contains(
        "storage_read_sequence_provider_blocking_requests_total{backend=\"postgres\",operation=\"\
         query_table\"} 1"
    ));
    assert!(snapshot.contains(
        "storage_read_sequence_provider_items_total{backend=\"postgres\",operation=\"query_table\"\
         } 3"
    ));
    assert!(snapshot.contains(
        "storage_read_sequence_provider_bytes_total{backend=\"postgres\",operation=\"query_table\"\
         } 1200"
    ));
    assert!(snapshot.contains(
        "storage_read_sequence_provider_continuation_pages_total{backend=\"postgres\",operation=\"\
         query_table\"} 1"
    ));
    assert!(snapshot.contains(
        "storage_read_sequence_provider_terminal_pages_total{backend=\"postgres\",operation=\"\
         query_table\"} 0"
    ));
    assert!(snapshot.contains(
        "storage_read_sequence_provider_max_serial_depth{backend=\"postgres\",operation=\"\
         query_table\"} 2"
    ));
}

#[test]
fn snapshot_reports_actual_database_await_dimensions() {
    record_read_sequence_database_await("postgres", "query_table", true, true, 3);
    let snapshot = read_sequence_provider_metrics_snapshot();
    assert!(snapshot.contains(concat!(
        "storage_read_sequence_database_awaits_total{backend=\"postgres\",",
        "operation=\"query_table\"} 1"
    )));
    assert!(snapshot.contains(
        "storage_read_sequence_database_serial_waves_total{backend=\"postgres\",operation=\"\
         query_table\"} 1"
    ));
    assert!(snapshot.contains(
        "storage_read_sequence_database_potentially_blocking_awaits_total{backend=\"postgres\",\
         operation=\"query_table\"} 1"
    ));
    assert!(snapshot.contains(
        "storage_read_sequence_database_1ms_projection_total{backend=\"postgres\",operation=\"\
         query_table\"} 1"
    ));
    assert!(snapshot.contains(
        "storage_read_sequence_database_max_serial_depth{backend=\"postgres\",operation=\"\
         query_table\"} 3"
    ));
}

#[test]
fn metric_series_writer_avoids_per_series_label_allocations() {
    let backend = "postgres";
    let operation = "query_table";
    let name = "storage_read_sequence_provider_requests_total";

    let mut baseline_output = String::new();
    push_counter_value_baseline(&mut baseline_output, name, backend, operation, 7);
    let mut current_output = String::new();
    push_counter_value(&mut current_output, name, backend, operation, 7);
    assert_eq!(current_output, baseline_output);

    let baseline = measure_metric_series_writer(push_counter_value_baseline);
    let current = measure_metric_series_writer(push_counter_value);
    let _ = alloc_counter::emit_report(&baseline);
    let _ = alloc_counter::emit_report(&current);
    assert!(
        current.allocation_count < baseline.allocation_count,
        "expected direct metric writer to allocate less often, baseline={} current={}",
        baseline.allocation_count,
        current.allocation_count
    );
    assert!(
        current.allocated_bytes < baseline.allocated_bytes,
        "expected direct metric writer to allocate fewer bytes, baseline={} current={}",
        baseline.allocated_bytes,
        current.allocated_bytes
    );
}

type MetricSeriesWriter = fn(&mut String, &str, &str, &str, u64);

fn measure_metric_series_writer(
    writer: MetricSeriesWriter,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "metric_series_writer",
        file!(),
        line!(),
        None,
    );
    let mut output = String::new();
    for _ in 0..256 {
        output.clear();
        writer(
            &mut output,
            "storage_read_sequence_provider_requests_total",
            "postgres",
            "query_table",
            7,
        );
        std::hint::black_box(&output);
    }
    guard.finish()
}

fn push_counter_value_baseline(
    output: &mut String,
    name: &str,
    backend: &str,
    operation: &str,
    value: u64,
) {
    let labels = format!("{{backend=\"{backend}\",operation=\"{operation}\"}}");
    output.push_str(name);
    output.push_str(&labels);
    output.push(' ');
    output.push_str(&value.to_string());
    output.push('\n');
}
