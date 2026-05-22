use super::{
    foundationdb_operation_metrics_reset, foundationdb_operation_metrics_snapshot,
    record_fdb_conflict_artifacts, record_fdb_operation, record_fdb_operation_bytes,
    record_fdb_operation_latency, record_fdb_point_read, record_fdb_range_read,
    record_fdb_transaction_start, record_fdb_write_shape,
};

#[test]
fn conflict_artifact_metrics_are_exposed_in_snapshot() {
    foundationdb_operation_metrics_reset();
    record_fdb_conflict_artifacts("queue_claim", 2, 3, 5, 7);

    let snapshot = foundationdb_operation_metrics_snapshot();

    assert!(snapshot.contains(
        "foundationdb_operations_total{path=\"queue_claim\",operation=\"conflict_retry\"} 1"
    ));
    assert!(snapshot.contains(
        "foundationdb_operations_total{path=\"queue_claim\",operation=\"conflict_key\"} 2"
    ));
    assert!(snapshot.contains(
        "foundationdb_operations_total{path=\"queue_claim\",operation=\"read_conflict_range\"} 3"
    ));
    assert!(snapshot.contains(
        "foundationdb_operations_total{path=\"queue_claim\",operation=\"write_conflict_range\"} 5"
    ));
    assert!(snapshot.contains(
        "foundationdb_operations_total{path=\"queue_claim\",operation=\"candidate_key\"} 7"
    ));
}

#[test]
fn read_write_shape_metrics_are_exposed_in_snapshot() {
    foundationdb_operation_metrics_reset();
    record_fdb_transaction_start("get");
    record_fdb_point_read("get", false, 2);
    record_fdb_point_read("get", true, 3);
    record_fdb_range_read("range", false, 5);
    record_fdb_range_read("range", true, 7);
    record_fdb_write_shape("put", 11, 13);
    record_fdb_operation_bytes("get", "read_key", 17);
    record_fdb_operation_bytes("put", "write_key", 19);
    record_fdb_operation_latency("get", "point_read", std::time::Duration::from_micros(23));

    let snapshot = foundationdb_operation_metrics_snapshot();

    for expected in [
        "foundationdb_operations_total{path=\"get\",operation=\"transaction_start\"} 1",
        "foundationdb_operations_total{path=\"get\",operation=\"ordinary_point_read\"} 2",
        "foundationdb_operations_total{path=\"get\",operation=\"snapshot_point_read\"} 3",
        "foundationdb_operations_total{path=\"range\",operation=\"ordinary_range_read\"} 5",
        "foundationdb_operations_total{path=\"range\",operation=\"snapshot_range_read\"} 7",
        "foundationdb_operations_total{path=\"put\",operation=\"blind_write\"} 11",
        "foundationdb_operations_total{path=\"put\",operation=\"read_modify_write\"} 13",
        "foundationdb_operation_bytes_total{path=\"get\",direction=\"read_key\"} 17",
        "foundationdb_operation_bytes_total{path=\"put\",direction=\"write_key\"} 19",
        "foundationdb_operation_latency_micros_total{path=\"get\",stage=\"point_read\"} 23",
        "foundationdb_operation_latency_count_total{path=\"get\",stage=\"point_read\"} 1",
    ] {
        assert!(
            snapshot.contains(expected),
            "missing metric line: {expected}"
        );
    }
}

#[test]
fn unknown_metric_labels_do_not_allocate_or_emit() {
    foundationdb_operation_metrics_reset();

    record_fdb_operation("unknown_path", "get", 10);
    record_fdb_operation("get", "unknown_operation", 10);
    record_fdb_operation_bytes("unknown_path", "read", 10);
    record_fdb_operation_bytes("get", "unknown_direction", 10);
    record_fdb_operation_latency(
        "unknown_path",
        "point_read",
        std::time::Duration::from_micros(10),
    );
    record_fdb_operation_latency("get", "unknown_stage", std::time::Duration::from_micros(10));

    let snapshot = foundationdb_operation_metrics_snapshot();

    assert!(!snapshot.contains("unknown"));
}
