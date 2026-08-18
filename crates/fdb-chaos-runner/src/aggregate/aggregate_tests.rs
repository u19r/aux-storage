use super::parse_read_sequence_metrics;

#[test]
fn read_sequence_checker_sums_client_metrics() {
    let report = parse_read_sequence_metrics(
        "Metric (0, 0): External.aux_storage_read_sequence_dag_attempts, 2.000000, 2\nMetric (0, \
         1): External.aux_storage_read_sequence_dag_published, 2.000000, 2\nMetric (0, 2): \
         External.aux_storage_read_sequence_dag_oracle_checks, 2.000000, 2\nMetric (0, 3): \
         External.aux_storage_read_sequence_dag_mismatches, 0.000000, 0\nMetric (0, 4): \
         External.aux_storage_read_sequence_dag_errors, 0.000000, 0\nMetric (1, 0): \
         External.aux_storage_read_sequence_dag_attempts, 1.000000, 1\nMetric (1, 1): \
         External.aux_storage_read_sequence_dag_published, 1.000000, 1\nMetric (1, 2): \
         External.aux_storage_read_sequence_dag_oracle_checks, 1.000000, 1",
    )
    .expect("read-sequence metrics");
    assert_eq!(report.attempts, 3);
    assert_eq!(report.published, 3);
    assert_eq!(report.oracle_checks, 3);
    assert_eq!(report.mismatches, 0);
    assert_eq!(report.errors, 0);
}
