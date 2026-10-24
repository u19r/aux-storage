use super::{
    ReadSequenceDagCheckReport, parse_read_sequence_metrics, validate_read_sequence_report,
};

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
    assert_eq!(report.retries, 0);
    assert_eq!(report.oracle_checks, 3);
    assert_eq!(report.ordinary_attempts, 0);
    assert_eq!(report.mapped_attempts, 0);
    assert_eq!(report.mapped_selected, 0);
    assert_eq!(report.mapped_fallbacks, 0);
    assert_eq!(report.mismatches, 0);
    assert_eq!(report.errors, 0);
}

#[test]
fn read_sequence_checker_accepts_retried_ordinary_attempts() {
    for (ordinary_attempts, mapped_attempts, retries) in [(17, 16, 2), (22, 22, 6)] {
        let report = ReadSequenceDagCheckReport {
            attempts: 16,
            published: 16,
            retries,
            oracle_checks: ordinary_attempts,
            ordinary_attempts,
            mapped_attempts,
            mapped_selected: mapped_attempts,
            mapped_fallbacks: 0,
            mismatches: 0,
            errors: 0,
        };
        validate_read_sequence_report(&report).expect("retried report is valid");
    }
}

#[test]
fn read_sequence_checker_rejects_unaccounted_mapped_outcome() {
    let report = ReadSequenceDagCheckReport {
        attempts: 1,
        published: 1,
        retries: 0,
        oracle_checks: 1,
        ordinary_attempts: 1,
        mapped_attempts: 1,
        mapped_selected: 0,
        mapped_fallbacks: 0,
        mismatches: 0,
        errors: 0,
    };
    let error = validate_read_sequence_report(&report).expect_err("missing mapped outcome");
    assert!(error.contains("mapped outcomes"));
}
