use super::table_atomicity_check_phase::recorded_workload_error_detail;
use crate::imports::*;

#[test]
fn check_gate_rejects_large_recorded_failure_batch() {
    let anomalies = (0..966)
        .map(|index| Anomaly {
            kind: AnomalyKind::OperationFailed,
            client_id: 0,
            key: format!("key-{index}"),
            expected: None,
            actual: None,
            detail: "Requested resource not found: Table: rep not found".to_string(),
        })
        .collect::<Vec<_>>();

    let detail = recorded_workload_error_detail(966, &anomalies, 0)
        .expect("recorded operation failures must block check completion");
    assert!(detail.contains("error_count=966"));
    assert!(detail.contains("operation_failed_anomalies=966"));
}

#[test]
fn check_gate_does_not_reject_explicit_unknown_outcomes() {
    assert!(recorded_workload_error_detail(0, &[], 2).is_none());
}
