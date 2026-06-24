use std::collections::{BTreeMap, BTreeSet};

use crate::{
    ARTIFACT_SCHEMA_VERSION, BackgroundLeaseEvent, GsiEntry, GsiIndexModel, HistoryEvent,
    OperationHistory, OperationKind, OperationOutcome, PossibleTableModel, SharedKeyAudit,
    SharedKeyRead, SimulationRunMetadata, SimulationRunMetadataInput, TableModel,
    TrimProviderSnapshot, TrimScopeExpectation, TrimScopeReport, TrimStateModel,
    check_aggregate_trim_scopes, check_background_lease_events, check_shared_key_audits,
};

#[test]
fn metadata_uses_stable_schema_version() {
    let metadata = SimulationRunMetadata::new(SimulationRunMetadataInput {
        workload: "noop".to_string(),
        profile: "smoke".to_string(),
        seed: 1,
        buggify: "on".to_string(),
        test_file: "noop.toml".to_string(),
        library_path: "target/release".to_string(),
        library_name: "aux_storage_fdb_chaos".to_string(),
        rerun_command: "just fdb-chaos-smoke noop 1".to_string(),
        options: BTreeMap::from([("operationCount".to_string(), "0".to_string())]),
    });

    assert_eq!(metadata.schema_version, ARTIFACT_SCHEMA_VERSION);
    assert_eq!(
        metadata.options.keys().collect::<Vec<_>>(),
        vec![&"operationCount".to_string()]
    );
}

#[test]
fn background_lease_checker_accepts_commit_inside_renewed_lease() {
    let report = check_background_lease_events(&[
        BackgroundLeaseEvent::acquire("stream-trim/scope-1", "worker-a", 10, 20),
        BackgroundLeaseEvent::renew("stream-trim/scope-1", "worker-a", 18, 35),
        BackgroundLeaseEvent::commit("stream-trim/scope-1", "worker-a", 30, "delete-page-1"),
    ]);

    assert_eq!(report.checked_event_count, 3);
    assert_eq!(report.checked_commit_count, 1);
    assert_eq!(report.anomaly_count, 0);
}

#[test]
fn background_lease_checker_rejects_commit_after_lease_expiry() {
    let report = check_background_lease_events(&[
        BackgroundLeaseEvent::acquire("gsi-backfill/table-a/index-a", "worker-a", 10, 20),
        BackgroundLeaseEvent::commit(
            "gsi-backfill/table-a/index-a",
            "worker-a",
            21,
            "backfill-page-1",
        ),
    ]);

    assert_eq!(report.checked_commit_count, 1);
    assert_eq!(report.anomaly_count, 1);
    assert!(matches!(
        report.anomalies[0].kind,
        crate::AnomalyKind::BackgroundLeaseViolation
    ));
    assert!(report.anomalies[0].detail.contains("without holding"));
}

#[test]
fn background_lease_checker_rejects_overlapping_worker_acquire() {
    let report = check_background_lease_events(&[
        BackgroundLeaseEvent::acquire("partition-reconcile/ordered-log/a", "worker-a", 10, 30),
        BackgroundLeaseEvent::acquire("partition-reconcile/ordered-log/a", "worker-b", 20, 40),
    ]);

    assert_eq!(report.anomaly_count, 1);
    assert!(
        report.anomalies[0]
            .detail
            .contains("another worker still held")
    );
}

#[test]
fn table_model_applies_committed_operations_only() {
    let mut history = OperationHistory::default();
    history.push(HistoryEvent::new(
        1,
        0,
        OperationKind::Put,
        "k1".to_string(),
        Some("v1".to_string()),
        OperationOutcome::Committed,
    ));
    history.push(HistoryEvent::new(
        2,
        0,
        OperationKind::PutIfAbsent,
        "k1".to_string(),
        Some("v2".to_string()),
        OperationOutcome::ConditionFailed {
            error: "conditional check failed".to_string(),
        },
    ));
    history.push(HistoryEvent::new(
        3,
        0,
        OperationKind::Delete,
        "k1".to_string(),
        None,
        OperationOutcome::Unknown {
            error: "commit_unknown_result".to_string(),
        },
    ));

    let mut model = TableModel::default();
    for event in history.events() {
        model.apply(event);
    }

    assert_eq!(history.committed_count(), 1);
    assert_eq!(history.condition_failed_count(), 1);
    assert_eq!(history.failed_count(), 0);
    assert_eq!(history.unknown_count(), 1);
    assert_eq!(model.get("k1"), Some("v1"));
}

#[test]
fn operation_history_exports_json_lines() {
    let mut history = OperationHistory::default();
    history.push(HistoryEvent::with_sim_interval(
        1,
        2,
        10,
        20,
        OperationKind::Put,
        "k1".to_string(),
        Some("v1".to_string()),
        OperationOutcome::Committed,
    ));

    let lines = history.to_json_lines().expect("serialize json lines");

    assert!(lines.contains("\"sequence\":1"));
    assert!(lines.contains("\"started_at_sim_us\":10"));
    assert!(lines.contains("\"completed_at_sim_us\":20"));
    assert!(lines.ends_with('\n'));

    let parsed = OperationHistory::from_json_lines(&lines).expect("parse json lines");
    assert_eq!(parsed, history);
}

#[test]
fn operation_error_classifier_preserves_unknown_commits() {
    assert!(matches!(
        crate::classify_operation_error("commit_unknown_result"),
        OperationOutcome::Unknown { .. }
    ));
    assert!(matches!(
        crate::classify_operation_error("not_committed"),
        OperationOutcome::Failed { .. }
    ));
}

#[test]
fn possible_table_model_branches_unknown_writes() {
    let mut model = PossibleTableModel::default();
    model.apply(&HistoryEvent::new(
        1,
        0,
        OperationKind::Put,
        "k1".to_string(),
        Some("v1".to_string()),
        OperationOutcome::Unknown {
            error: "commit_unknown_result".to_string(),
        },
    ));

    assert!(model.allows("k1", None));
    assert!(model.allows("k1", Some("v1")));
    assert!(!model.allows("k1", Some("v2")));

    model.apply(&HistoryEvent::new(
        2,
        0,
        OperationKind::Update,
        "k1".to_string(),
        Some("v2".to_string()),
        OperationOutcome::Committed,
    ));

    assert!(!model.allows("k1", None));
    assert!(model.allows_present("k1"));
    assert!(!model.allows("k1", Some("v1")));
    assert!(model.allows("k1", Some("v2")));
}

#[test]
fn possible_table_model_uses_condition_outcomes_to_narrow_state() {
    let mut model = PossibleTableModel::default();
    model.apply(&HistoryEvent::new(
        1,
        0,
        OperationKind::Put,
        "k1".to_string(),
        Some("v1".to_string()),
        OperationOutcome::Unknown {
            error: "maybe_committed".to_string(),
        },
    ));
    model.apply(&HistoryEvent::new(
        2,
        0,
        OperationKind::PutIfAbsent,
        "k1".to_string(),
        Some("v2".to_string()),
        OperationOutcome::ConditionFailed {
            error: "conditional check failed".to_string(),
        },
    ));

    assert!(!model.allows("k1", None));
    assert!(model.allows_present("k1"));
    assert!(model.allows("k1", Some("v1")));
    assert!(!model.allows("k1", Some("v2")));
}

#[test]
fn gsi_index_model_rewrites_membership_exactly() {
    let mut model = GsiIndexModel::default();
    model.put(
        "category-a".to_string(),
        GsiEntry::new("k1".to_string(), "10".to_string(), "v1".to_string()),
    );
    model.put(
        "category-a".to_string(),
        GsiEntry::new("k2".to_string(), "20".to_string(), "v2".to_string()),
    );
    model.put(
        "category-b".to_string(),
        GsiEntry::new("k1".to_string(), "30".to_string(), "v3".to_string()),
    );

    assert_eq!(
        model.entries_for_partition("category-a"),
        BTreeSet::from([GsiEntry::new(
            "k2".to_string(),
            "20".to_string(),
            "v2".to_string()
        )])
    );
    assert_eq!(
        model.entries_for_partition("category-b"),
        BTreeSet::from([GsiEntry::new(
            "k1".to_string(),
            "30".to_string(),
            "v3".to_string()
        )])
    );

    model.delete("k1");

    assert!(model.entries_for_partition("category-b").is_empty());
    assert_eq!(model.partitions(), vec!["category-a".to_string()]);
}

#[test]
fn trim_state_model_separates_classified_and_unclassified_scopes() {
    let mut model = TrimStateModel::default();
    let table = TrimScopeExpectation::table("orders".to_string());
    let item = TrimScopeExpectation::item("orders#1".to_string());
    model.expect_scope(table.clone());
    model.expect_scope(item.clone());
    model.unclassify(item);

    assert_eq!(model.classified_scopes(), vec![table]);
    assert_eq!(model.unclassified_count(), 1);
}

#[test]
fn aggregate_trim_checker_accepts_merged_client_scope_exactness() {
    let report_0 = TrimScopeReport::new(
        0,
        vec![
            TrimScopeExpectation::table("orders".to_string()),
            TrimScopeExpectation::item("scope-a".to_string()),
        ],
        vec![],
    );
    let report_1 = TrimScopeReport::new(
        1,
        vec![
            TrimScopeExpectation::table("orders".to_string()),
            TrimScopeExpectation::item("scope-b".to_string()),
        ],
        vec![],
    );
    let snapshot = TrimProviderSnapshot::new(
        0,
        vec!["kv-table-id:1".to_string()],
        vec!["scope-a".to_string(), "scope-b".to_string()],
    );

    let check = check_aggregate_trim_scopes(&[report_0, report_1], &snapshot);

    assert_eq!(check.checked_client_count, 2);
    assert_eq!(check.expected_table_scope_count, 1);
    assert_eq!(check.actual_table_scope_count, 1);
    assert_eq!(check.expected_item_scope_count, 2);
    assert_eq!(check.actual_item_scope_count, 2);
    assert_eq!(check.anomaly_count, 0);
}

#[test]
fn aggregate_trim_checker_reports_missing_and_unexpected_item_scopes() {
    let report = TrimScopeReport::new(
        0,
        vec![
            TrimScopeExpectation::table("orders".to_string()),
            TrimScopeExpectation::item("scope-a".to_string()),
        ],
        vec![TrimScopeExpectation::item("scope-unclassified".to_string())],
    );
    let snapshot = TrimProviderSnapshot::new(
        0,
        vec!["kv-table-id:1".to_string()],
        vec![
            "scope-unclassified".to_string(),
            "scope-unexpected".to_string(),
        ],
    );

    let check = check_aggregate_trim_scopes(&[report], &snapshot);

    assert_eq!(check.anomaly_count, 1);
    assert_eq!(check.unclassified_item_scope_count, 1);
    assert_eq!(check.anomalies[0].key, "stream-trim/item-scopes");
    assert!(check.anomalies[0].detail.contains("missing_count=1"));
    assert!(check.anomalies[0].detail.contains("unexpected_count=1"));
}

#[test]
fn shared_key_checker_accepts_explainable_final_state() {
    let mut history = OperationHistory::default();
    history.push(HistoryEvent::new(
        1,
        0,
        OperationKind::Put,
        "shared/key-1".to_string(),
        Some("v1".to_string()),
        OperationOutcome::Committed,
    ));
    history.push(HistoryEvent::new(
        1,
        1,
        OperationKind::Delete,
        "shared/key-1".to_string(),
        None,
        OperationOutcome::Committed,
    ));

    let report = check_shared_key_audits(
        &[history],
        &[SharedKeyAudit::new(
            0,
            vec![SharedKeyRead {
                key: "shared/key-1".to_string(),
                actual: None,
            }],
        )],
    );

    assert_eq!(report.checked_read_count, 1);
    assert_eq!(report.anomaly_count, 0);
    assert_eq!(report.unclassified_key_count, 0);
}

#[test]
fn shared_key_checker_reports_unexplained_final_state() {
    let mut history = OperationHistory::default();
    history.push(HistoryEvent::new(
        1,
        0,
        OperationKind::Put,
        "shared/key-1".to_string(),
        Some("v1".to_string()),
        OperationOutcome::Committed,
    ));

    let report = check_shared_key_audits(
        &[history],
        &[SharedKeyAudit::new(
            0,
            vec![SharedKeyRead {
                key: "shared/key-1".to_string(),
                actual: Some("v2".to_string()),
            }],
        )],
    );

    assert_eq!(report.anomaly_count, 1);
    assert!(matches!(
        report.anomalies[0].kind,
        crate::AnomalyKind::SharedFinalStateUnexplained
    ));
}

#[test]
fn shared_key_checker_accepts_read_with_valid_interleaving() {
    let mut client_zero = OperationHistory::default();
    client_zero.push(HistoryEvent::new(
        1,
        0,
        OperationKind::Put,
        "shared/key-1".to_string(),
        Some("v1".to_string()),
        OperationOutcome::Committed,
    ));
    client_zero.push(HistoryEvent::new(
        2,
        0,
        OperationKind::Read,
        "shared/key-1".to_string(),
        Some("v2".to_string()),
        OperationOutcome::Committed,
    ));
    let mut client_one = OperationHistory::default();
    client_one.push(HistoryEvent::new(
        1,
        1,
        OperationKind::Put,
        "shared/key-1".to_string(),
        Some("v2".to_string()),
        OperationOutcome::Committed,
    ));

    let report = check_shared_key_audits(
        &[client_zero, client_one],
        &[SharedKeyAudit::new(
            0,
            vec![SharedKeyRead {
                key: "shared/key-1".to_string(),
                actual: Some("v2".to_string()),
            }],
        )],
    );

    assert_eq!(report.checked_history_read_count, 1);
    assert_eq!(report.anomaly_count, 0);
}

#[test]
fn shared_key_checker_rejects_read_without_valid_interleaving() {
    let mut history = OperationHistory::default();
    history.push(HistoryEvent::new(
        1,
        0,
        OperationKind::Put,
        "shared/key-1".to_string(),
        Some("v1".to_string()),
        OperationOutcome::Committed,
    ));
    history.push(HistoryEvent::new(
        2,
        0,
        OperationKind::Read,
        "shared/key-1".to_string(),
        None,
        OperationOutcome::Committed,
    ));

    let report = check_shared_key_audits(
        &[history],
        &[SharedKeyAudit::new(
            0,
            vec![SharedKeyRead {
                key: "shared/key-1".to_string(),
                actual: Some("v1".to_string()),
            }],
        )],
    );

    assert_eq!(report.anomaly_count, 1);
    assert!(matches!(
        report.anomalies[0].kind,
        crate::AnomalyKind::SharedHistoryNotSerializable
    ));
}

#[test]
fn shared_key_checker_marks_unknown_commit_keys_unclassified() {
    let mut history = OperationHistory::default();
    history.push(HistoryEvent::new(
        1,
        0,
        OperationKind::Put,
        "shared/key-1".to_string(),
        Some("v1".to_string()),
        OperationOutcome::Unknown {
            error: "commit_unknown_result".to_string(),
        },
    ));

    let report = check_shared_key_audits(
        &[history],
        &[SharedKeyAudit::new(
            0,
            vec![SharedKeyRead {
                key: "shared/key-1".to_string(),
                actual: Some("v1".to_string()),
            }],
        )],
    );

    assert_eq!(report.anomaly_count, 0);
    assert_eq!(report.unclassified_keys, vec!["shared/key-1".to_string()]);
}

#[test]
fn shared_key_checker_rejects_reads_that_violate_realtime_order() {
    let mut client_zero = OperationHistory::default();
    client_zero.push(HistoryEvent::with_sim_interval(
        1,
        0,
        10,
        20,
        OperationKind::Put,
        "shared/key-1".to_string(),
        Some("v1".to_string()),
        OperationOutcome::Committed,
    ));
    let mut client_one = OperationHistory::default();
    client_one.push(HistoryEvent::with_sim_interval(
        1,
        1,
        30,
        40,
        OperationKind::Read,
        "shared/key-1".to_string(),
        None,
        OperationOutcome::Committed,
    ));

    let report = check_shared_key_audits(
        &[client_zero, client_one],
        &[SharedKeyAudit::new(
            0,
            vec![SharedKeyRead {
                key: "shared/key-1".to_string(),
                actual: Some("v1".to_string()),
            }],
        )],
    );

    assert_eq!(report.checked_order_constraint_count, 1);
    assert_eq!(report.anomaly_count, 1);
    assert!(matches!(
        report.anomalies[0].kind,
        crate::AnomalyKind::SharedHistoryNotSerializable
    ));
}
