use storage_types::{
    BoundTableInfo, IndexName, MaxIndexers, SemanticDecision, StorageError, StorageResult,
    StoredTableInfo, StreamRetentionDuration, TableName, TableStatus, TimestampMillis,
    UpdateTableRequest,
};

use super::{
    GsiReadinessSnapshot,
    table_update_transaction::{TableUpdateChange, TableUpdateRetention, TableUpdateTransaction},
};
use crate::{StreamTrimState, plan_table_stream_duration};

struct ObservedUpdate {
    table: BoundTableInfo,
    retention: Option<StreamTrimState>,
    calls: Vec<&'static str>,
    fail_persist: bool,
    rejected: bool,
    readiness: Vec<super::GsiReadinessRecord>,
    label_clock: Option<TimestampMillis>,
}

impl ObservedUpdate {
    fn new() -> Self {
        Self {
            table: BoundTableInfo {
                binding: Default::default(),
                history_id: 1_u32.into(),
                info: std::sync::Arc::new(StoredTableInfo {
                    table_name: TableName::new("table-update"),
                    table_status: TableStatus::Active,
                    created_at: TimestampMillis::from_timestamp(0),
                    attribute_definitions: vec![],
                    key_schema: vec![],
                    max_indexers: MaxIndexers::try_new(2).unwrap(),
                    global_secondary_indexes: None,
                    table_size_bytes: 0,
                    item_count: 0,
                    stream_specification: None,
                    stream_enabled_at: None,
                    table_stream_duration: StreamRetentionDuration::from_hours(24).unwrap(),
                    deletion_protection_enabled: false,
                }),
            },
            retention: Some(
                plan_table_stream_duration(
                    crate::retention_test_support::scope("table-update"),
                    crate::retention_test_support::frontier(1),
                    StreamRetentionDuration::from_hours(24).unwrap(),
                    TimestampMillis::from_timestamp(0),
                )
                .unwrap()
                .state,
            ),
            calls: Vec::new(),
            fail_persist: false,
            rejected: false,
            readiness: Vec::new(),
            label_clock: None,
        }
    }

    fn enable_request(&self) -> UpdateTableRequest {
        serde_json::from_value(serde_json::json!({
            "TableName": self.table.table_name,
            "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_IMAGE"}
        }))
        .unwrap()
    }

    fn request(&self, duration: Option<u64>) -> UpdateTableRequest {
        let mut value = serde_json::json!({"TableName": self.table.table_name, "DeletionProtectionEnabled": true});
        if let Some(hours) = duration {
            value["AuxStreamDurationHours"] = hours.into();
        }
        serde_json::from_value(value).unwrap()
    }
}

impl crate::StreamLabelClockAttempt for ObservedUpdate {
    async fn load_stream_label_clock(&mut self) -> StorageResult<Option<TimestampMillis>> {
        self.calls.push("clock-load");
        Ok(self.label_clock)
    }

    async fn store_stream_label_clock(&mut self, stamp: TimestampMillis) -> StorageResult<()> {
        self.calls.push("clock-store");
        self.label_clock = Some(stamp);
        Ok(())
    }
}

impl TableUpdateTransaction for ObservedUpdate {
    async fn load_stream_source(&mut self) -> StorageResult<stream_provider::SystemStreamHistory> {
        self.calls.push("source");
        Err(StorageError::internal("policy model has no stream source"))
    }

    async fn observe_table_update(
        &mut self,
        _: &TableName,
    ) -> StorageResult<Option<BoundTableInfo>> {
        self.calls.push("metadata");
        Ok(Some(self.table.clone()))
    }

    async fn observe_table_retention(
        &mut self,
        catalog: &BoundTableInfo,
    ) -> StorageResult<TableUpdateRetention> {
        self.calls.push("retention");
        Ok(TableUpdateRetention {
            scope: crate::retention_test_support::scope(catalog.table_name.as_ref()),
            state: self.retention.clone(),
        })
    }

    async fn load_index_name_lock(
        &mut self,
        _: &BoundTableInfo,
        _: &IndexName,
    ) -> StorageResult<Option<TimestampMillis>> {
        self.calls.push("name-lock");
        Ok(None)
    }

    async fn persist_table_update(
        &mut self,
        _: BoundTableInfo,
        change: TableUpdateChange,
    ) -> StorageResult<BoundTableInfo> {
        self.calls.push("persist");
        if self.fail_persist {
            return Err(StorageError::internal("injected persistence failure"));
        }
        let (table, retention, _) = change.into_parts();
        self.table.info = std::sync::Arc::new(table.next_table);
        if let Some(write) = retention {
            self.retention = Some(write.state);
        }
        Ok(self.table.clone())
    }

    async fn observe_table_update_readiness(
        &mut self,
        _: &BoundTableInfo,
    ) -> StorageResult<GsiReadinessSnapshot> {
        self.calls.push("readiness");
        Ok(GsiReadinessSnapshot::from(std::mem::take(
            &mut self.readiness,
        )))
    }
}

#[tokio::test]
async fn given_inactive_table_when_settings_change_then_reject_before_effects() {
    let mut operation = ObservedUpdate::new();
    operation.table.table_status = TableStatus::Updating;
    let request = operation.request(Some(48));
    assert!(matches!(
        operation
            .execute_table_update(
                &request,
                TimestampMillis::now(),
                &crate::RequestTableBindings::default()
            )
            .await,
        Ok(SemanticDecision::Rejected(_))
    ));
    assert_eq!(operation.calls, ["metadata"]);
}

#[tokio::test]
async fn given_lower_capacity_when_retention_changes_then_reject_before_effects() {
    let mut operation = ObservedUpdate::new();
    let mut request = operation.request(Some(48));
    request.max_indexers = Some(MaxIndexers::try_new(1).unwrap());
    assert!(matches!(
        operation
            .execute_table_update(
                &request,
                TimestampMillis::now(),
                &crate::RequestTableBindings::default()
            )
            .await,
        Ok(SemanticDecision::Rejected(_))
    ));
    assert_eq!(operation.calls, ["metadata"]);
}

#[tokio::test]
async fn given_no_duration_request_when_settings_change_then_retention_is_not_observed() {
    let mut operation = ObservedUpdate::new();
    let request = operation.request(None);
    let (_, response) = operation
        .execute_table_update(
            &request,
            TimestampMillis::now(),
            &crate::RequestTableBindings::default(),
        )
        .await
        .and_then(|decision| decision.into_result())
        .unwrap();
    assert!(operation.table.deletion_protection_enabled);
    assert!(response.table_description.deletion_protection_enabled);
    assert_eq!(operation.calls, ["metadata", "persist"]);
}

#[tokio::test]
async fn given_repeated_duration_when_updated_then_generation_advances_with_captured_time() {
    let mut operation = ObservedUpdate::new();
    let request = operation.request(Some(24));
    let captured = TimestampMillis::from_timestamp(17_000);
    let frontier = &mut operation.retention.as_mut().unwrap().frontier;
    frontier.lineages[0].retired_through =
        Some(storage_types::SystemStreamOffset::try_new("12").unwrap());
    frontier.lineages[0].cleaned_through =
        Some(storage_types::SystemStreamOffset::try_new("9").unwrap());
    let original = frontier.clone();
    for generation in 2..=4 {
        operation
            .execute_table_update(&request, captured, &crate::RequestTableBindings::default())
            .await
            .and_then(|decision| decision.into_result())
            .unwrap();
        let state = operation.retention.as_ref().unwrap();
        assert_eq!(state.policy_version, generation);
        assert_eq!(state.frontier.table_policy_version.get(), generation);
        assert_eq!(state.frontier.source_scope, original.source_scope);
        assert_eq!(state.frontier.lineages, original.lineages);
        assert_eq!(state.updated_at, captured);
        assert_eq!(
            state.retention,
            StreamRetentionDuration::from_hours(24).unwrap()
        );
    }
}

#[tokio::test]
async fn given_generation_overflow_when_updated_then_no_effects_are_persisted() {
    let mut operation = ObservedUpdate::new();
    operation.retention = Some(
        plan_table_stream_duration(
            crate::retention_test_support::scope("table-update"),
            crate::retention_test_support::frontier(u64::MAX),
            StreamRetentionDuration::from_hours(24).unwrap(),
            TimestampMillis::from_timestamp(0),
        )
        .unwrap()
        .state,
    );
    let request = operation.request(Some(48));
    assert!(
        operation
            .execute_table_update(
                &request,
                TimestampMillis::now(),
                &crate::RequestTableBindings::default()
            )
            .await
            .is_err()
    );
    assert_eq!(operation.calls, ["metadata", "retention"]);
    assert!(!operation.table.deletion_protection_enabled);
}

#[tokio::test]
async fn given_persistence_failure_when_updated_then_no_response_observation_follows() {
    let mut operation = ObservedUpdate::new();
    operation.fail_persist = true;
    let request = operation.request(Some(48));
    assert!(
        operation
            .execute_table_update(
                &request,
                TimestampMillis::now(),
                &crate::RequestTableBindings::default()
            )
            .await
            .is_err()
    );
    assert_eq!(operation.calls, ["metadata", "retention", "persist"]);
    assert!(!operation.table.deletion_protection_enabled);
}

#[tokio::test]
async fn given_unordered_readiness_when_updated_then_response_matches_index_identity() {
    let mut operation = ObservedUpdate::new();
    operation.table.global_secondary_indexes = Some(serde_json::from_value(serde_json::json!([
        {"IndexName": "first-index", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}], "Projection": {"ProjectionType": "ALL"}},
        {"IndexName": "second-index", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}], "Projection": {"ProjectionType": "ALL"}}
    ])).unwrap());
    operation.readiness = vec![
        super::GsiReadinessRecord {
            table_name: operation.table.table_name.clone(),
            index_name: storage_types::IndexName::new("second-index"),
            readiness: super::GsiIndexReadiness {
                index_status: storage_types::IndexStatus::Active,
                backfilling: false,
            },
        },
        super::GsiReadinessRecord {
            table_name: operation.table.table_name.clone(),
            index_name: storage_types::IndexName::new("first-index"),
            readiness: super::GsiIndexReadiness {
                index_status: storage_types::IndexStatus::Creating,
                backfilling: true,
            },
        },
    ];
    let request = operation.request(None);
    let (_, response) = operation
        .execute_table_update(
            &request,
            TimestampMillis::now(),
            &crate::RequestTableBindings::default(),
        )
        .await
        .and_then(|decision| decision.into_result())
        .unwrap();
    let indexes = response.table_description.global_secondary_indexes.unwrap();
    assert!(matches!(
        indexes[0].index_status,
        Some(storage_types::IndexStatus::Creating)
    ));
    assert_eq!(indexes[0].backfilling, Some(true));
    assert!(matches!(
        indexes[1].index_status,
        Some(storage_types::IndexStatus::Active)
    ));
    // AWS shows `Backfilling` only while an index is not ACTIVE, and the table
    // is UPDATING while one of its indexes builds.
    assert_eq!(indexes[1].backfilling, None);
    assert_eq!(
        response.table_description.table_status,
        storage_types::TableStatus::Updating
    );
    assert_eq!(operation.calls, ["metadata", "persist", "readiness"]);
}

#[tokio::test]
async fn given_missing_readiness_when_updated_then_no_success_response_is_returned() {
    let mut operation = ObservedUpdate::new();
    operation.table.global_secondary_indexes = Some(serde_json::from_value(serde_json::json!([
        {"IndexName": "missing-index", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}], "Projection": {"ProjectionType": "ALL"}}
    ])).unwrap());
    let request = operation.request(None);
    assert!(
        operation
            .execute_table_update(
                &request,
                TimestampMillis::now(),
                &crate::RequestTableBindings::default()
            )
            .await
            .is_err()
    );
    assert_eq!(operation.calls, ["metadata", "persist", "readiness"]);
}

#[tokio::test]
async fn given_replica_update_with_settings_when_executed_then_reject_before_effects() {
    let mut operation = ObservedUpdate::new();
    let request = serde_json::from_value(serde_json::json!({
        "TableName": operation.table.table_name,
        "DeletionProtectionEnabled": true,
        "AuxStreamDurationHours": 48,
        "ReplicaUpdates": [{"Create": {"RegionName": "us-east-2"}}]
    }))
    .unwrap();
    let error = operation
        .execute_table_update(
            &request,
            TimestampMillis::now(),
            &crate::RequestTableBindings::default(),
        )
        .await
        .and_then(|decision| decision.into_result())
        .unwrap_err();
    use storage_types::context::WrappedError as _;
    assert!(
        matches!(error.to_enum(), storage_types::StorageEnum::Unsupported { message } if message == "multi-region replication is not available")
    );
    assert!(operation.calls.is_empty());
    assert!(!operation.table.deletion_protection_enabled);
    assert_eq!(operation.retention.as_ref().unwrap().policy_version, 1);
}

#[tokio::test]
async fn given_never_enabled_stream_when_enabled_then_stamp_is_captured_time_and_clock_advances() {
    let mut operation = ObservedUpdate::new();
    let captured = TimestampMillis::from_timestamp(17_000);
    let request = operation.enable_request();
    operation
        .execute_table_update(&request, captured, &crate::RequestTableBindings::default())
        .await
        .and_then(|decision| decision.into_result())
        .unwrap();
    assert_eq!(operation.table.stream_enabled_at, Some(captured));
    assert_eq!(operation.label_clock, Some(captured));
    // A first enable has no earlier stream to retire.
    assert!(!operation.calls.contains(&"source"));
    assert!(operation.calls.contains(&"clock-store"));
}

#[tokio::test]
async fn given_store_clock_not_before_now_when_enabled_then_stamp_follows_clock() {
    let mut operation = ObservedUpdate::new();
    let captured = TimestampMillis::from_timestamp(17_000);
    operation.label_clock = Some(captured + 5);
    let request = operation.enable_request();
    let (_, response) = operation
        .execute_table_update(&request, captured, &crate::RequestTableBindings::default())
        .await
        .and_then(|decision| decision.into_result())
        .unwrap();
    assert_eq!(operation.table.stream_enabled_at, Some(captured + 6));
    assert_eq!(operation.label_clock, Some(captured + 6));
    assert_eq!(
        response.table_description.latest_stream_label.as_deref(),
        Some("1970-01-01T00:00:17.006")
    );
}

#[tokio::test]
async fn given_earlier_enable_when_reenabled_then_stream_source_is_read_before_effects() {
    let mut operation = ObservedUpdate::new();
    operation.table.stream_specification = Some(storage_types::StreamSpecification {
        stream_enabled: false,
        stream_view_type: Some(storage_types::StreamViewType::NewImage),
    });
    operation.table.stream_enabled_at = Some(TimestampMillis::from_timestamp(100));
    let request = operation.enable_request();
    assert!(
        operation
            .execute_table_update(
                &request,
                TimestampMillis::from_timestamp(17_000),
                &crate::RequestTableBindings::default()
            )
            .await
            .is_err()
    );
    assert_eq!(operation.calls, ["metadata", "retention", "source"]);
    assert_eq!(
        operation.table.stream_enabled_at,
        Some(TimestampMillis::from_timestamp(100))
    );
    assert_eq!(operation.label_clock, None);
}

fn restart_retention(
    retired: Option<&str>,
    bound: &str,
    captured: TimestampMillis,
) -> crate::StreamTrimState {
    let mut operation = ObservedUpdate::new();
    if let Some(retired) = retired {
        operation.retention.as_mut().unwrap().frontier.lineages[0].retired_through =
            Some(storage_types::SystemStreamOffset::try_new(retired).unwrap());
    }
    let mut change =
        TableUpdateChange::try_new(&operation.table, &operation.enable_request(), captured)
            .unwrap();
    let bound = storage_types::SystemStreamPosition::try_new("root", "root", bound).unwrap();
    change
        .refresh_retention(
            TableUpdateRetention {
                scope: crate::retention_test_support::scope("table-update"),
                state: operation.retention.clone(),
            },
            captured,
            Some(std::slice::from_ref(&bound)),
        )
        .unwrap();
    let (_, write, _) = change.into_parts();
    write.unwrap().state
}

#[test]
fn given_restart_bounds_when_retention_refreshed_then_earlier_history_is_retired_and_due_now() {
    let captured = TimestampMillis::from_timestamp(17_000);
    let state = restart_retention(None, "40", captured);
    assert_eq!(
        state.frontier.lineages[0].retired_through,
        Some(storage_types::SystemStreamOffset::try_new("40").unwrap())
    );
    assert_eq!(state.frontier.lineages[0].cleaned_through, None);
    assert_eq!(state.next_due_at, Some(captured));
}

#[test]
fn given_later_retired_prefix_when_restart_bounds_are_lower_then_prefix_does_not_shrink() {
    let state = restart_retention(Some("50"), "40", TimestampMillis::from_timestamp(17_000));
    assert_eq!(
        state.frontier.lineages[0].retired_through,
        Some(storage_types::SystemStreamOffset::try_new("50").unwrap())
    );
}

fn gsi_fixture() -> (
    ObservedUpdate,
    storage_types::maintenance::TargetIncarnation,
) {
    let mut operation = ObservedUpdate::new();
    let incarnation = storage_types::maintenance::TargetIncarnation::try_from(vec![9]).unwrap();
    operation.table.global_secondary_indexes = Some(serde_json::from_value(serde_json::json!([
        {"IndexName": "g1", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}], "Projection": {"ProjectionType": "ALL"}}
    ])).unwrap());
    let index = operation.table.global_secondary_indexes.as_ref().unwrap()[0].clone();
    operation.table.binding = storage_types::TableBinding::for_info(&operation.table)
        .with_added_gsi(
            &index,
            &operation.table.attribute_definitions,
            incarnation.clone(),
            storage_types::GsiPhase::Active,
        )
        .unwrap();
    (operation, incarnation)
}

fn plan_for(
    operation: &ObservedUpdate,
    request: serde_json::Value,
) -> storage_common::GsiTableUpdatePlan {
    let request: UpdateTableRequest = serde_json::from_value(request).unwrap();
    let change = TableUpdateChange::try_new(
        &operation.table,
        &request,
        TimestampMillis::from_timestamp(0),
    )
    .unwrap();
    let (plan, _, _) = change.into_parts();
    plan
}

fn delete_g1_plan(operation: &ObservedUpdate) -> storage_common::GsiTableUpdatePlan {
    plan_for(
        operation,
        serde_json::json!({
            "TableName": operation.table.table_name,
            "GlobalSecondaryIndexUpdates": [{"Delete": {"IndexName": "g1"}}]
        }),
    )
}

#[test]
fn given_gsi_delete_on_native_family_then_retirement_carries_bound_gsi() {
    let (operation, incarnation) = gsi_fixture();
    let plan = delete_g1_plan(&operation);
    let retirement =
        super::table_update_transaction::deleted_index_retirement(&operation.table, &plan, true)
            .unwrap()
            .expect("a GSI delete registers a retirement");
    assert!(retirement.target.incarnation == incarnation);
    assert_eq!(retirement.target.index_name, IndexName::new("g1"));
    assert_eq!(retirement.target.history_id, operation.table.history_id);
    assert_eq!(
        retirement.native_index,
        operation.table.binding.gsi(&IndexName::new("g1")).cloned()
    );
    assert!(retirement.native_index.is_some());
}

#[test]
fn given_gsi_delete_on_key_value_family_then_retirement_has_no_native_index() {
    let (operation, incarnation) = gsi_fixture();
    let plan = delete_g1_plan(&operation);
    let retirement =
        super::table_update_transaction::deleted_index_retirement(&operation.table, &plan, false)
            .unwrap()
            .expect("a GSI delete registers a retirement");
    assert!(retirement.target.incarnation == incarnation);
    assert_eq!(retirement.target.index_name, IndexName::new("g1"));
    assert_eq!(retirement.target.history_id, operation.table.history_id);
    assert!(retirement.native_index.is_none());
}

#[test]
fn given_no_gsi_lifecycle_then_no_retirement() {
    let (operation, _) = gsi_fixture();
    let plan = plan_for(
        &operation,
        serde_json::json!({
            "TableName": operation.table.table_name,
            "AuxStreamDurationHours": 48
        }),
    );
    assert!(
        super::table_update_transaction::deleted_index_retirement(&operation.table, &plan, true)
            .unwrap()
            .is_none()
    );
}

#[test]
fn given_deleted_gsi_absent_from_binding_then_internal() {
    let (mut operation, _) = gsi_fixture();
    let plan = delete_g1_plan(&operation);
    operation.table.binding = Default::default();
    let Err(error) =
        super::table_update_transaction::deleted_index_retirement(&operation.table, &plan, true)
    else {
        panic!("a deleted GSI without a binding entry must be rejected");
    };
    assert!(matches!(
        error.as_ref(),
        storage_types::StorageEnum::InternalServerError { message }
            if message == "deleted GSI is absent from the catalog binding"
    ));
}

// These deterministic policy fixtures do not model native durability.
impl infrastructure_provider::physical_attempt::PhysicalRecordAttempt for ObservedUpdate {
    fn read_protection(&self) -> storage_types::ReadProtection {
        storage_types::ReadProtection::Conflict
    }
    fn reject_settlement(&mut self) {
        self.rejected = true;
    }
    fn reject_settlement_error(&mut self, _: &storage_types::StorageError) {
        self.reject_settlement();
    }
    async fn has_records_in_namespace(
        &mut self,
        _: &str,
        _: storage_types::ReadProtection,
    ) -> StorageResult<bool> {
        panic!("policy model must not access protected physical records")
    }
    async fn find_record_generation(
        &mut self,
        _: &infrastructure_provider::record::DurableRecordKey,
        _: u64,
        _: storage_types::ReadProtection,
    ) -> StorageResult<Option<u64>> {
        panic!("policy model must not access protected physical records")
    }
    async fn load_records(
        &mut self,
        _: &[infrastructure_provider::record::DurableRecordKey],
        _: storage_types::ReadProtection,
        _: infrastructure_provider::limits::InfrastructureLimits,
    ) -> StorageResult<Vec<Option<infrastructure_provider::record::DurableRecord>>> {
        panic!("policy model must not access protected physical records")
    }
    async fn scan_records(
        &mut self,
        _: infrastructure_provider::page::RecordPageRequest,
        _: infrastructure_provider::limits::InfrastructureLimits,
    ) -> StorageResult<infrastructure_provider::page::RecordPage> {
        panic!("policy model must not access protected physical records")
    }
    async fn store_records(
        &mut self,
        _: &infrastructure_provider::batch::RecordBatch<'_>,
    ) -> StorageResult<infrastructure_provider::record::ConditionalWriteResult> {
        panic!("policy model must not access protected physical records")
    }
}

#[tokio::test]
async fn given_update_abort_when_name_is_recreated_then_retry_stops_before_effects() {
    let mut operation = ObservedUpdate::new();
    let request = operation.request(Some(48));
    let bindings = crate::RequestTableBindings::default();
    operation.fail_persist = true;
    assert!(
        operation
            .execute_table_update(&request, TimestampMillis::now(), &bindings)
            .await
            .is_err()
    );
    assert!(operation.calls.contains(&"persist"));
    operation.calls.clear();
    operation.fail_persist = false;
    operation.table.history_id = 2_u32.into();
    let error = operation
        .execute_table_update(&request, TimestampMillis::now(), &bindings.clone())
        .await
        .err()
        .unwrap();
    assert!(matches!(
        storage_types::context::WrappedError::to_enum(&error),
        storage_types::StorageEnum::TableNotFound { .. }
    ));
    assert_eq!(operation.calls, ["metadata"]);
}
