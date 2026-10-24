use storage_types::{
    IndexName, MaxIndexers, SemanticDecision, StorageError, StorageResult, StoredTableInfo,
    StreamRetentionDuration, TableName, TableStatus, TimestampMillis, UpdateTableRequest,
};

use super::{
    GsiReadinessSnapshot,
    table_update_transaction::{TableUpdateChange, TableUpdateRetention, TableUpdateTransaction},
};
use crate::{StreamTrimScope, StreamTrimState, plan_table_stream_duration};

struct ObservedUpdate {
    table: StoredTableInfo,
    retention: Option<StreamTrimState>,
    calls: Vec<&'static str>,
    fail_persist: bool,
    rejected: bool,
    readiness: Vec<super::GsiReadinessRecord>,
}

impl ObservedUpdate {
    fn new() -> Self {
        Self {
            table: StoredTableInfo {
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
                table_stream_duration: StreamRetentionDuration::FiniteHours(1),
                default_item_stream_duration: StreamRetentionDuration::Forever,
                deletion_protection_enabled: false,
            },
            retention: None,
            calls: Vec::new(),
            fail_persist: false,
            rejected: false,
            readiness: Vec::new(),
        }
    }

    fn request(&self, duration: Option<u64>) -> UpdateTableRequest {
        let mut value = serde_json::json!({"TableName": self.table.table_name, "DeletionProtectionEnabled": true});
        if let Some(hours) = duration {
            value["AuxStreamDurationHours"] = hours.into();
        }
        serde_json::from_value(value).unwrap()
    }
}

impl TableUpdateTransaction for ObservedUpdate {
    type Catalog = StoredTableInfo;

    async fn observe_table_update(
        &mut self,
        _: &TableName,
    ) -> StorageResult<Option<StoredTableInfo>> {
        self.calls.push("metadata");
        Ok(Some(self.table.clone()))
    }

    async fn observe_table_retention(
        &mut self,
        catalog: &StoredTableInfo,
    ) -> StorageResult<TableUpdateRetention> {
        self.calls.push("retention");
        Ok(TableUpdateRetention {
            scope: StreamTrimScope::table("table-scope", catalog.table_name.clone()),
            state: self.retention.clone(),
        })
    }

    async fn load_index_name_lock(
        &mut self,
        _: &StoredTableInfo,
        _: &IndexName,
    ) -> StorageResult<Option<TimestampMillis>> {
        self.calls.push("name-lock");
        Ok(None)
    }

    async fn persist_table_update(
        &mut self,
        _: StoredTableInfo,
        change: TableUpdateChange,
    ) -> StorageResult<StoredTableInfo> {
        self.calls.push("persist");
        if self.fail_persist {
            return Err(StorageError::internal("injected persistence failure"));
        }
        let (table, retention, _) = change.into_parts();
        self.table = table.next_table;
        if let Some(write) = retention {
            self.retention = Some(write.state);
        }
        Ok(self.table.clone())
    }

    async fn observe_table_update_readiness(
        &mut self,
        _: &StoredTableInfo,
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
    let request = operation.request(Some(7));
    assert!(matches!(
        operation
            .execute_table_update(&request, TimestampMillis::now())
            .await,
        Ok(SemanticDecision::Rejected(_))
    ));
    assert_eq!(operation.calls, ["metadata"]);
}

#[tokio::test]
async fn given_lower_capacity_when_retention_changes_then_reject_before_effects() {
    let mut operation = ObservedUpdate::new();
    let mut request = operation.request(Some(7));
    request.max_indexers = Some(MaxIndexers::try_new(1).unwrap());
    assert!(matches!(
        operation
            .execute_table_update(&request, TimestampMillis::now())
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
        .execute_table_update(&request, TimestampMillis::now())
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
    let request = operation.request(Some(1));
    let captured = TimestampMillis::from_timestamp(17_000);
    for generation in 1..=3 {
        operation
            .execute_table_update(&request, captured)
            .await
            .and_then(|decision| decision.into_result())
            .unwrap();
        let state = operation.retention.as_ref().unwrap();
        assert_eq!(state.policy_version, generation);
        assert_eq!(state.updated_at, captured);
        assert_eq!(state.retention, StreamRetentionDuration::FiniteHours(1));
    }
}

#[tokio::test]
async fn given_generation_overflow_when_updated_then_no_effects_are_persisted() {
    let mut operation = ObservedUpdate::new();
    operation.retention = Some(
        plan_table_stream_duration(
            operation.table.table_name.clone(),
            "table-scope",
            u64::MAX,
            StreamRetentionDuration::FiniteHours(1),
            StreamRetentionDuration::Forever,
            TimestampMillis::from_timestamp(0),
        )
        .trim_state,
    );
    let request = operation.request(Some(7));
    assert!(
        operation
            .execute_table_update(&request, TimestampMillis::now())
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
    let request = operation.request(Some(7));
    assert!(
        operation
            .execute_table_update(&request, TimestampMillis::now())
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
        .execute_table_update(&request, TimestampMillis::now())
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
    assert_eq!(indexes[1].backfilling, Some(false));
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
            .execute_table_update(&request, TimestampMillis::now())
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
        "AuxStreamDurationHours": 7,
        "ReplicaUpdates": [{"Create": {"RegionName": "us-east-2"}}]
    }))
    .unwrap();
    let error = operation
        .execute_table_update(&request, TimestampMillis::now())
        .await
        .and_then(|decision| decision.into_result())
        .unwrap_err();
    use storage_types::context::WrappedError as _;
    assert!(
        matches!(error.to_enum(), storage_types::StorageEnum::Unsupported { message } if message == "multi-region replication is not available")
    );
    assert!(operation.calls.is_empty());
    assert!(!operation.table.deletion_protection_enabled);
    assert!(operation.retention.is_none());
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
