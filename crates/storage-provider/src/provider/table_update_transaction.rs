//! Complete table-update decisions within a caller-owned native transaction.

use std::{borrow::Borrow, future::Future};

use storage_common::GsiTableUpdatePlan;
use storage_types::{
    IndexName, StorageError, StorageResult, StoredTableInfo, TableDescription, TableName,
    TableStatus, TimestampMillis, UpdateTableRequest, UpdateTableResponse,
};

use super::GsiReadinessSnapshot;
use crate::{SemanticDecision, StreamTrimScope, StreamTrimState, StreamTrimStateWrite};

pub struct TableUpdateRetention {
    pub scope: StreamTrimScope,
    pub state: Option<StreamTrimState>,
}

/// A content-bound effect timestamp and the participant's local decision clock.
pub struct TableUpdateTime {
    captured_at: TimestampMillis,
    observed_at: TimestampMillis,
}

impl From<TimestampMillis> for TableUpdateTime {
    fn from(captured_at: TimestampMillis) -> Self {
        Self {
            captured_at,
            observed_at: captured_at,
        }
    }
}

impl TableUpdateTime {
    pub fn for_control(captured_at: TimestampMillis, observed_at: TimestampMillis) -> Self {
        Self {
            captured_at,
            observed_at,
        }
    }
}

/// Only the shared operation constructs this complete, validated decision.
pub struct TableUpdateChange {
    table: GsiTableUpdatePlan,
    retention: Option<StreamTrimStateWrite>,
    name_locked_until: TimestampMillis,
}

impl TableUpdateChange {
    fn try_new(
        current: &StoredTableInfo,
        request: &UpdateTableRequest,
        observed_at: TimestampMillis,
    ) -> StorageResult<Self> {
        if current.table_status != TableStatus::Active {
            return Err(StorageError::validation("table must be ACTIVE to update"));
        }
        let mut table = GsiTableUpdatePlan::try_new(current, request)?;
        if let Some(capacity) = current
            .max_indexers
            .requested_increase(request.max_indexers)?
        {
            table.next_table.max_indexers = capacity;
        }
        if let Some(specification) = &request.stream_specification {
            table.next_table.stream_specification = Some(specification.clone());
        }
        if let Some(enabled) = request.deletion_protection_enabled {
            table.next_table.deletion_protection_enabled = enabled;
        }
        Ok(Self {
            table,
            retention: None,
            name_locked_until: crate::name_locked_until(observed_at),
        })
    }

    pub(crate) fn for_ttl_policy(
        current: &StoredTableInfo,
        change: &storage_common::ttl::TtlPolicyChange,
    ) -> Option<Self> {
        use storage_common::{GsiLifecycleAction, ttl::TtlPolicyChange};
        use storage_types::{
            CreateGlobalSecondaryIndex, DeleteGlobalSecondaryIndexAction, KeySchemaElement,
            KeyType, Projection,
        };
        let (config, lifecycle) = match change {
            TtlPolicyChange::Unchanged => return None,
            TtlPolicyChange::Enable(config) => (
                config,
                GsiLifecycleAction::Create(CreateGlobalSecondaryIndex {
                    index_name: config.gsi_name.clone(),
                    key_schema: vec![KeySchemaElement {
                        attribute_name: config.attribute_name.clone(),
                        key_type: KeyType::Range,
                    }],
                    projection: Projection {
                        projection_type: None,
                        non_key_attributes: None,
                    },
                    provisioned_throughput: None,
                }),
            ),
            TtlPolicyChange::Disable(config) => (
                config,
                GsiLifecycleAction::Delete(DeleteGlobalSecondaryIndexAction {
                    index_name: config.gsi_name.clone(),
                }),
            ),
        };
        let mut next_table = current.clone();
        let indexes = next_table
            .global_secondary_indexes
            .get_or_insert_with(Vec::new);
        indexes.retain(|index| index.index_name != config.gsi_name);
        if let GsiLifecycleAction::Create(create) = &lifecycle {
            indexes.push(create.clone().into());
        }
        if indexes.is_empty() {
            next_table.global_secondary_indexes = None;
        }
        Some(Self {
            table: GsiTableUpdatePlan {
                next_table,
                lifecycle: Some(lifecycle),
                attribute_definitions_changed: false,
            },
            retention: None,
            name_locked_until: crate::name_locked_until(config.updated_at),
        })
    }

    fn replace_retention(
        &mut self,
        request: &UpdateTableRequest,
        observation: TableUpdateRetention,
        captured_at: TimestampMillis,
    ) -> StorageResult<()> {
        let table = &mut self.table.next_table;
        let generation = StreamTrimState::next_policy_version(observation.state.as_ref())?;
        (
            table.table_stream_duration,
            table.default_item_stream_duration,
        ) = crate::merge_table_stream_duration_request(
            table.table_stream_duration,
            table.default_item_stream_duration,
            request.aux_stream_duration_hours,
            request.aux_default_item_stream_duration_hours,
        );
        let plan = crate::plan_table_stream_duration(
            table.table_name.clone(),
            observation.scope.scope_id,
            generation,
            table.table_stream_duration,
            table.default_item_stream_duration,
            captured_at,
        );
        self.retention = Some(
            StreamTrimStateWrite {
                state: plan.trim_state,
                next_marker: plan.due_marker,
            }
            .preserve_existing_history(observation.state.as_ref())?,
        );
        Ok(())
    }

    pub fn into_parts(
        self,
    ) -> (
        GsiTableUpdatePlan,
        Option<StreamTrimStateWrite>,
        TimestampMillis,
    ) {
        (self.table, self.retention, self.name_locked_until)
    }

    fn created_index_name(&self) -> Option<&IndexName> {
        match self.table.lifecycle.as_ref() {
            Some(storage_common::GsiLifecycleAction::Create(create)) => Some(&create.index_name),
            _ => None,
        }
    }
}

pub trait TableUpdateTransaction:
    infrastructure_provider::physical_attempt::PhysicalRecordAttempt
{
    type Catalog: Borrow<StoredTableInfo> + Send + Sync;

    fn observe_table_update(
        &mut self,
        table: &TableName,
    ) -> impl Future<Output = StorageResult<Option<Self::Catalog>>> + Send;

    fn observe_table_retention(
        &mut self,
        catalog: &Self::Catalog,
    ) -> impl Future<Output = StorageResult<TableUpdateRetention>> + Send;

    /// Read the physical name reservation within this update's native attempt.
    fn load_index_name_lock(
        &mut self,
        catalog: &Self::Catalog,
        name: &IndexName,
    ) -> impl Future<Output = StorageResult<Option<TimestampMillis>>> + Send;

    fn persist_table_update(
        &mut self,
        catalog: Self::Catalog,
        change: TableUpdateChange,
    ) -> impl Future<Output = StorageResult<Self::Catalog>> + Send;

    fn observe_table_update_readiness(
        &mut self,
        catalog: &Self::Catalog,
    ) -> impl Future<Output = StorageResult<GsiReadinessSnapshot>> + Send;

    fn prepare_table_update(
        &mut self,
        request: &UpdateTableRequest,
        time: TableUpdateTime,
    ) -> impl Future<Output = StorageResult<SemanticDecision<(Self::Catalog, TableUpdateChange)>>> + Send
    {
        async move {
            if let Err(error) = require_native_metadata_request(request) {
                return Ok(SemanticDecision::Rejected(error));
            }
            let Some(catalog) = self.observe_table_update(&request.table_name).await? else {
                return Ok(SemanticDecision::Rejected(StorageError::table_not_found(
                    &request.table_name,
                )));
            };
            let mut change =
                match TableUpdateChange::try_new(catalog.borrow(), request, time.observed_at) {
                    Ok(change) => change,
                    Err(error) => return Ok(SemanticDecision::Rejected(error)),
                };
            if crate::maintenance::update_table_request_has_custom_stream_duration(request) {
                let retention = self.observe_table_retention(&catalog).await?;
                change.replace_retention(request, retention, time.captured_at)?;
            }
            if let Some(name) = change.created_index_name()
                && self
                    .load_index_name_lock(&catalog, name)
                    .await?
                    .is_some_and(|until| crate::name_is_locked(time.observed_at, until))
            {
                return Ok(SemanticDecision::Rejected(StorageError::validation(
                    format!("GSI '{name}' name is reserved"),
                )));
            }
            Ok(SemanticDecision::Accepted((catalog, change)))
        }
    }

    fn execute_table_update(
        &mut self,
        request: &UpdateTableRequest,
        captured_at: TimestampMillis,
    ) -> impl Future<Output = StorageResult<SemanticDecision<(Self::Catalog, UpdateTableResponse)>>> + Send
    {
        async move {
            let mut settlement =
                infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
            let attempt = settlement.as_mut();
            let result = async {
                let (catalog, change) = match attempt
                    .prepare_table_update(request, captured_at.into())
                    .await?
                {
                    SemanticDecision::Accepted(prepared) => prepared,
                    SemanticDecision::Rejected(error) => {
                        return Ok(SemanticDecision::Rejected(error));
                    }
                };
                let updated = attempt.persist_table_update(catalog, change).await?;
                let response = describe_updated_table(attempt, &updated).await?;
                Ok(SemanticDecision::Accepted((updated, response)))
            }
            .await;
            settlement.complete(result)
        }
    }
}

/// Membership controls have their own shared domain owner and cannot be
/// reinterpreted as ordinary physical table updates.
fn require_native_metadata_request(request: &UpdateTableRequest) -> StorageResult<()> {
    if request.configuration_primary_region().is_some() {
        return Err(StorageError::unsupported(
            "regional primary transfer requires regional assembly",
        ));
    }
    if request
        .replica_updates
        .as_ref()
        .is_some_and(|updates| !updates.is_empty())
    {
        return Err(StorageError::unsupported(
            "multi-region replication is not available",
        ));
    }
    Ok(())
}

async fn describe_updated_table<A: TableUpdateTransaction + ?Sized>(
    attempt: &mut A,
    catalog: &A::Catalog,
) -> StorageResult<UpdateTableResponse> {
    let table = catalog.borrow();
    let readiness = if table
        .global_secondary_indexes
        .as_deref()
        .unwrap_or_default()
        .is_empty()
    {
        GsiReadinessSnapshot::from(Vec::new())
    } else {
        attempt.observe_table_update_readiness(catalog).await?
    };
    let mut table_description =
        TableDescription::from_stored_table_info(table.clone(), TableStatus::Active);
    readiness.apply_to_description(&mut table_description)?;
    Ok(UpdateTableResponse { table_description })
}
