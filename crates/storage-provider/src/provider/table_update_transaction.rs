//! Complete table-update decisions within a caller-owned native transaction.

use std::future::Future;

use storage_common::GsiTableUpdatePlan;
use storage_types::{
    BoundTableInfo, IndexName, StorageError, StorageResult, StoredTableInfo, StreamSpecification,
    StreamViewType, TableDescription, TableName, TableStatus, TimestampMillis, UpdateTableRequest,
    UpdateTableResponse,
};

use super::GsiReadinessSnapshot;
use crate::{
    RequestTableBindings, SemanticDecision, StreamTrimScope, StreamTrimState, StreamTrimStateWrite,
};

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
        let is_cancellation = current.table_status == TableStatus::Updating
            && current.has_public_stream_request()
            && request
                .stream_specification
                .as_ref()
                .is_some_and(|spec| !spec.stream_enabled);
        if current.table_status != TableStatus::Active && !is_cancellation {
            return Err(StorageError::validation("table must be ACTIVE to update"));
        }
        let mut table = GsiTableUpdatePlan::try_new(current, request)?;
        if let Some(capacity) = current
            .max_indexers
            .requested_increase(request.max_indexers)?
        {
            table.next_table.max_indexers = capacity;
        }
        Self::configure_stream(&mut table, current, request)?;
        if let Some(enabled) = request.deletion_protection_enabled {
            table.next_table.deletion_protection_enabled = enabled;
        }
        Ok(Self {
            table,
            retention: None,
            name_locked_until: crate::name_locked_until(observed_at),
        })
    }

    fn configure_stream(
        table: &mut GsiTableUpdatePlan,
        current: &StoredTableInfo,
        request: &UpdateTableRequest,
    ) -> StorageResult<()> {
        if let Some(specification) = &request.stream_specification {
            let specification =
                plan_stream_specification(current.stream_specification.as_ref(), specification)?;
            let stream_enabled = specification.stream_enabled;
            table.next_table.stream_specification = Some(specification);
            table.next_table.table_status =
                match (stream_enabled, current.has_public_stream_request()) {
                    (true, false) => TableStatus::Updating,
                    (false, _) => TableStatus::Active,
                    _ => current.table_status.clone(),
                };
        }
        if let Some(storage_common::GsiLifecycleAction::Create(create)) = &table.lifecycle
            && !table.next_table.has_public_stream_request()
        {
            let index = table
                .next_table
                .global_secondary_indexes
                .as_mut()
                .and_then(|indexes| {
                    indexes
                        .iter_mut()
                        .find(|index| index.index_name == create.index_name)
                })
                .ok_or_else(|| StorageError::internal("Created GSI is missing from its catalog"))?;
            index.capture_for_build = true;
        }
        Ok(())
    }

    pub(crate) fn complete_stream_initialization(current: &StoredTableInfo) -> Self {
        let mut next_table = current.clone();
        next_table.table_status = TableStatus::Active;
        for index in next_table.global_secondary_indexes.iter_mut().flatten() {
            index.capture_for_build = false;
        }
        Self {
            table: GsiTableUpdatePlan {
                next_table,
                lifecycle: None,
                attribute_definitions_changed: false,
            },
            retention: None,
            name_locked_until: current.created_at,
        }
    }

    pub(crate) fn release_gsi_capture(current: &StoredTableInfo, name: &IndexName) -> Option<Self> {
        if !current
            .global_secondary_indexes
            .as_deref()
            .unwrap_or_default()
            .iter()
            .any(|index| index.index_name == *name && index.capture_for_build)
        {
            return None;
        }
        let mut next_table = current.clone();
        for index in next_table.global_secondary_indexes.iter_mut().flatten() {
            if index.index_name == *name {
                index.capture_for_build = false;
            }
        }
        Some(Self {
            table: GsiTableUpdatePlan {
                next_table,
                lifecycle: None,
                attribute_definitions_changed: false,
            },
            retention: None,
            name_locked_until: current.created_at,
        })
    }

    pub(crate) fn is_capture_stop(&self, current: &StoredTableInfo) -> bool {
        current.requires_stream_entry() && !self.table.next_table.requires_stream_entry()
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
        self.table.next_table.table_stream_duration = request
            .aux_stream_duration_hours
            .unwrap_or(self.table.next_table.table_stream_duration);
        self.refresh_retention(observation, captured_at)
    }

    pub(crate) fn refresh_retention(
        &mut self,
        observation: TableUpdateRetention,
        captured_at: TimestampMillis,
    ) -> StorageResult<()> {
        let table = &self.table.next_table;
        let generation = StreamTrimState::next_policy_version(observation.state.as_ref())?;
        let mut frontier = observation
            .state
            .as_ref()
            .ok_or_else(|| StorageError::internal("Table retention state is missing"))?
            .frontier
            .clone();
        frontier.table_policy_version = std::num::NonZeroU64::new(generation)
            .ok_or_else(|| StorageError::internal("Invalid table policy generation"))?;
        let plan = crate::plan_table_stream_duration(
            observation.scope,
            frontier,
            table.table_stream_duration,
            captured_at,
        )?;
        let mut plan = plan.preserve_existing_history(observation.state.as_ref())?;
        if !table.requires_stream_entry() {
            plan.schedule_item_history_sweep(captured_at);
        }
        self.retention = Some(plan);
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
    fn observe_table_update(
        &mut self,
        table: &TableName,
    ) -> impl Future<Output = StorageResult<Option<BoundTableInfo>>> + Send;

    fn observe_table_retention(
        &mut self,
        catalog: &BoundTableInfo,
    ) -> impl Future<Output = StorageResult<TableUpdateRetention>> + Send;

    /// Read the physical name reservation within this update's native attempt.
    fn load_index_name_lock(
        &mut self,
        catalog: &BoundTableInfo,
        name: &IndexName,
    ) -> impl Future<Output = StorageResult<Option<TimestampMillis>>> + Send;

    fn persist_table_update(
        &mut self,
        catalog: BoundTableInfo,
        change: TableUpdateChange,
    ) -> impl Future<Output = StorageResult<BoundTableInfo>> + Send;

    fn observe_table_update_readiness(
        &mut self,
        catalog: &BoundTableInfo,
    ) -> impl Future<Output = StorageResult<GsiReadinessSnapshot>> + Send;

    fn prepare_table_update(
        &mut self,
        request: &UpdateTableRequest,
        time: TableUpdateTime,
        bindings: &RequestTableBindings,
    ) -> impl Future<Output = StorageResult<SemanticDecision<(BoundTableInfo, TableUpdateChange)>>> + Send
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
            bindings.observe(&request.table_name, catalog.history_id)?;
            let mut change =
                match TableUpdateChange::try_new(&catalog.info, request, time.observed_at) {
                    Ok(change) => change,
                    Err(error) => return Ok(SemanticDecision::Rejected(error)),
                };
            if catalog.info.has_public_stream_request()
                && request
                    .stream_specification
                    .as_ref()
                    .is_some_and(|spec| !spec.stream_enabled)
            {
                let names = change
                    .table
                    .next_table
                    .global_secondary_indexes
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .filter(|index| !index.index_name.is_ttl_index() && !index.capture_for_build)
                    .map(|index| index.index_name.clone())
                    .collect::<Vec<_>>();
                let readiness = self
                    .observe_table_update_readiness(&catalog)
                    .await?
                    .select(&names)?;
                if readiness.iter().any(|(_, state)| {
                    state.backfilling
                        || !matches!(state.index_status, storage_types::IndexStatus::Active)
                }) {
                    return Ok(SemanticDecision::Rejected(StorageError::validation(
                        "A GSI build still requires the table stream",
                    )));
                }
            }
            if crate::maintenance::update_table_request_has_custom_stream_duration(request)
                || change.is_capture_stop(&catalog.info)
            {
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
        bindings: &RequestTableBindings,
    ) -> impl Future<Output = StorageResult<SemanticDecision<(BoundTableInfo, UpdateTableResponse)>>>
    + Send {
        async move {
            let mut settlement =
                infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
            let attempt = settlement.as_mut();
            let result = async {
                let (catalog, change) = match attempt
                    .prepare_table_update(request, captured_at.into(), bindings)
                    .await?
                {
                    SemanticDecision::Accepted(prepared) => prepared,
                    SemanticDecision::Rejected(error) => {
                        return Ok(SemanticDecision::Rejected(error));
                    }
                };
                let was_disabled = !catalog.info.has_public_stream_request();
                let updated = attempt.persist_table_update(catalog, change).await?;
                if was_disabled && updated.info.has_public_stream_request() {
                    crate::table_stream_initialization::record::register_initialization(
                        attempt, &updated,
                    )
                    .await?;
                }
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
    catalog: &BoundTableInfo,
) -> StorageResult<UpdateTableResponse> {
    let table = &catalog.info;
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
        TableDescription::from_bound_table_info(catalog.clone(), table.table_status.clone());
    readiness.apply_to_description(&mut table_description)?;
    Ok(UpdateTableResponse { table_description })
}

fn plan_stream_specification(
    current: Option<&StreamSpecification>,
    requested: &StreamSpecification,
) -> StorageResult<StreamSpecification> {
    // The Streams adapter renders an enabled legacy None view as
    // NewAndOldImages. Preserve that effective view across disable and
    // re-enable operations.
    let current_view = current.and_then(|current| {
        current.stream_view_type.clone().or_else(|| {
            current
                .stream_enabled
                .then_some(StreamViewType::NewAndOldImages)
        })
    });
    if current_view
        .as_ref()
        .zip(requested.stream_view_type.as_ref())
        .is_some_and(|(current, requested)| current != requested)
    {
        return Err(StorageError::validation(
            "Changing StreamViewType for an existing stream is not supported",
        ));
    }
    Ok(StreamSpecification {
        stream_enabled: requested.stream_enabled,
        stream_view_type: requested.stream_view_type.clone().or(current_view),
    })
}
