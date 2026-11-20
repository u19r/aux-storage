//! Complete table-update decisions within a caller-owned native transaction.

use std::future::Future;

use storage_common::GsiTableUpdatePlan;
use storage_types::{
    BoundTableInfo, GlobalSecondaryIndex, IndexName, StorageError, StorageResult, StoredTableInfo,
    StreamSpecification, StreamViewType, TableDescription, TableName, TableStatus, TimestampMillis,
    UpdateTableRequest, UpdateTableResponse,
};

use super::GsiReadinessSnapshot;
use crate::{
    RequestTableBindings, SemanticDecision, StreamTrimScope, StreamTrimState, StreamTrimStateWrite,
};

/// The reclaim registration for the GSI an UpdateTable plan deletes.
pub struct IndexRetirement {
    pub target: storage_backfill::GsiBuildTarget,
    /// The bound GSI when the family keeps a native index for it; reclaim then
    /// drops that index and its unused columns.
    pub native_index: Option<storage_types::BoundGsi>,
}

/// `None` unless the plan deletes a GSI. `native`: the family keeps a native
/// index per GSI (`GsiPort::NATIVE_INDEX`).
pub fn deleted_index_retirement(
    current: &BoundTableInfo,
    plan: &GsiTableUpdatePlan,
    native: bool,
) -> StorageResult<Option<IndexRetirement>> {
    let Some(storage_common::GsiLifecycleAction::Delete(delete)) = plan.lifecycle.as_ref() else {
        return Ok(None);
    };
    let bound = current
        .binding
        .gsi(&delete.index_name)
        .ok_or_else(|| StorageError::internal("deleted GSI is absent from the catalog binding"))?;
    Ok(Some(IndexRetirement {
        target: storage_backfill::GsiBuildTarget {
            history_id: current.history_id,
            table_name: current.table_name.clone(),
            index_name: delete.index_name.clone(),
            incarnation: bound.build.clone(),
        },
        native_index: native.then(|| bound.clone()),
    }))
}

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
    pub(crate) fn try_new(
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
            if specification.stream_enabled && current.has_public_stream_request() {
                return Err(StorageError::validation(format!(
                    "Table already has an enabled stream: TableName: {}",
                    current.table_name
                )));
            }
            if !specification.stream_enabled && !current.has_public_stream_request() {
                return Err(StorageError::validation(format!(
                    "Table has no stream to disable: TableName: {}",
                    current.table_name
                )));
            }
            let specification =
                plan_stream_specification(current.stream_specification.as_ref(), specification)?;
            let stream_enabled = specification.stream_enabled;
            table.next_table.stream_specification = Some(specification);
            table.next_table.table_status =
                match (stream_enabled, current.has_public_stream_request()) {
                    (true, false) => TableStatus::Active,
                    (false, _) => TableStatus::Active,
                    _ => current.table_status.clone(),
                };
        }
        Ok(())
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

    /// `restart`: the source publication bounds at a re-enable. The new
    /// stream starts empty, as on AWS: history through them is retired, so no
    /// record of an earlier enable is served under the new ARN and view.
    pub(crate) fn refresh_retention(
        &mut self,
        observation: TableUpdateRetention,
        captured_at: TimestampMillis,
        restart: Option<&[storage_types::SystemStreamPosition]>,
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
        if let Some(bounds) = restart {
            frontier.retire_through(bounds)?;
        }
        let plan = crate::plan_table_stream_duration(
            observation.scope,
            frontier,
            table.table_stream_duration,
            captured_at,
        )?;
        let plan = plan.preserve_existing_history(observation.state.as_ref())?;
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

pub trait TableUpdateTransaction: crate::StreamLabelClockAttempt {
    /// The store's stream source, read without conflict protection: a
    /// disabled table captures nothing, so its history is settled.
    fn load_stream_source(
        &mut self,
    ) -> impl Future<Output = StorageResult<stream_provider::SystemStreamHistory>> + Send;

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
    ) -> impl Future<Output = StorageResult<GsiReadinessSnapshot>> + Send {
        async move { crate::gsi_readiness_from_catalog(catalog, None) }
    }

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
            if catalog.binding.regional.is_some()
                && request
                    .stream_specification
                    .as_ref()
                    .is_some_and(|spec| !spec.stream_enabled)
            {
                return Ok(SemanticDecision::Rejected(StorageError::raw_validation(
                    "Streams cannot be disabled on a table that has a replica",
                )));
            }
            if request
                .global_secondary_index_updates
                .as_ref()
                .is_some_and(|updates| {
                    updates
                        .iter()
                        .any(|update| update.create.is_some() || update.delete.is_some())
                })
                && catalog
                    .binding
                    .gsis
                    .iter()
                    .any(|gsi| gsi.phase != storage_types::GsiPhase::Active)
            {
                return Ok(SemanticDecision::Rejected(
                    storage_types::StorageEnum::LimitExceeded {
                        message: storage_types::ONE_ONLINE_INDEX_MESSAGE.to_owned(),
                    }
                    .into(),
                ));
            }
            let mut change =
                match TableUpdateChange::try_new(&catalog.info, request, time.observed_at) {
                    Ok(change) => change,
                    Err(error) => return Ok(SemanticDecision::Rejected(error)),
                };
            let enables_stream = !catalog.info.has_public_stream_request()
                && change.table.next_table.has_public_stream_request();
            if crate::maintenance::update_table_request_has_custom_stream_duration(request)
                || catalog.info.requires_stream_entry()
                    != change.table.next_table.requires_stream_entry()
            {
                let retention = self.observe_table_retention(&catalog).await?;
                let restart = if enables_stream && catalog.info.stream_enabled_at.is_some() {
                    Some(
                        self.load_stream_source()
                            .await?
                            .retirement_boundaries()
                            .map_err(|_| {
                                StorageError::internal("Invalid source publication bounds")
                            })?,
                    )
                } else {
                    None
                };
                change.table.next_table.table_stream_duration = request
                    .aux_stream_duration_hours
                    .unwrap_or(change.table.next_table.table_stream_duration);
                change.refresh_retention(retention, time.captured_at, restart.as_deref())?;
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
            if enables_stream {
                change.table.next_table.stream_enabled_at = Some(
                    crate::issue_stream_enable_stamp(
                        self,
                        time.captured_at,
                        catalog.info.stream_enabled_at,
                    )
                    .await?,
                );
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
                let view = UpdateResponseView::new(&catalog.info, &change);
                let updated = attempt.persist_table_update(catalog, change).await?;
                let response = describe_updated_table(attempt, &updated, view).await?;
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

/// What the AWS UpdateTable response shows while the change settles (P13);
/// aux-storage applies the change before it answers.
struct UpdateResponseView {
    /// A GSI create or delete.
    updating: bool,
    created_index: Option<IndexName>,
    /// The index this request deletes, as it was before the delete.
    deleted_index: Option<GlobalSecondaryIndex>,
}

impl UpdateResponseView {
    fn new(current: &StoredTableInfo, change: &TableUpdateChange) -> Self {
        let deleted_index = match change.table.lifecycle.as_ref() {
            Some(storage_common::GsiLifecycleAction::Delete(delete)) => current
                .global_secondary_indexes
                .as_deref()
                .unwrap_or_default()
                .iter()
                .find(|index| index.index_name == delete.index_name)
                .cloned(),
            _ => None,
        };
        Self {
            updating: change.table.lifecycle.is_some(),
            created_index: change.created_index_name().cloned(),
            deleted_index,
        }
    }

    fn apply(self, description: &mut TableDescription) {
        for index in description
            .global_secondary_indexes
            .as_deref_mut()
            .unwrap_or_default()
        {
            if self.created_index.as_ref() == Some(&index.index_name) {
                // The build has not started when the response is written.
                index.backfilling = Some(false);
            }
        }
        if let Some(index) = self.deleted_index {
            description.show_deleting_index(index);
        }
    }
}

async fn describe_updated_table<A: TableUpdateTransaction + ?Sized>(
    attempt: &mut A,
    catalog: &BoundTableInfo,
    view: UpdateResponseView,
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
    let status = if view.updating {
        TableStatus::Updating
    } else {
        table.table_status.clone()
    };
    let mut table_description = TableDescription::from_bound_table_info(catalog.clone(), status);
    readiness.apply_to_description(&mut table_description)?;
    view.apply(&mut table_description);
    Ok(UpdateTableResponse { table_description })
}

fn plan_stream_specification(
    current: Option<&StreamSpecification>,
    requested: &StreamSpecification,
) -> StorageResult<StreamSpecification> {
    // The Streams adapter renders an enabled legacy None view as
    // NewAndOldImages. A disable keeps that effective view. An enable of a
    // disabled stream may choose another view type (AWS accepts it); an
    // enable of an enabled stream is rejected before this runs.
    let current_view = current.and_then(|current| {
        current.stream_view_type.clone().or_else(|| {
            current
                .stream_enabled
                .then_some(StreamViewType::NewAndOldImages)
        })
    });
    Ok(StreamSpecification {
        stream_enabled: requested.stream_enabled,
        stream_view_type: requested.stream_view_type.clone().or(current_view),
    })
}
