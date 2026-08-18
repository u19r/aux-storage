use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use futures::{StreamExt, stream as futures_stream};
use storage_types::{
    AttributeValue, ItemKey, KeyAttributes, KeySchemaElement, ReplicationEventMetadata,
    ReplicationHybridLogicalClock, ReplicationMutation, ReplicationWriteSource, StorageError,
    StorageResult, StreamName, TableName,
};
use stream::{StreamError, StreamItem};

use crate::{
    constants::STORAGE_MULTI_REGION_CONFLICT_TOTAL_METRIC,
    database_manager::{DatabaseManager, ROUTED_DEFAULT_CONNECTION_ID, record_storage_operation},
    namespace_routing::{NamespaceRoute, NamespaceStorageMode},
    newtypes::DatabaseTrait,
};

const UNTRACKED_LOCAL_REPLICATION_ORIGIN_REGION: &str = "__local__";

struct PreparedReplicationMutation {
    original_index: usize,
    mutation: ReplicationMutation,
    route: Option<NamespaceRoute>,
    read_provider: Arc<dyn DatabaseTrait>,
    read_table_name: TableName,
    connection_id: Option<String>,
    key_schema: Vec<KeySchemaElement>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PreparedReplicationReadTargetKey {
    connection_id: Option<String>,
    table_name: TableName,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PreparedReplicationBatchGroupKey {
    connection_id: Option<String>,
    table_name: String,
    item_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PreparedReplicationConnectionBucketKey(Option<String>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationMutationApplyOutcome {
    Applied,
    SkippedStale,
    SkippedDuplicate,
}

impl DatabaseManager {
    async fn prepare_replication_mutation(
        &self,
        mutation: ReplicationMutation,
    ) -> StorageResult<PreparedReplicationMutation> {
        let mut prepared = self.prepare_replication_mutations(vec![mutation]).await?;
        prepared
            .pop()
            .ok_or_else(|| StorageError::internal("missing prepared replication mutation"))
    }

    async fn prepare_replication_mutations(
        &self,
        mutations: Vec<ReplicationMutation>,
    ) -> StorageResult<Vec<PreparedReplicationMutation>> {
        self.ensure_multi_region_replication_control_plane_supported()?;
        let mut route_cache: HashMap<TableName, Option<NamespaceRoute>> = HashMap::new();
        let mut key_schema_cache: HashMap<PreparedReplicationReadTargetKey, Vec<KeySchemaElement>> =
            HashMap::new();
        let mut prepared = Vec::with_capacity(mutations.len());

        for (original_index, mutation) in mutations.into_iter().enumerate() {
            let route = if let Some(cached) = route_cache.get(&mutation.table_name) {
                cached.clone()
            } else {
                let resolved = self
                    .resolve_namespace_route_for_table(&mutation.table_name)
                    .await?;
                route_cache.insert(mutation.table_name.clone(), resolved.clone());
                resolved
            };

            let (read_provider, read_table_name, connection_id) = match route.as_ref() {
                Some(route) => (
                    self.provider_for_connection(&route.read_target.connection_id)?,
                    route.read_target.table_name.clone(),
                    Some(route.read_target.connection_id.clone()),
                ),
                None => (Arc::clone(&self.storage), mutation.table_name.clone(), None),
            };

            let mut mutation = mutation;
            if let Some(route) = route.as_ref()
                && route.storage_mode == NamespaceStorageMode::SharedTable
            {
                self.request_rewriter
                    .rewrite_key_for_shared_table(&route.namespace, &mut mutation.key)?;
                if let Some(new_image) = mutation.new_image.as_mut() {
                    self.request_rewriter
                        .rewrite_item_for_shared_table(&route.namespace, new_image)?;
                }
                if let Some(old_image) = mutation.old_image.as_mut() {
                    self.request_rewriter
                        .rewrite_item_for_shared_table(&route.namespace, old_image)?;
                }
            }

            let read_target_key = PreparedReplicationReadTargetKey {
                connection_id: connection_id.clone(),
                table_name: read_table_name.clone(),
            };
            let key_schema = if let Some(cached) = key_schema_cache.get(&read_target_key) {
                cached.clone()
            } else {
                let admission_connection_id = connection_id
                    .as_deref()
                    .unwrap_or(ROUTED_DEFAULT_CONNECTION_ID);
                let schema =
                    record_storage_operation(
                        "get_table_info",
                        self.run_control_admitted(admission_connection_id, {
                            let read_table_name = read_table_name.clone();
                            move |provider| async move {
                                provider.get_table_info(&read_table_name).await
                            }
                        }),
                    )
                    .await?
                    .key_schema;
                key_schema_cache.insert(read_target_key, schema.clone());
                schema
            };

            prepared.push(PreparedReplicationMutation {
                original_index,
                mutation,
                route,
                read_provider,
                read_table_name,
                connection_id,
                key_schema,
            });
        }

        Ok(prepared)
    }

    async fn resolve_current_replication_winner(
        &self,
        provider: Arc<dyn DatabaseTrait>,
        table_name: &TableName,
        key_schema: &[KeySchemaElement],
        key: &KeyAttributes,
    ) -> StorageResult<Option<ReplicationEventMetadata>> {
        let item_key =
            ItemKey::from_key_schema(table_name.clone(), key_schema, key).map_err(|error| {
                StorageError::internal(&format!(
                    "build item key for multi-region conflict resolution: {error}"
                ))
            })?;
        let item_stream_name =
            StreamName::table_item_stream(table_name, &item_key).map_err(|error| {
                StorageError::internal(&format!(
                    "build item stream name for multi-region conflict resolution: {error}"
                ))
            })?;
        let latest_item_page = provider
            .read_backward(item_stream_name.clone(), None, 1)
            .await
            .map_err(StreamError::into_storage_enum)?;
        let Some(latest_item) = latest_item_page.items.into_iter().next() else {
            return Ok(None);
        };

        let latest_item_version = latest_item.id.into();
        let table_stream_name = StreamName::table_stream(table_name);
        let pointer_item = find_table_stream_pointer_for_item_version(
            Arc::clone(&provider),
            table_stream_name.clone(),
            &item_stream_name,
            latest_item_version,
        )
        .await?
        .ok_or_else(|| {
            StorageError::internal(&format!(
                "missing table stream pointer for stream item {} in {}",
                latest_item.id,
                String::from(&table_stream_name)
            ))
        })?;

        let stored_pointer = provider
            .decode_stored_stream_pointer(&pointer_item)
            .await
            .map_err(StreamError::into_storage_enum)?;
        Ok(Some(
            stored_pointer
                .replication_metadata()
                .cloned()
                .unwrap_or_else(|| synthesize_untracked_local_replication_metadata(&latest_item)),
        ))
    }

    async fn apply_prepared_replication_mutation(
        &self,
        prepared: PreparedReplicationMutation,
    ) -> StorageResult<ReplicationMutationApplyOutcome> {
        let connection_id = prepared
            .connection_id
            .as_deref()
            .unwrap_or(ROUTED_DEFAULT_CONNECTION_ID);
        let current_winner = self
            .run_control_admitted(connection_id, {
                let table_name = prepared.read_table_name.clone();
                let key_schema = prepared.key_schema.clone();
                let key = prepared.mutation.key.clone();
                move |provider| async move {
                    self.resolve_current_replication_winner(
                        provider,
                        &table_name,
                        &key_schema,
                        &key,
                    )
                    .await
                }
            })
            .await?;

        self.apply_prepared_replication_mutation_against_current_winner(prepared, current_winner)
            .await
    }

    async fn apply_prepared_replication_mutation_against_current_winner(
        &self,
        prepared: PreparedReplicationMutation,
        current_winner: Option<ReplicationEventMetadata>,
    ) -> StorageResult<ReplicationMutationApplyOutcome> {
        let outcome = evaluate_replication_apply_outcome(
            current_winner.as_ref(),
            &prepared.mutation.metadata,
        );
        if !matches!(outcome, ReplicationMutationApplyOutcome::Applied) {
            return Ok(outcome);
        }

        self.apply_prepared_winning_replication_mutation(prepared)
            .await?;
        Ok(ReplicationMutationApplyOutcome::Applied)
    }

    async fn apply_prepared_winning_replication_mutation(
        &self,
        prepared: PreparedReplicationMutation,
    ) -> StorageResult<()> {
        if let Some(route) = prepared.route.as_ref() {
            self.execute_routed_write_targets(
                route,
                crate::database_manager::core::AdmissionLane::Control,
                "replication mutation route had no write targets",
                |provider, target, _target_index, target_role| {
                    let mut provider_mutation = prepared.mutation.clone();
                    provider_mutation.table_name = target.table_name.clone();
                    async move {
                        crate::database_manager::record_storage_operation_for_target(
                            "apply_replication_mutation",
                            target_role,
                            provider.apply_replication_mutation(provider_mutation),
                        )
                        .await
                    }
                },
            )
            .await?;
            return Ok(());
        }

        let connection_id = prepared
            .connection_id
            .as_deref()
            .unwrap_or(ROUTED_DEFAULT_CONNECTION_ID);
        self.run_control_admitted(connection_id, |provider| async move {
            record_storage_operation(
                "apply_replication_mutation",
                provider.apply_replication_mutation(prepared.mutation),
            )
            .await
        })
        .await?;
        Ok(())
    }

    pub async fn apply_replication_mutation_with_outcome(
        &self,
        mutation: ReplicationMutation,
    ) -> StorageResult<ReplicationMutationApplyOutcome> {
        let prepared = self.prepare_replication_mutation(mutation).await?;
        self.apply_prepared_replication_mutation(prepared).await
    }

    pub async fn apply_replication_mutations_with_outcomes(
        &self,
        mutations: Vec<ReplicationMutation>,
    ) -> StorageResult<Vec<ReplicationMutationApplyOutcome>> {
        let prepared = self.prepare_replication_mutations(mutations).await?;
        if prepared.is_empty() {
            return Ok(Vec::new());
        }
        let total_mutations = prepared.len();

        let mut grouped =
            BTreeMap::<PreparedReplicationBatchGroupKey, Vec<PreparedReplicationMutation>>::new();
        for prepared_mutation in prepared {
            let group_key = prepared_replication_batch_group_key(&prepared_mutation)?;
            grouped
                .entry(group_key)
                .or_default()
                .push(prepared_mutation);
        }

        let mut buckets = BTreeMap::<
            PreparedReplicationConnectionBucketKey,
            Vec<Vec<PreparedReplicationMutation>>,
        >::new();
        for group in grouped.into_values() {
            let bucket_key = PreparedReplicationConnectionBucketKey(
                group
                    .first()
                    .and_then(|prepared| prepared.connection_id.clone()),
            );
            buckets.entry(bucket_key).or_default().push(group);
        }

        let mut outcomes = vec![ReplicationMutationApplyOutcome::SkippedDuplicate; total_mutations];
        for groups in buckets.into_values() {
            let parallelism = replication_apply_parallelism_hint_for_groups(&groups);
            if parallelism == 1 || groups.len() == 1 {
                for group in groups {
                    for (original_index, outcome) in self
                        .apply_prepared_replication_mutation_group(group)
                        .await?
                    {
                        outcomes[original_index] = outcome;
                    }
                }
                continue;
            }

            let group_outcomes = futures_stream::iter(
                groups
                    .into_iter()
                    .map(|group| self.apply_prepared_replication_mutation_group(group)),
            )
            .buffer_unordered(parallelism)
            .collect::<Vec<_>>()
            .await;

            for group_outcome in group_outcomes {
                for (original_index, outcome) in group_outcome? {
                    outcomes[original_index] = outcome;
                }
            }
        }

        Ok(outcomes)
    }

    pub async fn apply_replication_mutation(
        &self,
        mutation: ReplicationMutation,
    ) -> StorageResult<()> {
        let _ = self
            .apply_replication_mutation_with_outcome(mutation)
            .await?;
        Ok(())
    }

    pub async fn get_latest_item_replication_metadata(
        &self,
        table_name: &TableName,
        key: &HashMap<String, AttributeValue>,
    ) -> StorageResult<Option<ReplicationEventMetadata>> {
        let key = storage_types::KeyAttributes::from(key.clone());
        let table_name = table_name.clone();
        self.run_control_admitted(ROUTED_DEFAULT_CONNECTION_ID, move |provider| async move {
            let key_schema =
                record_storage_operation("get_table_info", provider.get_table_info(&table_name))
                    .await?
                    .key_schema;
            self.resolve_current_replication_winner(provider, &table_name, &key_schema, &key)
                .await
        })
        .await
    }

    async fn apply_prepared_replication_mutation_group(
        &self,
        group: Vec<PreparedReplicationMutation>,
    ) -> StorageResult<Vec<(usize, ReplicationMutationApplyOutcome)>> {
        let first = group
            .first()
            .ok_or_else(|| StorageError::internal("missing grouped replication mutation"))?;
        let connection_id = first
            .connection_id
            .as_deref()
            .unwrap_or(ROUTED_DEFAULT_CONNECTION_ID);
        let mut current_winner = self
            .run_control_admitted(connection_id, {
                let table_name = first.read_table_name.clone();
                let key_schema = first.key_schema.clone();
                let key = first.mutation.key.clone();
                move |provider| async move {
                    self.resolve_current_replication_winner(
                        provider,
                        &table_name,
                        &key_schema,
                        &key,
                    )
                    .await
                }
            })
            .await?;
        let mut outcomes = Vec::with_capacity(group.len());
        for prepared_mutation in group {
            let original_index = prepared_mutation.original_index;
            let mutation_metadata = prepared_mutation.mutation.metadata.clone();
            let outcome = self
                .apply_prepared_replication_mutation_against_current_winner(
                    prepared_mutation,
                    current_winner.clone(),
                )
                .await?;
            if matches!(outcome, ReplicationMutationApplyOutcome::Applied) {
                current_winner = Some(mutation_metadata);
            }
            outcomes.push((original_index, outcome));
        }
        Ok(outcomes)
    }
}

async fn find_table_stream_pointer_for_item_version(
    provider: Arc<dyn DatabaseTrait>,
    table_stream_name: StreamName,
    item_stream_name: &StreamName,
    item_stream_version: storage_types::ItemStreamVersion,
) -> StorageResult<Option<StreamItem>> {
    let mut cursor = None;
    loop {
        let page = provider
            .read_backward(table_stream_name.clone(), cursor, 100)
            .await
            .map_err(StreamError::into_storage_enum)?;
        for pointer_item in &page.items {
            let pointer = provider
                .decode_stored_stream_pointer(pointer_item)
                .await
                .map_err(StreamError::into_storage_enum)?;
            if pointer.stream_name() == item_stream_name
                && pointer.target_item_stream_version() == item_stream_version
            {
                return Ok(Some(pointer_item.clone()));
            }
        }
        if !page.has_more {
            return Ok(None);
        }
        cursor = page.last_evaluated_key;
        if cursor.is_none() {
            return Ok(None);
        }
    }
}

fn compare_replication_lww(
    current: &ReplicationEventMetadata,
    incoming: &ReplicationEventMetadata,
) -> Ordering {
    (
        current.origin_hlc.clone(),
        current.origin_region.as_str(),
        current.origin_sequence,
    )
        .cmp(&(
            incoming.origin_hlc.clone(),
            incoming.origin_region.as_str(),
            incoming.origin_sequence,
        ))
}

pub(super) fn evaluate_replication_apply_outcome(
    current_winner: Option<&ReplicationEventMetadata>,
    incoming: &ReplicationEventMetadata,
) -> ReplicationMutationApplyOutcome {
    match current_winner.map(|current| compare_replication_lww(current, incoming)) {
        Some(Ordering::Greater) => {
            record_multi_region_conflict("skipped", "stale");
            ReplicationMutationApplyOutcome::SkippedStale
        }
        Some(Ordering::Equal) => {
            record_multi_region_conflict("skipped", "duplicate");
            ReplicationMutationApplyOutcome::SkippedDuplicate
        }
        Some(Ordering::Less) => {
            record_multi_region_conflict("applied", "won_conflict");
            ReplicationMutationApplyOutcome::Applied
        }
        None => {
            record_multi_region_conflict("applied", "no_current_winner");
            ReplicationMutationApplyOutcome::Applied
        }
    }
}

fn replication_apply_parallelism_hint_for_groups(
    groups: &[Vec<PreparedReplicationMutation>],
) -> usize {
    groups
        .iter()
        .filter_map(|group| group.first())
        .map(|prepared| prepared.read_provider.replication_apply_parallelism_hint())
        .max()
        .unwrap_or(1)
        .max(1)
}

fn prepared_replication_batch_group_key(
    prepared: &PreparedReplicationMutation,
) -> StorageResult<PreparedReplicationBatchGroupKey> {
    let item_key = ItemKey::from_key_schema(
        prepared.read_table_name.clone(),
        &prepared.key_schema,
        &prepared.mutation.key,
    )
    .map_err(|error| {
        StorageError::internal(&format!(
            "build item key for grouped multi-region apply: {error}"
        ))
    })?;
    let item_key = serde_json::to_string(&item_key).map_err(|error| {
        StorageError::internal(&format!(
            "serialize item key for grouped multi-region apply: {error}"
        ))
    })?;

    Ok(PreparedReplicationBatchGroupKey {
        connection_id: prepared.connection_id.clone(),
        table_name: prepared.read_table_name.as_ref().to_string(),
        item_key,
    })
}

fn synthesize_untracked_local_replication_metadata(item: &StreamItem) -> ReplicationEventMetadata {
    ReplicationEventMetadata {
        origin_region: UNTRACKED_LOCAL_REPLICATION_ORIGIN_REGION.to_string(),
        origin_sequence: item.id,
        origin_hlc: ReplicationHybridLogicalClock {
            physical_ms: item.created_at,
            logical: 0,
        },
        origin_commit_ts: item.created_at,
        table_replica_epoch: 0,
        write_source: ReplicationWriteSource::Local,
    }
}

fn record_multi_region_conflict(outcome: &'static str, reason: &'static str) {
    metrics_facade::counter!(
        STORAGE_MULTI_REGION_CONFLICT_TOTAL_METRIC,
        "outcome" => outcome,
        "reason" => reason
    )
    .increment(1);
}
