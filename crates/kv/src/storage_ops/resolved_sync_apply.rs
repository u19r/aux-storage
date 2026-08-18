use std::collections::HashMap;

use storage_provider::StorageProvider;
use storage_types::{AttributeValue, KeyAttributes, StorageError, StorageResult, StreamItemId};

use super::provider_impl::kv_mutation_to_direct_with_literal_templates;
use crate::{
    SortedKvDbStorageProvider,
    backends::common::plan_table_write_with_codec,
    helpers::increment_bytes,
    partition_family::PartitionFamilyKvStore,
    sorted_kv_store::{DirectWriteOperation, RawKey, SortedKvStore, TransactWriteTableOperation},
};

pub(super) async fn apply_resolved_sync_mutations<S: PartitionFamilyKvStore + 'static>(
    provider: &SortedKvDbStorageProvider<S>,
    metadata: storage_sync::SyncCommitMetadata,
    batch: storage_sync::ResolvedSyncMutationBatch,
) -> StorageResult<Vec<storage_sync::SyncMutationResponse>> {
    let mutation_count = batch.mutations.len();
    let mut responses = Vec::with_capacity(mutation_count);
    let mut operations = Vec::with_capacity(mutation_count.saturating_mul(6).saturating_add(2));
    let mut table_cache = HashMap::new();
    let mut ttl_cache = HashMap::new();
    let marker_value = sync_marker_value(&metadata)?;

    for mutation in batch.mutations {
        let marker_key = sync_apply_marker_key(mutation.mutation_id().as_str());
        if provider.kv_store.get(&marker_key, true).await?.is_some() {
            responses.push(sync_mutation_response(&mutation).clone());
            continue;
        }

        match mutation {
            storage_sync::ResolvedSyncMutation::Put(mutation) => {
                let table_metadata = provider
                    .get_table_identity_cached(&mut table_cache, &mutation.table_name)
                    .await?;
                let table_info = table_metadata.table_info.clone();
                let ttl_config = provider
                    .load_ttl_config_cached(&mut ttl_cache, &mutation.table_name)
                    .await?;
                let item =
                    serde_json::from_str::<HashMap<String, AttributeValue>>(&mutation.item_json)?;
                let key_attributes = provider.get_key_attributes(&item, &table_info.key_schema)?;
                let current = old_item_bytes(
                    mutation.old_item_json.as_deref(),
                    mutation.old_indexers.as_deref(),
                    provider.kv_store.item_value_codec(),
                    table_info.max_indexers,
                )?;
                let plan = plan_table_write_with_codec(
                    &[TransactWriteTableOperation::Put {
                        table_identity: table_metadata.identity.clone(),
                        table_info,
                        item,
                        indexers: Some(mutation.indexers),
                        item_stream_ttl_hours: None,
                        condition: None,
                        return_values_on_condition_check_failure: None,
                        replication: None,
                        ttl_config,
                    }],
                    vec![current],
                    &[Some(StreamItemId::from(
                        mutation.target_item_stream_version,
                    ))],
                    provider.immediate_gsi_consistency,
                    provider.kv_store.item_value_codec(),
                )?;
                operations.extend(
                    plan.mutations
                        .into_iter()
                        .map(kv_mutation_to_direct_with_literal_templates),
                );
                operations.push(super::logical_backfill::revision_put_operation(
                    &mutation.table_name,
                    &key_attributes,
                    mutation.target_item_stream_version,
                )?);
                operations.push(DirectWriteOperation::CheckValue {
                    key: marker_key.clone(),
                    expected_value: None,
                });
                operations.push(DirectWriteOperation::Put {
                    key: marker_key,
                    value: marker_value.clone(),
                });
                responses.push(mutation.response);
            }
            storage_sync::ResolvedSyncMutation::Delete(mutation) => {
                let table_metadata = provider
                    .get_table_identity_cached(&mut table_cache, &mutation.table_name)
                    .await?;
                let table_info = table_metadata.table_info.clone();
                let ttl_config = provider
                    .load_ttl_config_cached(&mut ttl_cache, &mutation.table_name)
                    .await?;
                let key =
                    serde_json::from_str::<HashMap<String, AttributeValue>>(&mutation.key_json)?;
                let key_attributes = KeyAttributes::from(key);
                let current = old_item_bytes(
                    mutation.old_item_json.as_deref(),
                    mutation.old_indexers.as_deref(),
                    provider.kv_store.item_value_codec(),
                    table_info.max_indexers,
                )?;
                let plan = plan_table_write_with_codec(
                    &[TransactWriteTableOperation::Delete {
                        table_identity: table_metadata.identity.clone(),
                        table_info,
                        key: key_attributes.clone(),
                        item_stream_ttl_hours: None,
                        use_key_attributes_for_missing_item_condition: false,
                        condition: None,
                        return_values_on_condition_check_failure: None,
                        replication: None,
                        ttl_config,
                    }],
                    vec![current],
                    &[Some(StreamItemId::from(
                        mutation.target_item_stream_version,
                    ))],
                    provider.immediate_gsi_consistency,
                    provider.kv_store.item_value_codec(),
                )?;
                operations.extend(
                    plan.mutations
                        .into_iter()
                        .map(kv_mutation_to_direct_with_literal_templates),
                );
                operations.push(super::logical_backfill::revision_put_operation(
                    &mutation.table_name,
                    &key_attributes,
                    mutation.target_item_stream_version,
                )?);
                operations.push(DirectWriteOperation::CheckValue {
                    key: marker_key.clone(),
                    expected_value: None,
                });
                operations.push(DirectWriteOperation::Put {
                    key: marker_key,
                    value: marker_value.clone(),
                });
                responses.push(mutation.response);
            }
            storage_sync::ResolvedSyncMutation::CreateTable(_)
            | storage_sync::ResolvedSyncMutation::UpdateTable(_)
            | storage_sync::ResolvedSyncMutation::DeleteTable(_)
            | storage_sync::ResolvedSyncMutation::UpdateTimeToLive(_) => {
                return Err(StorageError::internal(
                    "lifecycle sync mutations must be applied by DatabaseManager",
                ));
            }
        }
    }

    if should_advance_last_sync_log(last_resolved_sync_log_id(provider).await?, metadata.log_id) {
        let last_key = sync_last_applied_key();
        let current_last = provider.kv_store.get(&last_key, true).await?;
        operations.push(DirectWriteOperation::CheckValue {
            key: last_key.clone(),
            expected_value: current_last,
        });
        operations.push(DirectWriteOperation::Put {
            key: last_key,
            value: marker_value,
        });
    }

    provider
        .kv_store
        .transact_write_unchecked(operations)
        .await?;
    Ok(responses)
}

pub(super) async fn last_resolved_sync_log_id<S: SortedKvStore>(
    provider: &SortedKvDbStorageProvider<S>,
) -> StorageResult<Option<storage_sync::SyncLogId>> {
    let Some(bytes) = provider
        .kv_store
        .get(&sync_last_applied_key(), true)
        .await?
    else {
        return Ok(None);
    };
    let metadata =
        storage_types::storage_serde::from_bytes::<storage_sync::SyncCommitMetadata>(&bytes)?;
    Ok(Some(metadata.log_id))
}

pub(super) async fn persist_resolved_sync_log_entry<S: SortedKvStore>(
    provider: &SortedKvDbStorageProvider<S>,
    metadata: &storage_sync::SyncCommitMetadata,
    batch: &storage_sync::ResolvedSyncMutationBatch,
) -> StorageResult<()> {
    let entry = storage_sync::ResolvedSyncLogEntry::new(metadata.clone(), batch.clone());
    provider
        .kv_store
        .put(
            &sync_log_entry_key(metadata.log_id),
            &storage_types::storage_serde::to_bytes(&entry)?,
            None,
        )
        .await
}

pub(super) async fn get_resolved_sync_log_entry<S: SortedKvStore>(
    provider: &SortedKvDbStorageProvider<S>,
    log_id: storage_sync::SyncLogId,
) -> StorageResult<Option<storage_sync::ResolvedSyncLogEntry>> {
    let Some(bytes) = provider
        .kv_store
        .get(&sync_log_entry_key(log_id), true)
        .await?
    else {
        return Ok(None);
    };
    Ok(Some(storage_types::storage_serde::from_bytes(&bytes)?))
}

pub(super) async fn resolved_sync_log_entries_after<S: SortedKvStore>(
    provider: &SortedKvDbStorageProvider<S>,
    log_id: Option<storage_sync::SyncLogId>,
    limit: usize,
) -> StorageResult<Vec<storage_sync::ResolvedSyncLogEntry>> {
    let limit = u32::try_from(limit)
        .map_err(|_| StorageError::validation("sync log scan limit exceeds u32"))?;
    let prefix = sync_log_entry_prefix();
    let start = log_id.map_or_else(
        || prefix.clone(),
        |id| increment_bytes(sync_log_entry_key(id)),
    );
    let end = increment_bytes(prefix);
    let range = provider
        .kv_store
        .get_range(&start, &end, Some(limit), None::<RawKey>, true)
        .await?;
    range
        .items
        .into_iter()
        .map(|(_, value)| storage_types::storage_serde::from_bytes(&value))
        .collect()
}

fn old_item_bytes(
    old_item_json: Option<&str>,
    old_indexers: Option<&[String]>,
    codec: crate::sorted_kv_store::ItemValueCodec,
    capacity: storage_types::MaxIndexers,
) -> StorageResult<Option<Vec<u8>>> {
    old_item_json
        .map(|json| {
            serde_json::from_str::<HashMap<String, AttributeValue>>(json)
                .map_err(StorageError::from)
                .and_then(|item| storage_types::WireItem::from_attribute_map(&item))
                .and_then(|item| {
                    super::encode_wire_item_storage_bytes(codec, &item, old_indexers, capacity)
                })
        })
        .transpose()
}

fn sync_mutation_response(
    mutation: &storage_sync::ResolvedSyncMutation,
) -> &storage_sync::SyncMutationResponse {
    match mutation {
        storage_sync::ResolvedSyncMutation::Put(mutation) => &mutation.response,
        storage_sync::ResolvedSyncMutation::Delete(mutation) => &mutation.response,
        storage_sync::ResolvedSyncMutation::CreateTable(_)
        | storage_sync::ResolvedSyncMutation::UpdateTable(_)
        | storage_sync::ResolvedSyncMutation::DeleteTable(_)
        | storage_sync::ResolvedSyncMutation::UpdateTimeToLive(_) => {
            static EMPTY: storage_sync::SyncMutationResponse = storage_sync::SyncMutationResponse {
                response_json: None,
            };
            &EMPTY
        }
    }
}

fn should_advance_last_sync_log(
    current: Option<storage_sync::SyncLogId>,
    incoming: storage_sync::SyncLogId,
) -> bool {
    current.is_none_or(|current| incoming > current)
}

fn sync_marker_value(metadata: &storage_sync::SyncCommitMetadata) -> StorageResult<Vec<u8>> {
    storage_types::storage_serde::to_bytes(metadata)
}

fn sync_apply_marker_key(mutation_id: &str) -> Vec<u8> {
    crate::keyspace::compact::sync_apply_marker_key(mutation_id)
}

fn sync_last_applied_key() -> Vec<u8> {
    crate::keyspace::compact::sync_last_applied_key()
}

fn sync_log_entry_prefix() -> Vec<u8> {
    crate::keyspace::compact::sync_log_entry_prefix()
}

fn sync_log_entry_key(log_id: storage_sync::SyncLogId) -> Vec<u8> {
    crate::keyspace::compact::sync_log_entry_key(log_id.term, log_id.index)
}
