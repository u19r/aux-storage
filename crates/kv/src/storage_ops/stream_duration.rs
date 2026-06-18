use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use storage_provider::{
    StorageProvider, StreamDurationTrimBackend, StreamDurationTrimPageRequest,
    StreamDurationTrimPageResult, StreamTrimBoundary, StreamTrimDueMarker, StreamTrimScope,
    StreamTrimScopeBoundaries, StreamTrimScopeKind, StreamTrimState, StreamTrimStateWrite,
    plan_validated_item_stream_duration,
};
use storage_types::{
    ItemKey, ItemStreamVersion, KeyAttributes, StorageError, StorageResult, StoredTableInfo,
    StreamItemId, StreamName, StreamRetentionDuration, TimestampMillis,
};
use stream_provider::{StoredStreamPointer, StreamDataType};
use uuid::Uuid;

use crate::{
    SortedKvDbStorageProvider,
    backends::common::KvMutation,
    constants,
    helpers::increment_bytes,
    keyspace::{compact, stream_keys, table_identity::TableIdentity},
    sorted_kv_store::{BatchItem, DirectWriteOperation},
    stream::{
        item_codec::decode_stream_item,
        pointer_codec::{decode_compact_pointer, item_stream_name},
    },
};

const ITEM_STREAM_SCOPE_PREFIX: &str = "kv-stream:";
const ITEM_KEY_HASH_PREFIX: &str = "kv-key:";
const HEX: &[u8; 16] = b"0123456789abcdef";
const FOREVER_POLICY_CODE: u32 = u32::MAX;
const TRIM_SCOPE_TABLE: u8 = b't';
const TRIM_SCOPE_ITEM: u8 = b'i';

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompactStreamTrimState {
    scope_key: Vec<u8>,
    policy_version: u64,
    retention: StreamRetentionDuration,
    effective_retention: StreamRetentionDuration,
    next_due_at: Option<TimestampMillis>,
    oldest_retained_version: Option<ItemStreamVersion>,
    oldest_retained_timestamp: Option<TimestampMillis>,
    latest_version: Option<ItemStreamVersion>,
    latest_timestamp: Option<TimestampMillis>,
    updated_at: TimestampMillis,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompactStreamTrimDueMarker {
    due_bucket: TimestampMillis,
    scope_key: Vec<u8>,
    policy_version: u64,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StreamTrimDebugCounts {
    pub state_rows: usize,
    pub due_markers: usize,
}

struct TableDrivenItemCleanupTarget {
    table_name: storage_types::TableName,
    item_stream: StreamName,
    max_item_id: StreamItemId,
}

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub(crate) async fn write_stream_trim_state_kv(
        &self,
        state: StreamTrimState,
    ) -> StorageResult<()> {
        let write = StreamTrimStateWrite {
            state: state.clone(),
            next_marker: state
                .next_due_at
                .map(|due_at| StreamTrimDueMarker::new(due_at, state.scope, state.policy_version)),
        };
        self.write_stream_trim_state_with_marker(write).await
    }

    pub(crate) async fn list_due_stream_trim_markers_kv(
        &self,
        due_before: TimestampMillis,
        limit: usize,
    ) -> StorageResult<Vec<StreamTrimDueMarker>> {
        let limit = u32::try_from(limit).map_err(|err| {
            StorageError::validation(format!("stream trim marker page limit is too large: {err}"))
        })?;
        let due_range = compact::stream_trim_due_prefix();
        let exclusive_end = compact::stream_trim_due_upper_bound(due_before.timestamp_millis());
        let range = self
            .kv_store
            .get_range(
                &due_range.start,
                &exclusive_end,
                Some(limit),
                None::<ItemKey>,
                true,
            )
            .await?;
        let mut markers = Vec::with_capacity(range.items.len());
        for (_key, value) in range.items {
            markers.push(self.decode_compact_trim_marker(&value).await?);
        }
        Ok(markers)
    }

    pub(crate) async fn write_stream_trim_state_with_marker(
        &self,
        write: StreamTrimStateWrite,
    ) -> StorageResult<()> {
        self.kv_store
            .transact_write_unchecked(self.stream_trim_state_write_ops(write).await?)
            .await
    }

    pub(crate) async fn load_stream_trim_state_kv(
        &self,
        scope: &StreamTrimScope,
    ) -> StorageResult<Option<StreamTrimState>> {
        let table_identity = stream_trim_table_identity(self, &scope.table_name).await?;
        let scope_key = trim_scope_key(&table_identity, scope)?;
        let Some(bytes) = self
            .kv_store
            .get(&compact::stream_trim_state_key(&scope_key), true)
            .await?
        else {
            return Ok(None);
        };
        self.decode_compact_trim_state(&bytes).await.map(Some)
    }

    #[cfg(test)]
    pub(crate) async fn stream_trim_debug_counts_kv(
        &self,
        due_before: TimestampMillis,
    ) -> StorageResult<StreamTrimDebugCounts> {
        let state_rows = self
            .kv_store
            .get_range(
                &[compact::KeyFamily::StreamTrimState.code()],
                &increment_bytes(vec![compact::KeyFamily::StreamTrimState.code()]),
                Some(u32::MAX),
                None::<ItemKey>,
                true,
            )
            .await?
            .items
            .len();
        let due_markers = self
            .list_due_stream_trim_markers_kv(due_before, u32::MAX as usize)
            .await?
            .len();
        Ok(StreamTrimDebugCounts {
            state_rows,
            due_markers,
        })
    }
}

pub(crate) fn stream_trim_state_write_ops_for_identity(
    table_identity: &TableIdentity,
    write: StreamTrimStateWrite,
) -> StorageResult<Vec<DirectWriteOperation>> {
    Ok(
        stream_trim_state_write_batch_items_for_identity(table_identity, write)?
            .into_iter()
            .map(|item| match item.value {
                Some(value) => DirectWriteOperation::Put {
                    key: item.key,
                    value,
                },
                None => DirectWriteOperation::Delete { key: item.key },
            })
            .collect(),
    )
}

pub(crate) fn stream_trim_state_write_batch_items_for_identity(
    table_identity: &TableIdentity,
    write: StreamTrimStateWrite,
) -> StorageResult<Vec<BatchItem>> {
    let scope_key = trim_scope_key(table_identity, &write.state.scope)?;
    let compact_state = compact_trim_state_from_public(&write.state, scope_key.clone());
    let mut items = vec![BatchItem {
        key: compact::stream_trim_state_key(&scope_key),
        value: Some(storage_types::storage_serde::to_bytes(&compact_state)?),
    }];
    if let Some(marker) = write.next_marker {
        let marker = CompactStreamTrimDueMarker {
            due_bucket: marker.due_bucket,
            scope_key: scope_key.clone(),
            policy_version: marker.policy_version,
        };
        items.push(BatchItem {
            key: compact::stream_trim_due_key(
                marker.due_bucket.timestamp_millis(),
                &marker.scope_key,
                marker.policy_version,
            ),
            value: Some(storage_types::storage_serde::to_bytes(&marker)?),
        });
    }
    Ok(items)
}

#[async_trait]
impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> StreamDurationTrimBackend
    for SortedKvDbStorageProvider<S>
{
    async fn list_due_stream_trim_markers(
        &self,
        due_before: TimestampMillis,
        limit: usize,
    ) -> StorageResult<Vec<StreamTrimDueMarker>> {
        self.list_due_stream_trim_markers_kv(due_before, limit)
            .await
    }

    async fn load_stream_trim_state(
        &self,
        scope: &StreamTrimScope,
    ) -> StorageResult<Option<StreamTrimState>> {
        self.load_stream_trim_state_kv(scope).await
    }

    async fn load_stream_trim_boundaries(
        &self,
        scope: &StreamTrimScope,
    ) -> StorageResult<StreamTrimScopeBoundaries> {
        let stream_name = stream_name_for_scope(scope)?;
        let latest_item_id = match scope.kind {
            StreamTrimScopeKind::Table => None,
            StreamTrimScopeKind::Item => {
                let table_identity = stream_trim_table_identity(self, &scope.table_name).await?;
                latest_stream_item_id(self, &stream_name, &table_identity).await?
            }
        };
        let retained_table_pointer_boundary = match scope.kind {
            StreamTrimScopeKind::Table => None,
            StreamTrimScopeKind::Item => {
                let table_identity = stream_trim_table_identity(self, &scope.table_name).await?;
                retained_item_pointer_boundary(self, &table_identity, &stream_name).await?
            }
        };
        let protected_boundary = match scope.kind {
            StreamTrimScopeKind::Table => self
                .oldest_protected_backfill_cursor()
                .await?
                .map(|item_id| StreamTrimBoundary { item_id }),
            StreamTrimScopeKind::Item => None,
        };
        Ok(StreamTrimScopeBoundaries {
            latest_item_id,
            protected_boundary,
            retained_table_pointer_boundary,
        })
    }

    async fn trim_table_stream_page(
        &self,
        request: StreamDurationTrimPageRequest,
    ) -> StorageResult<StreamDurationTrimPageResult> {
        self.trim_stream_page_kv(request).await
    }

    async fn trim_item_stream_page(
        &self,
        request: StreamDurationTrimPageRequest,
    ) -> StorageResult<StreamDurationTrimPageResult> {
        self.trim_stream_page_kv(request).await
    }

    async fn finish_stream_trim_marker(
        &self,
        marker: StreamTrimDueMarker,
        write: Option<StreamTrimStateWrite>,
    ) -> StorageResult<()> {
        let marker_scope_key = trim_scope_key(
            &stream_trim_table_identity(self, &marker.scope.table_name).await?,
            &marker.scope,
        )?;
        let mut operations = vec![DirectWriteOperation::Delete {
            key: compact::stream_trim_due_key(
                marker.due_bucket.timestamp_millis(),
                &marker_scope_key,
                marker.policy_version,
            ),
        }];
        if let Some(write) = write {
            operations.extend(self.stream_trim_state_write_ops(write).await?);
        }
        self.kv_store.transact_write_unchecked(operations).await
    }
}

pub(crate) fn item_stream_duration_write_items(
    table_identity: &TableIdentity,
    table_info: &StoredTableInfo,
    key_attributes: &KeyAttributes,
    _policy_version: u64,
    requested_retention: Option<StreamRetentionDuration>,
) -> StorageResult<Vec<BatchItem>> {
    let Some(retention) = requested_retention else {
        return Ok(Vec::new());
    };
    let item_key = ItemKey::from_key_schema(
        table_info.table_name.clone(),
        &table_info.key_schema,
        key_attributes,
    )
    .map_err(|err| StorageError::validation(format!("custom item stream TTL key failed: {err}")))?;
    let item_stream =
        StreamName::table_item_stream(&table_info.table_name, &item_key).map_err(|err| {
            StorageError::validation(format!("custom item stream TTL scope failed: {err}"))
        })?;
    let item_scope_id = item_stream_scope_id(&item_stream);
    let item_key_hash = item_stream_key_hash(&item_stream);
    let plan = plan_validated_item_stream_duration(
        table_info.table_name.clone(),
        item_scope_id.clone(),
        item_key_hash,
        item_stream_policy_version(retention, table_info.table_stream_duration),
        retention,
        table_info.table_stream_duration,
        TimestampMillis::now(),
    );
    stream_trim_state_write_batch_items_for_identity(
        table_identity,
        StreamTrimStateWrite {
            state: plan.trim_state,
            next_marker: plan.due_marker,
        },
    )
}

pub(crate) fn item_stream_duration_kv_mutations(
    table_identity: &TableIdentity,
    table_info: &StoredTableInfo,
    key_attributes: &KeyAttributes,
    policy_version: u64,
    requested_retention: Option<StreamRetentionDuration>,
) -> StorageResult<Vec<KvMutation>> {
    Ok(item_stream_duration_write_items(
        table_identity,
        table_info,
        key_attributes,
        policy_version,
        requested_retention,
    )?
    .into_iter()
    .map(|item| match item.value {
        Some(value) => KvMutation::Put {
            key: item.key,
            value,
        },
        None => KvMutation::Delete { key: item.key },
    })
    .collect())
}

pub(crate) fn table_stream_policy_version(
    table_retention: StreamRetentionDuration,
    default_item_retention: StreamRetentionDuration,
) -> u64 {
    combined_duration_policy_version(table_retention, default_item_retention)
}

pub(crate) fn item_stream_policy_version(
    requested_retention: StreamRetentionDuration,
    table_retention: StreamRetentionDuration,
) -> u64 {
    combined_duration_policy_version(
        requested_retention,
        StreamRetentionDuration::effective_item_retention(table_retention, requested_retention),
    )
}

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub(crate) async fn stream_trim_state_write_ops(
        &self,
        write: StreamTrimStateWrite,
    ) -> StorageResult<Vec<DirectWriteOperation>> {
        let table_identity =
            stream_trim_table_identity(self, &write.state.scope.table_name).await?;
        stream_trim_state_write_ops_for_identity(&table_identity, write)
    }

    async fn decode_compact_trim_state(&self, bytes: &[u8]) -> StorageResult<StreamTrimState> {
        let compact: CompactStreamTrimState = storage_types::storage_serde::from_bytes(bytes)?;
        let scope = self.decode_trim_scope_key(&compact.scope_key).await?;
        Ok(StreamTrimState {
            scope,
            policy_version: compact.policy_version,
            retention: compact.retention,
            effective_retention: compact.effective_retention,
            next_due_at: compact.next_due_at,
            oldest_retained_version: compact.oldest_retained_version,
            oldest_retained_timestamp: compact.oldest_retained_timestamp,
            latest_version: compact.latest_version,
            latest_timestamp: compact.latest_timestamp,
            updated_at: compact.updated_at,
        })
    }

    async fn decode_compact_trim_marker(&self, bytes: &[u8]) -> StorageResult<StreamTrimDueMarker> {
        let compact: CompactStreamTrimDueMarker = storage_types::storage_serde::from_bytes(bytes)?;
        Ok(StreamTrimDueMarker {
            due_bucket: compact.due_bucket,
            scope: self.decode_trim_scope_key(&compact.scope_key).await?,
            policy_version: compact.policy_version,
        })
    }

    async fn decode_trim_scope_key(&self, scope_key: &[u8]) -> StorageResult<StreamTrimScope> {
        let Some((&kind, payload)) = scope_key.split_first() else {
            return Err(StorageError::internal("compact stream trim scope is empty"));
        };
        if payload.len() < 4 {
            return Err(StorageError::internal(
                "compact stream trim scope is missing table id",
            ));
        }
        let mut table_id = [0u8; 4];
        table_id.copy_from_slice(&payload[..4]);
        let table_id = compact::TableStorageId::new(u32::from_be_bytes(table_id));
        let table = self
            .get_table_identity_from_id(table_id)
            .await?
            .ok_or_else(|| {
                StorageError::internal(&format!(
                    "compact stream trim scope table id {} is missing",
                    table_id.get()
                ))
            })?;
        match kind {
            TRIM_SCOPE_TABLE => Ok(StreamTrimScope::table(
                table_trim_scope_id(table.identity.table_id),
                table.identity.table_name.clone(),
            )),
            TRIM_SCOPE_ITEM => {
                let item_scope = &payload[4..];
                let item_stream = item_stream_name(&table.identity.table_name, item_scope);
                Ok(StreamTrimScope::item(
                    item_stream_scope_id(&item_stream),
                    table.identity.table_name.clone(),
                    item_stream_key_hash(&item_stream),
                ))
            }
            _ => Err(StorageError::internal(
                "compact stream trim scope has invalid kind",
            )),
        }
    }
}

fn compact_trim_state_from_public(
    state: &StreamTrimState,
    scope_key: Vec<u8>,
) -> CompactStreamTrimState {
    CompactStreamTrimState {
        scope_key,
        policy_version: state.policy_version,
        retention: state.retention,
        effective_retention: state.effective_retention,
        next_due_at: state.next_due_at,
        oldest_retained_version: state.oldest_retained_version,
        oldest_retained_timestamp: state.oldest_retained_timestamp,
        latest_version: state.latest_version,
        latest_timestamp: state.latest_timestamp,
        updated_at: state.updated_at,
    }
}

fn trim_scope_key(
    table_identity: &TableIdentity,
    scope: &StreamTrimScope,
) -> StorageResult<Vec<u8>> {
    let mut key = Vec::new();
    match scope.kind {
        StreamTrimScopeKind::Table => {
            key.push(TRIM_SCOPE_TABLE);
            key.extend_from_slice(&table_identity.table_id.get().to_be_bytes());
        }
        StreamTrimScopeKind::Item => {
            key.push(TRIM_SCOPE_ITEM);
            key.extend_from_slice(&table_identity.table_id.get().to_be_bytes());
            let stream_name = stream_name_for_scope(scope)?;
            let item_scope = stream_keys::compact_stream_range(&stream_name, Some(table_identity))?;
            match item_scope {
                stream_keys::CompactStreamRange::Item(range) => {
                    let prefix_len = 1 + 4;
                    key.extend_from_slice(&range.start[prefix_len..]);
                }
                _ => {
                    return Err(StorageError::internal(
                        "item stream trim scope did not resolve to item stream range",
                    ));
                }
            }
        }
    }
    Ok(key)
}

fn table_trim_scope_id(table_id: compact::TableStorageId) -> String {
    format!("kv-table-id:{}", table_id.get())
}

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    async fn trim_stream_page_kv(
        &self,
        request: StreamDurationTrimPageRequest,
    ) -> StorageResult<StreamDurationTrimPageResult> {
        let (page, item_cleanup_targets) = self.trim_stream_rows_page_kv(request).await?;
        if !item_cleanup_targets.is_empty() {
            self.trim_table_driven_item_targets(item_cleanup_targets)
                .await?;
        }
        Ok(page)
    }

    async fn trim_stream_rows_page_kv(
        &self,
        request: StreamDurationTrimPageRequest,
    ) -> StorageResult<(
        StreamDurationTrimPageResult,
        Vec<TableDrivenItemCleanupTarget>,
    )> {
        let stream_name = stream_name_for_scope(&request.scope)?;
        let table_identity = stream_trim_table_identity(self, &request.scope.table_name).await?;
        let stream_range = compact_stream_range_for_name(&stream_name, &table_identity)?;
        let range = self
            .kv_store
            .get_range(
                &stream_range.start,
                &stream_range.end,
                Some(u32::try_from(request.page_limit).unwrap_or(u32::MAX)),
                None::<ItemKey>,
                true,
            )
            .await?;
        let mut deletes = Vec::new();
        let mut deleted_rows = 0usize;
        let mut first_deleted_key = None;
        let mut last_deleted_key = None;
        let mut first_table_pointer_key = None;
        let mut last_table_pointer_key = None;
        let mut item_cleanup_targets = Vec::new();
        for (key, value) in range.items {
            let Some(item_id) = stream_keys::stream_item_id_from_compact_key(&key) else {
                continue;
            };
            if request
                .max_deleted_item_id
                .is_some_and(|max_deleted| item_id > max_deleted)
            {
                break;
            }
            let stored = decode_stream_item(&value)?;
            if stored.created_at >= request.cutoff_timestamp {
                break;
            }
            if first_deleted_key.is_none() {
                first_deleted_key = Some(key.to_vec());
            }
            last_deleted_key = Some(key.to_vec());
            if matches!(request.scope.kind, StreamTrimScopeKind::Table) {
                if let Some(target) = table_driven_item_cleanup_target(
                    &table_identity,
                    &request.scope.table_name,
                    item_id,
                    &stored,
                ) {
                    upsert_item_cleanup_target(&mut item_cleanup_targets, target);
                }
                let table_pointer_key =
                    stream_keys::stream_pointer_table_key_for_stream(&table_identity, item_id);
                if first_table_pointer_key.is_none() {
                    first_table_pointer_key = Some(table_pointer_key.clone());
                }
                last_table_pointer_key = Some(table_pointer_key);
            }
            deleted_rows = deleted_rows.saturating_add(1);
        }
        for target in &item_cleanup_targets {
            let item_pointer_prefix = stream_keys::stream_pointer_item_prefix_for_stream(
                &table_identity,
                &target.item_stream,
            )?;
            let item_pointer_key = stream_keys::stream_pointer_item_key_for_stream(
                &table_identity,
                &target.item_stream,
                target.max_item_id,
            )?;
            deletes.push(DirectWriteOperation::DeleteRange {
                start: item_pointer_prefix.start,
                exclusive_end: increment_bytes(item_pointer_key),
            });
        }
        if let (Some(start), Some(last)) = (first_table_pointer_key, last_table_pointer_key) {
            deletes.push(DirectWriteOperation::DeleteRange {
                start,
                exclusive_end: increment_bytes(last),
            });
        }
        if let (Some(start), Some(last)) = (first_deleted_key, last_deleted_key) {
            deletes.push(DirectWriteOperation::DeleteRange {
                start,
                exclusive_end: increment_bytes(last),
            });
        }
        let range_deletes = deletes
            .iter()
            .filter(|delete| matches!(delete, DirectWriteOperation::DeleteRange { .. }))
            .count();
        let point_deletes = deletes
            .iter()
            .filter(|delete| matches!(delete, DirectWriteOperation::Delete { .. }))
            .count();
        if !deletes.is_empty() {
            self.kv_store.transact_write_unchecked(deletes).await?;
        }
        storage_common::provider_perf::record_amount(
            "kv",
            "custom_stream_duration_rows_deleted",
            deleted_rows as u64,
        );
        storage_common::provider_perf::record_amount(
            "kv",
            "custom_stream_duration_range_deletes",
            range_deletes as u64,
        );
        storage_common::provider_perf::record_amount(
            "kv",
            "custom_stream_duration_point_deletes",
            point_deletes as u64,
        );
        let first_remaining =
            first_remaining_stream_item(self, &stream_name, &table_identity).await?;
        Ok((
            StreamDurationTrimPageResult {
                deleted_rows,
                first_remaining_version: first_remaining
                    .as_ref()
                    .map(|item| ItemStreamVersion::from(item.id)),
                first_remaining_timestamp: first_remaining.map(|item| item.created_at),
            },
            item_cleanup_targets,
        ))
    }

    async fn trim_table_driven_item_targets(
        &self,
        targets: Vec<TableDrivenItemCleanupTarget>,
    ) -> StorageResult<()> {
        if targets.is_empty() {
            return Ok(());
        }
        let now = TimestampMillis::now();
        let mut table_info = None;
        for target in targets {
            let scope_id = item_stream_scope_id(&target.item_stream);
            let scope = StreamTrimScope::item(
                scope_id,
                target.table_name.clone(),
                item_stream_key_hash(&target.item_stream),
            );
            let retention = if let Some(state) = self.load_stream_trim_state_kv(&scope).await? {
                state.effective_retention
            } else {
                let table = match &table_info {
                    Some(table) => table,
                    None => {
                        table_info.get_or_insert(self.get_table_info(&target.table_name).await?)
                    }
                };
                StreamRetentionDuration::effective_item_retention(
                    table.table_stream_duration,
                    table.default_item_stream_duration,
                )
            };
            let Some(cutoff_timestamp) = cutoff_timestamp(now, retention) else {
                continue;
            };
            let boundaries = self.load_stream_trim_boundaries(&scope).await?;
            let Some(max_deleted_item_id) =
                table_driven_item_ceiling(target.max_item_id, &boundaries)
            else {
                continue;
            };
            let _ = self
                .trim_stream_rows_page_kv(StreamDurationTrimPageRequest {
                    scope,
                    cutoff_timestamp,
                    max_deleted_item_id: Some(max_deleted_item_id),
                    page_limit: constants::STREAM_TRIM_READ_LIMIT as usize,
                })
                .await?;
        }
        Ok(())
    }
}

fn table_driven_item_cleanup_target(
    table_identity: &TableIdentity,
    table_name: &storage_types::TableName,
    table_stream_item_id: StreamItemId,
    stored: &crate::stream::item_codec::StoredStreamItem,
) -> Option<TableDrivenItemCleanupTarget> {
    if stored.data_type != StreamDataType::StreamPointer {
        return None;
    }
    if let Ok(pointer) = decode_compact_pointer(&stored.data) {
        if pointer.table_id != table_identity.table_id {
            return None;
        }
        return Some(TableDrivenItemCleanupTarget {
            table_name: table_name.clone(),
            item_stream: item_stream_name(table_name, &pointer.item_scope),
            max_item_id: StreamItemId::from(pointer.item_stream_version),
        });
    }
    let pointer: StoredStreamPointer = match storage_types::storage_serde::from_bytes(&stored.data)
    {
        Ok(pointer) => pointer,
        Err(err) => {
            tracing::warn!(
                error = %err,
                stream_item_id = ?table_stream_item_id,
                "custom stream duration table pointer decode failed"
            );
            return None;
        }
    };
    if pointer.table_name() != table_name {
        return None;
    }
    Some(TableDrivenItemCleanupTarget {
        table_name: table_name.clone(),
        item_stream: pointer.stream_name().clone(),
        max_item_id: StreamItemId::from(pointer.target_item_stream_version()),
    })
}

fn upsert_item_cleanup_target(
    targets: &mut Vec<TableDrivenItemCleanupTarget>,
    target: TableDrivenItemCleanupTarget,
) {
    if let Some(existing) = targets
        .iter_mut()
        .find(|existing| existing.item_stream == target.item_stream)
    {
        existing.max_item_id = existing.max_item_id.max(target.max_item_id);
        return;
    }
    targets.push(target);
}

fn table_driven_item_ceiling(
    target_item_id: StreamItemId,
    boundaries: &StreamTrimScopeBoundaries,
) -> Option<StreamItemId> {
    [
        Some(target_item_id),
        boundaries.latest_item_id.and_then(previous_item_id),
        boundaries
            .retained_table_pointer_boundary
            .as_ref()
            .and_then(|boundary| previous_item_id(boundary.item_id)),
    ]
    .into_iter()
    .flatten()
    .min()
}

fn previous_item_id(item_id: StreamItemId) -> Option<StreamItemId> {
    let mut bytes = *item_id.as_bytes();
    for byte in bytes.iter_mut().rev() {
        if *byte > 0 {
            *byte -= 1;
            return Some(StreamItemId::from(bytes));
        }
        *byte = u8::MAX;
    }
    None
}

fn cutoff_timestamp(
    now: TimestampMillis,
    retention: StreamRetentionDuration,
) -> Option<TimestampMillis> {
    match retention {
        StreamRetentionDuration::Forever => None,
        StreamRetentionDuration::FiniteHours(hours) => {
            Some(now - (i64::from(hours) * constants::MILLIS_PER_HOUR))
        }
    }
}

async fn latest_stream_item_id<S: crate::partition_family::PartitionFamilyKvStore + 'static>(
    provider: &SortedKvDbStorageProvider<S>,
    stream_name: &StreamName,
    table_identity: &TableIdentity,
) -> StorageResult<Option<StreamItemId>> {
    let range = compact_stream_range_for_name(stream_name, table_identity)?;
    let range = provider
        .kv_store
        .get_range(&range.end, &range.start, Some(1), None::<ItemKey>, true)
        .await?;
    Ok(range
        .items
        .first()
        .and_then(|(key, _)| stream_keys::stream_item_id_from_compact_key(key)))
}

async fn first_remaining_stream_item<
    S: crate::partition_family::PartitionFamilyKvStore + 'static,
>(
    provider: &SortedKvDbStorageProvider<S>,
    stream_name: &StreamName,
    table_identity: &TableIdentity,
) -> StorageResult<Option<stream_provider::StreamItem>> {
    let range = compact_stream_range_for_name(stream_name, table_identity)?;
    let range = provider
        .kv_store
        .get_range(&range.start, &range.end, Some(1), None::<ItemKey>, true)
        .await?;
    let Some((key, value)) = range.items.first() else {
        return Ok(None);
    };
    let Some(item_id) = stream_keys::stream_item_id_from_compact_key(key) else {
        return Ok(None);
    };
    Ok(Some(decode_stream_item(value)?.into_stream_item(item_id)))
}

async fn retained_item_pointer_boundary<
    S: crate::partition_family::PartitionFamilyKvStore + 'static,
>(
    provider: &SortedKvDbStorageProvider<S>,
    table_identity: &TableIdentity,
    item_stream: &StreamName,
) -> StorageResult<Option<StreamTrimBoundary>> {
    let range = stream_keys::stream_pointer_item_prefix_for_stream(table_identity, item_stream)?;
    let range = provider
        .kv_store
        .get_range(&range.start, &range.end, Some(1), None::<ItemKey>, true)
        .await?;
    Ok(range.items.first().and_then(|(key, _)| {
        stream_keys::stream_item_id_from_compact_key(key)
            .map(|item_id| StreamTrimBoundary { item_id })
    }))
}

async fn stream_trim_table_identity<
    S: crate::partition_family::PartitionFamilyKvStore + 'static,
>(
    provider: &SortedKvDbStorageProvider<S>,
    table_name: &storage_types::TableName,
) -> StorageResult<TableIdentity> {
    provider
        .get_table_identity_from_name(table_name)
        .await?
        .map(|metadata| metadata.identity.clone())
        .ok_or_else(|| StorageError::table_not_found(table_name))
}

fn compact_stream_range_for_name(
    stream_name: &StreamName,
    table_identity: &TableIdentity,
) -> StorageResult<crate::keyspace::compact::KeyRange> {
    match stream_keys::compact_stream_range(stream_name, Some(table_identity))? {
        stream_keys::CompactStreamRange::System(range)
        | stream_keys::CompactStreamRange::Table(range)
        | stream_keys::CompactStreamRange::Item(range) => Ok(range),
        stream_keys::CompactStreamRange::Legacy => Err(StorageError::internal(
            "custom stream trim scope resolved to a non-table stream",
        )),
    }
}

pub(crate) fn item_stream_scope_id(stream_name: &StreamName) -> String {
    let mut scope_id =
        String::with_capacity(ITEM_STREAM_SCOPE_PREFIX.len() + stream_name.len() * 2);
    scope_id.push_str(ITEM_STREAM_SCOPE_PREFIX);
    for byte in stream_name.as_ref() {
        scope_id.push(char::from(HEX[usize::from(byte >> 4)]));
        scope_id.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    scope_id
}

pub(crate) fn item_stream_key_hash(stream_name: &StreamName) -> String {
    let digest = Uuid::new_v5(&Uuid::NAMESPACE_OID, stream_name.as_ref())
        .as_hyphenated()
        .to_string();
    let mut key_hash = String::with_capacity(ITEM_KEY_HASH_PREFIX.len() + digest.len());
    key_hash.push_str(ITEM_KEY_HASH_PREFIX);
    key_hash.push_str(&digest);
    key_hash
}

pub(crate) fn stream_name_for_scope(scope: &StreamTrimScope) -> StorageResult<StreamName> {
    match scope.kind {
        StreamTrimScopeKind::Table => Ok(StreamName::table_stream(&scope.table_name)),
        StreamTrimScopeKind::Item => {
            decode_item_stream_scope_id(&scope.scope_id).map(StreamName::from)
        }
    }
}

fn decode_item_stream_scope_id(scope_id: &str) -> StorageResult<Vec<u8>> {
    let hex = scope_id
        .strip_prefix(ITEM_STREAM_SCOPE_PREFIX)
        .ok_or_else(|| {
            StorageError::internal("item stream trim scope id is missing kv-stream prefix")
        })?;
    if hex.len() % 2 != 0 {
        return Err(StorageError::internal(
            "item stream trim scope id has odd hex length",
        ));
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for pair in hex.as_bytes().chunks_exact(2) {
        let high = decode_hex_nibble(pair[0])?;
        let low = decode_hex_nibble(pair[1])?;
        bytes.push((high << 4) | low);
    }
    Ok(bytes)
}

fn decode_hex_nibble(byte: u8) -> StorageResult<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(StorageError::internal(
            "item stream trim scope id contains non-hex byte",
        )),
    }
}

fn combined_duration_policy_version(
    requested: StreamRetentionDuration,
    effective: StreamRetentionDuration,
) -> u64 {
    (u64::from(duration_policy_code(requested)) << 32) | u64::from(duration_policy_code(effective))
}

fn duration_policy_code(duration: StreamRetentionDuration) -> u32 {
    match duration {
        StreamRetentionDuration::Forever => FOREVER_POLICY_CODE,
        StreamRetentionDuration::FiniteHours(hours) => u32::from(hours),
    }
}
