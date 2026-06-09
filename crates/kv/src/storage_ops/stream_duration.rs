use async_trait::async_trait;
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
    sorted_kv_store::{BatchItem, DirectWriteOperation},
    stream::item_codec::decode_stream_item,
};

const TRIM_STATE_PREFIX: &[u8] = b"sys/stream-duration/state/";
const TRIM_DUE_MARKER_PREFIX: &[u8] = b"sys/stream-duration/due/";
pub(crate) const STREAM_POINTER_INDEX_PREFIX: &[u8] = b"sys/stream-duration/pointer/";
const ITEM_STREAM_SCOPE_PREFIX: &str = "kv-stream:";
const ITEM_KEY_HASH_PREFIX: &str = "kv-key:";
const ESCAPE_BYTE: u8 = b'%';
const ESCAPED_SLASH: &[u8; 3] = b"%2f";
const ESCAPED_PERCENT: &[u8; 3] = b"%25";
const HEX: &[u8; 16] = b"0123456789abcdef";
const FOREVER_POLICY_CODE: u32 = u32::MAX;

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
        let start = TRIM_DUE_MARKER_PREFIX.to_vec();
        let exclusive_end = due_marker_upper_bound(due_before);
        let range = self
            .kv_store
            .get_range(&start, &exclusive_end, Some(limit), None::<ItemKey>, true)
            .await?;
        range
            .items
            .into_iter()
            .map(|(_key, value)| decode_marker(&value))
            .collect()
    }

    pub(crate) async fn write_stream_trim_state_with_marker(
        &self,
        write: StreamTrimStateWrite,
    ) -> StorageResult<()> {
        self.kv_store
            .transact_write_unchecked(stream_trim_state_write_ops(write)?)
            .await
    }

    pub(crate) async fn load_stream_trim_state_kv(
        &self,
        scope: &StreamTrimScope,
    ) -> StorageResult<Option<StreamTrimState>> {
        self.kv_store
            .get(&state_key(&scope.scope_id), true)
            .await?
            .map(|bytes| storage_types::storage_serde::from_bytes(&bytes))
            .transpose()
    }

    #[cfg(test)]
    pub(crate) async fn stream_trim_debug_counts_kv(
        &self,
        due_before: TimestampMillis,
    ) -> StorageResult<StreamTrimDebugCounts> {
        let state_rows = self
            .kv_store
            .get_range(
                TRIM_STATE_PREFIX,
                &increment_bytes(TRIM_STATE_PREFIX.to_vec()),
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
            StreamTrimScopeKind::Item => latest_stream_item_id(self, &stream_name).await?,
        };
        let retained_table_pointer_boundary = match scope.kind {
            StreamTrimScopeKind::Table => None,
            StreamTrimScopeKind::Item => {
                retained_item_pointer_boundary(self, &scope.table_name, &stream_name).await?
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
        let mut operations = vec![DirectWriteOperation::Delete {
            key: due_marker_key(&marker),
        }];
        if let Some(write) = write {
            operations.extend(stream_trim_state_write_ops(write)?);
        }
        self.kv_store.transact_write_unchecked(operations).await
    }
}

pub(crate) fn item_stream_duration_write_items(
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
    stream_trim_state_write_batch_items(StreamTrimStateWrite {
        state: plan.trim_state,
        next_marker: plan.due_marker,
    })
}

pub(crate) fn item_stream_duration_kv_mutations(
    table_info: &StoredTableInfo,
    key_attributes: &KeyAttributes,
    policy_version: u64,
    requested_retention: Option<StreamRetentionDuration>,
) -> StorageResult<Vec<KvMutation>> {
    Ok(item_stream_duration_write_items(
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

pub(crate) fn stream_trim_state_write_ops(
    write: StreamTrimStateWrite,
) -> StorageResult<Vec<DirectWriteOperation>> {
    Ok(stream_trim_state_write_batch_items(write)?
        .into_iter()
        .map(|item| match item.value {
            Some(value) => DirectWriteOperation::Put {
                key: item.key,
                value,
            },
            None => DirectWriteOperation::Delete { key: item.key },
        })
        .collect())
}

pub(crate) fn stream_trim_state_write_batch_items(
    write: StreamTrimStateWrite,
) -> StorageResult<Vec<BatchItem>> {
    let scope_id = write.state.scope.scope_id.clone();
    let mut items = vec![BatchItem {
        key: state_key(&scope_id),
        value: Some(storage_types::storage_serde::to_bytes(&write.state)?),
    }];
    if let Some(marker) = write.next_marker {
        items.push(BatchItem {
            key: due_marker_key(&marker),
            value: Some(storage_types::storage_serde::to_bytes(&marker)?),
        });
    }
    Ok(items)
}

pub(crate) fn state_key(scope_id: &str) -> Vec<u8> {
    let mut key = TRIM_STATE_PREFIX.to_vec();
    encode_component(scope_id, &mut key);
    key
}

pub(crate) fn due_marker_key(marker: &StreamTrimDueMarker) -> Vec<u8> {
    let mut key = TRIM_DUE_MARKER_PREFIX.to_vec();
    append_padded_i64(marker.due_bucket.timestamp_millis(), &mut key);
    key.push(b'/');
    encode_component(&marker.scope.scope_id, &mut key);
    key.push(b'/');
    append_padded_u64(marker.policy_version, &mut key);
    key
}

pub(crate) fn stream_pointer_index_prefix(table_name: &storage_types::TableName) -> Vec<u8> {
    let mut key = STREAM_POINTER_INDEX_PREFIX.to_vec();
    encode_component(table_name.as_ref(), &mut key);
    key.push(b'/');
    key
}

pub(crate) fn stream_pointer_item_prefix(
    table_name: &storage_types::TableName,
    item_stream: &StreamName,
) -> Vec<u8> {
    let mut key = stream_pointer_index_prefix(table_name);
    encode_component(&item_stream_scope_id(item_stream), &mut key);
    key.push(b'/');
    key
}

pub(crate) fn stream_pointer_table_prefix(table_name: &storage_types::TableName) -> Vec<u8> {
    let mut key = stream_pointer_index_prefix(table_name);
    key.extend(b"table/");
    key
}

pub(crate) fn stream_pointer_index_key(
    table_name: &storage_types::TableName,
    item_stream: &StreamName,
    item_id: storage_types::StreamItemId,
) -> Vec<u8> {
    let mut key = stream_pointer_item_prefix(table_name, item_stream);
    key.extend_from_slice(item_id.as_bytes());
    key
}

pub(crate) fn stream_pointer_table_key(
    table_name: &storage_types::TableName,
    item_id: storage_types::StreamItemId,
) -> Vec<u8> {
    let mut key = stream_pointer_table_prefix(table_name);
    key.extend_from_slice(item_id.as_bytes());
    key
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
        let prefix = stream_key_prefix(&stream_name);
        let range = self
            .kv_store
            .get_range(
                &prefix,
                &increment_bytes(prefix.clone()),
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
            let Some(item_id) = stream_item_id_from_key_prefix(&prefix, &key) else {
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
                if let Some(target) =
                    table_driven_item_cleanup_target(&request.scope.table_name, item_id, &stored)
                {
                    upsert_item_cleanup_target(&mut item_cleanup_targets, target);
                }
                let table_pointer_key =
                    stream_pointer_table_key(&request.scope.table_name, item_id);
                if first_table_pointer_key.is_none() {
                    first_table_pointer_key = Some(table_pointer_key.clone());
                }
                last_table_pointer_key = Some(table_pointer_key);
            }
            deleted_rows = deleted_rows.saturating_add(1);
        }
        for target in &item_cleanup_targets {
            deletes.push(DirectWriteOperation::DeleteRange {
                start: stream_pointer_item_prefix(&request.scope.table_name, &target.item_stream),
                exclusive_end: increment_bytes(stream_pointer_index_key(
                    &request.scope.table_name,
                    &target.item_stream,
                    target.max_item_id,
                )),
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
        let first_remaining = first_remaining_stream_item(self, &stream_name).await?;
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
                        table_info = Some(self.get_table_info(&target.table_name).await?);
                        table_info.as_ref().expect("table info inserted")
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
    table_name: &storage_types::TableName,
    table_stream_item_id: StreamItemId,
    stored: &crate::stream::item_codec::StoredStreamItem,
) -> Option<TableDrivenItemCleanupTarget> {
    if stored.data_type != StreamDataType::StreamPointer {
        return None;
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
) -> StorageResult<Option<StreamItemId>> {
    let prefix = stream_key_prefix(stream_name);
    let range = provider
        .kv_store
        .get_range(
            &increment_bytes(prefix.clone()),
            &prefix,
            Some(1),
            None::<ItemKey>,
            true,
        )
        .await?;
    Ok(range
        .items
        .first()
        .and_then(|(key, _)| stream_item_id_from_key_prefix(&prefix, key)))
}

async fn first_remaining_stream_item<
    S: crate::partition_family::PartitionFamilyKvStore + 'static,
>(
    provider: &SortedKvDbStorageProvider<S>,
    stream_name: &StreamName,
) -> StorageResult<Option<stream_provider::StreamItem>> {
    let prefix = stream_key_prefix(stream_name);
    let range = provider
        .kv_store
        .get_range(
            &prefix,
            &increment_bytes(prefix.clone()),
            Some(1),
            None::<ItemKey>,
            true,
        )
        .await?;
    let Some((key, value)) = range.items.first() else {
        return Ok(None);
    };
    let Some(item_id) = stream_item_id_from_key_prefix(&prefix, key) else {
        return Ok(None);
    };
    Ok(Some(decode_stream_item(value)?.into_stream_item(item_id)))
}

async fn retained_item_pointer_boundary<
    S: crate::partition_family::PartitionFamilyKvStore + 'static,
>(
    provider: &SortedKvDbStorageProvider<S>,
    table_name: &storage_types::TableName,
    item_stream: &StreamName,
) -> StorageResult<Option<StreamTrimBoundary>> {
    let prefix = stream_pointer_item_prefix(table_name, item_stream);
    let range = provider
        .kv_store
        .get_range(
            &prefix,
            &increment_bytes(prefix.clone()),
            Some(1),
            None::<ItemKey>,
            true,
        )
        .await?;
    Ok(range.items.first().and_then(|(key, _)| {
        stream_item_id_from_key_prefix(&prefix, key).map(|item_id| StreamTrimBoundary { item_id })
    }))
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

fn stream_key_prefix(stream_name: &StreamName) -> Vec<u8> {
    let mut prefix: Vec<u8> = stream_name.into();
    prefix.push(b'/');
    prefix
}

fn stream_item_id_from_key_prefix(prefix: &[u8], key: &[u8]) -> Option<StreamItemId> {
    if key.len() <= prefix.len() || !key.starts_with(prefix) {
        return None;
    }
    StreamItemId::try_from(&key[prefix.len()..]).ok()
}

fn due_marker_upper_bound(due_before: TimestampMillis) -> Vec<u8> {
    let mut key = TRIM_DUE_MARKER_PREFIX.to_vec();
    append_padded_i64(due_before.timestamp_millis(), &mut key);
    key.push(b'0');
    increment_bytes(key)
}

fn decode_marker(bytes: &[u8]) -> StorageResult<StreamTrimDueMarker> {
    storage_types::storage_serde::from_bytes(bytes)
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

fn append_padded_i64(value: i64, output: &mut Vec<u8>) {
    append_padded_u64(u64::try_from(value).unwrap_or(0), output);
}

fn append_padded_u64(mut value: u64, output: &mut Vec<u8>) {
    let mut digits = [b'0'; 20];
    for digit in digits.iter_mut().rev() {
        *digit = b'0' + (value % 10) as u8;
        value /= 10;
    }
    output.extend_from_slice(&digits);
}

fn encode_component(component: &str, output: &mut Vec<u8>) {
    for byte in component.as_bytes() {
        match *byte {
            b'/' => output.extend_from_slice(ESCAPED_SLASH),
            ESCAPE_BYTE => output.extend_from_slice(ESCAPED_PERCENT),
            other => output.push(other),
        }
    }
}
