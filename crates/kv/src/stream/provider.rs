use std::{collections::BTreeSet, time::Instant};

use async_trait::async_trait;
use bg_jobs::{BackgroundJob, BackgroundJobName, JobConfig, errors::JobError};
use futures::{StreamExt, TryStreamExt};
use storage_types::{
    DurationSeconds, ItemKey, ItemStreamVersion, StorageError, StorageResult, StreamItemId,
    StreamKey, StreamName, TableName, TimestampMillis, UserStreamName, context::ErrorContext,
};
use stream_provider::{
    CursorName, CursorPage, CursorPosition, PointerRecordsResult, StoredStreamPointer, Stream,
    StreamCursor, StreamDataType, StreamError, StreamItem, StreamPage, StreamPartitioningMode,
    StreamPointer, StreamProvider, StreamResult, StreamValidationKind, validate_limit,
};
use uuid::Uuid;

use crate::{
    constants::{
        STREAM_TTL_CLEANUP_ITEMS_DELETED_TOTAL_METRIC, STREAM_TTL_CLEANUP_RUNS_TOTAL_METRIC,
        STREAM_TTL_CLEANUP_RUNTIME_MS_METRIC, STREAM_TTL_CLEANUP_STREAMS_SCANNED_TOTAL_METRIC,
    },
    helpers::increment_bytes,
    key_template::{KeyTemplate, UniquePlaceholderBinding},
    keyspace::{
        compact,
        stream_keys::{self, CompactStreamRange},
    },
    partition_family::{
        DEFAULT_ORDERED_LOG_PARTITION_COUNT, OrderedLogSplitBoundary, PartitionFamilyKind,
        PartitionFamilyKvStore, PartitionInfo, ResolvedPartitionFamily,
        apply_ordered_log_split_boundaries, default_partition_family_config,
        find_partition_for_hash, initial_partition_infos, ordered_log_family_component,
        ordered_log_hash, ordered_log_partition_prefix_with_slot,
        ordered_log_partition_prefixes_for_infos, ordered_log_split_marker_family_prefix,
        parse_ordered_log_split_boundary_from_key, parse_ordered_log_split_marker,
        parse_partitioned_stream_item_id, partition_family_config_bytes,
        partition_family_epoch_bytes, partition_info_bytes, stream_partition_marker_bytes,
        stream_partition_marker_key, supports_pointer_stream_partitioning,
    },
    sorted_kv::SortedKvDbStorageProvider,
    sorted_kv_store::{RangeResult, RawKey, TransactWriteOperation, TransactionPriority},
    stream::{
        item_codec::{decode_stream_item, encode_stream_item},
        metadata_keys::{stream_cursor_key, stream_cursors_prefix, stream_metadata_key},
        pointer_codec::decode_compact_pointer,
    },
};

const TTL_CLEANUP_JOB_ID: BackgroundJobName = BackgroundJobName::Database {
    kind: bg_jobs::DatabaseJobKind::StreamTtlCleanup,
};

#[derive(Debug, Default)]
pub struct DirectStreamPointerAudit {
    pub table_stream_rows: u64,
    pub table_pointer_index_rows: u64,
    pub decoded_pointer_rows: u64,
    pub embedded_pointer_rows: u64,
    pub missing_system_rows: u64,
    pub missing_table_pointer_indexes: u64,
    pub missing_item_stream_rows: u64,
    pub missing_item_pointer_indexes: u64,
    pub orphaned_table_pointer_indexes: u64,
    pub decoupled_pointer_target_rows: u64,
}

impl DirectStreamPointerAudit {
    #[must_use]
    pub const fn anomaly_count(&self) -> u64 {
        self.missing_system_rows
            + self.missing_table_pointer_indexes
            + self.missing_item_stream_rows
            + self.missing_item_pointer_indexes
            + self.orphaned_table_pointer_indexes
    }
}

/// Background job for cleaning up expired stream items based on TTL
pub struct TtlCleanupJob<S: PartitionFamilyKvStore + 'static> {
    provider: std::sync::Arc<SortedKvDbStorageProvider<S>>,
}

impl<S: PartitionFamilyKvStore + 'static> TtlCleanupJob<S> {
    pub fn new(provider: std::sync::Arc<SortedKvDbStorageProvider<S>>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl<S: PartitionFamilyKvStore + 'static> BackgroundJob for TtlCleanupJob<S> {
    async fn execute(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let cleaned_count = self.provider.cleanup_expired_items().await?;
        Ok(cleaned_count > 0)
    }
}

async fn get_cursor_position_id<S: PartitionFamilyKvStore + 'static>(
    provider: &SortedKvDbStorageProvider<S>,
    stream_name: &StreamName,
    position: &CursorPosition,
) -> StreamResult<StreamItemId> {
    match position {
        CursorPosition::Tail => {
            let page = provider.read_backward(stream_name.clone(), None, 1).await?;
            Ok(page
                .items
                .first()
                .map(|item| item.id)
                .unwrap_or_else(|| StreamItemId::from(Uuid::now_v7())))
        }
        CursorPosition::Head => Ok(StreamItemId::from(Uuid::nil())), /* Position before the first
                                                                      * item */
    }
}

#[async_trait]
impl<S: PartitionFamilyKvStore + 'static> StreamProvider for SortedKvDbStorageProvider<S> {
    async fn initialize_stream(&self) -> StreamResult<()> {
        // KV store doesn't require explicit initialization for streams
        // Ensure TTL cleanup job is running (idempotent)
        self.start_cleanup_task(1).await?;
        self.start_partition_reconcile_task().await?;
        Ok(())
    }

    async fn create_stream(
        &self,
        user_stream_name: UserStreamName,
        ttl_seconds: Option<DurationSeconds>,
        partitioning_mode: StreamPartitioningMode,
    ) -> StreamResult<StreamName> {
        let partitioning_mode = if self.kv_store.supports_partition_families() {
            partitioning_mode
        } else {
            StreamPartitioningMode::Single
        };
        let internal_id: StreamName = StreamName::new(Uuid::now_v7().to_string().as_bytes());
        let internal_id_for_return = internal_id.clone();
        let created_at = TimestampMillis::now();
        let stream_metadata_key = stream_metadata_key(&user_stream_name);

        // Check if stream already exists
        if self
            .kv_store
            .get(&stream_metadata_key, true)
            .await?
            .is_some()
        {
            return Err(StreamError::stream_already_exists(
                user_stream_name.to_string(),
            ));
        }

        // Create stream metadata
        let stream = Stream {
            name: user_stream_name,
            internal_id,
            ttl_seconds,
            partitioning_mode,
            created_at,
        };

        let stream_bytes = serialize_stream(&stream)?;

        self.kv_store
            .put(&stream_metadata_key, &stream_bytes, None)
            .await?;

        if self.kv_store.supports_partition_families()
            && partitioning_mode == StreamPartitioningMode::KeyOrdered
        {
            let _ = self
                .ensure_ordered_log_family_state(
                    &internal_id_for_return,
                    DEFAULT_ORDERED_LOG_PARTITION_COUNT,
                )
                .await?;
            let marker_key = stream_partition_marker_key(&internal_id_for_return);
            let marker_bytes = stream_partition_marker_bytes(DEFAULT_ORDERED_LOG_PARTITION_COUNT)?;
            self.kv_store.put(&marker_key, &marker_bytes, None).await?;
        }

        Ok(internal_id_for_return)
    }

    async fn delete_stream(&self, user_stream_name: UserStreamName) -> StreamResult<()> {
        // Get stream to check if it exists and get internal name
        let stream = self
            .get_stream(user_stream_name.clone())
            .await?
            .ok_or_else(|| StreamError::stream_not_found(user_stream_name.to_string()))?;

        let internal_stream_name = stream.internal_id;
        if let Some(family) = self
            .load_ordered_log_family_state(&internal_stream_name)
            .await?
        {
            for prefix in
                ordered_log_partition_prefixes_for_infos(&internal_stream_name, &family.partitions)
            {
                self.kv_store.delete_prefix(prefix).await?;
            }
            self.delete_partition_family_state(
                PartitionFamilyKind::OrderedLog,
                &ordered_log_family_component(&internal_stream_name),
            )
            .await?;
            let marker_key = stream_partition_marker_key(&internal_stream_name);
            let _ = self.kv_store.delete(&marker_key).await;
        }

        // Delete all items
        let items_range = self
            .kv_store
            .get_prefix(
                &internal_stream_name,
                true,
                None, // Delete all
                true,
            )
            .await?;

        for (key_bytes, _) in items_range.items {
            self.kv_store.delete(&key_bytes).await?;
        }

        // Delete all cursors
        let cursors_prefix = stream_cursors_prefix(&internal_stream_name);
        let cursors_range = self
            .kv_store
            .get_prefix(
                &cursors_prefix,
                true,
                None, // Delete all
                true,
            )
            .await?;

        for (key_bytes, _) in cursors_range.items {
            self.kv_store.delete(&key_bytes).await?;
        }

        // Delete stream metadata
        let stream_metadata_key = stream_metadata_key(&user_stream_name);
        self.kv_store.delete(&stream_metadata_key).await?;

        Ok(())
    }

    async fn get_stream(&self, user_stream_name: UserStreamName) -> StreamResult<Option<Stream>> {
        let stream_metadata_key = stream_metadata_key(&user_stream_name);

        match self.kv_store.get(&stream_metadata_key, true).await? {
            Some(data) => {
                let stream = deserialize_stream(&data)?;
                Ok(Some(stream))
            }
            None => Ok(None),
        }
    }

    async fn append_item(
        &self,
        stream_name: StreamName,
        item_data: &[u8],
        partition_key: Option<&str>,
    ) -> StreamResult<StreamItemId> {
        let item_id = StreamItemId::from(Uuid::now_v7());
        let created_at = TimestampMillis::now();

        let stream_item = StreamItem {
            id: item_id,
            stream_name: Some(stream_name.clone()),
            data: item_data.to_vec(),
            data_type: StreamDataType::Text,
            created_at,
        };

        let item_bytes = serialize_stream_item(&stream_item)?;

        if let Some(routing_state) = self.ordered_log_routing_state(&stream_name).await? {
            let routing_key = partition_key.ok_or_else(|| {
                StreamError::validation(StreamValidationKind::MissingPartitionKey)
            })?;
            if let Some(assigned_id) = self
                .kv_store
                .append_partitioned_ordered_log_item(
                    &stream_name,
                    routing_key.as_bytes(),
                    &item_bytes,
                    item_id,
                )
                .await?
            {
                return Ok(assigned_id);
            }
            let template_prefix = match routing_state {
                OrderedLogRoutingState::Control(family) => {
                    let hash = ordered_log_hash(routing_key.as_bytes());
                    let partition =
                        find_partition_for_hash(&family.partitions, hash).ok_or_else(|| {
                            StreamError::validation(StreamValidationKind::NoWritablePartition)
                        })?;
                    ordered_log_partition_prefix_with_slot(
                        &stream_name,
                        partition.placement_slot,
                        partition.partition_id,
                    )
                }
            };
            let binding = UniquePlaceholderBinding::new(item_id.as_bytes().to_vec());
            let binding_id = binding.id();
            let template = KeyTemplate::unique_placeholder(template_prefix, Vec::new(), binding);
            let output = self
                .kv_store
                .transact_write(vec![TransactWriteOperation::PutTemplate {
                    template,
                    value: item_bytes,
                    condition: None,
                }])
                .await?;

            let assigned_id = output
                .placeholder_versions
                .get(&binding_id)
                .map_or(item_id, |bytes| StreamItemId::from(*bytes));
            return Ok(assigned_id);
        }

        let binding = UniquePlaceholderBinding::new(item_id.as_bytes().to_vec());
        let binding_id = binding.id();
        let template =
            KeyTemplate::unique_placeholder(stream_key_prefix(&stream_name), Vec::new(), binding);

        let output = self
            .kv_store
            .transact_write(vec![TransactWriteOperation::PutTemplate {
                template,
                value: item_bytes,
                condition: None,
            }])
            .await?;

        let assigned_id = output
            .placeholder_versions
            .get(&binding_id)
            .map_or(item_id, |bytes| StreamItemId::from(*bytes));

        Ok(assigned_id)
    }

    async fn read_forward(
        &self,
        stream_name: StreamName,
        page_token: Option<StreamItemId>,
        limit: u32,
    ) -> StreamResult<StreamPage> {
        validate_limit(limit)?;

        if let Some(range) = self.compact_stream_range_for_name(&stream_name).await? {
            return self
                .read_compact_stream_forward(range, page_token, limit)
                .await;
        }

        if let Some(routing_state) = self.ordered_log_routing_state(&stream_name).await? {
            return self
                .read_partitioned_stream(stream_name, page_token, limit, false, routing_state)
                .await;
        }

        let start_key = match page_token {
            Some(token) => {
                let incremented = token.increment();
                let key = &stream_name + &incremented;
                key.to_vec()
            }
            None => stream_name.clone().to_vec(),
        };

        let range_result = self
            .kv_store
            .get_range(
                &start_key,
                &increment_bytes(stream_name.clone().into()), // Exclusive end
                Some(limit),
                None::<ItemKey>,
                true,
            )
            .await?;

        let mut items = Vec::new();
        for (key_bytes, value_bytes) in range_result.items {
            let stored = deserialize_stream_item(&value_bytes)?;
            if let Some(derived) = stream_item_id_from_key(&stream_name, &key_bytes) {
                items.push(stored.into_stream_item(derived));
            }
        }

        let has_more = range_result.has_more;

        let last_evaluated_key = items.last().map(|item| item.id);

        Ok(StreamPage {
            items,
            last_evaluated_key,
            has_more,
        })
    }

    async fn read_backward(
        &self,
        stream_name: StreamName,
        exclusive_start_key: Option<StreamItemId>,
        limit: u32,
    ) -> StreamResult<StreamPage> {
        validate_limit(limit)?;

        if let Some(range) = self.compact_stream_range_for_name(&stream_name).await? {
            return self
                .read_compact_stream_backward(range, exclusive_start_key, limit)
                .await;
        }

        if let Some(routing_state) = self.ordered_log_routing_state(&stream_name).await? {
            return self
                .read_partitioned_stream(
                    stream_name,
                    exclusive_start_key,
                    limit,
                    true,
                    routing_state,
                )
                .await;
        }

        let (range_start, range_end, page_token) = if let Some(token) = exclusive_start_key {
            // For backward reading with exclusive_start_key, we want items < token
            // Start from the beginning of the stream
            let range_start = Vec::from(&stream_name);
            // End at the token (exclusive)
            let end_key: StreamKey = &stream_name + &token;
            let range_end = end_key.as_ref().to_vec();
            (range_start, range_end, Some(end_key))
        } else {
            // No exclusive_start_key means read from the end
            let range_start = Vec::from(&stream_name);
            let range_end = increment_bytes(Vec::from(&stream_name));
            (range_start, range_end, None)
        };

        let range_result = if let Some(page_token) = page_token {
            self.kv_store
                .get_range(
                    &range_end,
                    &range_start,
                    Some(limit),
                    Some(page_token),
                    true,
                )
                .await?
        } else {
            self.kv_store
                .get_range(
                    &range_end,
                    &range_start,
                    Some(limit),
                    None::<StreamKey>,
                    true,
                )
                .await?
        };

        let mut items = Vec::new();
        for (key_bytes, value_bytes) in range_result.items {
            let stored = deserialize_stream_item(&value_bytes)
                .context("Failed to deserialize stream item")?;
            if let Some(derived) = stream_item_id_from_key(&stream_name, &key_bytes) {
                items.push(stored.into_stream_item(derived));
            }
        }

        let has_more = range_result.has_more;
        let last_evaluated_key = items.last().map(|item| item.id);

        Ok(StreamPage {
            items,
            last_evaluated_key,
            has_more,
        })
    }

    async fn decode_stored_stream_pointer(
        &self,
        pointer_item: &StreamItem,
    ) -> StreamResult<StoredStreamPointer> {
        if let Ok(pointer) = decode_compact_pointer(&pointer_item.data) {
            let table = self
                .get_table_identity_from_id(pointer.table_id)
                .await?
                .ok_or_else(|| StreamError::internal("compact stream pointer table is missing"))?;
            return pointer
                .into_stored_pointer(&table.identity)
                .map_err(StreamError::from);
        }
        storage_types::storage_serde::from_bytes(&pointer_item.data).map_err(|error| {
            StreamError::internal_with_detail(
                stream_provider::StreamInternalKind::ParseStreamPointer,
                format_args!("pointer {}: {error}", pointer_item.id),
            )
        })
    }

    async fn read_item_stream_backward_from_pointer(
        &self,
        stream_name: StreamName,
        _pointer_stream_item_id: StreamItemId,
        target_item_stream_version: ItemStreamVersion,
        limit: u32,
    ) -> StreamResult<StreamPage> {
        self.read_backward(
            stream_name,
            Some(StreamItemId::from(target_item_stream_version).increment()),
            limit,
        )
        .await
    }

    async fn get_items_from_pointer_stream(
        &self,
        pointer_stream_name: StreamName,
        starting_item_id: Option<StreamItemId>,
        limit: Option<u32>,
    ) -> StreamResult<PointerRecordsResult> {
        let item_pointers = self
            .read_forward(
                pointer_stream_name,
                starting_item_id,
                limit.unwrap_or(100).clamp(1, 1000),
            )
            .await?;

        let mut decoded = Vec::with_capacity(item_pointers.items.len());
        for pointer_item in item_pointers.items {
            decoded.push(self.decode_pointer_item(&pointer_item).await?);
        }
        let records = futures::stream::iter(decoded)
            .map(|decoded| async move {
                match decoded {
                    DecodedPointerItem::Embedded { pointer, items } => {
                        Ok::<_, StreamError>((pointer, items))
                    }
                    DecodedPointerItem::Pointer(pointer) => {
                        let items = self
                            .read_item_stream_backward_from_pointer(
                                pointer.stream_name.clone(),
                                pointer.stream_item_id,
                                pointer.item_stream_version,
                                2,
                            )
                            .await?
                            .items;
                        Ok((pointer, items))
                    }
                }
            })
            .buffered(32)
            .try_collect::<Vec<_>>()
            .await?;

        let last_scanned_key = item_pointers.last_evaluated_key;
        Ok(PointerRecordsResult {
            last_evaluated_key: item_pointers.has_more.then_some(last_scanned_key).flatten(),
            last_scanned_key,
            has_more: item_pointers.has_more,
            records,
        })
    }

    async fn create_cursor(
        &self,
        stream_name: StreamName,
        cursor_name: CursorName,
        position: CursorPosition,
    ) -> StreamResult<()> {
        let cursor_key = stream_cursor_key(&stream_name, &cursor_name);

        // Check if cursor already exists
        if self.kv_store.get(&cursor_key, true).await?.is_some() {
            return Err(StreamError::cursor_already_exists(cursor_name.to_string()));
        }

        // Get position item ID based on cursor position
        let position_id = get_cursor_position_id(self, &stream_name, &position).await?;

        let created_at = TimestampMillis::now();
        let cursor = StreamCursor {
            name: cursor_name,
            stream_name: stream_name.clone(),
            position: position_id,
            created_at,
        };

        let cursor_bytes = serialize_cursor(&cursor)?;

        self.kv_store.put(&cursor_key, &cursor_bytes, None).await?;

        Ok(())
    }

    async fn delete_cursor(
        &self,
        stream_name: StreamName,
        cursor_name: CursorName,
    ) -> StreamResult<()> {
        let cursor_key = stream_cursor_key(&stream_name, &cursor_name);

        // Check if cursor exists
        if self.kv_store.get(&cursor_key, true).await?.is_none() {
            return Err(StreamError::cursor_not_found(cursor_name.to_string()));
        }

        self.kv_store.delete(&cursor_key).await?;
        Ok(())
    }

    async fn read_from_cursor(
        &self,
        stream_name: StreamName,
        cursor_name: CursorName,
        limit: u32,
    ) -> StreamResult<CursorPage> {
        validate_limit(limit)?;

        // Get cursor position
        let cursor = self
            .get_cursor(stream_name.clone(), cursor_name.clone())
            .await?
            .ok_or_else(|| StreamError::cursor_not_found(cursor_name.to_string()))?;
        let page = self
            .read_forward(stream_name, Some(cursor.position), limit)
            .await?;

        Ok(CursorPage {
            items: page.items,
            cursor_position: cursor.position,
            has_more: page.has_more,
        })
    }

    async fn advance_cursor(
        &self,
        stream_name: StreamName,
        cursor_name: CursorName,
        to_item_id: StreamItemId,
    ) -> StreamResult<()> {
        let cursor_key = stream_cursor_key(&stream_name, &cursor_name);

        // Check if cursor exists and get current cursor
        let mut cursor = self
            .get_cursor(stream_name.clone(), cursor_name.clone())
            .await?
            .ok_or_else(|| StreamError::cursor_not_found(cursor_name.to_string()))?;

        // Check if the target item exists in the stream

        if !self.stream_item_exists(&stream_name, to_item_id).await? {
            return Err(StreamError::validation(
                StreamValidationKind::TargetItemNotFound,
            ));
        }

        // Update cursor position
        cursor.position = to_item_id;

        let cursor_bytes = serialize_cursor(&cursor)?;

        self.kv_store.put(&cursor_key, &cursor_bytes, None).await?;

        Ok(())
    }

    async fn get_cursor(
        &self,
        stream_name: StreamName,
        cursor_name: CursorName,
    ) -> StreamResult<Option<StreamCursor>> {
        let cursor_key = stream_cursor_key(&stream_name, &cursor_name);

        match self.kv_store.get(&cursor_key, true).await? {
            Some(data) => {
                let cursor = deserialize_cursor(&data)?;
                Ok(Some(cursor))
            }
            None => Ok(None),
        }
    }

    async fn start_cleanup_task(&self, _parallelism: usize) -> StreamResult<()> {
        if !self.database_jobs_enabled {
            return Ok(());
        }

        if self.job_manager.is_job_running(TTL_CLEANUP_JOB_ID).await {
            return Ok(());
        }

        let cleanup_provider = self.with_transaction_priority(TransactionPriority::Batch);
        let cleanup_job = TtlCleanupJob::new(std::sync::Arc::new(cleanup_provider));
        let config = JobConfig {
            start_immediately: true,
            sleep_duration: std::time::Duration::from_millis(
                self.database_job_intervals.stream_ttl_cleanup_interval_ms.0,
            ),
            jitter_percent: 10,
        };

        match self
            .job_manager
            .register_job(TTL_CLEANUP_JOB_ID, cleanup_job, config)
            .await
        {
            Ok(_) => Ok(()),
            Err(JobError::JobAlreadyRunning) => Ok(()),
            Err(e) => Err(StreamError::cleanup_task(format!(
                "Failed to start TTL cleanup: {e}"
            ))),
        }
    }

    async fn stop_cleanup_task(&self) -> StreamResult<()> {
        match self.job_manager.stop_job(TTL_CLEANUP_JOB_ID).await {
            Ok(()) => Ok(()),
            Err(JobError::JobNotFound { .. } | JobError::JobNotRunning) => Ok(()),
            Err(e) => Err(StreamError::cleanup_task(format!(
                "Failed to stop TTL cleanup: {e}"
            ))),
        }
    }

    async fn cleanup_expired_items(&self) -> StreamResult<u64> {
        let run_start = Instant::now();
        metrics_facade::counter!(STREAM_TTL_CLEANUP_RUNS_TOTAL_METRIC).increment(1);

        let mut cleaned_count = 0u64;
        let mut streams_scanned = 0u64;
        let current_time = TimestampMillis::now();

        // Get all streams
        let streams_prefix = "streams/";
        let streams_result = self
            .kv_store
            .get_prefix(streams_prefix.as_bytes(), true, None, true)
            .await?;

        for (key_bytes, value_bytes) in streams_result.items {
            let key_str = String::from_utf8_lossy(&key_bytes);
            if !key_str.starts_with("streams/") {
                continue;
            }

            // Extract user stream name from key
            let parts: Vec<&str> = key_str.split('/').collect();
            if parts.len() != 2 {
                continue;
            }

            // Deserialize stream metadata
            let stream: Stream = match storage_types::storage_serde::from_bytes(&value_bytes) {
                Ok(stream) => stream,
                Err(_) => continue, // Skip invalid metadata
            };

            // Skip streams without TTL
            let Some(ttl_seconds) = stream.ttl_seconds else {
                continue;
            };
            streams_scanned += 1;

            // Calculate cutoff time
            let cutoff_time = current_time - (i64::from(*ttl_seconds) * 1000);

            // Get all items for this stream

            let items_result = self
                .kv_store
                .get_prefix(&stream.internal_id, true, None, true)
                .await?;

            for (item_key_bytes, item_value_bytes) in items_result.items {
                let stored = match decode_stream_item(&item_value_bytes) {
                    Ok(item) => item,
                    Err(_) => continue,
                };

                if stored.created_at < cutoff_time {
                    // Delete the expired item
                    self.kv_store.delete(&item_key_bytes).await?;
                    cleaned_count += 1;
                } else {
                    // Items are stored in chronological order, so we can break early
                    break;
                }
            }
        }

        let elapsed_ms = run_start.elapsed().as_secs_f64() * 1000.0;
        metrics_facade::histogram!(STREAM_TTL_CLEANUP_RUNTIME_MS_METRIC).record(elapsed_ms);
        if cleaned_count > 0 {
            metrics_facade::counter!(STREAM_TTL_CLEANUP_ITEMS_DELETED_TOTAL_METRIC)
                .increment(cleaned_count);
        }
        if streams_scanned > 0 {
            metrics_facade::counter!(STREAM_TTL_CLEANUP_STREAMS_SCANNED_TOTAL_METRIC)
                .increment(streams_scanned);
        }
        Ok(cleaned_count)
    }
}

enum OrderedLogRoutingState {
    Control(ResolvedPartitionFamily),
}

enum DecodedPointerItem {
    Pointer(StreamPointer),
    Embedded {
        pointer: StreamPointer,
        items: Vec<StreamItem>,
    },
}

type RawRangeItem = (Box<[u8]>, Box<[u8]>);

fn compact_range(range: CompactStreamRange) -> crate::keyspace::compact::KeyRange {
    match range {
        CompactStreamRange::System(range)
        | CompactStreamRange::Table(range)
        | CompactStreamRange::Item(range) => range,
        CompactStreamRange::Legacy => unreachable!("legacy stream has no compact range"),
    }
}

fn stream_page_from_compact_range(range_result: RangeResult) -> StreamResult<StreamPage> {
    let mut items = Vec::new();
    for (key_bytes, value_bytes) in range_result.items {
        let Some(derived) = stream_keys::stream_item_id_from_compact_key(&key_bytes) else {
            continue;
        };
        let stored = deserialize_stream_item(&value_bytes)?;
        items.push(stored.into_stream_item(derived));
    }

    let last_evaluated_key = items.last().map(|item| item.id);
    Ok(StreamPage {
        items,
        last_evaluated_key,
        has_more: range_result.has_more,
    })
}

fn stream_item_ids_from_compact_keys(
    items: &[RawRangeItem],
) -> StorageResult<BTreeSet<StreamItemId>> {
    Ok(items
        .iter()
        .filter_map(|(key, _)| stream_keys::stream_item_id_from_compact_key(key))
        .collect::<BTreeSet<_>>())
}

fn item_stream_ids_from_compact_keys(items: &[RawRangeItem]) -> StorageResult<BTreeSet<Vec<u8>>> {
    let mut out = BTreeSet::new();
    for (key, _) in items {
        match compact::parse_compact_key(key)
            .map_err(|err| StorageError::internal(&err.to_string()))?
        {
            compact::ParsedCompactKey::ItemStreamRow { item_scope, .. }
            | compact::ParsedCompactKey::StreamPointerItemIndex { item_scope, .. } => {
                out.insert(item_scope.to_vec());
            }
            _ => {}
        }
    }
    Ok(out)
}

impl<S: PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub async fn audit_table_stream_pointer_integrity(
        &self,
        table_name: &TableName,
        limit: u32,
    ) -> StorageResult<DirectStreamPointerAudit> {
        let table = self
            .get_table_identity_from_name(table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(table_name))?;
        let table_identity = &table.identity;
        let limit = limit.max(1);
        let table_range = compact::table_stream_prefix(table_identity.table_id);
        let table_rows = self
            .kv_store
            .get_range(
                &table_range.start,
                &table_range.end,
                Some(limit),
                None::<RawKey>,
                true,
            )
            .await?;
        let table_pointer_range = compact::stream_pointer_table_prefix(table_identity.table_id);
        let table_pointer_rows = self
            .kv_store
            .get_range(
                &table_pointer_range.start,
                &table_pointer_range.end,
                Some(limit),
                None::<RawKey>,
                true,
            )
            .await?;
        let system_range = compact::system_stream_prefix();
        let system_rows = self
            .kv_store
            .get_range(
                &system_range.start,
                &system_range.end,
                Some(limit),
                None::<RawKey>,
                true,
            )
            .await?;
        let item_range = compact::item_stream_table_prefix(table_identity.table_id);
        let item_rows = self
            .kv_store
            .get_range(
                &item_range.start,
                &item_range.end,
                Some(limit),
                None::<RawKey>,
                true,
            )
            .await?;
        let item_pointer_range = compact::stream_pointer_item_table_prefix(table_identity.table_id);
        let item_pointer_rows = self
            .kv_store
            .get_range(
                &item_pointer_range.start,
                &item_pointer_range.end,
                Some(limit),
                None::<RawKey>,
                true,
            )
            .await?;

        let mut audit = DirectStreamPointerAudit::default();
        let mut table_stream_ids = BTreeSet::new();
        let system_stream_ids = stream_item_ids_from_compact_keys(&system_rows.items)?;
        let table_pointer_ids = stream_item_ids_from_compact_keys(&table_pointer_rows.items)?;
        let item_stream_ids = item_stream_ids_from_compact_keys(&item_rows.items)?;
        let item_pointer_ids = item_stream_ids_from_compact_keys(&item_pointer_rows.items)?;

        for (key, value) in &table_rows.items {
            audit.table_stream_rows += 1;
            let Some(pointer_id) = stream_keys::stream_item_id_from_compact_key(key) else {
                audit.missing_table_pointer_indexes += 1;
                continue;
            };
            table_stream_ids.insert(pointer_id);
            let stored = decode_stream_item(value)?;
            if stored.data_type != StreamDataType::StreamPointer {
                audit.missing_table_pointer_indexes += 1;
                continue;
            }
            let pointer = decode_compact_pointer(&stored.data)?;
            audit.decoded_pointer_rows += 1;
            if pointer.items.is_some() {
                audit.embedded_pointer_rows += 1;
            }
            if pointer.table_id != table_identity.table_id {
                audit.missing_item_stream_rows += 1;
                continue;
            }
            let target_item_id = StreamItemId::from(pointer.item_stream_version);
            if target_item_id != pointer_id {
                audit.decoupled_pointer_target_rows += 1;
            }

            if !system_stream_ids.contains(&pointer_id) {
                audit.missing_system_rows += 1;
            }

            if !table_pointer_ids.contains(&pointer_id) {
                audit.missing_table_pointer_indexes += 1;
            }

            let mut item_target_key = pointer.item_scope.clone();
            item_target_key.extend_from_slice(target_item_id.as_bytes());
            if !item_stream_ids.contains(&item_target_key) {
                audit.missing_item_stream_rows += 1;
            }

            if !item_pointer_ids.contains(&item_target_key) {
                audit.missing_item_pointer_indexes += 1;
            }
        }

        for (key, _) in table_pointer_rows.items {
            audit.table_pointer_index_rows += 1;
            match compact::parse_compact_key(&key)
                .map_err(|err| StorageError::internal(&err.to_string()))?
            {
                compact::ParsedCompactKey::StreamPointerTableIndex { stream_item_id, .. } => {
                    let pointer_id = StreamItemId::try_from(stream_item_id)
                        .map_err(|err| StorageError::internal(&err.to_string()))?;
                    if !table_stream_ids.contains(&pointer_id) {
                        audit.orphaned_table_pointer_indexes += 1;
                    }
                }
                _ => audit.orphaned_table_pointer_indexes += 1,
            }
        }

        Ok(audit)
    }

    async fn decode_pointer_item(
        &self,
        pointer_item: &StreamItem,
    ) -> StreamResult<DecodedPointerItem> {
        let stored_pointer = self.decode_stored_stream_pointer(pointer_item).await?;
        match stored_pointer {
            StoredStreamPointer::Embedded {
                stream_name,
                table_name,
                item_stream_version,
                items,
                indexers,
                old_indexers,
                ..
            } => {
                let pointer = StreamPointer {
                    stream_name,
                    table_name,
                    item_stream_version,
                    stream_item_id: pointer_item.id,
                    indexers,
                    old_indexers,
                };
                let items = items
                    .into_iter()
                    .map(|item| StreamItem {
                        id: StreamItemId::from(item_stream_version),
                        stream_name: None,
                        data: item.data,
                        data_type: item.data_type,
                        created_at: pointer_item.created_at,
                    })
                    .collect();
                Ok(DecodedPointerItem::Embedded { pointer, items })
            }
            StoredStreamPointer::Pointer {
                stream_name,
                table_name,
                item_stream_version,
                indexers,
                old_indexers,
                ..
            } => Ok(DecodedPointerItem::Pointer(StreamPointer {
                stream_name,
                table_name,
                item_stream_version,
                stream_item_id: pointer_item.id,
                indexers,
                old_indexers,
            })),
        }
    }

    async fn compact_stream_range_for_name(
        &self,
        stream_name: &StreamName,
    ) -> StreamResult<Option<CompactStreamRange>> {
        let table_identity =
            if let Some(table_name) = stream_keys::table_name_for_stream(stream_name) {
                self.get_table_identity_from_name(&table_name).await?
            } else {
                None
            };
        let range = stream_keys::compact_stream_range(
            stream_name,
            table_identity.as_deref().map(|metadata| &metadata.identity),
        )?;
        match range {
            CompactStreamRange::Legacy => Ok(None),
            compact => Ok(Some(compact)),
        }
    }

    async fn read_compact_stream_forward(
        &self,
        range: CompactStreamRange,
        page_token: Option<StreamItemId>,
        limit: u32,
    ) -> StreamResult<StreamPage> {
        let range = compact_range(range);
        let start = page_token
            .map(|token| {
                let mut start = range.start.clone();
                start.extend_from_slice(token.increment().as_bytes());
                start
            })
            .unwrap_or_else(|| range.start.clone());
        let range_result = self
            .kv_store
            .get_range(&start, &range.end, Some(limit), None::<RawKey>, true)
            .await?;
        stream_page_from_compact_range(range_result)
    }

    async fn read_compact_stream_backward(
        &self,
        range: CompactStreamRange,
        exclusive_start_key: Option<StreamItemId>,
        limit: u32,
    ) -> StreamResult<StreamPage> {
        let range = compact_range(range);
        let (range_end, page_token) = if let Some(token) = exclusive_start_key {
            let mut end_key = range.start.clone();
            end_key.extend_from_slice(token.as_bytes());
            (end_key.clone(), Some(RawKey(end_key)))
        } else {
            (range.end.clone(), None)
        };
        let range_result = self
            .kv_store
            .get_range(&range_end, &range.start, Some(limit), page_token, true)
            .await?;
        stream_page_from_compact_range(range_result)
    }

    async fn load_ordered_log_split_boundaries(
        &self,
        family_component: &str,
    ) -> StorageResult<Vec<OrderedLogSplitBoundary>> {
        let prefix = ordered_log_split_marker_family_prefix(family_component);
        let entries = self.kv_store.get_prefix(&prefix, true, None, true).await?;
        let mut boundaries = Vec::with_capacity(entries.items.len());

        for (key, value) in entries.items {
            let Some((parent_partition_id, boundary)) =
                parse_ordered_log_split_boundary_from_key(family_component, &key)
            else {
                continue;
            };
            let marker = parse_ordered_log_split_marker(&value)?;
            boundaries.push(OrderedLogSplitBoundary {
                parent_partition_id,
                left_child_partition_id: marker.left_child_partition_id,
                right_child_partition_id: marker.right_child_partition_id,
                boundary,
            });
        }

        boundaries.sort_unstable_by(|left, right| {
            left.boundary
                .cmp(&right.boundary)
                .then(left.parent_partition_id.cmp(&right.parent_partition_id))
        });
        Ok(boundaries)
    }

    pub(crate) async fn load_partition_family_state(
        &self,
        family_kind: PartitionFamilyKind,
        family_component: &str,
    ) -> StorageResult<Option<ResolvedPartitionFamily>> {
        if !self.kv_store.supports_partition_families() {
            return Ok(None);
        }

        let cache_key =
            crate::sorted_kv::PartitionFamilyCacheKey::new(family_kind, family_component);
        if let Some(cached) = self.cached_partition_family(&cache_key) {
            return Ok(Some(cached));
        }

        let Some(mut family) = self
            .kv_store
            .load_partition_family_state_raw(family_kind, family_component)
            .await?
        else {
            self.invalidate_partition_family_cache(&cache_key);
            return Ok(None);
        };

        if family_kind == PartitionFamilyKind::OrderedLog {
            let boundaries = self
                .load_ordered_log_split_boundaries(family_component)
                .await?;
            apply_ordered_log_split_boundaries(&mut family.partitions, &boundaries);
        }
        family.sort_by_hash_range();
        self.cache_partition_family(cache_key, family.clone());

        Ok(Some(family))
    }

    pub(crate) async fn save_partition_family_state(
        &self,
        family_kind: PartitionFamilyKind,
        family_component: &str,
        family: &ResolvedPartitionFamily,
    ) -> StorageResult<()> {
        let cache_key =
            crate::sorted_kv::PartitionFamilyCacheKey::new(family_kind, family_component);
        let mut operations = Vec::with_capacity(family.partitions.len() + 1);
        operations.push(TransactWriteOperation::Put {
            key: crate::partition_family::partition_family_config_key(
                family_kind,
                family_component,
            ),
            value: partition_family_config_bytes(family_component, &family.config)?,
            condition: None,
        });
        operations.push(TransactWriteOperation::Put {
            key: crate::partition_family::partition_family_epoch_key(family_kind, family_component),
            value: partition_family_epoch_bytes(&family.config),
            condition: None,
        });
        for partition in &family.partitions {
            operations.push(TransactWriteOperation::Put {
                key: crate::partition_family::partition_info_key(
                    family_kind,
                    family_component,
                    partition.partition_id,
                ),
                value: partition_info_bytes(partition)?,
                condition: None,
            });
        }
        let _ = self.kv_store.transact_write(operations).await?;
        self.invalidate_partition_family_cache(&cache_key);
        Ok(())
    }

    pub(crate) async fn delete_partition_family_state(
        &self,
        family_kind: PartitionFamilyKind,
        family_component: &str,
    ) -> StorageResult<()> {
        let cache_key =
            crate::sorted_kv::PartitionFamilyCacheKey::new(family_kind, family_component);
        self.kv_store
            .delete(&crate::partition_family::partition_family_config_key(
                family_kind,
                family_component,
            ))
            .await?;
        self.kv_store
            .delete_prefix(crate::partition_family::partition_info_prefix(
                family_kind,
                family_component,
            ))
            .await?;
        if family_kind == PartitionFamilyKind::OrderedLog {
            self.kv_store
                .delete_prefix(ordered_log_split_marker_family_prefix(family_component))
                .await?;
        }
        self.invalidate_partition_family_cache(&cache_key);
        Ok(())
    }

    pub(crate) async fn load_ordered_log_family_state(
        &self,
        stream_name: &StreamName,
    ) -> StreamResult<Option<ResolvedPartitionFamily>> {
        Ok(self
            .load_partition_family_state(
                PartitionFamilyKind::OrderedLog,
                &ordered_log_family_component(stream_name),
            )
            .await?)
    }

    pub(crate) async fn ensure_ordered_log_family_state(
        &self,
        stream_name: &StreamName,
        initial_partition_count: u16,
    ) -> StreamResult<ResolvedPartitionFamily> {
        if let Some(existing) = self.load_ordered_log_family_state(stream_name).await? {
            return Ok(existing);
        }

        let partitions = initial_partition_infos(initial_partition_count);
        let family = ResolvedPartitionFamily {
            config: default_partition_family_config(
                PartitionFamilyKind::OrderedLog,
                initial_partition_count,
            ),
            partitions,
        };
        self.save_partition_family_state(
            PartitionFamilyKind::OrderedLog,
            &ordered_log_family_component(stream_name),
            &family,
        )
        .await?;
        Ok(family)
    }

    async fn ordered_log_routing_state(
        &self,
        stream_name: &StreamName,
    ) -> StreamResult<Option<OrderedLogRoutingState>> {
        if let Some(family) = self.load_ordered_log_family_state(stream_name).await? {
            return Ok(Some(OrderedLogRoutingState::Control(family)));
        }
        Ok(None)
    }

    #[cfg_attr(not(all(test, feature = "foundationdb-backend")), expect(dead_code))]
    pub(crate) async fn split_ordered_log_partition(
        &self,
        stream_name: &StreamName,
        partition_id: u16,
    ) -> StreamResult<()> {
        let family_component = ordered_log_family_component(stream_name);
        let cache_key = crate::sorted_kv::PartitionFamilyCacheKey::new(
            PartitionFamilyKind::OrderedLog,
            &family_component,
        );
        let changed = self
            .kv_store
            .split_partitioned_ordered_log_family(
                &family_component,
                partition_id,
                TimestampMillis::now().timestamp_millis(),
            )
            .await?;
        self.invalidate_partition_family_cache(&cache_key);
        if !changed {
            return Err(StreamError::validation(
                StreamValidationKind::SplitPartitionNotFound,
            ));
        }
        Ok(())
    }

    async fn read_partitioned_stream(
        &self,
        stream_name: StreamName,
        page_token: Option<StreamItemId>,
        limit: u32,
        reverse: bool,
        routing_state: OrderedLogRoutingState,
    ) -> StreamResult<StreamPage> {
        let mut merged = Vec::new();
        let mut has_more = false;
        let mut prefixes: Vec<Vec<u8>> = match routing_state {
            OrderedLogRoutingState::Control(ref family) => family
                .partitions
                .iter()
                .filter(|partition| partition_is_candidate_for_read(partition, page_token, reverse))
                .map(|partition| {
                    ordered_log_partition_prefix_with_slot(
                        &stream_name,
                        partition.placement_slot,
                        partition.partition_id,
                    )
                })
                .collect(),
        };
        if supports_pointer_stream_partitioning(&stream_name) {
            prefixes.push(stream_key_prefix(&stream_name));
        }

        for prefix in prefixes {
            let range_result =
                partitioned_stream_range(&self.kv_store, &prefix, page_token, limit, reverse)
                    .await?;
            if range_result.has_more {
                has_more = true;
            }

            for (key_bytes, value_bytes) in range_result.items {
                let Some(item_id) =
                    stream_item_id_from_prefixed_key(&stream_name, &prefix, &key_bytes)
                else {
                    continue;
                };
                if let Some(token) = page_token {
                    if reverse && item_id >= token {
                        continue;
                    }
                    if !reverse && item_id <= token {
                        continue;
                    }
                }
                let stored = deserialize_stream_item(&value_bytes)?;
                merged.push(stored.into_stream_item(item_id));
            }
        }

        if reverse {
            merged.sort_unstable_by_key(|item| std::cmp::Reverse(item.id));
        } else {
            merged.sort_unstable_by_key(|left| left.id);
        }
        if merged.len() > usize::try_from(limit).unwrap_or(usize::MAX) {
            has_more = true;
            merged.truncate(usize::try_from(limit).unwrap_or(merged.len()));
        }

        let last_evaluated_key = merged.last().map(|item| item.id);
        Ok(StreamPage {
            items: merged,
            last_evaluated_key,
            has_more,
        })
    }

    async fn stream_item_exists(
        &self,
        stream_name: &StreamName,
        item_id: StreamItemId,
    ) -> StreamResult<bool> {
        let table_identity =
            if let Some(table_name) = stream_keys::table_name_for_stream(stream_name) {
                self.get_table_identity_from_name(&table_name).await?
            } else {
                None
            };
        if let Some(key) = stream_keys::stream_row_key(
            stream_name,
            table_identity.as_deref().map(|metadata| &metadata.identity),
            item_id,
        )? && self.kv_store.get(&key, true).await?.is_some()
        {
            return Ok(true);
        }

        if let Some(routing_state) = self.ordered_log_routing_state(stream_name).await? {
            let mut prefixes: Vec<Vec<u8>> = match routing_state {
                OrderedLogRoutingState::Control(family) => family
                    .partitions
                    .iter()
                    .map(|partition| {
                        ordered_log_partition_prefix_with_slot(
                            stream_name,
                            partition.placement_slot,
                            partition.partition_id,
                        )
                    })
                    .collect(),
            };
            if supports_pointer_stream_partitioning(stream_name) {
                prefixes.push(stream_key_prefix(stream_name));
            }
            for mut key in prefixes {
                key.extend_from_slice(item_id.as_bytes());
                if self.kv_store.get(&key, true).await?.is_some() {
                    return Ok(true);
                }
            }
        }

        let item_key = &stream_name.clone() + &item_id;
        Ok(self.kv_store.get(&item_key, true).await?.is_some())
    }
}

fn stream_item_id_from_key(stream_name: &StreamName, key: &[u8]) -> Option<StreamItemId> {
    let prefix = stream_key_prefix(stream_name);
    if key.len() <= prefix.len() || !key.starts_with(&prefix) {
        return None;
    }
    StreamItemId::try_from(&key[prefix.len()..]).ok()
}

fn stream_key_prefix(stream_name: &StreamName) -> Vec<u8> {
    let mut prefix: Vec<u8> = stream_name.into();
    prefix.push(b'/');
    prefix
}

fn stream_item_id_from_prefixed_key(
    stream_name: &StreamName,
    prefix: &[u8],
    key: &[u8],
) -> Option<StreamItemId> {
    if prefix == stream_key_prefix(stream_name) {
        return stream_item_id_from_key(stream_name, key);
    }
    parse_partitioned_stream_item_id(key)
}

async fn partitioned_stream_range<S: PartitionFamilyKvStore + 'static>(
    kv_store: &S,
    prefix: &[u8],
    page_token: Option<StreamItemId>,
    limit: u32,
    reverse: bool,
) -> StorageResult<crate::sorted_kv_store::RangeResult> {
    let Some(token) = page_token else {
        return kv_store
            .get_prefix(prefix, !reverse, Some(limit), true)
            .await;
    };

    let mut token_key = prefix.to_vec();
    token_key.extend_from_slice(token.as_bytes());

    if reverse {
        return kv_store
            .get_range(&token_key, prefix, Some(limit), None::<ItemKey>, true)
            .await;
    }

    let mut start_key = prefix.to_vec();
    start_key.extend_from_slice(token.increment().as_bytes());
    kv_store
        .get_range(
            &start_key,
            &increment_bytes(prefix.to_vec()),
            Some(limit),
            None::<ItemKey>,
            true,
        )
        .await
}

pub(crate) fn partition_is_candidate_for_read(
    partition: &PartitionInfo,
    page_token: Option<StreamItemId>,
    reverse: bool,
) -> bool {
    if !partition.is_readable() {
        return false;
    }

    let Some(token) = page_token else {
        return true;
    };

    if reverse {
        return partition
            .opened_after_id
            .is_none_or(|opened_after_id| opened_after_id < token);
    }

    if matches!(
        partition.state,
        crate::partition_family::PartitionState::WriteClosed
    ) {
        return partition
            .sealed_after_id
            .is_none_or(|sealed_after_id| sealed_after_id > token);
    }

    partition
        .sealed_after_id
        .is_none_or(|sealed_after_id| sealed_after_id > token)
}

// Helper functions for common operations
fn serialize_stream(stream: &Stream) -> StreamResult<Vec<u8>> {
    storage_types::storage_serde::to_bytes(stream).map_err(Into::into)
}

fn deserialize_stream(data: &[u8]) -> StreamResult<Stream> {
    storage_types::storage_serde::from_bytes(data).map_err(Into::into)
}

fn serialize_stream_item(item: &StreamItem) -> StreamResult<Vec<u8>> {
    encode_stream_item(item).map_err(Into::into)
}

fn deserialize_stream_item(
    data: &[u8],
) -> StreamResult<crate::stream::item_codec::StoredStreamItem> {
    decode_stream_item(data).map_err(Into::into)
}

fn serialize_cursor(cursor: &StreamCursor) -> StreamResult<Vec<u8>> {
    storage_types::storage_serde::to_bytes(cursor).map_err(Into::into)
}

fn deserialize_cursor(data: &[u8]) -> StreamResult<StreamCursor> {
    storage_types::storage_serde::from_bytes(data).map_err(Into::into)
}
