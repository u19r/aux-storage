use std::{sync::atomic::AtomicU64, time::Instant};

use crate::storage_ops::imports::{
    AttributeValue, BackfillConfig, BackfillCoordinator, BackfillDriver, BackfillError,
    BackfillResult, BackfillStatus, BackgroundJob, BatchItem, CursorName, CursorPosition,
    GsiBackfillDescriptor, HashMap, HashSet, ItemKey, PointerRecordsResult, SerializesToKey,
    SortedKvDbStorageProvider, StorageError, StorageResult, StoredTableInfo, StreamDataType,
    StreamItem, StreamItemId, StreamName, StreamPointer, StreamProvider, TableName,
    TimeToLiveStatus, TtlConfigRecord, async_trait, constants, info, now_ms_u64, project_gsi_item,
    should_log_job, ttl,
};

static GSI_UPDATE_LAST_LOG_MS: AtomicU64 = AtomicU64::new(0);

struct PointerBatch {
    records: Vec<(StreamPointer, Vec<StreamItem>)>,
    last_item: Option<StreamItemId>,
    stream_items: usize,
    had_more_pages: bool,
}

struct StreamBatchWrite {
    put_item_value: Option<Vec<u8>>,
    put_item_key: Option<Vec<u8>>,
    delete_item_key: Option<Vec<u8>>,
    put_tombstone_key: Option<Vec<u8>>,
}

impl PointerBatch {
    fn from_result(result: PointerRecordsResult) -> Option<Self> {
        if result.records.is_empty() {
            return None;
        }
        let last_record = result.records.last().map(|(ptr, _)| ptr.stream_item_id);
        let last_item = result.last_evaluated_key.or(last_record);
        let stream_items = result.records.iter().map(|(_, items)| items.len()).sum();
        Some(Self {
            records: result.records,
            last_item,
            stream_items,
            had_more_pages: result.last_evaluated_key.is_some(),
        })
    }
}

struct GsiUpdateRun {
    start: Instant,
    pointer_batches: usize,
    stream_items: usize,
    operations: usize,
    empty_batches: usize,
    had_more_pages: bool,
    work_done: bool,
}

impl GsiUpdateRun {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            pointer_batches: 0,
            stream_items: 0,
            operations: 0,
            empty_batches: 0,
            had_more_pages: false,
            work_done: false,
        }
    }

    fn record_batch(&mut self, batch: &PointerBatch) {
        self.pointer_batches += batch.records.len();
        self.stream_items += batch.stream_items;
        if batch.had_more_pages {
            self.had_more_pages = true;
        }
    }

    fn record_ops(&mut self, ops: usize) {
        self.operations += ops;
        if ops > 0 {
            self.work_done = true;
        }
    }

    fn record_empty(&mut self) {
        self.empty_batches += 1;
    }

    fn finish(self, cursor_advanced: bool) -> bool {
        let elapsed_ms = u64::try_from(self.start.elapsed().as_millis()).unwrap_or(u64::MAX);
        #[expect(clippy::cast_precision_loss)]
        let elapsed_ms_f64 = elapsed_ms as f64;
        metrics_facade::histogram!(metrics_facade::HistogramMetric::GsiUpdateRuntimeMs)
            .record(elapsed_ms_f64);
        metrics_facade::counter!(metrics_facade::CounterMetric::GsiUpdatePointerBatches)
            .increment(self.pointer_batches as u64);
        metrics_facade::counter!(metrics_facade::CounterMetric::GsiUpdateStreamItems)
            .increment(self.stream_items as u64);
        metrics_facade::counter!(metrics_facade::CounterMetric::GsiUpdateOps)
            .increment(self.operations as u64);
        if self.empty_batches > 0 {
            metrics_facade::counter!(metrics_facade::CounterMetric::GsiUpdateEmptyBatches)
                .increment(self.empty_batches as u64);
        }

        let now_ms = now_ms_u64();
        if should_log_job(
            &GSI_UPDATE_LAST_LOG_MS,
            now_ms,
            constants::GSI_UPDATE_LOG_INTERVAL_MS,
        ) && (elapsed_ms >= constants::GSI_UPDATE_SLOW_LOG_MS
            || self.operations > 0
            || self.empty_batches > 0)
        {
            info!(
                elapsed_ms,
                pointer_batches = self.pointer_batches,
                stream_items = self.stream_items,
                operations = self.operations,
                empty_batches = self.empty_batches,
                work_done = self.work_done,
                had_more_pages = self.had_more_pages,
                cursor_advanced,
                "gsi.update.summary"
            );
        }
        self.work_done
    }
}

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    async fn refresh_gsi_update_lag(
        &self,
        stream_name: &StreamName,
        cursor_position: Option<StreamItemId>,
    ) -> StorageResult<()> {
        let page = self
            .read_forward(stream_name.clone(), cursor_position, 1)
            .await
            .map_err(|e| StorageError::internal(&e.to_string()))?;
        let now_ms = now_ms_u64();
        storage_common::observe_gsi_lag(
            &self.gsi_propagation_governor,
            page.items.first().map(|item| item.created_at),
            now_ms,
        );
        Ok(())
    }

    async fn ensure_gsi_cursor(
        &self,
        stream_name: &StreamName,
        cursor_name: &CursorName,
    ) -> StorageResult<Option<StreamItemId>> {
        let cursor_position = self
            .get_cursor(stream_name.clone(), cursor_name.clone())
            .await
            .ok()
            .flatten()
            .map(|cursor| cursor.position);

        if cursor_position.is_none() {
            self.create_cursor(
                stream_name.clone(),
                cursor_name.clone(),
                CursorPosition::Head,
            )
            .await
            .map_err(|e| StorageError::internal(&format!("create cursor failed: {e}")))?;
        }

        Ok(cursor_position)
    }

    async fn advance_gsi_cursor(
        &self,
        stream_name: &StreamName,
        cursor_name: &CursorName,
        last: StreamItemId,
    ) -> StorageResult<()> {
        self.advance_cursor(stream_name.clone(), cursor_name.clone(), last)
            .await
            .map_err(|e| StorageError::internal(&format!("update cursor failed: {e}")))
    }

    async fn advance_cursor_if(
        &self,
        stream_name: &StreamName,
        cursor_name: &CursorName,
        last_item: Option<StreamItemId>,
        cursor_advanced: &mut bool,
    ) -> StorageResult<Option<StreamItemId>> {
        let Some(last) = last_item else {
            return Ok(None);
        };
        self.advance_gsi_cursor(stream_name, cursor_name, last)
            .await?;
        *cursor_advanced = true;
        Ok(Some(last))
    }

    async fn fetch_pointer_batch(
        &self,
        stream_name: &StreamName,
        cursor_position: Option<StreamItemId>,
    ) -> StorageResult<Option<PointerBatch>> {
        let records_result = self
            .get_items_from_pointer_stream(
                stream_name.clone(),
                cursor_position,
                Some(constants::GSI_UPDATE_STREAM_FETCH_LIMIT),
            )
            .await
            .map_err(|e| StorageError::internal(&e.to_string()))?;
        Ok(PointerBatch::from_result(records_result))
    }

    async fn gsi_operations_for_records(
        &self,
        records: Vec<(StreamPointer, Vec<StreamItem>)>,
        table_infos: &mut HashMap<TableName, StoredTableInfo>,
        ttl_configs: &mut HashMap<TableName, Option<TtlConfigRecord>>,
    ) -> StorageResult<Vec<StreamBatchWrite>> {
        let mut operations = Vec::new();
        for (stream_pointer, stream_items) in records {
            let table_name = &stream_pointer.table_name;

            let table_info = if let Some(info) = table_infos.get(table_name) {
                info
            } else {
                let Ok(Some(info)) = self.get_table_metadata_from_name(table_name).await else {
                    continue;
                };
                table_infos.insert(table_name.clone(), info);
                let Some(info) = table_infos.get(table_name) else {
                    continue;
                };
                info
            };

            let Some(ref gsis) = table_info.global_secondary_indexes else {
                continue;
            };
            if gsis.iter().all(|gsi| ttl::is_ttl_index(&gsi.index_name)) {
                continue;
            }

            let ttl_config = if let Some(cfg) = ttl_configs.get(table_name) {
                cfg.clone()
            } else {
                let cfg = self.load_ttl_config(table_name).await?;
                ttl_configs.insert(table_name.clone(), cfg.clone());
                cfg
            };

            for gsi in gsis
                .iter()
                .filter(|gsi| !ttl::is_ttl_index(&gsi.index_name))
            {
                let ttl_attr = ttl_config
                    .as_ref()
                    .filter(|cfg| cfg.gsi_name() == gsi.index_name)
                    .map(|cfg| cfg.attribute_name.as_str());
                let op = Self::stream_updates_to_batch_write_item(
                    &stream_items,
                    table_info,
                    gsi,
                    ttl_attr,
                )?;
                operations.push(op);
            }
        }
        Ok(operations)
    }

    fn dedupe_gsi_operations(mut operations: Vec<StreamBatchWrite>) -> Vec<StreamBatchWrite> {
        let mut modify_keys = HashSet::new();
        operations
            .drain(..)
            .rev()
            .filter_map(|mut op| {
                if let Some(ref put_key) = op.put_item_key
                    && !modify_keys.insert(put_key.clone())
                {
                    op.put_item_key = None;
                    op.put_item_value = None;

                    op.delete_item_key.as_ref()?;
                }

                if let Some(ref delete_key) = op.delete_item_key
                    && !modify_keys.insert(delete_key.clone())
                {
                    op.delete_item_key = None;
                    op.put_item_key.as_ref()?;
                }

                Some(op)
            })
            .collect()
    }

    fn batch_items_from_operations(
        operations: Vec<StreamBatchWrite>,
    ) -> StorageResult<Vec<BatchItem>> {
        let mut batch_items = Vec::new();
        for write_batch in operations {
            let StreamBatchWrite {
                put_item_key,
                put_item_value,
                delete_item_key,
                put_tombstone_key,
            } = write_batch;
            if let (Some(put_key), Some(put_value)) = (put_item_key, put_item_value) {
                batch_items.push(BatchItem {
                    key: put_key,
                    value: Some(put_value),
                });
            }
            if let Some(delete_key) = delete_item_key {
                batch_items.push(BatchItem {
                    key: delete_key,
                    value: None,
                });
            }
            if let Some(tombstone_key) = put_tombstone_key {
                batch_items.push(BatchItem {
                    key: tombstone_key,
                    value: Some(Vec::new()),
                });
            }
        }
        Ok(batch_items)
    }

    pub async fn process_gsi_updates(&self) -> StorageResult<bool> {
        let mut cursor_advanced = false;
        let mut run = GsiUpdateRun::new();

        let cursor_name: CursorName = "gsi-update-cursor".to_string().into();
        let stream_name: StreamName = StreamName::system_table_stream();

        let mut cursor_position = self.ensure_gsi_cursor(&stream_name, &cursor_name).await?;
        self.refresh_gsi_update_lag(&stream_name, cursor_position)
            .await?;

        let mut table_infos: HashMap<TableName, StoredTableInfo> = HashMap::new();
        let mut ttl_configs: HashMap<TableName, Option<TtlConfigRecord>> = HashMap::new();

        'outer: loop {
            let Some(batch) = self
                .fetch_pointer_batch(&stream_name, cursor_position)
                .await?
            else {
                run.record_empty();
                self.refresh_gsi_update_lag(&stream_name, cursor_position)
                    .await?;
                break;
            };

            run.record_batch(&batch);
            let PointerBatch {
                records, last_item, ..
            } = batch;

            let operations = self
                .gsi_operations_for_records(records, &mut table_infos, &mut ttl_configs)
                .await?;
            let operations = Self::dedupe_gsi_operations(operations);
            let batch_items = Self::batch_items_from_operations(operations)?;

            if batch_items.is_empty() {
                run.record_empty();
                cursor_position = self
                    .advance_cursor_if(&stream_name, &cursor_name, last_item, &mut cursor_advanced)
                    .await?;
                self.refresh_gsi_update_lag(&stream_name, cursor_position)
                    .await?;
                break;
            }

            run.record_ops(batch_items.len());
            self.kv_store.batch_write(batch_items).await?;

            cursor_position = self
                .advance_cursor_if(&stream_name, &cursor_name, last_item, &mut cursor_advanced)
                .await?;
            self.refresh_gsi_update_lag(&stream_name, cursor_position)
                .await?;
            if cursor_position.is_none() {
                break 'outer;
            }
        }
        Ok(run.finish(cursor_advanced))
    }

    #[tracing::instrument(level = "debug", skip_all, fields(op = "process_gsi_backfills"))]
    pub(crate) async fn process_gsi_backfills_with(
        &self,
        coordinator: &BackfillCoordinator<SortedKvDbStorageProvider<S>>,
    ) -> StorageResult<bool> {
        let result = coordinator.run_once().await.map_err(|err| match err {
            BackfillError::Storage(inner) => inner,
        })?;
        if matches!(result, BackfillResult::DidWork) {
            let configs = self.list_ttl_configs().await?;
            for (table_name, mut config) in configs {
                if config.status != TimeToLiveStatus::Enabling {
                    continue;
                }
                let descriptor =
                    GsiBackfillDescriptor::new(table_name.as_ref(), config.gsi_name.clone());
                if let Some(state) = self.reload_state(&descriptor).await?
                    && matches!(state.status, BackfillStatus::Done)
                {
                    config.status = TimeToLiveStatus::Enabled;
                    config.touch();
                    self.save_ttl_config(&table_name, &config).await?;
                }
            }
        }
        Ok(matches!(result, BackfillResult::DidWork))
    }

    pub async fn process_gsi_backfills(&self) -> StorageResult<bool> {
        let coordinator =
            BackfillCoordinator::new(std::sync::Arc::new(self.clone()), BackfillConfig::default());
        self.process_gsi_backfills_with(&coordinator).await
    }

    fn stream_updates_to_batch_write_item(
        stream_item: &[StreamItem],
        table_info: &StoredTableInfo,
        gsi: &storage_types::GlobalSecondaryIndex,
        ttl_attribute: Option<&str>,
    ) -> StorageResult<StreamBatchWrite> {
        let table_name = table_info.table_name.clone();
        let table_schema = &table_info.key_schema;
        let gsi_name = &gsi.index_name;
        let gsi_schema = &gsi.key_schema;
        let gsi_projection = &gsi.projection;
        let tombstone_key = |index_key: Option<&Vec<u8>>| {
            index_key.and_then(|key| {
                crate::keys::gsi_tombstone_key_from_index_key(&table_name, gsi_name, key)
            })
        };

        let Some(si) = stream_item.first() else {
            return Ok(StreamBatchWrite {
                put_item_value: None,
                put_item_key: None,
                delete_item_key: None,
                put_tombstone_key: None,
            });
        };

        let data1 = if si.data_type == StreamDataType::DeleteMarker {
            let Some(data) = stream_item.last().map(|si| &si.data) else {
                return Ok(StreamBatchWrite {
                    put_item_value: None,
                    put_item_key: None,
                    delete_item_key: None,
                    put_tombstone_key: None,
                });
            };
            data
        } else {
            &si.data
        };

        let Ok(mut item1) =
            storage_types::storage_serde::from_bytes::<HashMap<String, AttributeValue>>(data1)
        else {
            tracing::warn!("gsi update first stream item decode failed");
            return Ok(StreamBatchWrite {
                put_item_value: None,
                put_item_key: None,
                delete_item_key: None,
                put_tombstone_key: None,
            });
        };

        if let Some(attr) = ttl_attribute
            && let Some(prepared) = ttl::augment_item_with_ttl_partition(table_info, &item1, attr)?
        {
            item1 = prepared;
        }

        let first_gsi_key = match ItemKey::from_key_schema_for_index(
            table_name.clone(),
            table_schema,
            gsi_name,
            gsi_schema,
            &item1,
        )
        .map(|ik| ik.map(|ik| ik.serialize_to_bytes().ok()))
        {
            Ok(Some(gsi_key)) => gsi_key,
            _ => None,
        };

        if si.data_type == StreamDataType::DeleteMarker {
            return Ok(StreamBatchWrite {
                put_item_value: None,
                put_item_key: None,
                delete_item_key: first_gsi_key.clone(),
                put_tombstone_key: tombstone_key(first_gsi_key.as_ref()),
            });
        }

        let Some(si2) = stream_item.get(1) else {
            let filtered_item = project_gsi_item(item1, gsi_projection, table_schema, gsi_schema);
            let put_item_value = storage_types::storage_serde::to_bytes(&filtered_item)?;

            return Ok(StreamBatchWrite {
                put_item_value: Some(put_item_value),
                put_item_key: first_gsi_key,
                delete_item_key: None,
                put_tombstone_key: None,
            });
        };

        if si2.data_type == StreamDataType::DeleteMarker {
            let filtered_item = project_gsi_item(item1, gsi_projection, table_schema, gsi_schema);
            let put_item_value = storage_types::storage_serde::to_bytes(&filtered_item)?;

            return Ok(StreamBatchWrite {
                put_item_value: Some(put_item_value),
                put_item_key: first_gsi_key,
                delete_item_key: None,
                put_tombstone_key: None,
            });
        }

        let Ok(mut item2) =
            storage_types::storage_serde::from_bytes::<HashMap<String, AttributeValue>>(&si2.data)
        else {
            tracing::warn!("gsi update second stream item decode failed");
            return Ok(StreamBatchWrite {
                put_item_value: None,
                put_item_key: None,
                delete_item_key: None,
                put_tombstone_key: None,
            });
        };

        if let Some(attr) = ttl_attribute
            && let Some(prepared) = ttl::augment_item_with_ttl_partition(table_info, &item2, attr)?
        {
            item2 = prepared;
        }

        let second_gsi_key = match ItemKey::from_key_schema_for_index(
            table_name.clone(),
            table_schema,
            gsi_name,
            gsi_schema,
            &item2,
        )
        .map(|ik| ik.map(|ik| ik.serialize_to_bytes().ok()))
        {
            Ok(Some(gsi_key)) => gsi_key,
            _ => None,
        };

        if let Some(gsi2) = second_gsi_key.as_ref()
            && let Some(gsi1) = first_gsi_key.as_ref()
            && gsi2 == gsi1
        {
            // Same key, treat as update
            let filtered_item = project_gsi_item(item1, gsi_projection, table_schema, gsi_schema);
            let put_item_value = storage_types::storage_serde::to_bytes(&filtered_item)?;
            return Ok(StreamBatchWrite {
                put_item_value: Some(put_item_value),
                put_item_key: first_gsi_key,
                delete_item_key: None,
                put_tombstone_key: None,
            });
        }

        // different key, delete previous
        let filtered_item = project_gsi_item(item1, gsi_projection, table_schema, gsi_schema);
        let put_item_value = storage_types::storage_serde::to_bytes(&filtered_item)?;
        Ok(StreamBatchWrite {
            put_item_value: Some(put_item_value),
            put_item_key: first_gsi_key,
            delete_item_key: second_gsi_key.clone(),
            put_tombstone_key: tombstone_key(second_gsi_key.as_ref()),
        })
    }

    pub async fn cleanup_gsi_backfill_tombstones(
        &self,
        table_name: &TableName,
        index_name: &storage_types::IndexName,
    ) -> StorageResult<()> {
        self.kv_store
            .delete_prefix(crate::keys::gsi_tombstone_prefix_from_name(
                table_name, index_name,
            ))
            .await
    }
}

/// Background job for updating global secondary indexes
pub struct GsiUpdateJob<S: crate::partition_family::PartitionFamilyKvStore> {
    provider: std::sync::Arc<SortedKvDbStorageProvider<S>>,
    run_budget: std::time::Duration,
}

impl<S: crate::partition_family::PartitionFamilyKvStore> GsiUpdateJob<S> {
    #[cfg(test)]
    pub fn new(provider: std::sync::Arc<SortedKvDbStorageProvider<S>>) -> Self {
        Self::new_with_interval(
            provider,
            storage_common::GsiJobConfig::default().update_interval_ms,
        )
    }

    pub fn new_with_interval(
        provider: std::sync::Arc<SortedKvDbStorageProvider<S>>,
        interval_ms: storage_common::JobIntervalMillis,
    ) -> Self {
        Self {
            provider,
            run_budget: std::time::Duration::from_millis(interval_ms.0.saturating_mul(95) / 100),
        }
    }
}

#[async_trait]
impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> BackgroundJob
    for GsiUpdateJob<S>
{
    async fn execute(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let mut work_done = false;
        let started = std::time::Instant::now();
        loop {
            let progressed = self.provider.process_gsi_updates().await?;
            work_done |= progressed;
            if !progressed
                || !self.provider.gsi_propagation_governor.lag_above_target()
                || started.elapsed() >= self.run_budget
            {
                break;
            }
        }
        Ok(work_done)
    }
}

/// Background job to resume/persist GSI backfills
pub struct GsiBackfillJob<S: crate::partition_family::PartitionFamilyKvStore> {
    provider: std::sync::Arc<SortedKvDbStorageProvider<S>>,
}

impl<S: crate::partition_family::PartitionFamilyKvStore> GsiBackfillJob<S> {
    pub fn new(provider: std::sync::Arc<SortedKvDbStorageProvider<S>>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> BackgroundJob
    for GsiBackfillJob<S>
{
    async fn execute(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let work_done = self.provider.process_gsi_backfills().await?;
        Ok(work_done)
    }
}
