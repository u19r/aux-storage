use std::sync::Arc;

use async_trait::async_trait;
use bg_jobs::BackgroundJob;
use futures::StreamExt;
use storage_backfill::merge_protected_backfill_cursor;
use storage_common::STREAM_TRIM_JOB;
use storage_provider::{StorageProvider, StreamDurationTrimConfig, StreamDurationTrimWorker};
use storage_types::{
    AttributeValue, ScanTableRequest, StorageError, StorageResult, StreamItemId, StreamName,
    TableName, TimestampMillis,
};
use stream_provider::{StoredStreamPointer, StreamProvider};
use tracing::{debug, warn};

use crate::{
    SQLiteStorageProvider, constants, sql_statements, transaction_manager::with_transaction,
};

const MULTI_REGION_CONTROL_TABLE: &str = "sys_storage_replication";
const PAYLOAD_ATTR: &str = "payload";
const PK_ATTR: &str = "pk";

pub struct StreamTrimJob {
    provider: Arc<SQLiteStorageProvider>,
}

impl StreamTrimJob {
    pub fn new(provider: Arc<SQLiteStorageProvider>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl BackgroundJob for StreamTrimJob {
    async fn execute(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let work_done = self.provider.run_stream_trim().await?;
        Ok(work_done)
    }
}

struct StreamTrimGroup {
    pointer_stream_item_id: StreamItemId,
    item_stream_item_id: StreamItemId,
    table_name: TableName,
    item_stream: StreamName,
}

struct StreamTrimDelete {
    stream_name: StreamName,
    stream_item_id: StreamItemId,
}

impl SQLiteStorageProvider {
    #[tracing::instrument(level = "debug", skip_all, fields(job = %STREAM_TRIM_JOB))]
    pub(crate) async fn run_stream_trim(&self) -> StorageResult<bool> {
        let job_start = std::time::Instant::now();
        let custom_stats = StreamDurationTrimWorker::new(
            self.clone(),
            StreamDurationTrimConfig {
                marker_page_size: 250,
                stream_page_size: constants::STREAM_TRIM_READ_LIMIT as usize,
            },
        )
        .run_due_page(TimestampMillis::now(), TimestampMillis::now())
        .await?;
        let cutoff = stream_trim_cutoff();
        let protected_cursor_floor = self.oldest_protected_replication_cursor().await?;
        let mut page_token = None;
        let mut stats = StreamTrimStats::default();

        loop {
            let page = StreamProvider::read_forward(
                self,
                StreamName::system_table_stream(),
                page_token,
                constants::STREAM_TRIM_READ_LIMIT,
            )
            .await
            .map_err(|err| {
                tracing::error!(error = %err, "stream trim read failed");
                StorageError::internal(&format!("stream trim read failed: {err}"))
            })?;

            if page.items.is_empty() {
                break;
            }

            let outcome = collect_stream_trim_groups(page.items, cutoff, protected_cursor_floor);
            stats.record_page(&outcome);

            let group_count = outcome.groups.len();
            if group_count > 0 {
                let batch_count = self.delete_stream_trim_groups(outcome.groups).await?;
                stats.record_deletes(group_count, batch_count);
            }

            if outcome.stop_scan || !page.has_more {
                break;
            }

            page_token = page.last_evaluated_key;
            if page_token.is_none() {
                break;
            }
        }

        let runtime_ms = u64::try_from(job_start.elapsed().as_millis()).unwrap_or(u64::MAX);
        emit_stream_trim_metrics(&stats, runtime_ms);
        debug!(
            pages = stats.pages,
            scanned_items = stats.scanned_items,
            deleted_groups = stats.deleted_groups,
            "stream trim completed"
        );

        Ok(stats.deleted_groups > 0 || custom_stats.did_work())
    }

    async fn delete_stream_trim_groups(
        &self,
        groups: Vec<StreamTrimGroup>,
    ) -> StorageResult<usize> {
        let mut delete_batches = Vec::new();
        let mut groups = groups;
        while !groups.is_empty() {
            let take = groups.len().min(constants::STREAM_TRIM_DELETE_BATCH_SIZE);
            let chunk: Vec<StreamTrimGroup> = groups.drain(..take).collect();
            let mut batch = Vec::with_capacity(chunk.len() * 3);
            for group in chunk {
                let StreamTrimGroup {
                    pointer_stream_item_id,
                    item_stream_item_id,
                    table_name,
                    item_stream,
                } = group;
                batch.push(StreamTrimDelete {
                    stream_name: StreamName::system_table_stream(),
                    stream_item_id: pointer_stream_item_id,
                });
                batch.push(StreamTrimDelete {
                    stream_name: StreamName::table_stream(&table_name),
                    stream_item_id: pointer_stream_item_id,
                });
                batch.push(StreamTrimDelete {
                    stream_name: item_stream,
                    stream_item_id: item_stream_item_id,
                });
            }
            delete_batches.push(batch);
        }

        let batch_count = delete_batches.len();
        let results = futures::stream::iter(delete_batches.into_iter().map(|batch| {
            let provider = self.clone();
            async move {
                if constants::STREAM_TRIM_BATCH_DELAY_MS > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(
                        constants::STREAM_TRIM_BATCH_DELAY_MS,
                    ))
                    .await;
                }

                provider.delete_stream_trim_batch(batch).await
            }
        }))
        .buffer_unordered(constants::STREAM_TRIM_DELETE_BATCH_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;

        for result in results {
            if let Err(err) = result {
                tracing::error!(error = %err, "stream trim batch delete failed");
                return Err(err);
            }
        }

        debug!(deleted_batches = batch_count, "stream trim batches applied");
        Ok(batch_count)
    }

    async fn delete_stream_trim_batch(&self, batch: Vec<StreamTrimDelete>) -> StorageResult<()> {
        with_transaction(&self.connection, move |sqlite| {
            for entry in &batch {
                let (sql, params) =
                    sql_statements::delete_stream_item(&entry.stream_name, &entry.stream_item_id);
                sqlite.execute(sql, params).map_err(map_sqlite_error)?;
            }
            Ok(())
        })
        .await
    }

    pub(crate) async fn oldest_protected_replication_cursor(
        &self,
    ) -> StorageResult<Option<StreamItemId>> {
        let table_name = TableName::new(MULTI_REGION_CONTROL_TABLE);
        if !<Self as StorageProvider>::table_exists(self, &table_name).await? {
            return Ok(None);
        }

        let mut protected_floor: Option<StreamItemId> = None;
        let mut exclusive_start_key = None;
        loop {
            let (items, next_key) = <Self as StorageProvider>::scan_table(
                self,
                &ScanTableRequest {
                    table_name: table_name.clone(),
                    index_name: None,
                    limit: Some(250),
                    exclusive_start_key: exclusive_start_key.clone(),
                    consistent_read: true,
                },
            )
            .await?;

            for item in items {
                let item = item.into_attribute_map()?;
                let Some(pk) = string_attr(&item, PK_ATTR) else {
                    continue;
                };
                let Some(payload) = string_attr(&item, PAYLOAD_ATTR) else {
                    continue;
                };
                protected_floor = merge_protected_backfill_cursor(protected_floor, pk, payload)
                    .map_err(|error| {
                        StorageError::internal(&format!(
                            "stream trim active backfill session decode failed: {error}"
                        ))
                    })?;
            }

            if next_key.is_none() {
                break;
            }
            exclusive_start_key = next_key;
        }

        Ok(protected_floor)
    }
}

#[derive(Default)]
struct StreamTrimStats {
    pages: usize,
    scanned_items: usize,
    deleted_groups: usize,
    delete_batches: usize,
    decode_failures: usize,
    protected_groups: usize,
}

impl StreamTrimStats {
    fn record_page(&mut self, outcome: &StreamTrimPageOutcome) {
        self.pages = self.pages.saturating_add(1);
        self.scanned_items = self.scanned_items.saturating_add(outcome.scanned_items);
        self.decode_failures = self.decode_failures.saturating_add(outcome.decode_failures);
        self.protected_groups = self
            .protected_groups
            .saturating_add(outcome.protected_groups);
    }

    fn record_deletes(&mut self, group_count: usize, batch_count: usize) {
        self.deleted_groups = self.deleted_groups.saturating_add(group_count);
        self.delete_batches = self.delete_batches.saturating_add(batch_count);
    }
}

struct StreamTrimPageOutcome {
    groups: Vec<StreamTrimGroup>,
    stop_scan: bool,
    scanned_items: usize,
    decode_failures: usize,
    protected_groups: usize,
}

fn stream_trim_cutoff() -> TimestampMillis {
    TimestampMillis::now() - (constants::STREAM_TRIM_RETENTION_HOURS * constants::MILLIS_PER_HOUR)
}

fn collect_stream_trim_groups(
    items: Vec<stream_provider::StreamItem>,
    cutoff: TimestampMillis,
    protected_cursor_floor: Option<StreamItemId>,
) -> StreamTrimPageOutcome {
    let scanned_items = items.len();
    let mut groups = Vec::new();
    let mut decode_failures = 0usize;
    let mut protected_groups = 0usize;

    for item in items {
        if item.created_at >= cutoff {
            return StreamTrimPageOutcome {
                groups,
                stop_scan: true,
                scanned_items,
                decode_failures,
                protected_groups,
            };
        }
        if protected_cursor_floor.is_some_and(|cursor| item.id >= cursor) {
            protected_groups = protected_groups.saturating_add(1);
            return StreamTrimPageOutcome {
                groups,
                stop_scan: true,
                scanned_items,
                decode_failures,
                protected_groups,
            };
        }

        let stored_pointer: StoredStreamPointer =
            match storage_types::storage_serde::from_bytes(&item.data) {
                Ok(pointer) => pointer,
                Err(err) => {
                    decode_failures += 1;
                    warn!(
                        error = %err,
                        stream_item_id = ?item.id,
                        "stream trim pointer decode failed"
                    );
                    continue;
                }
            };

        groups.push(StreamTrimGroup {
            pointer_stream_item_id: item.id,
            item_stream_item_id: StreamItemId::from(stored_pointer.target_item_stream_version()),
            table_name: stored_pointer.table_name().clone(),
            item_stream: stored_pointer.stream_name().clone(),
        });
    }

    StreamTrimPageOutcome {
        groups,
        stop_scan: false,
        scanned_items,
        decode_failures,
        protected_groups,
    }
}

fn emit_stream_trim_metrics(stats: &StreamTrimStats, runtime_ms: u64) {
    let pages = u64::try_from(stats.pages).unwrap_or(u64::MAX);
    let scanned_items = u64::try_from(stats.scanned_items).unwrap_or(u64::MAX);
    let deleted_groups = u64::try_from(stats.deleted_groups).unwrap_or(u64::MAX);
    let delete_batches = u64::try_from(stats.delete_batches).unwrap_or(u64::MAX);

    #[expect(clippy::cast_precision_loss)]
    {
        metrics_facade::histogram!(metrics_facade::HistogramMetric::StreamTrimRuntimeMs)
            .record(runtime_ms as f64);
    }

    metrics_facade::counter!(metrics_facade::CounterMetric::StreamTrimPagesScanned)
        .increment(pages);
    metrics_facade::counter!(metrics_facade::CounterMetric::StreamTrimItemsScanned)
        .increment(scanned_items);
    metrics_facade::counter!(metrics_facade::CounterMetric::StreamTrimGroupsDeleted)
        .increment(deleted_groups);
    metrics_facade::counter!(metrics_facade::CounterMetric::StreamTrimDeleteBatches)
        .increment(delete_batches);
    if stats.protected_groups > 0 {
        metrics_facade::counter!(
            metrics_facade::CounterMetric::StreamTrimGroupsProtectedByReplication
        )
        .increment(u64::try_from(stats.protected_groups).unwrap_or(u64::MAX));
    }

    if stats.decode_failures > 0 {
        metrics_facade::counter!(metrics_facade::CounterMetric::StreamTrimDecodeFailures)
            .increment(u64::try_from(stats.decode_failures).unwrap_or(u64::MAX));
    }
}

fn string_attr<'a>(
    item: &'a std::collections::HashMap<String, AttributeValue>,
    attr_name: &str,
) -> Option<&'a str> {
    item.get(attr_name).and_then(|value| value.inner_str().ok())
}

fn map_sqlite_error(err: rusqlite::Error) -> StorageError {
    crate::error_handler::map_sqlite_error(err)
}
