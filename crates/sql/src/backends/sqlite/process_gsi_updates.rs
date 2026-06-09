use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use async_trait::async_trait;
use bg_jobs::BackgroundJob;
use serde::ser::SerializeMap as _;
use storage_backfill::{GsiCatchupApplyCase, GsiCatchupOutcome, plan_gsi_catchup_apply};
use storage_common::{
    GsiKeyPart, GsiWriteAction, observe_gsi_lag, plan_gsi_write_actions, ttl::is_ttl_index,
};
use storage_provider::StorageProvider as _;
use storage_types::{
    AttributeValue, IndexName, ItemStreamVersion, KeyAttributes, Projection, ProjectionType,
    StorageError, StorageResult, StoredTableInfo, StreamItemId, StreamName, TableName,
    TimestampMillis, context::ErrorContext as _,
};
use stream_provider::{
    CursorName, CursorPosition, PointerRecordsResult, StreamDataType, StreamItem, StreamPointer,
    StreamProvider as _,
};

use crate::{
    GsiPhysicalName, SQLiteStorageProvider, constants, error_handler::map_sqlite_error,
    transaction_manager::with_transaction, utils::call_sqlite,
};

static GSI_UPDATE_LAST_LOG_MS: AtomicU64 = AtomicU64::new(0);

struct PointerBatch {
    records: Vec<(StreamPointer, Vec<StreamItem>)>,
    last_item: Option<StreamItemId>,
    stream_items: usize,
    had_more_pages: bool,
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
            tracing::info!(
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

fn now_ms_u64() -> u64 {
    let now = *TimestampMillis::now();
    u64::try_from(now).unwrap_or(0)
}

fn should_log_job(last_log_ms: &AtomicU64, now_ms: u64, interval_ms: u64) -> bool {
    let last = last_log_ms.load(Ordering::Relaxed);
    if now_ms.saturating_sub(last) >= interval_ms {
        last_log_ms.store(now_ms, Ordering::Relaxed);
        true
    } else {
        false
    }
}

impl SQLiteStorageProvider {
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
        observe_gsi_lag(
            &self.gsi_propagation_governor,
            page.items.first().map(|item| item.created_at),
            now_ms,
        );
        Ok(())
    }

    pub(crate) fn apply_immediate_gsi_updates(
        txn: &rusqlite::Connection,
        table_info: &StoredTableInfo,
        old_item: Option<&HashMap<String, AttributeValue>>,
        new_item: Option<&HashMap<String, AttributeValue>>,
        item_version: ItemStreamVersion,
    ) -> StorageResult<()> {
        let item_version = item_version_i64(item_version)?;
        let table_info = GsiUpdateTableInfo::from(table_info.clone());
        for action in plan_gsi_write_actions(table_info.source.as_ref(), old_item, new_item)? {
            match action {
                GsiWriteAction::Delete {
                    index,
                    gsi_key,
                    table_key,
                } => {
                    let Some(gsi) = table_info.gsi_by_name(&index.index_name) else {
                        continue;
                    };
                    ensure_gsi_metadata_columns(txn, &gsi.gsi_table_name)?;
                    apply_gsi_delete(
                        txn,
                        gsi,
                        &table_info,
                        &key_part_refs(&gsi_key),
                        &key_part_refs(&table_key),
                    )?;
                }
                GsiWriteAction::Put {
                    index,
                    gsi_key,
                    table_key,
                    projected_item,
                } => {
                    let Some(gsi) = table_info.gsi_by_name(&index.index_name) else {
                        continue;
                    };
                    ensure_gsi_metadata_columns(txn, &gsi.gsi_table_name)?;
                    let attributes_blob = build_attributes_blob(&projected_item, gsi, &table_info)?;
                    apply_gsi_put(
                        txn,
                        gsi,
                        &table_info,
                        &key_part_refs(&gsi_key),
                        &key_part_refs(&table_key),
                        attributes_blob,
                        item_version,
                    )?;
                }
            }
        }
        Ok(())
    }

    #[tracing::instrument(
        level = "debug",
        skip(self),
        fields(op = "sqlite_process_gsi_backfills")
    )]
    pub async fn process_gsi_backfills(&self) -> StorageResult<bool> {
        // Resume any in-progress backfills from gsi_backfill table
        let pending: Vec<(TableName, IndexName, Option<String>, Option<String>)> =
            call_sqlite(&self.connection, |conn| {
                let (sql, params) =
                    crate::backends::sqlite::sql_statements::list_pending_gsi_backfills();
                let mut stmt = conn.prepare(sql).map_err(map_sqlite_error)?;
                let rows = stmt
                    .query_map(params, |row| {
                        Ok((
                            TableName::new(&row.get::<_, String>(0)?), // table_name
                            IndexName::new(&row.get::<_, String>(1)?), // index_name
                            row.get::<_, Option<String>>(3)?,          // scan_lek
                            row.get::<_, Option<String>>(4)?,          // captured_stream_tail
                        ))
                    })
                    .map_err(map_sqlite_error)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(map_sqlite_error)?;
                Ok::<_, storage_types::StorageError>(rows)
            })
            .await?;

        if pending.is_empty() {
            return Ok(false);
        }

        for (table_name, index_name, mut lek, captured_stream_tail) in pending {
            // Look up table info and GSI schema
            let Ok(table_info) = self.get_table_info(&table_name).await else {
                continue;
            };
            let Some(ref gsis) = table_info.global_secondary_indexes else {
                continue;
            };
            let Some(gsi) = gsis
                .iter()
                .find(|g| g.index_name == index_name && !is_ttl_index(&g.index_name))
            else {
                continue;
            };
            let update_table_info = GsiUpdateTableInfo::from(table_info.clone());
            let Some(update_gsi) = update_table_info.gsi_by_name(&gsi.index_name).cloned() else {
                continue;
            };
            self.ensure_captured_gsi_stream_tail_available(captured_stream_tail.as_deref())
                .await?;
            let update_gsi_table_name = Arc::clone(&update_gsi.gsi_table_name);
            call_sqlite(&self.connection, move |conn| {
                ensure_gsi_metadata_columns(conn, &update_gsi_table_name)
            })
            .await?;

            loop {
                let (wire_items, next_lek) =
                    <SQLiteStorageProvider as storage_provider::StorageProvider>::scan_table(
                        self,
                        &storage_types::ScanTableRequest {
                            table_name: table_name.clone(),
                            index_name: None,
                            limit: Some(1000),
                            exclusive_start_key: lek.take(),
                            consistent_read: true,
                        },
                    )
                    .await?;
                if wire_items.is_empty() {
                    break;
                }

                let update_table_info = update_table_info.clone();
                let update_gsi = update_gsi.clone();
                let scan_table_name = table_name.clone();
                with_transaction(&self.connection, move |sqlite| {
                    for wire_item in &wire_items {
                        let item = wire_item.to_attribute_map()?;
                        let Some(gsi_key) = full_key(&item, &update_gsi.key_names) else {
                            continue;
                        };
                        let Some(main_key) = full_key(&item, &update_table_info.key_names) else {
                            continue;
                        };
                        let main_key_attributes = key_attributes_from_refs(&main_key);
                        let item_version = SQLiteStorageProvider::do_get_item_revision(
                            &scan_table_name,
                            &main_key_attributes,
                            &crate::utils::SqliteConn::Connection(sqlite),
                        )?;
                        let attributes_blob =
                            build_attributes_blob(&item, &update_gsi, &update_table_info)?;
                        apply_gsi_put(
                            sqlite,
                            &update_gsi,
                            &update_table_info,
                            &gsi_key,
                            &main_key,
                            attributes_blob,
                            item_version,
                        )?;
                    }
                    Ok::<(), StorageError>(())
                })
                .await?;

                if next_lek.is_some() {
                    let now = TimestampMillis::now();
                    call_sqlite(&self.connection, {
                        let t = table_name.clone();
                        let i = index_name.clone();
                        let l2 = next_lek.clone();
                        move |conn| {
                            let (sql, params) = crate::backends::sqlite::sql_statements::update_gsi_backfill_progress(
                                &t,
                                &i,
                                l2.as_deref(),
                                &now,
                            );
                            conn.execute(sql, params).map_err(map_sqlite_error)
                        }
                    })
                    .await?;
                }

                lek = next_lek;
                if lek.is_none() {
                    break;
                }
            }

            self.process_gsi_updates().await?;

            let now = TimestampMillis::now();
            call_sqlite(&self.connection, {
                let t = table_name.clone();
                let i = index_name.clone();
                move |conn| {
                    let (sql, params) =
                        crate::backends::sqlite::sql_statements::mark_gsi_backfill_done(
                            &t, &i, &now,
                        );
                    conn.execute(sql, params).map_err(map_sqlite_error)
                }
            })
            .await?;
        }

        Ok(true)
    }

    pub async fn cleanup_gsi_backfill_tombstones(
        &self,
        table_name: &TableName,
        index_name: &IndexName,
    ) -> StorageResult<usize> {
        let gsi_table_name =
            GsiPhysicalName::compose(&table_name.sanitized_name(), &index_name.sanitized_name())
                .to_string();
        call_sqlite(&self.connection, move |conn| {
            ensure_gsi_metadata_columns(conn, &gsi_table_name)?;
            conn.execute(
                &format!("DELETE FROM \"{gsi_table_name}\" WHERE __aux_tombstone = 1"),
                [],
            )
            .map_err(map_sqlite_error)
        })
        .await
    }

    async fn ensure_captured_gsi_stream_tail_available(
        &self,
        captured_stream_tail: Option<&str>,
    ) -> StorageResult<()> {
        let Some(captured_stream_tail) = captured_stream_tail else {
            return Ok(());
        };
        let captured_tail = captured_stream_tail
            .parse::<StreamItemId>()
            .map_err(|err| StorageError::internal(&format!("parse captured stream tail: {err}")))?;
        if captured_tail == StreamItemId::default() {
            return Ok(());
        }

        let page = self
            .read_forward(StreamName::system_table_stream(), None, 1)
            .await
            .map_err(|err| StorageError::internal(&format!("read captured stream tail: {err}")))?;
        let Some(oldest_item) = page.items.first() else {
            return Err(missing_gsi_backfill_history_error());
        };
        if oldest_item.id > captured_tail {
            return Err(missing_gsi_backfill_history_error());
        }
        Ok(())
    }

    async fn ensure_gsi_cursor(
        &self,
        stream_name: &StreamName,
        cursor_name: &CursorName,
    ) -> StorageResult<Option<StreamItemId>> {
        let mut cursor_position = self
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
            cursor_position = self
                .get_cursor(stream_name.clone(), cursor_name.clone())
                .await
                .ok()
                .flatten()
                .map(|cursor| cursor.position);
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

    async fn gsi_table_info(
        &self,
        table_name: TableName,
        table_infos: &mut HashMap<TableName, Arc<GsiUpdateTableInfo>>,
    ) -> Option<Arc<GsiUpdateTableInfo>> {
        if let Some(info) = table_infos.get(&table_name) {
            return Some(Arc::clone(info));
        }
        let Ok(info) = self.get_table_info(&table_name).await else {
            return None;
        };
        let cached = Arc::new(GsiUpdateTableInfo::from(info));
        table_infos.insert(table_name, Arc::clone(&cached));
        Some(cached)
    }

    async fn build_gsi_batch_records(
        &self,
        records: Vec<(StreamPointer, Vec<StreamItem>)>,
        table_infos: &mut HashMap<TableName, Arc<GsiUpdateTableInfo>>,
    ) -> Vec<(Arc<GsiUpdateTableInfo>, ItemStreamVersion, Vec<StreamItem>)> {
        let mut batch_records = Vec::new();
        for (stream_pointer, stream_items) in records {
            let item_stream_version = stream_pointer.item_stream_version;
            let Some(table_info) = self
                .gsi_table_info(stream_pointer.table_name, table_infos)
                .await
            else {
                continue;
            };
            if table_info.gsis.is_empty() {
                continue;
            }
            batch_records.push((table_info, item_stream_version, stream_items));
        }
        batch_records
    }

    async fn apply_gsi_batch_records(
        &self,
        batch_records: Vec<(Arc<GsiUpdateTableInfo>, ItemStreamVersion, Vec<StreamItem>)>,
    ) -> StorageResult<GsiApplyStats> {
        call_sqlite(&self.connection, move |txn| {
            let mut stats = GsiApplyStats::default();
            for (table_info, item_stream_version, stream_items) in batch_records {
                for gsi in &table_info.gsis {
                    ensure_gsi_metadata_columns(txn, &gsi.gsi_table_name)?;
                    let gsi_stats = apply_stream_items_to_gsi(
                        txn,
                        &stream_items,
                        item_stream_version,
                        &table_info,
                        gsi,
                    )?;
                    stats.add(gsi_stats);
                }
            }
            Ok::<GsiApplyStats, StorageError>(stats)
        })
        .await
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

    pub async fn process_gsi_updates(&self) -> StorageResult<bool> {
        let mut cursor_advanced = false;
        let mut run = GsiUpdateRun::new();

        let cursor_name: CursorName = "gsi-update-cursor".to_string().into();
        let stream_name: StreamName = StreamName::system_table_stream();

        let mut cursor_position = self.ensure_gsi_cursor(&stream_name, &cursor_name).await?;
        self.refresh_gsi_update_lag(&stream_name, cursor_position)
            .await?;

        let mut table_infos: HashMap<TableName, Arc<GsiUpdateTableInfo>> = HashMap::new();

        'outer: loop {
            let Some(batch) = self
                .fetch_pointer_batch(&stream_name, cursor_position)
                .await?
            else {
                self.refresh_gsi_update_lag(&stream_name, cursor_position)
                    .await?;
                break;
            };

            run.record_batch(&batch);
            let PointerBatch {
                records, last_item, ..
            } = batch;

            let batch_records = self
                .build_gsi_batch_records(records, &mut table_infos)
                .await;
            if batch_records.is_empty() {
                run.record_empty();
                cursor_position = self
                    .advance_cursor_if(&stream_name, &cursor_name, last_item, &mut cursor_advanced)
                    .await?;
                self.refresh_gsi_update_lag(&stream_name, cursor_position)
                    .await?;
                break;
            }

            let apply_stats = self.apply_gsi_batch_records(batch_records).await?;

            let batch_operations = apply_stats.total_ops();
            if batch_operations == 0 {
                run.record_empty();
                cursor_position = self
                    .advance_cursor_if(&stream_name, &cursor_name, last_item, &mut cursor_advanced)
                    .await?;
                self.refresh_gsi_update_lag(&stream_name, cursor_position)
                    .await?;
                break;
            }

            run.record_ops(batch_operations);
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
}

#[derive(Debug, Clone)]
pub(crate) struct GsiUpdateTableInfo {
    pub(crate) source: Arc<StoredTableInfo>,
    pub(crate) key_names: Vec<String>,
    pub(crate) gsis: Vec<GsiUpdateIndex>,
}

impl From<StoredTableInfo> for GsiUpdateTableInfo {
    fn from(info: StoredTableInfo) -> Self {
        let source = Arc::new(info);
        let table_name = source.table_name.clone();
        let key_schema = source.key_schema.clone();
        let global_secondary_indexes = source.global_secondary_indexes.clone();
        let table_sanitized = table_name.sanitized_name();
        let key_names: Vec<String> = key_schema
            .into_iter()
            .map(|key| key.attribute_name)
            .collect();
        let table_key_columns: Vec<String> = key_names
            .iter()
            .map(|name| format!("table_{name}"))
            .collect();
        let gsis = global_secondary_indexes
            .unwrap_or_default()
            .into_iter()
            .filter(|gsi| !is_ttl_index(&gsi.index_name))
            .map(|gsi| {
                let gsi_table_name =
                    GsiPhysicalName::compose(&table_sanitized, &gsi.index_name.sanitized_name())
                        .to_string();
                let key_names: Vec<String> = gsi
                    .key_schema
                    .into_iter()
                    .map(|key| key.attribute_name)
                    .collect();
                let projection_plan = ProjectionPlan::from(&gsi.projection);
                let insert_sql =
                    build_gsi_insert_sql(&gsi_table_name, &key_names, &table_key_columns);
                let delete_sql =
                    build_gsi_delete_sql(&gsi_table_name, &key_names, &table_key_columns);
                let metadata_sql =
                    build_gsi_metadata_sql(&gsi_table_name, &key_names, &table_key_columns);
                GsiUpdateIndex {
                    index_name: gsi.index_name,
                    key_names,
                    projection_plan,
                    gsi_table_name: Arc::from(gsi_table_name),
                    insert_sql,
                    delete_sql,
                    metadata_sql,
                }
            })
            .collect();
        Self {
            source,
            key_names,
            gsis,
        }
    }
}

impl GsiUpdateTableInfo {
    pub(crate) fn gsi_by_name(&self, index_name: &IndexName) -> Option<&GsiUpdateIndex> {
        self.gsis.iter().find(|gsi| &gsi.index_name == index_name)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct GsiUpdateIndex {
    pub(crate) index_name: IndexName,
    pub(crate) key_names: Vec<String>,
    pub(crate) projection_plan: ProjectionPlan,
    pub(crate) gsi_table_name: Arc<str>,
    pub(crate) insert_sql: String,
    pub(crate) delete_sql: String,
    pub(crate) metadata_sql: String,
}

#[derive(Debug, Clone)]
pub(crate) enum ProjectionPlan {
    All,
    KeysOnly,
    Include(Vec<String>),
}

impl From<&Projection> for ProjectionPlan {
    fn from(projection: &Projection) -> Self {
        match projection.projection_type.as_ref() {
            Some(ProjectionType::KeysOnly) => ProjectionPlan::KeysOnly,
            Some(ProjectionType::Include) => {
                ProjectionPlan::Include(projection.non_key_attributes.clone().unwrap_or_default())
            }
            Some(ProjectionType::All) | None => ProjectionPlan::All,
        }
    }
}

fn build_gsi_insert_sql(
    gsi_table_name: &str,
    gsi_key_names: &[String],
    table_key_columns: &[String],
) -> String {
    let mut columns = Vec::with_capacity(gsi_key_names.len() + table_key_columns.len() + 3);
    columns.extend(gsi_key_names.iter().cloned());
    columns.extend(table_key_columns.iter().cloned());
    columns.push("attributes_blob".to_string());
    columns.push("__aux_tombstone".to_string());
    columns.push("__aux_item_version".to_string());

    let placeholders: String = (1..=columns.len())
        .map(|i| format!("?{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let columns_str = columns.join(", ");
    format!(
        "INSERT INTO \"{gsi_table_name}\" ({columns_str}) VALUES ({placeholders}) ON CONFLICT DO \
         UPDATE SET attributes_blob = excluded.attributes_blob, __aux_tombstone = \
         excluded.__aux_tombstone, __aux_item_version = excluded.__aux_item_version WHERE \
         excluded.__aux_item_version >= __aux_item_version"
    )
}

fn ensure_gsi_metadata_columns(
    conn: &rusqlite::Connection,
    gsi_table_name: &str,
) -> StorageResult<()> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info(\"{gsi_table_name}\")"))
        .map_err(map_sqlite_error)?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(map_sqlite_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(map_sqlite_error)?;
    if !columns.iter().any(|column| column == "__aux_tombstone") {
        conn.execute(
            &format!(
                "ALTER TABLE \"{gsi_table_name}\" ADD COLUMN __aux_tombstone INTEGER NOT NULL \
                 DEFAULT 0"
            ),
            [],
        )
        .map_err(map_sqlite_error)?;
    }
    if !columns.iter().any(|column| column == "__aux_item_version") {
        conn.execute(
            &format!(
                "ALTER TABLE \"{gsi_table_name}\" ADD COLUMN __aux_item_version INTEGER NOT NULL \
                 DEFAULT 0"
            ),
            [],
        )
        .map_err(map_sqlite_error)?;
    }
    Ok(())
}

fn build_gsi_delete_sql(
    gsi_table_name: &str,
    gsi_key_names: &[String],
    table_key_columns: &[String],
) -> String {
    let mut where_conditions = Vec::with_capacity(gsi_key_names.len() + table_key_columns.len());
    for name in gsi_key_names {
        where_conditions.push(format!("{name} = ?"));
    }
    for name in table_key_columns {
        where_conditions.push(format!("{name} = ?"));
    }
    let where_clause = where_conditions.join(" AND ");
    format!("DELETE FROM \"{gsi_table_name}\" WHERE {where_clause}")
}

fn build_gsi_metadata_sql(
    gsi_table_name: &str,
    gsi_key_names: &[String],
    table_key_columns: &[String],
) -> String {
    let mut where_conditions = Vec::with_capacity(gsi_key_names.len() + table_key_columns.len());
    for name in gsi_key_names {
        where_conditions.push(format!("{name} = ?"));
    }
    for name in table_key_columns {
        where_conditions.push(format!("{name} = ?"));
    }
    let where_clause = where_conditions.join(" AND ");
    format!(
        "SELECT __aux_tombstone, __aux_item_version FROM \"{gsi_table_name}\" WHERE {where_clause}"
    )
}

#[derive(Clone, Copy, Default)]
struct GsiApplyStats {
    puts: usize,
    deletes: usize,
    tombstones: usize,
}

impl GsiApplyStats {
    fn add(&mut self, other: Self) {
        self.puts += other.puts;
        self.deletes += other.deletes;
        self.tombstones += other.tombstones;
    }

    fn total_ops(&self) -> usize {
        self.puts + self.deletes + self.tombstones
    }
}

fn parse_stream_item(data: &[u8]) -> Option<HashMap<String, AttributeValue>> {
    storage_types::storage_serde::from_bytes::<HashMap<String, AttributeValue>>(data).ok()
}

pub(crate) fn full_key<'a>(
    item: &'a HashMap<String, AttributeValue>,
    key_names: &'a [String],
) -> Option<Vec<(&'a str, &'a AttributeValue)>> {
    let key_attrs = extract_key_attributes(item, key_names);
    if key_attrs.len() == key_names.len() {
        Some(key_attrs)
    } else {
        None
    }
}

fn keys_equal(left: &[(&str, &AttributeValue)], right: &[(&str, &AttributeValue)]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right.iter())
        .all(|((lk, lv), (rk, rv))| lk == rk && *lv == *rv)
}

fn key_part_refs<'a>(parts: &'a [GsiKeyPart<'a>]) -> Vec<(&'a str, &'a AttributeValue)> {
    parts.iter().map(|part| (part.name, part.value)).collect()
}

fn key_attributes_from_refs(parts: &[(&str, &AttributeValue)]) -> KeyAttributes {
    let mut attributes = KeyAttributes::with_capacity(parts.len());
    for (name, value) in parts {
        attributes.insert(*name, (*value).clone());
    }
    attributes
}

fn item_version_i64(version: ItemStreamVersion) -> StorageResult<i64> {
    i64::try_from(version.get())
        .map_err(|_| StorageError::internal("item stream version exceeds sqlite integer range"))
}

fn missing_gsi_backfill_history_error() -> StorageError {
    let outcome = plan_gsi_catchup_apply(&GsiCatchupApplyCase {
        current_version: 0,
        observation_version: 0,
        current_projects: false,
        observation_projects: false,
        history_available: false,
        scan_complete: false,
        drain_complete: false,
    });
    match outcome {
        GsiCatchupOutcome::RejectedMissingHistory => {
            StorageError::internal("gsi backfill captured stream tail is unavailable")
        }
        GsiCatchupOutcome::RejectedStaleObservation
        | GsiCatchupOutcome::ActivationAllowed
        | GsiCatchupOutcome::AppliedProjection
        | GsiCatchupOutcome::AppliedTombstone => StorageError::internal(
            "gsi backfill missing history planning returned an unexpected outcome",
        ),
    }
}

fn apply_stream_items_to_gsi(
    txn: &rusqlite::Connection,
    stream_items: &[StreamItem],
    item_stream_version: ItemStreamVersion,
    table_info: &GsiUpdateTableInfo,
    gsi: &GsiUpdateIndex,
) -> Result<GsiApplyStats, StorageError> {
    let mut stats = GsiApplyStats::default();
    let Some(first) = stream_items.first() else {
        return Ok(stats);
    };
    let item_version = item_version_i64(item_stream_version)?;

    if first.data_type == StreamDataType::DeleteMarker {
        let Some(old_data) = stream_items.last().map(|item| item.data.as_slice()) else {
            return Ok(stats);
        };
        let Some(old_item) = parse_stream_item(old_data) else {
            return Ok(stats);
        };
        let Some(gsi_key) = full_key(&old_item, &gsi.key_names) else {
            return Ok(stats);
        };
        let Some(main_table_key) = full_key(&old_item, &table_info.key_names) else {
            return Ok(stats);
        };
        apply_gsi_tombstone(
            txn,
            gsi,
            table_info,
            &gsi_key,
            &main_table_key,
            item_version,
        )?;
        stats.tombstones += 1;
        return Ok(stats);
    }

    let Some(new_item) = parse_stream_item(&first.data) else {
        return Ok(stats);
    };
    let Some(main_table_key) = full_key(&new_item, &table_info.key_names) else {
        return Ok(stats);
    };
    let new_gsi_key = full_key(&new_item, &gsi.key_names);

    let old_item = stream_items
        .get(1)
        .filter(|item| item.data_type != StreamDataType::DeleteMarker)
        .and_then(|item| parse_stream_item(&item.data));

    if new_gsi_key.is_none() {
        if let Some(old_item) = old_item
            && let (Some(old_gsi_key), Some(old_main_key)) = (
                full_key(&old_item, &gsi.key_names),
                full_key(&old_item, &table_info.key_names),
            )
        {
            apply_gsi_tombstone(
                txn,
                gsi,
                table_info,
                &old_gsi_key,
                &old_main_key,
                item_version,
            )?;
            stats.tombstones += 1;
        }
        return Ok(stats);
    }

    let Some(new_gsi_key) = new_gsi_key else {
        return Ok(stats);
    };
    let attributes_blob = build_attributes_blob(&new_item, gsi, table_info)?;
    apply_gsi_put(
        txn,
        gsi,
        table_info,
        &new_gsi_key,
        &main_table_key,
        attributes_blob,
        item_version,
    )?;
    stats.puts += 1;

    if let Some(old_item) = old_item
        && let (Some(old_gsi_key), Some(old_main_key)) = (
            full_key(&old_item, &gsi.key_names),
            full_key(&old_item, &table_info.key_names),
        )
        && !keys_equal(&new_gsi_key, &old_gsi_key)
    {
        apply_gsi_tombstone(
            txn,
            gsi,
            table_info,
            &old_gsi_key,
            &old_main_key,
            item_version,
        )?;
        stats.tombstones += 1;
    }

    Ok(stats)
}

fn apply_gsi_put(
    txn: &rusqlite::Connection,
    gsi: &GsiUpdateIndex,
    table_info: &GsiUpdateTableInfo,
    gsi_key: &[(&str, &AttributeValue)],
    main_table_key: &[(&str, &AttributeValue)],
    attributes_blob: Cow<'static, str>,
    item_version: i64,
) -> Result<(), StorageError> {
    if gsi_key.len() != gsi.key_names.len() || main_table_key.len() != table_info.key_names.len() {
        return Ok(());
    }

    let mut key_values = Vec::with_capacity(gsi_key.len() + main_table_key.len());
    push_key_values(&mut key_values, gsi_key, "gsi key scalar conversion")?;
    push_key_values(
        &mut key_values,
        main_table_key,
        "gsi main key scalar conversion",
    )?;
    if matches!(
        plan_gsi_apply(txn, gsi, &key_values, item_version, true)?,
        GsiCatchupOutcome::RejectedStaleObservation
    ) {
        return Ok(());
    }

    let item_version = item_version.to_string();
    let mut all_values = Vec::with_capacity(key_values.len() + 3);
    all_values.extend(key_values);
    all_values.push(attributes_blob);
    all_values.push(Cow::Borrowed("0"));
    all_values.push(Cow::Owned(item_version));

    txn.execute(
        &gsi.insert_sql,
        rusqlite::params_from_iter(all_values.iter().map(std::convert::AsRef::as_ref)),
    )
    .map_err(map_sqlite_error)
    .context("execute GSI put operation")?;
    Ok(())
}

fn apply_gsi_tombstone(
    txn: &rusqlite::Connection,
    gsi: &GsiUpdateIndex,
    table_info: &GsiUpdateTableInfo,
    gsi_key: &[(&str, &AttributeValue)],
    main_table_key: &[(&str, &AttributeValue)],
    item_version: i64,
) -> Result<(), StorageError> {
    if gsi_key.len() != gsi.key_names.len() || main_table_key.len() != table_info.key_names.len() {
        return Ok(());
    }

    let mut key_values = Vec::with_capacity(gsi_key.len() + main_table_key.len());
    push_key_values(
        &mut key_values,
        gsi_key,
        "gsi tombstone key scalar conversion",
    )?;
    push_key_values(
        &mut key_values,
        main_table_key,
        "gsi tombstone main key scalar conversion",
    )?;
    if matches!(
        plan_gsi_apply(txn, gsi, &key_values, item_version, false)?,
        GsiCatchupOutcome::RejectedStaleObservation
    ) {
        return Ok(());
    }

    let item_version = item_version.to_string();
    let mut all_values = Vec::with_capacity(key_values.len() + 3);
    all_values.extend(key_values);
    all_values.push(Cow::Borrowed("{}"));
    all_values.push(Cow::Borrowed("1"));
    all_values.push(Cow::Owned(item_version));

    txn.execute(
        &gsi.insert_sql,
        rusqlite::params_from_iter(all_values.iter().map(std::convert::AsRef::as_ref)),
    )
    .map_err(map_sqlite_error)
    .context("execute GSI tombstone operation")?;
    Ok(())
}

fn apply_gsi_delete(
    txn: &rusqlite::Connection,
    gsi: &GsiUpdateIndex,
    table_info: &GsiUpdateTableInfo,
    gsi_key: &[(&str, &AttributeValue)],
    main_table_key: &[(&str, &AttributeValue)],
) -> Result<(), StorageError> {
    if gsi_key.len() == gsi.key_names.len() && main_table_key.len() == table_info.key_names.len() {
        let mut where_values = Vec::with_capacity(gsi_key.len() + main_table_key.len());
        push_key_values(&mut where_values, gsi_key, "gsi key scalar conversion")?;
        push_key_values(
            &mut where_values,
            main_table_key,
            "gsi main key scalar conversion",
        )?;
        txn.execute(
            &gsi.delete_sql,
            rusqlite::params_from_iter(where_values.iter().map(std::convert::AsRef::as_ref)),
        )
        .map_err(map_sqlite_error)
        .context("execute GSI delete operation")?;
        return Ok(());
    }

    let mut where_conditions = Vec::with_capacity(gsi_key.len() + main_table_key.len());
    let mut where_values = Vec::with_capacity(gsi_key.len() + main_table_key.len());

    for (key_name, key_value) in gsi_key {
        where_conditions.push(format!("{key_name} = ?"));
        where_values.push(
            key_value.inner_string().map_err(|err| {
                StorageError::internal(&format!("gsi key scalar conversion: {err}"))
            })?,
        );
    }

    for (key_name, key_value) in main_table_key {
        where_conditions.push(format!("table_{key_name} = ?"));
        where_values.push(key_value.inner_string().map_err(|err| {
            StorageError::internal(&format!("gsi main key scalar conversion: {err}"))
        })?);
    }

    let where_clause = where_conditions.join(" AND ");
    let sql = format!(
        "DELETE FROM \"{}\" WHERE {where_clause}",
        gsi.gsi_table_name
    );
    txn.execute(&sql, rusqlite::params_from_iter(where_values.iter()))
        .map_err(map_sqlite_error)
        .context("execute GSI delete operation")?;
    Ok(())
}

fn plan_gsi_apply(
    txn: &rusqlite::Connection,
    gsi: &GsiUpdateIndex,
    key_values: &[Cow<'static, str>],
    observation_version: i64,
    observation_projects: bool,
) -> Result<GsiCatchupOutcome, StorageError> {
    let (current_version, current_projects) = current_gsi_apply_state(txn, gsi, key_values)?;
    Ok(plan_gsi_catchup_apply(&GsiCatchupApplyCase {
        current_version,
        observation_version,
        current_projects,
        observation_projects,
        history_available: true,
        scan_complete: false,
        drain_complete: false,
    }))
}

fn current_gsi_apply_state(
    txn: &rusqlite::Connection,
    gsi: &GsiUpdateIndex,
    key_values: &[Cow<'static, str>],
) -> Result<(i64, bool), StorageError> {
    let result = txn.query_row(
        &gsi.metadata_sql,
        rusqlite::params_from_iter(key_values.iter().map(std::convert::AsRef::as_ref)),
        |row| {
            let tombstone = row.get::<_, i64>(0)?;
            let item_version = row.get::<_, i64>(1)?;
            Ok((item_version, tombstone == 0))
        },
    );

    match result {
        Ok(state) => Ok(state),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok((0, false)),
        Err(err) => Err(map_sqlite_error(err)).context("read GSI apply state"),
    }
}

pub(crate) fn push_key_values(
    values: &mut Vec<Cow<'static, str>>,
    key_values: &[(&str, &AttributeValue)],
    context: &str,
) -> Result<(), StorageError> {
    for (_, key_value) in key_values {
        let value = key_value
            .inner_string()
            .map_err(|err| StorageError::internal(&format!("{context}: {err}")))?;
        values.push(Cow::Owned(value));
    }
    Ok(())
}

pub(crate) fn build_attributes_blob(
    item: &HashMap<String, AttributeValue>,
    gsi: &GsiUpdateIndex,
    table_info: &GsiUpdateTableInfo,
) -> Result<Cow<'static, str>, StorageError> {
    if matches!(gsi.projection_plan, ProjectionPlan::KeysOnly) {
        return Ok(Cow::Borrowed("{}"));
    }

    let mut non_key_attributes: Vec<(&str, &AttributeValue)> = Vec::new();

    match &gsi.projection_plan {
        ProjectionPlan::KeysOnly => {}
        ProjectionPlan::Include(attrs) => {
            for attr in attrs {
                if let Some(value) = item.get(attr)
                    && !is_key_attribute(attr.as_str(), &gsi.key_names, &table_info.key_names)
                {
                    non_key_attributes.push((attr.as_str(), value));
                }
            }
        }
        ProjectionPlan::All => {
            for (key, value) in item {
                if is_key_attribute(key.as_str(), &gsi.key_names, &table_info.key_names) {
                    continue;
                }
                non_key_attributes.push((key.as_str(), value));
            }
        }
    }

    if non_key_attributes.is_empty() {
        return Ok(Cow::Borrowed("{}"));
    }

    let blob = serde_json::to_string(&AttributePairs(&non_key_attributes))
        .map_err(|err| StorageError::internal(&format!("serialize attributes failed: {err}")))?;
    Ok(Cow::Owned(blob))
}

struct AttributePairs<'a>(&'a [(&'a str, &'a AttributeValue)]);

impl serde::Serialize for AttributePairs<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (name, value) in self.0 {
            map.serialize_entry(name, value)?;
        }
        map.end()
    }
}

fn extract_key_attributes<'a>(
    item: &'a HashMap<String, AttributeValue>,
    key_names: &'a [String],
) -> Vec<(&'a str, &'a AttributeValue)> {
    let mut key_attributes = Vec::with_capacity(key_names.len());
    for key_name in key_names {
        if let Some(value) = item.get(key_name) {
            key_attributes.push((key_name.as_str(), value));
        }
    }
    key_attributes
}

pub(crate) fn is_key_attribute(key: &str, gsi_key: &[String], main_table_key: &[String]) -> bool {
    gsi_key.iter().any(|name| name == key) || main_table_key.iter().any(|name| name == key)
}

pub struct GsiBackfillJob {
    provider: std::sync::Arc<SQLiteStorageProvider>,
}

impl GsiBackfillJob {
    pub fn new(provider: std::sync::Arc<SQLiteStorageProvider>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl BackgroundJob for GsiBackfillJob {
    async fn execute(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let did = self.provider.process_gsi_backfills().await?;
        Ok(did)
    }
}

pub struct GsiUpdateJob {
    provider: std::sync::Arc<SQLiteStorageProvider>,
    run_budget: std::time::Duration,
}

impl GsiUpdateJob {
    #[cfg(test)]
    #[allow(dead_code)]
    pub fn new(provider: std::sync::Arc<SQLiteStorageProvider>) -> Self {
        Self::new_with_interval(
            provider,
            storage_common::GsiJobConfig::default().update_interval_ms,
        )
    }

    pub fn new_with_interval(
        provider: std::sync::Arc<SQLiteStorageProvider>,
        interval_ms: storage_common::JobIntervalMillis,
    ) -> Self {
        Self {
            provider,
            run_budget: std::time::Duration::from_millis(interval_ms.0.saturating_mul(95) / 100),
        }
    }
}

#[async_trait]
impl BackgroundJob for GsiUpdateJob {
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
