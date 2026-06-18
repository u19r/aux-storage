#[cfg(test)]
use std::time::Instant;
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use bg_jobs::JobManager;
use storage_common::GsiPropagationGovernor;
#[cfg(test)]
use storage_common::provider_perf;
use storage_condition::{Condition, evaluate_condition, parse_condition_expression};
use storage_provider::{StorageProvider as _, split_item_into_key_and_attributes_sync};
use storage_types::{
    AttributeDefinition, AttributeValue, DurablePointReadGuard, ItemKey, KeyAttributeType,
    KeyAttributes, KeySchemaElement, KeyType, ReplicationEventMetadata, SplitDynamoItem,
    StorageEnum, StorageError, StorageResult, StoredTableInfo, StreamItemId, StreamName,
    StreamRetentionDuration, TableName, TableStatus, TimestampMillis, WireItem,
    WireItemKeyAttributes, context::ErrorContext as _, normalize_attribute_map_numbers_for_write,
};
use stream_provider::{
    CursorName, CursorPosition, EmbeddedStreamItem, StoredStreamPointer, StreamDataType,
    StreamItem, StreamProvider,
};
use tracing::instrument;
use turso::{Builder, Connection as TursoConnection, Error as TursoError, Value as TursoValue};
use uuid::Uuid;

use super::stream_duration::TursoStreamPointerIndexEntry;
use crate::{
    GsiPhysicalName,
    backends::turso::sql_statements,
    change_index,
    constants::{BASE_BACKOFF_MS, MAX_PUT_ITEM_ATTEMPTS},
    provider_core::gsi_write::{
        GsiAttributesBlobStyle, GsiSqlPlanOptions, GsiUpsertStyle, PlaceholderNumbering,
        TableKeyColumnStyle, plan_gsi_sql_statements,
    },
    sqlite_cache_config::sqlite_page_cache_size_kb,
    write_plan::WriteMaintenancePlan,
};

const TURSO_CONNECTION_POOL_SIZE: usize = 64;
const STREAM_EMBEDDED_MAX_BYTES: usize = 1024;

type TxFuture<'a, T> = Pin<Box<dyn Future<Output = StorageResult<T>> + Send + 'a>>;

#[derive(Clone, Copy)]
pub(crate) struct TursoWriteStreamEntriesInput<'a> {
    pub old_item: Option<&'a HashMap<String, AttributeValue>>,
    pub is_deleted: bool,
    pub item_stream_version: storage_types::ItemStreamVersion,
    pub replication: Option<&'a ReplicationEventMetadata>,
}

pub(super) fn condition_item_ref(
    old_item: Option<&HashMap<String, AttributeValue>>,
) -> &HashMap<String, AttributeValue> {
    static EMPTY_ITEM: LazyLock<HashMap<String, AttributeValue>> = LazyLock::new(HashMap::new);
    old_item.unwrap_or(&EMPTY_ITEM)
}

#[cfg(test)]
static TURSO_QUERY_CALLS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static TURSO_EXECUTE_CALLS: AtomicUsize = AtomicUsize::new(0);

pub(crate) trait TursoSqlConnection {
    fn as_turso_connection(&self) -> &TursoConnection;
    fn retry_conflicts(&self) -> bool;
}

impl TursoSqlConnection for TursoConnection {
    fn as_turso_connection(&self) -> &TursoConnection {
        self
    }

    fn retry_conflicts(&self) -> bool {
        true
    }
}

impl TursoSqlConnection for tokio::sync::OwnedMutexGuard<TursoConnection> {
    fn as_turso_connection(&self) -> &TursoConnection {
        self
    }

    fn retry_conflicts(&self) -> bool {
        true
    }
}

impl TursoSqlConnection for tokio::sync::MutexGuard<'_, TursoConnection> {
    fn as_turso_connection(&self) -> &TursoConnection {
        self
    }

    fn retry_conflicts(&self) -> bool {
        true
    }
}

pub(crate) struct TursoTransactionConnection<'a> {
    connection: &'a TursoConnection,
}

impl<'a> TursoTransactionConnection<'a> {
    pub(crate) fn new(connection: &'a TursoConnection) -> Self {
        Self { connection }
    }
}

impl TursoSqlConnection for TursoTransactionConnection<'_> {
    fn as_turso_connection(&self) -> &TursoConnection {
        self.connection
    }

    fn retry_conflicts(&self) -> bool {
        false
    }
}

#[derive(Clone, Copy)]
enum TursoQueryShape {
    MappedRows,
    RowSet,
}

enum TursoQueryOutput {
    MappedRows(Vec<HashMap<String, TursoValue>>),
    RowSet(TursoRowSet),
}

pub(crate) struct TursoRowSet {
    columns: Vec<String>,
    rows: Vec<Vec<TursoValue>>,
}

impl TursoRowSet {
    pub(crate) fn from_parts(columns: Vec<String>, rows: Vec<Vec<TursoValue>>) -> Self {
        Self { columns, rows }
    }

    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.rows.len()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = TursoRowView<'_>> {
        self.rows.iter().map(|values| TursoRowView {
            columns: &self.columns,
            values,
        })
    }
}

#[derive(Clone, Copy)]
pub(crate) struct TursoRowView<'a> {
    columns: &'a [String],
    values: &'a [TursoValue],
}

impl TursoRowView<'_> {
    pub(crate) fn get(&self, column: &str) -> Option<&TursoValue> {
        self.columns
            .iter()
            .position(|name| name == column)
            .and_then(|index| self.values.get(index))
    }
}

#[derive(Clone)]
pub struct TursoStorageProvider {
    connection_pool: Arc<Vec<Arc<tokio::sync::Mutex<TursoConnection>>>>,
    next_connection: Arc<AtomicUsize>,
    job_manager: JobManager,
    table_info_cache: Arc<tokio::sync::RwLock<HashMap<TableName, Arc<StoredTableInfo>>>>,
    pub(crate) immediate_gsi_consistency: bool,
    pub(crate) gsi_propagation_governor: Arc<GsiPropagationGovernor>,
    pub(crate) ddl_lock: Arc<tokio::sync::Mutex<()>>,
}

impl TursoStorageProvider {
    #[instrument(level = "info", fields(feature = "storage", backend = "turso", database_path = tracing::field::Empty, use_memory = tracing::field::Empty))]
    pub async fn new(database_path: &str) -> StorageResult<Self> {
        let final_path = database_path
            .strip_prefix("sqlite:")
            .unwrap_or(database_path)
            .split('?')
            .next()
            .unwrap_or(database_path)
            .to_string();
        let use_memory_db = final_path == ":memory:";

        tracing::Span::current().record("database_path", &final_path);
        tracing::Span::current().record("use_memory", use_memory_db);

        let database = Builder::new_local(&final_path)
            .build()
            .await
            .map_err(|error| {
                tracing::error!(error = %error, "failed to build turso database");
                StorageError::internal(&format!(
                    "failed to open turso database at '{final_path}': {error}"
                ))
            })?;
        let connection_pool_size = std::env::var("AUXFN_TURSO_CONNECTION_POOL_SIZE")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .filter(|size| *size > 0)
            .unwrap_or(TURSO_CONNECTION_POOL_SIZE);
        let connection_pool = Self::build_connection_pool(&database, connection_pool_size)?;

        let provider = Self {
            connection_pool: Arc::new(connection_pool),
            next_connection: Arc::new(AtomicUsize::new(0)),
            job_manager: JobManager::new_for_test(),
            table_info_cache: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            immediate_gsi_consistency: false,
            gsi_propagation_governor: Arc::new(GsiPropagationGovernor::default()),
            ddl_lock: Arc::new(tokio::sync::Mutex::new(())),
        };

        provider.configure_connection_pragmas(use_memory_db).await?;

        Ok(provider)
    }

    #[must_use]
    pub fn with_job_manager(mut self, job_manager: JobManager) -> Self {
        self.job_manager = job_manager;
        self
    }

    #[must_use]
    pub fn with_immediate_gsi_consistency(mut self, immediate_gsi_consistency: bool) -> Self {
        self.immediate_gsi_consistency = immediate_gsi_consistency;
        self
    }

    fn build_connection_pool(
        database: &turso::Database,
        pool_size: usize,
    ) -> StorageResult<Vec<Arc<tokio::sync::Mutex<TursoConnection>>>> {
        let mut connections = Vec::with_capacity(pool_size.max(1));
        for _ in 0..pool_size.max(1) {
            let connection = database.connect().map_err(|error| {
                StorageError::internal(&format!("failed to open turso connection: {error}"))
            })?;
            connection
                .busy_timeout(Duration::from_millis(5_000))
                .map_err(map_turso_error)
                .context("set turso busy_timeout on pooled connection")?;
            connections.push(Arc::new(tokio::sync::Mutex::new(connection)));
        }
        Ok(connections)
    }

    async fn configure_connection_pragmas(&self, use_memory_db: bool) -> StorageResult<()> {
        let conn = self.primary_connection().await?;

        conn.pragma_update("journal_mode", "'mvcc'")
            .await
            .map_err(map_turso_error)
            .context("set turso journal_mode")?;

        let journal_mode = read_pragma_text(&conn, "journal_mode").await?;
        if !journal_mode.eq_ignore_ascii_case("mvcc") {
            return Err(StorageError::internal(&format!(
                "turso journal_mode did not apply mvcc (actual='{journal_mode}')"
            )));
        }
        drop(conn);

        let page_cache_size_kb = sqlite_page_cache_size_kb();
        for pooled_conn in self.connection_pool.iter() {
            let pooled_conn = pooled_conn.clone().lock_owned().await;
            pooled_conn
                .pragma_update("synchronous", "'FULL'")
                .await
                .map_err(map_turso_error)
                .context("set turso synchronous")?;
            pooled_conn
                .pragma_update("cache_size", page_cache_size_kb)
                .await
                .map_err(map_turso_error)
                .context("set turso cache_size")?;
            if use_memory_db {
                pooled_conn
                    .pragma_update("temp_store", "'MEMORY'")
                    .await
                    .map_err(map_turso_error)
                    .context("set turso temp_store")?;
            }
        }

        Ok(())
    }

    pub(crate) async fn primary_connection(
        &self,
    ) -> StorageResult<tokio::sync::OwnedMutexGuard<TursoConnection>> {
        let conn = self.connection_pool.first().cloned().ok_or_else(|| {
            StorageError::internal(
                "turso connection pool is empty; at least one connection is required",
            )
        })?;
        Ok(conn.lock_owned().await)
    }

    pub(crate) async fn connect(
        &self,
    ) -> StorageResult<tokio::sync::OwnedMutexGuard<TursoConnection>> {
        let pool_len = self.connection_pool.len();
        if pool_len == 0 {
            return Err(StorageError::internal(
                "turso connection pool is empty; at least one connection is required",
            ));
        }
        let index = self.next_connection.fetch_add(1, Ordering::Relaxed) % pool_len;
        let conn = self.connection_pool.get(index).cloned().ok_or_else(|| {
            StorageError::internal("failed to acquire turso connection from pool")
        })?;
        Ok(conn.lock_owned().await)
    }

    pub(crate) async fn with_transaction<T>(
        &self,
        retry_conflicts: bool,
        operation: impl for<'a> Fn(&'a TursoTransactionConnection<'a>) -> TxFuture<'a, T>,
    ) -> StorageResult<T> {
        self.with_transaction_mode(sql_statements::BEGIN_CONCURRENT, retry_conflicts, operation)
            .await
    }

    pub(crate) async fn with_exclusive_transaction<T>(
        &self,
        retry_conflicts: bool,
        operation: impl for<'a> Fn(&'a TursoTransactionConnection<'a>) -> TxFuture<'a, T>,
    ) -> StorageResult<T> {
        self.with_transaction_mode(sql_statements::BEGIN_EXCLUSIVE, retry_conflicts, operation)
            .await
    }

    async fn with_transaction_mode<T>(
        &self,
        begin_sql: &'static str,
        retry_conflicts: bool,
        operation: impl for<'a> Fn(&'a TursoTransactionConnection<'a>) -> TxFuture<'a, T>,
    ) -> StorageResult<T> {
        let max_attempts = if retry_conflicts {
            MAX_PUT_ITEM_ATTEMPTS
        } else {
            1
        };

        for attempt in 0..max_attempts {
            #[cfg(test)]
            provider_perf::record_amount("turso", "tx_attempt", 1);
            let conn = self.connect().await?;

            #[cfg(test)]
            let begin_started = Instant::now();
            let begin_result = conn.execute(begin_sql, ()).await.map_err(map_turso_error);
            #[cfg(test)]
            provider_perf::record("turso", "tx_begin", begin_started.elapsed());
            if let Err(begin_err) = begin_result {
                if is_conflict_storage_error(&begin_err) {
                    #[cfg(test)]
                    provider_perf::record_amount("turso", "tx_begin_conflict", 1);
                    if retry_conflicts && attempt + 1 < max_attempts {
                        sleep_backoff(attempt).await;
                        continue;
                    }
                    return Err(begin_err);
                }

                return Err(StorageError::internal(&format!(
                    "transaction begin failed: {begin_err}"
                )));
            }

            let tx_conn = TursoTransactionConnection::new(&conn);
            match operation(&tx_conn).await {
                Ok(value) => {
                    #[cfg(test)]
                    let commit_started = Instant::now();
                    let commit_result = conn
                        .execute(sql_statements::commit(), ())
                        .await
                        .map_err(map_turso_error);
                    #[cfg(test)]
                    provider_perf::record("turso", "tx_commit", commit_started.elapsed());
                    match commit_result {
                        Ok(_) => return Ok(value),
                        Err(commit_err) => {
                            let _ = conn.execute(sql_statements::rollback(), ()).await;
                            if is_conflict_storage_error(&commit_err) {
                                #[cfg(test)]
                                provider_perf::record_amount("turso", "tx_commit_conflict", 1);
                                if retry_conflicts && attempt + 1 < max_attempts {
                                    sleep_backoff(attempt).await;
                                    continue;
                                }
                                return Err(commit_err);
                            }

                            return Err(StorageError::internal(&format!(
                                "transaction commit failed: {commit_err}"
                            )));
                        }
                    }
                }
                Err(op_err) => {
                    let _ = conn.execute(sql_statements::rollback(), ()).await;
                    if retry_conflicts
                        && is_conflict_storage_error(&op_err)
                        && attempt + 1 < max_attempts
                    {
                        #[cfg(test)]
                        provider_perf::record_amount("turso", "tx_operation_conflict", 1);
                        sleep_backoff(attempt).await;
                        continue;
                    }
                    return Err(op_err);
                }
            }
        }

        Err(StorageEnum::TransactionConflict {
            message: "Turso concurrent transaction retry budget exhausted".to_string(),
        }
        .into())
    }

    pub(crate) async fn query_rows<C>(
        &self,
        conn: &C,
        sql: &str,
        params: Vec<TursoValue>,
    ) -> StorageResult<Vec<HashMap<String, TursoValue>>>
    where
        C: TursoSqlConnection + ?Sized,
    {
        match self
            .query_with_retry(conn, sql, params, TursoQueryShape::MappedRows)
            .await?
        {
            TursoQueryOutput::MappedRows(rows) => Ok(rows),
            TursoQueryOutput::RowSet(_) => Err(StorageError::internal(
                "turso query_rows received row-set output",
            )),
        }
    }

    pub(crate) async fn query_row_set<C>(
        &self,
        conn: &C,
        sql: &str,
        params: Vec<TursoValue>,
    ) -> StorageResult<TursoRowSet>
    where
        C: TursoSqlConnection + ?Sized,
    {
        match self
            .query_with_retry(conn, sql, params, TursoQueryShape::RowSet)
            .await?
        {
            TursoQueryOutput::RowSet(rows) => Ok(rows),
            TursoQueryOutput::MappedRows(_) => Err(StorageError::internal(
                "turso query_row_set received mapped-row output",
            )),
        }
    }

    async fn query_with_retry<C>(
        &self,
        conn: &C,
        sql: &str,
        params: Vec<TursoValue>,
        shape: TursoQueryShape,
    ) -> StorageResult<TursoQueryOutput>
    where
        C: TursoSqlConnection + ?Sized,
    {
        #[cfg(test)]
        TURSO_QUERY_CALLS.fetch_add(1, Ordering::Relaxed);

        let max_attempts = if conn.retry_conflicts() {
            MAX_PUT_ITEM_ATTEMPTS
        } else {
            1
        };
        let raw_conn = conn.as_turso_connection();

        for attempt in 0..max_attempts {
            #[cfg(test)]
            let query_started = Instant::now();
            let result = query_once(raw_conn, sql, params.clone(), shape).await;
            #[cfg(test)]
            provider_perf::record("turso", classify_query_sql(sql), query_started.elapsed());

            match result {
                Ok(rows) => return Ok(rows),
                Err(error) if is_conflict_storage_error(&error) && attempt + 1 < max_attempts => {
                    #[cfg(test)]
                    provider_perf::record_amount("turso", "sql_query_conflict", 1);
                    sleep_backoff(attempt).await;
                }
                Err(error) => return Err(error),
            }
        }

        Err(StorageEnum::TransactionConflict {
            message: "Turso query retry budget exhausted".to_string(),
        }
        .into())
    }

    pub(crate) async fn execute<C>(
        &self,
        conn: &C,
        sql: &str,
        params: Vec<TursoValue>,
    ) -> StorageResult<u64>
    where
        C: TursoSqlConnection + ?Sized,
    {
        #[cfg(test)]
        TURSO_EXECUTE_CALLS.fetch_add(1, Ordering::Relaxed);

        let max_attempts = if conn.retry_conflicts() {
            MAX_PUT_ITEM_ATTEMPTS
        } else {
            1
        };
        let raw_conn = conn.as_turso_connection();

        for attempt in 0..max_attempts {
            #[cfg(test)]
            let execute_started = Instant::now();
            let result = raw_conn
                .execute(sql, params.clone())
                .await
                .map_err(map_turso_error)
                .with_context(|| format!("execute sql {}", classify_execute_sql(sql)));
            #[cfg(test)]
            provider_perf::record(
                "turso",
                classify_execute_sql(sql),
                execute_started.elapsed(),
            );

            match result {
                Ok(affected) => return Ok(affected),
                Err(error) if is_conflict_storage_error(&error) && attempt + 1 < max_attempts => {
                    #[cfg(test)]
                    provider_perf::record_amount("turso", "sql_execute_conflict", 1);
                    sleep_backoff(attempt).await;
                }
                Err(error) => return Err(error),
            }
        }

        Err(StorageEnum::TransactionConflict {
            message: "Turso execute retry budget exhausted".to_string(),
        }
        .into())
    }

    pub(crate) async fn load_table_info_cached(
        &self,
        table_name: &TableName,
    ) -> StorageResult<StoredTableInfo> {
        if let Some(cached) = self.table_info_cache.read().await.get(table_name).cloned() {
            return Ok((*cached).clone());
        }

        let conn = self.connect().await?;
        let rows = self
            .query_rows(
                &conn,
                sql_statements::get_table_info(),
                vec![TursoValue::Text(table_name.to_string())],
            )
            .await?;

        let Some(row) = rows.into_iter().next() else {
            return Err(StorageError::table_not_found(table_name));
        };

        let info = row_to_table_info(&row)?;
        self.table_info_cache
            .write()
            .await
            .insert(table_name.clone(), Arc::new(info.clone()));
        Ok(info)
    }

    pub(crate) async fn load_table_info_uncached<C>(
        &self,
        conn: &C,
        table_name: &TableName,
    ) -> StorageResult<StoredTableInfo>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let rows = self
            .query_rows(
                conn,
                sql_statements::get_table_info(),
                vec![TursoValue::Text(table_name.to_string())],
            )
            .await?;

        let Some(row) = rows.into_iter().next() else {
            return Err(StorageError::table_not_found(table_name));
        };

        row_to_table_info(&row)
    }

    pub(crate) async fn table_exists_conn<C>(
        &self,
        conn: &C,
        table_name: &TableName,
    ) -> StorageResult<bool>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let rows = self
            .query_rows(
                conn,
                sql_statements::table_exists(),
                vec![TursoValue::Text(table_name.to_string())],
            )
            .await?;
        let count = rows
            .first()
            .and_then(|row| row.get("count"))
            .map(value_to_i64)
            .transpose()?
            .unwrap_or_default();
        Ok(count > 0)
    }

    pub(crate) async fn invalidate_table_cache(&self, table_name: &TableName) {
        self.table_info_cache.write().await.remove(table_name);
    }

    pub(crate) async fn get_item_map_by_key<C>(
        &self,
        conn: &C,
        table_info: &StoredTableInfo,
        key: &KeyAttributes,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>>
    where
        C: TursoSqlConnection + ?Sized,
    {
        if key.is_empty() {
            return Ok(None);
        }

        let table_name_safe = table_info.table_name.sanitized_name();
        let (where_clause, mut params) = build_key_where_clause(key, &table_info.key_schema)?;
        let sql = sql_statements::select_main_row(&table_name_safe, &where_clause);
        let rows = self
            .query_rows(conn, &sql, std::mem::take(&mut params))
            .await?;

        rows.into_iter()
            .next()
            .map(|row| row_to_item_map_main(&row, table_info))
            .transpose()
    }

    pub(crate) async fn put_item_txn<C>(
        &self,
        conn: &C,
        table_info: &StoredTableInfo,
        item: &HashMap<String, AttributeValue>,
        condition: Option<&Condition>,
        aux_item_stream_ttl_hours: Option<StreamRetentionDuration>,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let mut item = item.clone();
        normalize_attribute_map_numbers_for_write(&mut item);
        let SplitDynamoItem {
            key_attributes,
            all_attributes,
            ..
        } = split_item_into_key_and_attributes_sync(item, table_info)?;

        if is_key_absence_condition(condition, table_info) {
            self.insert_main_row(conn, table_info, &key_attributes, &all_attributes)
                .await?;
            let item_stream_version = storage_types::ItemStreamVersion::try_from(
                self.bump_item_revision(conn, &table_info.table_name, &key_attributes)
                    .await?,
            )?;
            self.write_stream_entries_for_item_change(
                conn,
                table_info,
                &all_attributes,
                TursoWriteStreamEntriesInput {
                    old_item: None,
                    is_deleted: false,
                    item_stream_version,
                    replication: None,
                },
            )
            .await?;
            self.apply_item_stream_duration(
                conn,
                table_info,
                &key_attributes,
                aux_item_stream_ttl_hours,
            )
            .await?;
            if self.immediate_gsi_consistency {
                self.apply_gsi_rows_for_item_change(conn, table_info, None, Some(&all_attributes))
                    .await?;
            }
            return Ok(None);
        }

        let old_item = self
            .get_item_map_by_key(conn, table_info, &key_attributes)
            .await?;

        if let Some(condition) = condition
            && !evaluate_condition(condition_item_ref(old_item.as_ref()), condition)
        {
            return Err(StorageEnum::ConditionalCheckFailed.into());
        }

        self.upsert_main_row(conn, table_info, &key_attributes, &all_attributes)
            .await?;
        let item_stream_version = storage_types::ItemStreamVersion::try_from(
            self.bump_item_revision(conn, &table_info.table_name, &key_attributes)
                .await?,
        )?;
        self.write_stream_entries_for_item_change(
            conn,
            table_info,
            &all_attributes,
            TursoWriteStreamEntriesInput {
                old_item: old_item.as_ref(),
                is_deleted: false,
                item_stream_version,
                replication: None,
            },
        )
        .await?;
        self.apply_item_stream_duration(
            conn,
            table_info,
            &key_attributes,
            aux_item_stream_ttl_hours,
        )
        .await?;
        if self.immediate_gsi_consistency {
            self.apply_gsi_rows_for_item_change(
                conn,
                table_info,
                old_item.as_ref(),
                Some(&all_attributes),
            )
            .await?;
        }

        Ok(old_item)
    }

    pub(crate) async fn overwrite_item_txn<C>(
        &self,
        conn: &C,
        table_info: &StoredTableInfo,
        item: &HashMap<String, AttributeValue>,
        old_item: Option<&HashMap<String, AttributeValue>>,
        aux_item_stream_ttl_hours: Option<StreamRetentionDuration>,
    ) -> StorageResult<()>
    where
        C: TursoSqlConnection + ?Sized,
    {
        self.overwrite_item_txn_with_replication(
            conn,
            table_info,
            item,
            old_item,
            None,
            aux_item_stream_ttl_hours,
        )
        .await
    }

    pub(crate) async fn overwrite_item_txn_with_replication<C>(
        &self,
        conn: &C,
        table_info: &StoredTableInfo,
        item: &HashMap<String, AttributeValue>,
        old_item: Option<&HashMap<String, AttributeValue>>,
        replication: Option<&ReplicationEventMetadata>,
        aux_item_stream_ttl_hours: Option<StreamRetentionDuration>,
    ) -> StorageResult<()>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let mut item = item.clone();
        normalize_attribute_map_numbers_for_write(&mut item);
        let SplitDynamoItem {
            key_attributes,
            all_attributes,
            ..
        } = split_item_into_key_and_attributes_sync(item, table_info)?;

        self.upsert_main_row(conn, table_info, &key_attributes, &all_attributes)
            .await?;
        let item_stream_version = storage_types::ItemStreamVersion::try_from(
            self.bump_item_revision(conn, &table_info.table_name, &key_attributes)
                .await?,
        )?;
        self.write_stream_entries_for_item_change(
            conn,
            table_info,
            &all_attributes,
            TursoWriteStreamEntriesInput {
                old_item,
                is_deleted: false,
                item_stream_version,
                replication,
            },
        )
        .await?;
        self.apply_item_stream_duration(
            conn,
            table_info,
            &key_attributes,
            aux_item_stream_ttl_hours,
        )
        .await?;
        if self.immediate_gsi_consistency {
            self.apply_gsi_rows_for_item_change(conn, table_info, old_item, Some(&all_attributes))
                .await?;
        }

        Ok(())
    }

    pub(crate) async fn delete_item_txn_with_replication<C>(
        &self,
        conn: &C,
        table_info: &StoredTableInfo,
        key: &KeyAttributes,
        condition: Option<&Condition>,
        replication: Option<&ReplicationEventMetadata>,
        aux_item_stream_ttl_hours: Option<StreamRetentionDuration>,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let old_item = self.get_item_map_by_key(conn, table_info, key).await?;
        if old_item.is_none() {
            return Ok(None);
        }

        if let Some(condition) = condition
            && !evaluate_condition(condition_item_ref(old_item.as_ref()), condition)
        {
            return Err(StorageEnum::ConditionalCheckFailed.into());
        }

        let table_name_safe = table_info.table_name.sanitized_name();
        let (where_clause, params) = build_key_where_clause(key, &table_info.key_schema)?;
        let delete_sql = sql_statements::delete_main_row(&table_name_safe, &where_clause);
        let _ = self.execute(conn, &delete_sql, params).await?;
        let item_stream_version = storage_types::ItemStreamVersion::try_from(
            self.bump_item_revision(conn, &table_info.table_name, key)
                .await?,
        )?;
        self.write_stream_entries_for_item_change(
            conn,
            table_info,
            &key.to_attribute_map(),
            TursoWriteStreamEntriesInput {
                old_item: old_item.as_ref(),
                is_deleted: true,
                item_stream_version,
                replication,
            },
        )
        .await?;
        self.apply_item_stream_duration(conn, table_info, key, aux_item_stream_ttl_hours)
            .await?;

        if self.immediate_gsi_consistency {
            self.apply_gsi_rows_for_item_change(conn, table_info, old_item.as_ref(), None)
                .await?;
        }

        Ok(old_item)
    }

    pub(crate) async fn write_stream_entries_for_item_change<C>(
        &self,
        conn: &C,
        table_info: &StoredTableInfo,
        item_data: &HashMap<String, AttributeValue>,
        input: TursoWriteStreamEntriesInput<'_>,
    ) -> StorageResult<()>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let TursoWriteStreamEntriesInput {
            old_item,
            is_deleted,
            item_stream_version,
            replication,
        } = input;
        if !crate::stream_writer::should_write_stream_entries_for_gsi_mode(
            table_info,
            self.immediate_gsi_consistency,
        ) {
            return Ok(());
        }

        let stream_tables_exist = self
            .query_rows(
                conn,
                sql_statements::stream_items_table_exists(),
                Vec::new(),
            )
            .await
            .is_ok_and(|rows| !rows.is_empty());
        if !stream_tables_exist {
            return Ok(());
        }

        let created_at = TimestampMillis::now();
        let item_key = ItemKey::from_key_schema(
            table_info.table_name.clone(),
            &table_info.key_schema,
            item_data,
        )
        .map_err(|err| StorageError::internal(&format!("stream item key error: {err}")))?;
        let item_stream = StreamName::table_item_stream(&table_info.table_name, &item_key)
            .map_err(|err| StorageError::internal(&format!("stream name error: {err}")))?;
        let item_stream_name = String::from(&item_stream);

        let data = storage_types::storage_serde::to_bytes(item_data)?;
        let old_bytes = old_item
            .filter(|old| !old.is_empty())
            .map(storage_types::storage_serde::to_bytes)
            .transpose()?;
        let embedded_bytes = old_bytes.as_ref().map_or(0, Vec::len) + data.len();
        let data_type = if is_deleted {
            StreamDataType::DeleteMarker
        } else {
            StreamDataType::DynamoDbJson
        };

        self.insert_stream_row(
            conn,
            &item_stream,
            storage_types::StreamItemId::from(item_stream_version),
            data.clone(),
            created_at,
            data_type,
        )
        .await?;

        let stored_pointer = if embedded_bytes <= STREAM_EMBEDDED_MAX_BYTES {
            let mut items = Vec::with_capacity(1 + usize::from(old_bytes.is_some()));
            items.push(EmbeddedStreamItem {
                data: data.clone(),
                data_type,
            });
            if let Some(old) = old_bytes {
                items.push(EmbeddedStreamItem {
                    data: old,
                    data_type: StreamDataType::DynamoDbJson,
                });
            }
            StoredStreamPointer::embedded(
                item_stream,
                table_info.table_name.clone(),
                item_stream_version,
                items,
            )
        } else {
            StoredStreamPointer::pointer(
                item_stream,
                table_info.table_name.clone(),
                item_stream_version,
            )
        };
        let stored_pointer = if let Some(replication) = replication.cloned() {
            stored_pointer.with_replication_metadata(replication)
        } else {
            stored_pointer
        };
        let pointer_data = storage_types::storage_serde::to_bytes(&stored_pointer)?;

        let table_pointer_stream_item_id = StreamItemId::from(Uuid::now_v7());
        self.insert_stream_row(
            conn,
            &StreamName::table_stream(&table_info.table_name),
            table_pointer_stream_item_id,
            pointer_data.clone(),
            created_at,
            StreamDataType::StreamPointer,
        )
        .await?;
        let system_pointer_stream_item_id = StreamItemId::from(Uuid::now_v7());
        self.insert_stream_row(
            conn,
            &StreamName::system_table_stream(),
            system_pointer_stream_item_id,
            pointer_data,
            created_at,
            StreamDataType::StreamPointer,
        )
        .await?;
        self.insert_stream_pointer_index(
            conn,
            TursoStreamPointerIndexEntry {
                table_name: &table_info.table_name,
                item_stream_name: &item_stream_name,
                item_stream_version,
                table_stream_item_id: table_pointer_stream_item_id,
                system_stream_item_id: system_pointer_stream_item_id,
                created_at,
            },
        )
        .await?;
        self.insert_change_index_marker(conn, table_info, table_pointer_stream_item_id, created_at)
            .await?;

        Ok(())
    }

    async fn insert_change_index_marker<C>(
        &self,
        conn: &C,
        table_info: &StoredTableInfo,
        pointer_stream_item_id: storage_types::StreamItemId,
        created_at: TimestampMillis,
    ) -> StorageResult<()>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let slot = change_index::slot_for_table(&table_info.table_name);
        let versionstamp = change_index::sortable_version(pointer_stream_item_id);
        let _ = self
            .execute(
                conn,
                sql_statements::insert_change_index_marker(),
                vec![
                    TursoValue::Integer(i64::from(slot)),
                    TursoValue::Text(versionstamp),
                    TursoValue::Text(table_info.table_name.as_ref().to_owned()),
                    TursoValue::Integer(created_at.timestamp_millis()),
                ],
            )
            .await?;
        Ok(())
    }

    async fn insert_stream_row<C>(
        &self,
        conn: &C,
        stream_name: &StreamName,
        item_id: storage_types::StreamItemId,
        data: Vec<u8>,
        created_at: TimestampMillis,
        data_type: StreamDataType,
    ) -> StorageResult<()>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let _ = self
            .execute(
                conn,
                sql_statements::insert_stream_entry(),
                vec![
                    TursoValue::Text(String::from(stream_name)),
                    TursoValue::Text(item_id.to_string()),
                    TursoValue::Blob(data),
                    TursoValue::Integer(created_at.timestamp_millis()),
                    TursoValue::Integer(data_type as i64),
                ],
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn upsert_main_row<C>(
        &self,
        conn: &C,
        table_info: &StoredTableInfo,
        key_attributes: &KeyAttributes,
        full_item: &HashMap<String, AttributeValue>,
    ) -> StorageResult<()>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let table_name_safe = table_info.table_name.sanitized_name();
        let mut columns = Vec::with_capacity(table_info.key_schema.len() + 1);
        let mut values = Vec::with_capacity(table_info.key_schema.len() + 1);

        for key in &table_info.key_schema {
            let value = key_attributes
                .get(&key.attribute_name)
                .ok_or_else(StorageError::invalid_or_missing_key)?;
            columns.push(key.attribute_name.clone());
            values.push(attribute_scalar_to_turso_value(value)?);
        }

        columns.push("attributes_blob".to_string());
        values.push(TursoValue::Text(serde_json::to_string(full_item)?));

        let placeholders = (1..=columns.len())
            .map(|idx| format!("?{idx}"))
            .collect::<Vec<_>>()
            .join(", ");
        let conflict_target = table_info
            .key_schema
            .iter()
            .map(|key| key.attribute_name.clone())
            .collect::<Vec<_>>()
            .join(", ");
        let assignments = columns
            .iter()
            .map(|column| format!("{column} = excluded.{column}"))
            .collect::<Vec<_>>()
            .join(", ");

        let sql = sql_statements::upsert_main_row(
            &table_name_safe,
            &columns,
            &placeholders,
            &conflict_target,
            &assignments,
        );

        let _ = self.execute(conn, &sql, values).await?;
        Ok(())
    }

    pub(crate) async fn insert_main_row<C>(
        &self,
        conn: &C,
        table_info: &StoredTableInfo,
        key_attributes: &KeyAttributes,
        full_item: &HashMap<String, AttributeValue>,
    ) -> StorageResult<()>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let table_name_safe = table_info.table_name.sanitized_name();
        let mut columns = Vec::with_capacity(table_info.key_schema.len() + 1);
        let mut values = Vec::with_capacity(table_info.key_schema.len() + 1);

        for key in &table_info.key_schema {
            let value = key_attributes
                .get(&key.attribute_name)
                .ok_or_else(StorageError::invalid_or_missing_key)?;
            columns.push(key.attribute_name.clone());
            values.push(attribute_scalar_to_turso_value(value)?);
        }

        columns.push("attributes_blob".to_string());
        values.push(TursoValue::Text(serde_json::to_string(full_item)?));

        let placeholders = (1..=columns.len())
            .map(|idx| format!("?{idx}"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = sql_statements::insert_main_row(&table_name_safe, &columns, &placeholders);

        match self.execute(conn, &sql, values).await {
            Ok(_) => Ok(()),
            Err(error) if is_constraint_storage_error(&error) => {
                Err(StorageEnum::ConditionalCheckFailed.into())
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn get_item_revision<C>(
        &self,
        conn: &C,
        table_name: &TableName,
        key: &KeyAttributes,
    ) -> StorageResult<i64>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let key_json = canonical_revision_key(key)?;
        let rows = self
            .query_rows(
                conn,
                sql_statements::get_item_revision(),
                vec![
                    TursoValue::Text(table_name.to_string()),
                    TursoValue::Text(key_json),
                ],
            )
            .await?;
        rows.first()
            .and_then(|row| row.get("revision"))
            .map(value_to_i64)
            .transpose()
            .map(|revision| revision.unwrap_or_default())
    }

    pub(crate) async fn bump_item_revision<C>(
        &self,
        conn: &C,
        table_name: &TableName,
        key: &KeyAttributes,
    ) -> StorageResult<i64>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let key_json = canonical_revision_key(key)?;
        let rows = self
            .query_rows(
                conn,
                sql_statements::bump_item_revision(),
                vec![
                    TursoValue::Text(table_name.to_string()),
                    TursoValue::Text(key_json),
                ],
            )
            .await
            .map_err(|error| {
                tracing::warn!(
                    backend = "turso",
                    table = %table_name,
                    error = %error,
                    "item stream version allocation failed"
                );
                error
            })?;
        rows.first()
            .and_then(|row| row.get("revision"))
            .map(value_to_i64)
            .transpose()?
            .ok_or_else(|| {
                tracing::warn!(
                    backend = "turso",
                    table = %table_name,
                    "item stream version allocation returned no revision"
                );
                StorageError::internal("bump item revision did not return revision")
            })
    }

    pub(crate) async fn validate_durable_guard<C>(
        &self,
        conn: &C,
        table_name: &TableName,
        key: &KeyAttributes,
        guard: &DurablePointReadGuard,
    ) -> StorageResult<()>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let expected_revision = match guard {
            DurablePointReadGuard::Present(revision) => {
                revision_from_guard_bytes(revision.as_bytes())?
            }
            DurablePointReadGuard::Absent(proof) => revision_from_guard_bytes(proof.as_bytes())?,
        };
        let key_json = canonical_revision_key(key)?;
        let _ = self
            .execute(
                conn,
                sql_statements::ensure_item_revision(),
                vec![
                    TursoValue::Text(table_name.to_string()),
                    TursoValue::Text(key_json.clone()),
                ],
            )
            .await?;
        let rows = self
            .query_rows(
                conn,
                sql_statements::get_item_revision(),
                vec![
                    TursoValue::Text(table_name.to_string()),
                    TursoValue::Text(key_json),
                ],
            )
            .await?;
        let current_revision = rows
            .first()
            .and_then(|row| row.get("revision"))
            .map(value_to_i64)
            .transpose()?
            .unwrap_or_default();
        if current_revision == expected_revision {
            return Ok(());
        }
        Err(StorageError::guard_conflict(&format!(
            "guard revision mismatch for {table_name}: expected {expected_revision}, got \
             {current_revision}"
        )))
    }

    pub(crate) async fn apply_gsi_rows_for_item_change<C>(
        &self,
        conn: &C,
        table_info: &StoredTableInfo,
        old_item: Option<&HashMap<String, AttributeValue>>,
        new_item: Option<&HashMap<String, AttributeValue>>,
    ) -> StorageResult<()>
    where
        C: TursoSqlConnection + ?Sized,
    {
        #[cfg(test)]
        let plan_started = Instant::now();
        let plan = plan_turso_gsi_sql_statements(table_info, old_item, new_item)?;

        #[cfg(test)]
        {
            provider_perf::record_amount(
                "turso",
                "table_write_gsi_mutations",
                plan.statements().len() as u64,
            );
            provider_perf::record_amount(
                "turso",
                "table_write_applied_mutations",
                plan.statements().len() as u64,
            );
            provider_perf::record_amount("turso", "table_write_gsi_key_overlap", 0);
        }
        #[cfg(test)]
        provider_perf::record("turso", "gsi_change_plan", plan_started.elapsed());

        #[cfg(test)]
        let execute_started = Instant::now();
        for statement in plan.statements() {
            self.execute(conn, &statement.sql, statement.params.clone())
                .await?;
        }
        #[cfg(test)]
        provider_perf::record("turso", "gsi_change_execute", execute_started.elapsed());
        Ok(())
    }

    pub(crate) async fn build_wire_item_from_main_row_view(
        &self,
        row: TursoRowView<'_>,
        table_info: &StoredTableInfo,
    ) -> StorageResult<WireItem> {
        row_view_to_main_wire_item(row, table_info)
    }

    pub(crate) async fn build_wire_item_from_gsi_row_view(
        &self,
        row: TursoRowView<'_>,
        table_info: &StoredTableInfo,
        gsi_key_schema: &[KeySchemaElement],
    ) -> StorageResult<WireItem> {
        row_view_to_gsi_wire_item(row, table_info, gsi_key_schema)
    }

    pub(crate) async fn parse_condition(
        &self,
        condition_expression: Option<String>,
        expression_attribute_names: &Option<HashMap<String, String>>,
        expression_attribute_values: &Option<HashMap<String, AttributeValue>>,
    ) -> StorageResult<Option<Condition>> {
        let Some(expr) = condition_expression else {
            return Ok(None);
        };

        let parsed = parse_condition_expression(
            &expr,
            expression_attribute_names.as_ref(),
            expression_attribute_values.as_ref(),
        )
        .map_err(|error| {
            tracing::warn!(error = %error, "condition parse failed");
            StorageEnum::ConditionalCheckFailed
        })?;

        Ok(Some(parsed))
    }

    pub(crate) async fn process_gsi_updates(&self) -> StorageResult<bool> {
        let cursor_name: CursorName = "gsi-update-cursor".to_string().into();
        let stream_name = StreamName::system_table_stream();
        let mut cursor_position = self
            .ensure_gsi_update_cursor(&stream_name, &cursor_name)
            .await?;
        self.refresh_gsi_update_lag(&stream_name, cursor_position)
            .await?;
        let mut did_work = false;
        let mut table_infos: HashMap<TableName, Option<StoredTableInfo>> = HashMap::new();

        loop {
            let records_result = self
                .get_items_from_pointer_stream(
                    stream_name.clone(),
                    cursor_position,
                    Some(crate::constants::GSI_UPDATE_STREAM_FETCH_LIMIT),
                )
                .await
                .map_err(|error| {
                    StorageError::internal(&format!("turso gsi stream read failed: {error}"))
                })?;

            let had_more = records_result.has_more;
            let last_item = records_result.last_evaluated_key.or_else(|| {
                records_result.last_scanned_key.or_else(|| {
                    records_result
                        .records
                        .last()
                        .map(|(pointer, _)| pointer.stream_item_id)
                })
            });
            let records = records_result.records;

            if records.is_empty() {
                self.refresh_gsi_update_lag(&stream_name, cursor_position)
                    .await?;
                return Ok(did_work);
            }

            let batch_infos = table_infos.clone();
            let this = self.clone();
            let batch_did_work = self
                .with_exclusive_transaction(true, move |conn| {
                    let this = this.clone();
                    let records = records.clone();
                    let mut table_infos = batch_infos.clone();
                    Box::pin(async move {
                        let mut batch_did_work = false;
                        for (pointer, stream_items) in records {
                            let filtered_info =
                                if let Some(cached) = table_infos.get(&pointer.table_name) {
                                    cached.clone()
                                } else {
                                    let loaded = this
                                        .get_table_info(&pointer.table_name)
                                        .await
                                        .ok()
                                        .and_then(|info| turso_user_gsi_table_info(&info));
                                    table_infos.insert(pointer.table_name.clone(), loaded.clone());
                                    loaded
                                };
                            let Some(table_info) = filtered_info.as_ref() else {
                                continue;
                            };

                            let (old_item, new_item) = turso_gsi_images(&stream_items);
                            if old_item.is_some() || new_item.is_some() {
                                this.apply_gsi_rows_for_item_change(
                                    conn,
                                    table_info,
                                    old_item.as_ref(),
                                    new_item.as_ref(),
                                )
                                .await?;
                                batch_did_work = true;
                            }
                        }
                        Ok((batch_did_work, table_infos))
                    })
                })
                .await?;
            did_work |= batch_did_work.0;
            table_infos = batch_did_work.1;

            let Some(last_item) = last_item else {
                self.refresh_gsi_update_lag(&stream_name, cursor_position)
                    .await?;
                return Ok(did_work);
            };
            self.advance_cursor(stream_name.clone(), cursor_name.clone(), last_item)
                .await
                .map_err(|error| {
                    StorageError::internal(&format!("turso advance gsi cursor failed: {error}"))
                })?;
            cursor_position = Some(last_item);
            self.refresh_gsi_update_lag(&stream_name, cursor_position)
                .await?;

            if !had_more {
                return Ok(did_work);
            }
        }
    }

    async fn ensure_gsi_update_cursor(
        &self,
        stream_name: &StreamName,
        cursor_name: &CursorName,
    ) -> StorageResult<Option<StreamItemId>> {
        let cursor_position = self
            .get_cursor(stream_name.clone(), cursor_name.clone())
            .await
            .map_err(|error| {
                StorageError::internal(&format!("turso get gsi cursor failed: {error}"))
            })?
            .map(|cursor| cursor.position);

        if cursor_position.is_none() {
            self.create_cursor(
                stream_name.clone(),
                cursor_name.clone(),
                CursorPosition::Head,
            )
            .await
            .map_err(|error| {
                StorageError::internal(&format!("turso create gsi cursor failed: {error}"))
            })?;
        }

        Ok(cursor_position)
    }

    async fn refresh_gsi_update_lag(
        &self,
        stream_name: &StreamName,
        cursor_position: Option<StreamItemId>,
    ) -> StorageResult<()> {
        let page = self
            .read_forward(stream_name.clone(), cursor_position, 1)
            .await
            .map_err(|error| {
                StorageError::internal(&format!("turso gsi lag read failed: {error}"))
            })?;
        storage_common::observe_gsi_lag(
            &self.gsi_propagation_governor,
            page.items.first().map(|item| item.created_at),
            current_ms_u64(),
        );
        Ok(())
    }
}

fn current_ms_u64() -> u64 {
    u64::try_from(*TimestampMillis::now()).unwrap_or(0)
}

fn turso_user_gsi_table_info(table_info: &StoredTableInfo) -> Option<StoredTableInfo> {
    let mut filtered = table_info.clone();
    filtered.global_secondary_indexes = table_info.global_secondary_indexes.as_ref().map(|gsis| {
        gsis.iter()
            .filter(|gsi| !storage_common::ttl::is_ttl_index(&gsi.index_name))
            .cloned()
            .collect::<Vec<_>>()
    });
    filtered
        .global_secondary_indexes
        .as_ref()
        .filter(|gsis| !gsis.is_empty())?;
    Some(filtered)
}

type TursoGsiImage = Option<HashMap<String, AttributeValue>>;

fn turso_gsi_images(stream_items: &[StreamItem]) -> (TursoGsiImage, TursoGsiImage) {
    let Some(first) = stream_items.first() else {
        return (None, None);
    };

    if first.data_type == StreamDataType::DeleteMarker {
        let old_item = stream_items
            .last()
            .and_then(|item| storage_types::storage_serde::from_bytes(&item.data).ok());
        return (old_item, None);
    }

    let new_item = storage_types::storage_serde::from_bytes(&first.data).ok();
    let old_item = stream_items
        .get(1)
        .filter(|item| item.data_type != StreamDataType::DeleteMarker)
        .and_then(|item| storage_types::storage_serde::from_bytes(&item.data).ok());
    (old_item, new_item)
}

async fn query_once(
    raw_conn: &TursoConnection,
    sql: &str,
    params: Vec<TursoValue>,
    shape: TursoQueryShape,
) -> StorageResult<TursoQueryOutput> {
    let mut rows = raw_conn
        .query(sql, params)
        .await
        .map_err(map_turso_error)
        .context("query rows")?;
    let columns = rows.column_names();

    match shape {
        TursoQueryShape::MappedRows => {
            let mut out = Vec::new();
            while let Some(row) = rows.next().await.map_err(map_turso_error)? {
                let mut mapped = HashMap::with_capacity(columns.len());
                for (index, name) in columns.iter().enumerate() {
                    let value = row.get_value(index).map_err(map_turso_error)?;
                    mapped.insert(name.clone(), value);
                }
                out.push(mapped);
            }
            Ok(TursoQueryOutput::MappedRows(out))
        }
        TursoQueryShape::RowSet => {
            let mut out = Vec::new();
            while let Some(row) = rows.next().await.map_err(map_turso_error)? {
                let mut values = Vec::with_capacity(columns.len());
                for index in 0..columns.len() {
                    values.push(row.get_value(index).map_err(map_turso_error)?);
                }
                out.push(values);
            }
            Ok(TursoQueryOutput::RowSet(TursoRowSet::from_parts(
                columns, out,
            )))
        }
    }
}

pub(crate) fn row_to_table_info(
    row: &HashMap<String, TursoValue>,
) -> StorageResult<StoredTableInfo> {
    let table_name = TableName::new(&row_required_text(row, "table_name")?);
    let table_status: TableStatus = row_required_text(row, "table_status")?.as_str().into();
    let created_at = row_required_i64(row, "created_at")?.into();

    let attribute_definitions = parse_json_or_default::<Vec<AttributeDefinition>>(
        row_required_text(row, "attribute_definitions")?.as_str(),
    )?;
    let key_schema = parse_json_or_default::<Vec<KeySchemaElement>>(
        row_required_text(row, "key_schema")?.as_str(),
    )?;

    let global_secondary_indexes = row_optional_text(row, "global_secondary_indexes")?
        .filter(|value| !value.trim().is_empty() && !value.trim().eq_ignore_ascii_case("null"))
        .map(|value| serde_json::from_str(&value))
        .transpose()
        .map_err(|error| StorageError::internal(&format!("parse gsi json failed: {error}")))?;

    let stream_specification = row_optional_text(row, "stream_specification")?
        .filter(|value| !value.trim().is_empty() && !value.trim().eq_ignore_ascii_case("null"))
        .map(|value| serde_json::from_str(&value))
        .transpose()
        .map_err(|error| {
            StorageError::internal(&format!("parse stream spec json failed: {error}"))
        })?;

    let table_size_bytes =
        u64::try_from(row_required_i64(row, "table_size_bytes")?).unwrap_or_default();
    let item_count = u64::try_from(row_required_i64(row, "item_count")?).unwrap_or_default();
    let deletion_protection_enabled = row_required_i64(row, "deletion_protection_enabled")? != 0;
    let table_stream_duration = storage_types::StreamRetentionDuration::try_from(row_required_i64(
        row,
        "table_stream_duration_hours",
    )?)
    .map_err(|error| {
        StorageError::validation(format!("invalid table stream duration metadata: {error}"))
    })?;
    let default_item_stream_duration = storage_types::StreamRetentionDuration::try_from(
        row_required_i64(row, "default_item_stream_duration_hours")?,
    )
    .map_err(|error| {
        StorageError::validation(format!(
            "invalid default item stream duration metadata: {error}"
        ))
    })?;

    Ok(StoredTableInfo {
        table_name,
        table_status,
        created_at,
        attribute_definitions,
        key_schema,
        global_secondary_indexes,
        table_size_bytes,
        item_count,
        stream_specification,
        table_stream_duration,
        default_item_stream_duration,
        deletion_protection_enabled,
    })
}

pub(crate) fn row_to_item_map_main(
    row: &HashMap<String, TursoValue>,
    table_info: &StoredTableInfo,
) -> StorageResult<HashMap<String, AttributeValue>> {
    let mut out: HashMap<String, AttributeValue> = row_optional_text(row, "attributes_blob")?
        .filter(|value| !value.trim().is_empty() && value != "{}")
        .map(|value| serde_json::from_str(&value))
        .transpose()
        .map_err(|error| StorageError::internal(&format!("parse attributes_blob failed: {error}")))?
        .unwrap_or_default();

    for key in &table_info.key_schema {
        if out.contains_key(&key.attribute_name) {
            continue;
        }
        let value = row
            .get(&key.attribute_name)
            .ok_or_else(StorageError::invalid_or_missing_key)?;
        let key_type = attribute_type_for_key(table_info, &key.attribute_name);
        out.insert(
            key.attribute_name.clone(),
            key_attr_from_row_value(value, &key_type)?,
        );
    }

    Ok(out)
}

pub(crate) fn row_view_to_item_map_main(
    row: TursoRowView<'_>,
    table_info: &StoredTableInfo,
) -> StorageResult<HashMap<String, AttributeValue>> {
    let mut out: HashMap<String, AttributeValue> = row_view_optional_text(row, "attributes_blob")?
        .filter(|value| !value.trim().is_empty() && value != "{}")
        .map(|value| serde_json::from_str(&value))
        .transpose()
        .map_err(|error| StorageError::internal(&format!("parse attributes_blob failed: {error}")))?
        .unwrap_or_default();

    for key in &table_info.key_schema {
        if out.contains_key(&key.attribute_name) {
            continue;
        }
        let value = row
            .get(&key.attribute_name)
            .ok_or_else(StorageError::invalid_or_missing_key)?;
        let key_type = attribute_type_for_key(table_info, &key.attribute_name);
        out.insert(
            key.attribute_name.clone(),
            key_attr_from_row_value(value, &key_type)?,
        );
    }

    Ok(out)
}

pub(crate) fn row_view_to_main_wire_item(
    row: TursoRowView<'_>,
    table_info: &StoredTableInfo,
) -> StorageResult<WireItem> {
    let item = row_view_to_item_map_main(row, table_info)?;
    WireItem::from_attribute_map(&item)
}

pub(crate) fn row_view_to_gsi_wire_item(
    row: TursoRowView<'_>,
    table_info: &StoredTableInfo,
    gsi_key_schema: &[KeySchemaElement],
) -> StorageResult<WireItem> {
    let primary_key =
        row_view_to_wire_key_attributes(row, table_info, gsi_key_schema, KeyColumn::Named)?;
    let secondary_key = row_view_to_wire_key_attributes(
        row,
        table_info,
        &table_info.key_schema,
        KeyColumn::TursoGsiTableKey,
    )?;
    Ok(WireItem::local_split(
        primary_key,
        Some(secondary_key),
        row_view_optional_text(row, "attributes_blob")?.map(String::into_bytes),
    ))
}

#[derive(Clone, Copy)]
enum KeyColumn {
    Named,
    TursoGsiTableKey,
}

fn row_view_to_wire_key_attributes(
    row: TursoRowView<'_>,
    table_info: &StoredTableInfo,
    key_schema: &[KeySchemaElement],
    column: KeyColumn,
) -> StorageResult<WireItemKeyAttributes> {
    let hash_key = key_schema
        .iter()
        .find(|key| key.key_type == KeyType::Hash)
        .ok_or_else(StorageError::invalid_or_missing_key)?;
    let hash_value = row_view_key_attr_from_column(row, table_info, hash_key, column)?;

    let range_key = key_schema.iter().find(|key| key.key_type == KeyType::Range);
    let range_value = range_key
        .map(|key| row_view_key_attr_from_column(row, table_info, key, column))
        .transpose()?;

    Ok(WireItemKeyAttributes::new(
        hash_key.attribute_name.clone(),
        hash_value,
        range_key.map(|key| key.attribute_name.clone()),
        range_value,
    ))
}

fn row_view_key_attr_from_column(
    row: TursoRowView<'_>,
    table_info: &StoredTableInfo,
    key: &KeySchemaElement,
    column: KeyColumn,
) -> StorageResult<AttributeValue> {
    let column_name = match (column, &key.key_type) {
        (KeyColumn::Named, _) => key.attribute_name.as_str(),
        (KeyColumn::TursoGsiTableKey, KeyType::Hash) => "table_pk",
        (KeyColumn::TursoGsiTableKey, KeyType::Range) => "table_sk",
    };
    let value = row
        .get(column_name)
        .ok_or_else(StorageError::invalid_or_missing_key)?;
    let key_type = attribute_type_for_key(table_info, &key.attribute_name);
    key_attr_from_row_value(value, &key_type)
}

pub(crate) fn build_key_where_clause(
    key: &KeyAttributes,
    key_schema: &[KeySchemaElement],
) -> StorageResult<(String, Vec<TursoValue>)> {
    let mut clauses = Vec::new();
    let mut params = Vec::new();

    for (index, key_attr) in key_schema.iter().enumerate() {
        let value = key
            .get(&key_attr.attribute_name)
            .ok_or_else(StorageError::invalid_or_missing_key)?;
        clauses.push(format!("{} = ?{}", key_attr.attribute_name, index + 1));
        params.push(attribute_scalar_to_turso_value(value)?);
    }

    Ok((clauses.join(" AND "), params))
}

pub(crate) fn gsi_table_name(
    table_name: &TableName,
    index_name: &storage_types::IndexName,
) -> String {
    GsiPhysicalName::compose(&table_name.sanitized_name(), &index_name.sanitized_name()).to_string()
}

pub(crate) fn parse_json_or_default<T>(raw: &str) -> StorageResult<T>
where T: serde::de::DeserializeOwned + Default {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("null") {
        return Ok(T::default());
    }

    serde_json::from_str(trimmed)
        .map_err(|error| StorageError::internal(&format!("json parse failed: {error}")))
}

fn attribute_type_for_key(table_info: &StoredTableInfo, key_name: &str) -> KeyAttributeType {
    table_info
        .attribute_definitions
        .iter()
        .find(|definition| definition.attribute_name == key_name)
        .map(|definition| definition.attribute_type.clone())
        .unwrap_or(KeyAttributeType::S)
}

fn key_attr_from_row_value(
    value: &TursoValue,
    key_type: &KeyAttributeType,
) -> StorageResult<AttributeValue> {
    let scalar = value_to_string(value)?;
    Ok(match key_type {
        KeyAttributeType::S => AttributeValue::S(scalar),
        KeyAttributeType::N => AttributeValue::N(scalar),
        KeyAttributeType::B => AttributeValue::B(scalar),
    })
}

pub(crate) fn attribute_scalar_to_turso_value(value: &AttributeValue) -> StorageResult<TursoValue> {
    match value {
        AttributeValue::S(raw) | AttributeValue::B(raw) => Ok(TursoValue::Text(raw.clone())),
        AttributeValue::N(raw) => {
            if let Ok(int_value) = raw.parse::<i64>() {
                return Ok(TursoValue::Integer(int_value));
            }
            if let Ok(float_value) = raw.parse::<f64>() {
                return Ok(TursoValue::Real(float_value));
            }
            Ok(TursoValue::Text(raw.clone()))
        }
        AttributeValue::BOOL(value) => Ok(TursoValue::Integer(if *value { 1 } else { 0 })),
        AttributeValue::NULL(_) => Ok(TursoValue::Null),
        _ => value
            .inner_str()
            .map(|raw| TursoValue::Text(raw.to_string()))
            .map_err(|error| {
                StorageError::validation(format!("attribute must be scalar: {error}"))
            }),
    }
}

pub(crate) fn row_required_text(
    row: &HashMap<String, TursoValue>,
    column: &str,
) -> StorageResult<String> {
    row.get(column)
        .ok_or_else(|| StorageError::internal(&format!("missing column '{column}'")))
        .and_then(value_to_string)
}

pub(crate) fn row_optional_text(
    row: &HashMap<String, TursoValue>,
    column: &str,
) -> StorageResult<Option<String>> {
    let Some(value) = row.get(column) else {
        return Ok(None);
    };
    match value {
        TursoValue::Null => Ok(None),
        _ => value_to_string(value).map(Some),
    }
}

fn row_view_optional_text(row: TursoRowView<'_>, column: &str) -> StorageResult<Option<String>> {
    let Some(value) = row.get(column) else {
        return Ok(None);
    };
    match value {
        TursoValue::Null => Ok(None),
        _ => value_to_string(value).map(Some),
    }
}

pub(crate) fn row_required_i64(
    row: &HashMap<String, TursoValue>,
    column: &str,
) -> StorageResult<i64> {
    let value = row
        .get(column)
        .ok_or_else(|| StorageError::internal(&format!("missing column '{column}'")))?;
    value_to_i64(value)
}

pub(crate) fn row_required_blob(
    row: &HashMap<String, TursoValue>,
    column: &str,
) -> StorageResult<Vec<u8>> {
    match row
        .get(column)
        .ok_or_else(|| StorageError::internal(&format!("missing column '{column}'")))?
    {
        TursoValue::Blob(raw) => Ok(raw.clone()),
        _ => Err(StorageError::internal(&format!(
            "column '{column}' is not a blob"
        ))),
    }
}

pub(crate) fn value_to_i64(value: &TursoValue) -> StorageResult<i64> {
    match value {
        TursoValue::Integer(raw) => Ok(*raw),
        TursoValue::Real(raw) => Ok(*raw as i64),
        TursoValue::Text(raw) => raw
            .parse::<i64>()
            .map_err(|error| StorageError::internal(&format!("parse i64 failed: {error}"))),
        TursoValue::Null => Ok(0),
        TursoValue::Blob(_) => Err(StorageError::internal("cannot convert blob to i64")),
    }
}

pub(crate) fn value_to_string(value: &TursoValue) -> StorageResult<String> {
    match value {
        TursoValue::Null => Ok(String::new()),
        TursoValue::Integer(raw) => Ok(raw.to_string()),
        TursoValue::Real(raw) => Ok(raw.to_string()),
        TursoValue::Text(raw) => Ok(raw.clone()),
        TursoValue::Blob(raw) => String::from_utf8(raw.clone())
            .map_err(|_| StorageError::internal("blob value is not utf8")),
    }
}

pub(crate) fn option_string_to_value(value: Option<String>) -> TursoValue {
    match value {
        Some(value) => TursoValue::Text(value),
        None => TursoValue::Null,
    }
}

pub(crate) fn canonical_revision_key(key: &KeyAttributes) -> StorageResult<String> {
    if key.is_empty() {
        return Err(StorageError::invalid_or_missing_key());
    }
    key.canonical_dynamo_json().map_err(|error| {
        StorageError::validation(format!(
            "revision key must be Dynamo JSON encodable: {error}"
        ))
    })
}

pub(crate) fn revision_from_guard_bytes(bytes: &[u8]) -> StorageResult<i64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| StorageError::validation("durable guard revision must be 8 bytes"))?;
    Ok(i64::from_be_bytes(bytes))
}

fn is_key_absence_condition(condition: Option<&Condition>, table_info: &StoredTableInfo) -> bool {
    let Some(Condition::NotExists { field }) = condition else {
        return false;
    };
    table_info
        .key_schema
        .iter()
        .any(|key| key.key_type == KeyType::Hash && key.attribute_name == *field)
}

fn is_constraint_storage_error(error: &StorageError) -> bool {
    matches!(error.as_ref(), StorageEnum::Validation { .. })
}

fn plan_turso_gsi_sql_statements(
    table_info: &StoredTableInfo,
    old_item: Option<&HashMap<String, AttributeValue>>,
    new_item: Option<&HashMap<String, AttributeValue>>,
) -> StorageResult<WriteMaintenancePlan<TursoValue>> {
    let options = GsiSqlPlanOptions::new(
        gsi_table_name,
        attribute_scalar_to_turso_value,
        || TursoValue::Null,
        |index, _| format!("?{index}"),
        |attribute_name, prefix| match prefix {
            Some(prefix) => format!("{prefix}{attribute_name}"),
            None => attribute_name.to_string(),
        },
        GsiUpsertStyle::OnConflictUpdateNonKey,
        TableKeyColumnStyle::FixedPkSk,
        PlaceholderNumbering::PerStatement,
        GsiAttributesBlobStyle::FullProjectedItem,
    );
    plan_gsi_sql_statements(table_info, old_item, new_item, &options)
}

#[cfg(test)]
fn classify_query_sql(sql: &str) -> &'static str {
    if sql.contains("FROM tables") {
        "sql_query_table_info"
    } else if sql.contains("FROM \"table_") {
        "sql_query_main_row"
    } else if sql.contains("item_revisions") {
        "sql_query_revision"
    } else {
        "sql_query_other"
    }
}

fn classify_execute_sql(sql: &str) -> &'static str {
    if sql.starts_with("INSERT INTO \"table_") {
        "sql_execute_main_upsert"
    } else if sql.starts_with("INSERT INTO \"gsi_") {
        "sql_execute_gsi_upsert"
    } else if sql.starts_with("DELETE FROM \"gsi_") {
        "sql_execute_gsi_delete"
    } else if sql.contains("item_revisions") {
        "sql_execute_revision"
    } else if sql.contains("ttl") {
        "sql_execute_ttl"
    } else if sql.contains("stream") {
        "sql_execute_stream"
    } else {
        "sql_execute_other"
    }
}

pub(crate) async fn read_pragma_text(
    conn: &TursoConnection,
    pragma_name: &str,
) -> StorageResult<String> {
    let sql = sql_statements::read_pragma(pragma_name);
    let mut rows = conn
        .query(sql, ())
        .await
        .map_err(map_turso_error)
        .context("read pragma")?;

    let Some(row) = rows.next().await.map_err(map_turso_error)? else {
        return Err(StorageError::internal("pragma query returned no value"));
    };

    row.get::<String>(0).map_err(map_turso_error)
}

pub(crate) fn map_turso_error(error: TursoError) -> StorageError {
    match error {
        TursoError::Busy(message)
        | TursoError::BusySnapshot(message)
        | TursoError::Interrupt(message) => {
            tracing::debug!(message, "turso transaction conflict");
            StorageEnum::TransactionConflict { message }.into()
        }
        TursoError::Error(message) => {
            if is_turso_conflict_message(&message) {
                tracing::debug!(message, "turso transaction conflict");
                return StorageEnum::TransactionConflict { message }.into();
            }
            tracing::error!(message, "turso backend sql error");
            StorageError::internal(&format!("turso error: {message}"))
        }
        TursoError::Constraint(message) => {
            if is_turso_conflict_message(&message) {
                tracing::debug!(message, "turso transaction conflict");
                return StorageEnum::TransactionConflict { message }.into();
            }
            tracing::debug!(message, "turso constraint error");
            StorageError::validation(message)
        }
        other => {
            tracing::error!(error = ?other, "turso backend sql error");
            StorageError::internal(&format!("turso error: {other}"))
        }
    }
}

fn is_turso_conflict_message(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("conflict")
        || lower.contains("database is locked")
        || lower.contains("locked")
        || lower.contains("busy")
        || lower.contains("schema changed")
        || lower.contains("no transaction is active")
        || lower.contains("ongoing transaction")
}

pub(crate) fn is_conflict_storage_error(error: &StorageError) -> bool {
    matches!(
        error.as_ref(),
        StorageEnum::TransactionConflict { .. } | StorageEnum::TransactionInProgress { .. }
    )
}

pub(crate) async fn sleep_backoff(attempt: u32) {
    let exp = BASE_BACKOFF_MS.saturating_mul(1_u64 << attempt.min(8));
    let jitter = rand::random::<u64>() % (exp + 1);
    tokio::time::sleep(std::time::Duration::from_millis(exp + jitter / 2)).await;
}

#[cfg(test)]
pub(crate) fn reset_turso_statement_counters() {
    TURSO_QUERY_CALLS.store(0, Ordering::Relaxed);
    TURSO_EXECUTE_CALLS.store(0, Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) fn turso_statement_counters() -> (usize, usize) {
    (
        TURSO_QUERY_CALLS.load(Ordering::Relaxed),
        TURSO_EXECUTE_CALLS.load(Ordering::Relaxed),
    )
}
