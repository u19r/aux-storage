use bg_jobs::{BackgroundJob, BackgroundJobName, JobConfig};
use storage_common::{
    GSI_BACKFILL_JOB, JobIntervalMillis, RegistersJobs, STREAM_TRIM_JOB, TTL_SWEEP_JOB,
    register_gsi_jobs,
};
use storage_provider::{StorageProvider, StreamTrimStateWrite, plan_table_stream_duration};
use storage_types::{
    CreateTableRequest, StorageError, StorageResult, StoredTableInfo, StreamName, TableName,
    TableStatus, context::ErrorContext,
};

use super::SQLiteStorageProvider;
use crate::{
    GsiPhysicalName,
    backends::sqlite::stream_duration::write_stream_trim_state_tx,
    error_handler::map_sqlite_error,
    naming,
    process_gsi_updates::GsiUpdateJob,
    provider_core::table_lifecycle::{prepare_table_metadata, validate_create_table_request},
    sql_statements,
    utils::{call_sqlite, call_sqlite_raw, sql_row_to_stored_stable_info},
};
impl SQLiteStorageProvider {
    pub(crate) async fn do_initialize_storage(&self) -> StorageResult<()> {
        call_sqlite_raw(&self.connection, |conn| {
            let (sql, params) = sql_statements::create_tables_table();
            conn.execute(sql, params)
        })
        .await
        .context("initialize sqlite")?;

        call_sqlite_raw(&self.connection, |conn| {
            let has_column: bool = conn.query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM pragma_table_info('tables')
                    WHERE name = 'deletion_protection_enabled'
                )",
                [],
                |row| row.get(0),
            )?;
            if !has_column {
                let (sql, params) = sql_statements::add_deletion_protection_column();
                conn.execute(sql, params)?;
            }
            Ok(())
        })
        .await
        .context("initialize sqlite deletion protection column")?;

        migrate_table_integer_column(
            self,
            "table_stream_duration_hours",
            sql_statements::add_table_stream_duration_column,
            "initialize sqlite table stream duration column",
        )
        .await?;
        migrate_table_integer_column(
            self,
            "default_item_stream_duration_hours",
            sql_statements::add_default_item_stream_duration_column,
            "initialize sqlite default item stream duration column",
        )
        .await?;

        call_sqlite_raw(&self.connection, |conn| {
            let (sql, params) = sql_statements::create_gsi_backfill_table();
            conn.execute(sql, params)
        })
        .await
        .context("initialize sqlite gsi_backfill")?;

        call_sqlite_raw(&self.connection, |conn| {
            let (sql, params) = sql_statements::create_ttl_config_table();
            conn.execute(sql, params)
        })
        .await
        .context("initialize sqlite ttl configs")?;

        call_sqlite_raw(&self.connection, |conn| {
            let (sql, params) = sql_statements::create_item_revisions_table();
            conn.execute(sql, params)
        })
        .await
        .context("initialize sqlite item revisions")?;

        self.initialize_stream_duration_tables()
            .await
            .context("initialize sqlite custom stream duration tables")?;

        let disable_background_timers = std::env::var("AUX_DISABLE_BG_TIMERS")
            .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        if disable_background_timers || !self.database_jobs_enabled {
            return Ok(());
        }

        // Adapter implementing RegistersJobs for sqlite job manager
        #[expect(
            clippy::items_after_statements,
            reason = "Scoped helper defined near use"
        )]
        struct SqliteRegistrar<'a> {
            mgr: &'a bg_jobs::JobManager,
        }
        #[async_trait::async_trait]
        #[expect(
            clippy::items_after_statements,
            reason = "Scoped impl defined near struct for readability"
        )]
        impl RegistersJobs for SqliteRegistrar<'_> {
            type Error = StorageError;
            async fn register_timed_job<J>(
                &self,
                name: BackgroundJobName,
                interval_ms: JobIntervalMillis,
                job: J,
            ) -> Result<(), Self::Error>
            where
                J: BackgroundJob + 'static,
            {
                let config = JobConfig {
                    start_immediately: true,
                    sleep_duration: std::time::Duration::from_millis(interval_ms.0),
                    jitter_percent: 10,
                };
                self.mgr
                    .register_job(name, job, config)
                    .await
                    .map_err(|e| {
                        StorageError::internal(&format!("register job {name} failed: {e}"))
                    })?;
                Ok(())
            }
        }
        let registrar = SqliteRegistrar {
            mgr: &self.job_manager,
        };
        let job_intervals = self.database_job_intervals;
        let gsi_cfg = job_intervals.gsi_config();
        let update_job = GsiUpdateJob::new_with_interval(
            std::sync::Arc::new(self.clone()),
            gsi_cfg.update_interval_ms,
        );
        let backfill_job =
            crate::process_gsi_updates::GsiBackfillJob::new(std::sync::Arc::new(self.clone()));
        if self.immediate_gsi_consistency {
            registrar
                .register_timed_job(GSI_BACKFILL_JOB, gsi_cfg.backfill_interval_ms, backfill_job)
                .await
                .map_err(|e| {
                    StorageError::internal(&format!("register gsi backfill job failed: {e}"))
                })?;
        } else {
            register_gsi_jobs(&registrar, gsi_cfg, update_job, backfill_job)
                .await
                .map_err(|e| StorageError::internal(&format!("register gsi jobs failed: {e}")))?;
        }

        let ttl_job = crate::ttl_sweep::TtlSweepJob::new(std::sync::Arc::new(self.clone()));
        let ttl_interval_ms = job_intervals.ttl_sweep_interval_ms;
        registrar
            .register_timed_job(TTL_SWEEP_JOB, ttl_interval_ms, ttl_job)
            .await
            .map_err(|e| StorageError::internal(&format!("register ttl sweep job failed: {e}")))?;

        let trim_job = crate::stream_trim::StreamTrimJob::new(std::sync::Arc::new(self.clone()));
        let trim_interval_ms = job_intervals.stream_trim_interval_ms;
        registrar
            .register_timed_job(STREAM_TRIM_JOB, trim_interval_ms, trim_job)
            .await
            .map_err(|e| {
                StorageError::internal(&format!("register stream trim job failed: {e}"))
            })?;

        Ok(())
    }

    pub(crate) async fn do_table_exists(&self, table_name: &TableName) -> StorageResult<bool> {
        let table_name = table_name.to_string();
        let count: i64 = call_sqlite_raw(&self.connection, move |conn| {
            let (sql, params) = sql_statements::check_table_exists(&table_name);
            conn.query_row(sql, params, |row| row.get(0))
        })
        .await?;

        Ok(count > 0)
    }

    pub(crate) async fn do_create_table(&self, request: &CreateTableRequest) -> StorageResult<()> {
        let table_name = request.table_name.clone();
        let table_name_clone = table_name.clone();

        validate_create_table_request(request)?;

        tracing::Span::current().record("table_name", table_name.as_ref());

        if self.table_exists(&table_name).await? {
            return Err(StorageError::table_already_exists(&table_name));
        }

        let metadata = prepare_table_metadata(request)?;

        let table_id = uuid::Uuid::now_v7().as_u128();
        let table_scope_id = sqlite_table_scope_id(table_id);
        let table_duration_plan = plan_table_stream_duration(
            table_name.clone(),
            table_scope_id,
            1,
            metadata.table_stream_duration,
            metadata.default_item_stream_duration,
            metadata.created_at,
        );

        let rows_affected: usize = call_sqlite_raw(&self.connection, move |conn| {
            let tx = conn.transaction()?;
            let (sql, params) = sql_statements::insert_table(
                table_id,
                &table_name,
                &metadata.created_at,
                &metadata.attribute_definitions_json,
                &metadata.key_schema_json,
                metadata.max_indexers,
                metadata.global_secondary_indexes_json.as_deref(),
                metadata.stream_specification_json.as_deref(),
                metadata.deletion_protection_enabled,
                metadata.table_stream_duration,
                metadata.default_item_stream_duration,
            );
            let rows_affected = tx.execute(sql, params)?;
            write_stream_trim_state_tx(
                &tx,
                StreamTrimStateWrite {
                    state: table_duration_plan.trim_state,
                    next_marker: table_duration_plan.due_marker,
                },
            )
            .map_err(|err| {
                crate::backends::sqlite::storage_provider::storage_error_to_rusqlite(&err)
            })?;
            tx.commit()?;
            Ok(rows_affected)
        })
        .await
        .context("insert table sql")?;

        tracing::Span::current().record("rows_affected", rows_affected);

        if rows_affected == 0 {
            return Err(StorageError::internal(
                "No rows were inserted into tables table",
            ));
        }

        self.create_table_storage(&table_name_clone, request)
            .await
            .context("create table storage")?;

        self.update_table_status(&table_name_clone, TableStatus::Active)
            .await
            .context("update table status")?;

        Ok(())
    }

    pub(crate) async fn do_update_table_status(
        &self,
        table_name: &TableName,
        status: TableStatus,
    ) -> StorageResult<()> {
        self.update_table_status_internal(table_name, status)
            .await?;
        self.invalidate_table_info_cache(table_name).await;
        Ok(())
    }

    pub(crate) async fn do_list_tables(
        &self,
        limit: u32,
        exclusive_start_table_name: Option<TableName>,
    ) -> StorageResult<Vec<StoredTableInfo>> {
        call_sqlite(&self.connection, move |conn| {
            let mut tables = Vec::new();
            if let Some(start_name) = exclusive_start_table_name.map(|table| table.to_string()) {
                let (sql, params) = sql_statements::list_tables_after(limit, start_name);
                let mut stmt = conn.prepare(sql).map_err(map_sqlite_error)?;

                let table_iter = stmt
                    .query_map(params, sql_row_to_stored_stable_info)
                    .map_err(map_sqlite_error)
                    .context("query list tables")?;

                for table in table_iter {
                    tables.push(table.map_err(map_sqlite_error)?);
                }
            } else {
                let (sql, params) = sql_statements::list_all_tables(limit);
                let mut stmt = conn.prepare(sql).map_err(map_sqlite_error)?;

                let table_iter = stmt
                    .query_map(params, sql_row_to_stored_stable_info)
                    .map_err(map_sqlite_error)
                    .context("query list tables")?;

                for table in table_iter {
                    tables.push(table.map_err(map_sqlite_error)?);
                }
            }

            Ok(tables)
        })
        .await
    }

    pub(crate) async fn do_delete_table(&self, table_name: &TableName) -> StorageResult<()> {
        let table_name = table_name.clone();
        let table_name_for_sql = table_name.clone();

        let table_info = self.get_table_info(&table_name).await?;
        if table_info.deletion_protection_enabled {
            return Err(StorageError::deletion_protection_enabled(&table_name));
        }
        call_sqlite_raw(&self.connection, move |conn| {
            let (delete_sql, delete_params) = sql_statements::delete_table(&table_name_for_sql);
            conn.execute(delete_sql, delete_params)?;

            // Also drop the actual table if it exists
            let table_name_safe = table_name_for_sql.sanitized_name();
            let (drop_sql, drop_params) = sql_statements::drop_table(&table_name_safe);
            conn.execute(&drop_sql, drop_params)?;

            // Drop GSI tables if they exist
            if let Some(gsis) = table_info.global_secondary_indexes {
                for gsi in gsis {
                    let gsi_table_name = GsiPhysicalName::compose(
                        &table_name_safe,
                        &gsi.index_name.sanitized_name(),
                    )
                    .to_string();
                    let gsi_drop_sql = format!("DROP TABLE IF EXISTS \"{gsi_table_name}\"");
                    conn.execute(&gsi_drop_sql, [])?;
                }
            }

            let ttl_table = naming::physical_ttl_index_table_name(&table_name_for_sql);
            let ttl_drop_sql = format!("DROP TABLE IF EXISTS \"{ttl_table}\"");
            conn.execute(&ttl_drop_sql, [])?;

            let stream_tables_exist: bool = conn.query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM sqlite_master
                    WHERE type = 'table' AND name = 'sys_stream_items'
                )",
                [],
                |row| row.get(0),
            )?;
            if stream_tables_exist {
                let table_stream_name =
                    String::from(&StreamName::table_stream(&table_name_for_sql));
                let item_stream_prefix = format!("{table_name_safe}/stream-item/%");
                conn.execute(
                    "DELETE FROM sys_stream_items WHERE stream_name = ?1 OR stream_name LIKE ?2",
                    rusqlite::params![table_stream_name, item_stream_prefix],
                )?;
                conn.execute(
                    "DELETE FROM sys_stream_cursors WHERE stream_name = ?1 OR stream_name LIKE ?2",
                    rusqlite::params![table_stream_name, item_stream_prefix],
                )?;
            }
            conn.execute(
                "DELETE FROM item_revisions WHERE table_name = ?1",
                rusqlite::params![table_name_for_sql.as_ref()],
            )?;

            Ok(())
        })
        .await
        .context("delete tables")?;

        self.delete_ttl_config(&table_name).await?;
        self.invalidate_table_info_cache(&table_name).await;

        Ok(())
    }

    pub(crate) async fn do_create_table_storage(
        &self,
        table_name: &TableName,
        request: &CreateTableRequest,
    ) -> StorageResult<()> {
        self.create_table_storage_internal(table_name, request)
            .await
    }
}

pub(crate) fn sqlite_table_scope_id(table_id: u128) -> String {
    format!("sqlite-table:{table_id}")
}

pub(crate) async fn load_sqlite_table_scope_id(
    provider: &SQLiteStorageProvider,
    table_name: &TableName,
) -> StorageResult<String> {
    let table_name = table_name.to_string();
    call_sqlite_raw(&provider.connection, move |conn| {
        let table_id: String = conn.query_row(
            "SELECT id FROM tables WHERE table_name = ?1",
            [table_name],
            |row| row.get(0),
        )?;
        Ok(format!("sqlite-table:{table_id}"))
    })
    .await
}

pub(crate) async fn next_table_policy_version(
    provider: &SQLiteStorageProvider,
    table_scope_id: &str,
) -> StorageResult<u64> {
    let scope = storage_provider::StreamTrimScope::table(table_scope_id, TableName::new(""));
    let current = provider.load_stream_trim_state_by_scope(&scope).await?;
    Ok(current
        .and_then(|state| state.policy_version.checked_add(1))
        .unwrap_or(1))
}

async fn migrate_table_integer_column<F, P>(
    provider: &SQLiteStorageProvider,
    column_name: &'static str,
    statement: F,
    context: &'static str,
) -> StorageResult<()>
where
    F: FnOnce() -> (&'static str, P) + Send + 'static,
    P: rusqlite::Params,
{
    call_sqlite_raw(&provider.connection, move |conn| {
        let has_column: bool = conn.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM pragma_table_info('tables')
                WHERE name = ?1
            )",
            [column_name],
            |row| row.get(0),
        )?;
        if !has_column {
            let (sql, params) = statement();
            conn.execute(sql, params)?;
        }
        Ok(())
    })
    .await
    .context(context)?;
    Ok(())
}
