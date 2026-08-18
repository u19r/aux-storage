use std::time::{Duration, Instant};

use async_trait::async_trait;
use storage_common::{GSI_UPDATE_JOB, JobIntervalMillis, RegistersJobs, TTL_SWEEP_JOB};
use storage_provider::StorageProvider;
use storage_types::{
    CreateTableRequest, ScanTableRequest, StorageError, StorageResult, StoredTableInfo, StreamName,
    TableName, TableStatus,
};
use stream_provider::StreamProvider;
use tokio_postgres::error::SqlState;
use uuid::Uuid;

use crate::{
    backends::postgres::{PostgresStorageProvider, physical_names, sql_statements},
    helpers::MAX_SCAN_LIMIT,
    provider_core::table_lifecycle::{prepare_table_metadata, validate_create_table_request},
};

impl PostgresStorageProvider {
    pub(crate) async fn do_initialize_storage(&self) -> StorageResult<()> {
        {
            let client = self
                .pool
                .get()
                .await
                .map_err(Self::map_postgres_client_acquire_error)?;
            client
                .batch_execute(sql_statements::create_storage_metadata_tables())
                .await
                .map_err(|err| StorageError::internal(&format!("postgres init failed: {err}")))?;
            client
                .batch_execute(sql_statements::add_deletion_protection_column())
                .await
                .map_err(|err| {
                    StorageError::internal(&format!(
                        "postgres deletion protection migration failed: {err}"
                    ))
                })?;
            client
                .batch_execute(sql_statements::add_table_stream_duration_column())
                .await
                .map_err(|err| {
                    StorageError::internal(&format!(
                        "postgres table stream duration migration failed: {err}"
                    ))
                })?;
            client
                .batch_execute(sql_statements::add_default_item_stream_duration_column())
                .await
                .map_err(|err| {
                    StorageError::internal(&format!(
                        "postgres default item stream duration migration failed: {err}"
                    ))
                })?;
        }

        self.initialize_stream().await.map_err(|err| {
            StorageError::internal(&format!("postgres stream init failed: {err}"))
        })?;
        self.initialize_stream_duration_tables().await?;

        if !self.database_jobs_enabled {
            return Ok(());
        }

        struct PostgresRegistrar<'a> {
            mgr: &'a bg_jobs::JobManager,
        }

        #[async_trait]
        impl RegistersJobs for PostgresRegistrar<'_> {
            type Error = StorageError;

            async fn register_timed_job<J>(
                &self,
                name: bg_jobs::BackgroundJobName,
                interval_ms: JobIntervalMillis,
                job: J,
            ) -> Result<(), Self::Error>
            where
                J: bg_jobs::BackgroundJob + 'static,
            {
                let config = bg_jobs::JobConfig {
                    start_immediately: true,
                    sleep_duration: std::time::Duration::from_millis(interval_ms.0),
                    jitter_percent: 10,
                };
                self.mgr
                    .register_job(name, job, config)
                    .await
                    .map_err(|err| {
                        StorageError::internal(&format!(
                            "register postgres job failed: {name}: {err}"
                        ))
                    })?;
                Ok(())
            }
        }

        #[derive(Clone)]
        struct PostgresGsiUpdateJob {
            provider: PostgresStorageProvider,
            run_budget: Duration,
        }

        #[async_trait]
        impl bg_jobs::BackgroundJob for PostgresGsiUpdateJob {
            async fn execute(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
                let mut work_done = false;
                let started = Instant::now();
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

        #[derive(Clone)]
        struct PostgresTtlSweepJob {
            provider: PostgresStorageProvider,
        }

        #[async_trait]
        impl bg_jobs::BackgroundJob for PostgresTtlSweepJob {
            async fn execute(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
                let ttl_progressed = self.provider.run_ttl_sweep_once().await?;
                let trim_progressed = self.provider.run_custom_stream_trim_once().await?;
                Ok(ttl_progressed || trim_progressed)
            }
        }

        let registrar = PostgresRegistrar {
            mgr: &self.job_manager,
        };
        if !self.immediate_gsi_consistency {
            let gsi_cfg = self.database_job_intervals.gsi_config();
            registrar
                .register_timed_job(
                    GSI_UPDATE_JOB,
                    gsi_cfg.update_interval_ms,
                    PostgresGsiUpdateJob {
                        provider: self.for_background_operations(),
                        run_budget: gsi_update_run_budget(gsi_cfg.update_interval_ms),
                    },
                )
                .await?;
        }
        registrar
            .register_timed_job(
                TTL_SWEEP_JOB,
                self.database_job_intervals.ttl_sweep_interval_ms,
                PostgresTtlSweepJob {
                    provider: self.for_background_operations(),
                },
            )
            .await?;

        Ok(())
    }

    pub(crate) async fn do_table_exists(&self, table_name: &TableName) -> StorageResult<bool> {
        let client = self
            .pool
            .get()
            .await
            .map_err(Self::map_postgres_client_acquire_error)?;
        let row = client
            .query_one(sql_statements::table_exists(), &[&table_name.as_ref()])
            .await
            .map_err(|err| Self::map_postgres_error("table_exists query", err))?;
        let count: i64 = row
            .try_get(0)
            .map_err(|err| Self::map_postgres_error("table_exists decode", err))?;
        Ok(count > 0)
    }

    pub(crate) async fn do_create_table(&self, request: &CreateTableRequest) -> StorageResult<()> {
        validate_create_table_request(request)?;

        if self.table_exists(&request.table_name).await? {
            return Err(StorageError::table_already_exists(&request.table_name));
        }

        let metadata = prepare_table_metadata(request)?;

        self.retry_postgres_conflicts("create_table", || {
            let request = request.clone();
            let metadata = metadata.clone();
            async move {
                let table_id = Uuid::now_v7().to_string();
                let table_scope_id =
                    super::stream_duration::postgres_table_scope_id(table_id.as_str());
                let table_duration_plan = storage_provider::plan_table_stream_duration(
                    request.table_name.clone(),
                    table_scope_id,
                    1,
                    metadata.table_stream_duration,
                    metadata.default_item_stream_duration,
                    metadata.created_at,
                );
                let table_name = request.table_name.to_string();
                let creating_status: String = String::from(&TableStatus::Creating);
                let created_at_millis = *metadata.created_at;
                let mut client = self
                    .pool
                    .get()
                    .await
                    .map_err(Self::map_postgres_client_acquire_error)?;
                let transaction = client.transaction().await.map_err(|err| {
                    Self::map_postgres_write_error("start create_table transaction", err)
                })?;

                let rows = transaction
                    .execute(
                        sql_statements::insert_table_metadata(),
                        &[
                            &table_id,
                            &table_name,
                            &creating_status,
                            &created_at_millis,
                            &metadata.attribute_definitions_json,
                            &metadata.key_schema_json,
                            &i16::from(metadata.max_indexers.get()),
                            &metadata.global_secondary_indexes_json,
                            &metadata.stream_specification_json,
                            &metadata.deletion_protection_enabled,
                            &metadata.table_stream_duration.as_hours_wire_value(),
                            &metadata.default_item_stream_duration.as_hours_wire_value(),
                        ],
                    )
                    .await
                    .map_err(|err| {
                        if let Some(db_err) = err.as_db_error()
                            && db_err.code() == &SqlState::UNIQUE_VIOLATION
                        {
                            return StorageError::table_already_exists(&request.table_name);
                        }
                        Self::map_postgres_write_error("insert table metadata", err)
                    })?;

                if rows == 0 {
                    return Err(StorageError::internal(
                        "No rows were inserted into tables metadata",
                    ));
                }

                self.create_table_storage_with_client(&transaction, &request.table_name, &request)
                    .await?;
                Self::write_stream_trim_state_with_client(
                    &transaction,
                    storage_provider::StreamTrimStateWrite {
                        state: table_duration_plan.trim_state,
                        next_marker: table_duration_plan.due_marker,
                    },
                )
                .await?;
                let active_status: String = String::from(&TableStatus::Active);
                transaction
                    .execute(
                        sql_statements::update_table_status(),
                        &[&active_status, &table_name],
                    )
                    .await
                    .map_err(|err| {
                        Self::map_postgres_write_error("update create_table status", err)
                    })?;
                transaction.commit().await.map_err(|err| {
                    Self::map_postgres_write_error("commit create_table transaction", err)
                })?;
                Ok(())
            }
        })
        .await?;
        self.invalidate_table_info_cache(&request.table_name).await;
        Ok(())
    }

    pub(crate) async fn do_update_table_status(
        &self,
        table_name: &TableName,
        status: TableStatus,
    ) -> StorageResult<()> {
        let table_name_value = table_name.to_string();
        let status: String = String::from(&status);
        let client = self
            .pool
            .get()
            .await
            .map_err(Self::map_postgres_client_acquire_error)?;
        let rows = client
            .execute(
                sql_statements::update_table_status(),
                &[&status, &table_name_value],
            )
            .await
            .map_err(|err| Self::map_postgres_error("update table status", err))?;
        if rows == 0 {
            return Err(StorageError::table_not_found(table_name_value.as_str()));
        }
        self.invalidate_table_info_cache(table_name).await;
        Ok(())
    }

    pub(crate) async fn do_list_tables(
        &self,
        limit: u32,
        exclusive_start_table_name: Option<TableName>,
    ) -> StorageResult<Vec<StoredTableInfo>> {
        let client = self
            .pool
            .get()
            .await
            .map_err(Self::map_postgres_client_acquire_error)?;
        let limit = i64::from(limit);
        let rows = if let Some(start_name) = exclusive_start_table_name.map(|name| name.to_string())
        {
            client
                .query(sql_statements::list_tables_after(), &[&start_name, &limit])
                .await
                .map_err(|err| Self::map_postgres_error("list_tables query", err))?
        } else {
            client
                .query(sql_statements::list_all_tables(), &[&limit])
                .await
                .map_err(|err| Self::map_postgres_error("list_tables query", err))?
        };

        rows.iter().map(Self::row_to_stored_table_info).collect()
    }

    pub(crate) async fn do_delete_table(&self, table_name: &TableName) -> StorageResult<()> {
        let table_name_value = table_name.to_string();
        let physical_table_name = physical_names::physical_table_name(table_name);
        let table_info = self.get_table_info(table_name).await.ok();
        if let Some(table_info) = table_info.as_ref()
            && table_info.deletion_protection_enabled
        {
            return Err(StorageError::deletion_protection_enabled(table_name));
        }

        self.retry_postgres_conflicts("delete_table", || {
            let table_name_value = table_name_value.clone();
            let physical_table_name = physical_table_name.clone();
            let table_info = table_info.clone();
            let table_name = table_name.clone();
            async move {
                let mut client = self
                    .pool
                    .get()
                    .await
                    .map_err(Self::map_postgres_client_acquire_error)?;
                let transaction = client.transaction().await.map_err(|err| {
                    Self::map_postgres_write_error("start delete_table transaction", err)
                })?;

                let rows = transaction
                    .execute(
                        sql_statements::delete_table_metadata(),
                        &[&table_name_value],
                    )
                    .await
                    .map_err(|err| Self::map_postgres_write_error("delete table metadata", err))?;
                if rows == 0 {
                    return Err(StorageError::table_not_found(table_name_value.as_str()));
                }

                transaction
                    .batch_execute(&sql_statements::drop_physical_table(&physical_table_name))
                    .await
                    .map_err(|err| Self::map_postgres_write_error("drop physical table", err))?;

                if let Some(table_info) = table_info
                    && let Some(gsis) = table_info.global_secondary_indexes
                {
                    for gsi in gsis {
                        let gsi_table_name =
                            physical_names::physical_gsi_table_name(&table_name, &gsi.index_name);
                        transaction
                            .batch_execute(&sql_statements::drop_named_table(&gsi_table_name))
                            .await
                            .map_err(|err| Self::map_postgres_write_error("drop gsi table", err))?;
                    }
                }

                let ttl_table = physical_names::physical_ttl_index_table_name(&table_name);
                transaction
                    .batch_execute(&sql_statements::drop_named_table(&ttl_table))
                    .await
                    .map_err(|err| Self::map_postgres_write_error("drop ttl index table", err))?;
                transaction
                    .execute(sql_statements::delete_ttl_config(), &[&table_name_value])
                    .await
                    .map_err(|err| Self::map_postgres_write_error("delete ttl config", err))?;

                let table_stream = StreamName::table_stream(&table_name);
                let table_stream_value = Self::encode_stream_name(&table_stream);
                let item_stream_prefix = format!("{}/stream-item/", table_name.sanitized_name());
                let item_stream_prefix_value =
                    Self::encode_stream_name(&StreamName::from(item_stream_prefix.into_bytes()));
                let item_stream_like = format!("{item_stream_prefix_value}%");
                transaction
                    .execute(
                        sql_statements::delete_stream_cursors_for_table(),
                        &[&table_stream_value, &item_stream_like],
                    )
                    .await
                    .map_err(|err| {
                        Self::map_postgres_write_error("delete stream cursors for table", err)
                    })?;
                transaction
                    .execute(
                        sql_statements::delete_stream_items_for_table(),
                        &[&table_stream_value, &item_stream_like],
                    )
                    .await
                    .map_err(|err| {
                        Self::map_postgres_write_error("delete stream items for table", err)
                    })?;

                transaction.commit().await.map_err(|err| {
                    Self::map_postgres_write_error("commit delete_table transaction", err)
                })?;
                Ok(())
            }
        })
        .await?;
        self.invalidate_table_info_cache(table_name).await;
        self.ttl_config_cache.write().await.remove(table_name);
        Ok(())
    }

    pub(crate) async fn do_create_table_storage(
        &self,
        table_name: &TableName,
        request: &CreateTableRequest,
    ) -> StorageResult<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(Self::map_postgres_client_acquire_error)?;
        self.create_table_storage_with_client(&client, table_name, request)
            .await
    }

    pub(super) async fn do_update_table(
        &self,
        request: storage_types::UpdateTableRequest,
    ) -> StorageResult<storage_types::UpdateTableResponse> {
        let table_name = request.table_name.clone();
        let mut table_info = self.get_table_info(&table_name).await?;
        let capacity_increase = crate::provider_core::table_lifecycle::requested_capacity_increase(
            table_info.max_indexers,
            request.max_indexers,
        )?;

        if let Some(target) = capacity_increase {
            let mut client = self
                .pool
                .get()
                .await
                .map_err(Self::map_postgres_client_acquire_error)?;
            let transaction = client.transaction().await.map_err(|error| {
                Self::map_postgres_write_error("start max indexers transaction", error)
            })?;
            transaction
                .execute(
                    sql_statements::update_table_status(),
                    &[&"UPDATING", &table_name.as_ref()],
                )
                .await
                .map_err(|error| {
                    Self::map_postgres_write_error("mark max indexers update", error)
                })?;
            let mut physical_tables = Vec::with_capacity(
                1 + table_info
                    .global_secondary_indexes
                    .as_ref()
                    .map_or(0, Vec::len),
            );
            physical_tables.push(physical_names::physical_table_name(&table_name));
            if let Some(gsis) = table_info.global_secondary_indexes.as_ref() {
                physical_tables.extend(gsis.iter().map(|gsi| {
                    physical_names::physical_gsi_table_name(&table_name, &gsi.index_name)
                }));
            }
            for ordinal in table_info.max_indexers.as_usize()..target.as_usize() {
                let column = crate::utils::indexer_column_name(ordinal);
                for physical_table in &physical_tables {
                    transaction
                        .batch_execute(&format!(
                            "ALTER TABLE \"{physical_table}\" ADD COLUMN \"{column}\" TEXT"
                        ))
                        .await
                        .map_err(|error| {
                            Self::map_postgres_write_error("add max indexer column", error)
                        })?;
                }
            }
            let target_value = i16::from(target.get());
            let changed = transaction
                .execute(
                    "UPDATE tables SET max_indexers = $1, table_status = 'ACTIVE' WHERE \
                     table_name = $2",
                    &[&target_value, &table_name.as_ref()],
                )
                .await
                .map_err(|error| Self::map_postgres_write_error("persist max indexers", error))?;
            if changed != 1 {
                return Err(StorageError::internal(
                    "max indexer metadata update did not affect one table",
                ));
            }
            transaction.commit().await.map_err(|error| {
                Self::map_postgres_write_error("commit max indexers transaction", error)
            })?;
            table_info.max_indexers = target;
            self.invalidate_table_info_cache(&table_name).await;
        }

        self.update_table_status(&table_name, TableStatus::Updating)
            .await?;

        if let Some(spec) = request.stream_specification {
            table_info.stream_specification = Some(spec);
            let spec_json = serde_json::to_string(&table_info.stream_specification)
                .map_err(|err| Self::map_postgres_error("serialize stream specification", err))?;
            let spec_value = Some(spec_json);
            let client = self
                .pool
                .get()
                .await
                .map_err(Self::map_postgres_client_acquire_error)?;
            client
                .execute(
                    sql_statements::update_stream_specification(),
                    &[&spec_value, &table_name.as_ref()],
                )
                .await
                .map_err(|err| Self::map_postgres_error("update stream specification", err))?;
        }

        if let Some(deletion_protection_enabled) = request.deletion_protection_enabled {
            table_info.deletion_protection_enabled = deletion_protection_enabled;
            let client = self
                .pool
                .get()
                .await
                .map_err(Self::map_postgres_client_acquire_error)?;
            client
                .execute(
                    sql_statements::update_deletion_protection(),
                    &[&deletion_protection_enabled, &table_name.as_ref()],
                )
                .await
                .map_err(|err| Self::map_postgres_error("update deletion protection", err))?;
        }

        if request.aux_stream_duration_hours.is_some()
            || request.aux_default_item_stream_duration_hours.is_some()
        {
            if let Some(table_stream_duration) = request.aux_stream_duration_hours {
                table_info.table_stream_duration = table_stream_duration;
            }
            if let Some(default_item_stream_duration) =
                request.aux_default_item_stream_duration_hours
            {
                table_info.default_item_stream_duration = default_item_stream_duration;
            }
            let table_scope_id = self.load_postgres_table_scope_id(&table_name).await?;
            let policy_version = self
                .next_postgres_table_policy_version(&table_scope_id)
                .await?;
            let table_duration_plan = storage_provider::plan_table_stream_duration(
                table_name.clone(),
                table_scope_id,
                policy_version,
                table_info.table_stream_duration,
                table_info.default_item_stream_duration,
                storage_types::TimestampMillis::now(),
            );
            let mut client = self
                .pool
                .get()
                .await
                .map_err(Self::map_postgres_client_acquire_error)?;
            let transaction = client.transaction().await.map_err(|err| {
                Self::map_postgres_write_error("start update stream durations transaction", err)
            })?;
            transaction
                .execute(
                    sql_statements::update_stream_durations(),
                    &[
                        &table_info.table_stream_duration.as_hours_wire_value(),
                        &table_info
                            .default_item_stream_duration
                            .as_hours_wire_value(),
                        &table_name.as_ref(),
                    ],
                )
                .await
                .map_err(|err| Self::map_postgres_write_error("update stream durations", err))?;
            Self::write_stream_trim_state_with_client(
                &transaction,
                storage_provider::StreamTrimStateWrite {
                    state: table_duration_plan.trim_state,
                    next_marker: table_duration_plan.due_marker,
                },
            )
            .await?;
            transaction.commit().await.map_err(|err| {
                Self::map_postgres_write_error("commit update stream durations", err)
            })?;
            self.invalidate_table_info_cache(&table_name).await;
        }

        if let Some(gsi_updates) = request.global_secondary_index_updates {
            for gsi_update in gsi_updates {
                let action_count = usize::from(gsi_update.create.is_some())
                    + usize::from(gsi_update.update.is_some())
                    + usize::from(gsi_update.delete.is_some());
                if action_count == 0 {
                    continue;
                }
                if action_count > 1 {
                    return Err(StorageError::validation(
                        "Each GlobalSecondaryIndexUpdate must include exactly one action",
                    ));
                }

                if let Some(create) = gsi_update.create {
                    let mut gsis = table_info
                        .global_secondary_indexes
                        .clone()
                        .unwrap_or_default();
                    if gsis.iter().any(|gsi| gsi.index_name == create.index_name) {
                        return Err(StorageError::validation(format!(
                            "Global secondary index '{}' already exists",
                            create.index_name
                        )));
                    }
                    if gsis.len() + 1 > crate::constants::MAX_GSI_COUNT {
                        return Err(StorageError::validation(format!(
                            "Too many global secondary indexes: {} (max {})",
                            gsis.len() + 1,
                            crate::constants::MAX_GSI_COUNT
                        )));
                    }
                    for key in &create.key_schema {
                        if !table_info
                            .attribute_definitions
                            .iter()
                            .any(|def| def.attribute_name == key.attribute_name)
                        {
                            return Err(StorageError::validation(format!(
                                "GSI '{}' key attribute '{}' missing from attribute definitions",
                                create.index_name, key.attribute_name
                            )));
                        }
                    }

                    let new_gsi: storage_types::GlobalSecondaryIndex = create.clone().into();
                    gsis.push(new_gsi.clone());
                    table_info.global_secondary_indexes = Some(gsis.clone());
                    let gsis_json = serde_json::to_string(&table_info.global_secondary_indexes)
                        .map_err(|err| Self::map_postgres_error("serialize gsi metadata", err))?;
                    let gsis_value = Some(gsis_json);
                    let client = self
                        .pool
                        .get()
                        .await
                        .map_err(Self::map_postgres_client_acquire_error)?;
                    client
                        .execute(
                            sql_statements::update_global_secondary_indexes(),
                            &[&gsis_value, &table_name.as_ref()],
                        )
                        .await
                        .map_err(|err| Self::map_postgres_error("update gsi metadata", err))?;

                    let create_sqls = Self::build_postgres_gsi_creation_sqls(
                        &table_name,
                        &table_info.attribute_definitions,
                        &table_info.key_schema,
                        std::slice::from_ref(&new_gsi),
                        table_info.max_indexers,
                    );
                    for create_sql in create_sqls {
                        client
                            .batch_execute(&create_sql)
                            .await
                            .map_err(|err| Self::map_postgres_error("create gsi table", err))?;
                    }

                    let mut exclusive_start_key: Option<String> = None;
                    let mut backfill_table_info = table_info.clone();
                    backfill_table_info.global_secondary_indexes = Some(vec![create.into()]);
                    loop {
                        let (items, lek) = self
                            .scan_table_with_item_stream_versions(&ScanTableRequest {
                                table_name: table_name.clone(),
                                index_name: None,
                                limit: Some(MAX_SCAN_LIMIT),
                                exclusive_start_key: exclusive_start_key.clone(),
                                consistent_read: true,
                            })
                            .await?;
                        if items.is_empty() {
                            break;
                        }
                        for versioned in items {
                            let item_map = versioned.item.into_attribute_map()?;
                            self.upsert_gsi_entries_for_item_with_client(
                                &client,
                                &table_name,
                                &backfill_table_info,
                                &item_map,
                                &versioned.indexers,
                            )
                            .await?;
                        }
                        exclusive_start_key = lek;
                        if exclusive_start_key.is_none() {
                            break;
                        }
                    }
                }

                if let Some(delete) = gsi_update.delete {
                    if let Some(mut gsis) = table_info.global_secondary_indexes.clone() {
                        gsis.retain(|gsi| gsi.index_name != delete.index_name);
                        table_info.global_secondary_indexes =
                            if gsis.is_empty() { None } else { Some(gsis) };
                    }
                    let gsis_json = match &table_info.global_secondary_indexes {
                        Some(gsis) => Some(serde_json::to_string(gsis).map_err(|err| {
                            Self::map_postgres_error("serialize gsi metadata", err)
                        })?),
                        None => None,
                    };
                    let gsi_table_name =
                        physical_names::physical_gsi_table_name(&table_name, &delete.index_name);
                    let client = self
                        .pool
                        .get()
                        .await
                        .map_err(Self::map_postgres_client_acquire_error)?;
                    client
                        .execute(
                            sql_statements::update_global_secondary_indexes(),
                            &[&gsis_json, &table_name.as_ref()],
                        )
                        .await
                        .map_err(|err| Self::map_postgres_error("update gsi metadata", err))?;
                    client
                        .batch_execute(&sql_statements::drop_named_table(&gsi_table_name))
                        .await
                        .map_err(|err| Self::map_postgres_error("drop gsi table", err))?;
                }
            }
        }

        self.update_table_status(&table_name, TableStatus::Active)
            .await?;
        table_info.table_status = TableStatus::Active;

        Ok(storage_types::UpdateTableResponse {
            table_description: storage_types::TableDescription {
                table_name: table_info.table_name.clone(),
                table_status: TableStatus::Active,
                created_at: table_info.created_at.into(),
                attribute_definitions: table_info.attribute_definitions.clone(),
                key_schema: table_info.key_schema.clone(),
                max_indexers: table_info.max_indexers,
                table_size_bytes: table_info.table_size_bytes,
                item_count: table_info.item_count,
                table_arn: format!(
                    "arn:aws:dynamodb:us-east-1:123456789012:table/{}",
                    table_info.table_name
                ),
                replicas: None,
                multi_region_consistency: None,
                billing_mode_summary: Some(storage_types::BillingModeSummary {
                    billing_mode: Some(storage_types::BillingMode::PayPerRequest),
                    last_update_to_pay_per_request_date_time: None,
                }),
                global_secondary_indexes: table_info.global_secondary_indexes.clone().map(
                    |indexes| {
                        indexes
                            .into_iter()
                            .map(|index| storage_types::GlobalSecondaryIndexDescription {
                                index_name: index.index_name,
                                key_schema: index.key_schema,
                                projection: index.projection,
                                index_status: None,
                                backfilling: None,
                                provisioned_throughput: None,
                                index_size_bytes: None,
                                item_count: None,
                                index_arn: None,
                            })
                            .collect()
                    },
                ),
                local_secondary_indexes: None,
                provisioned_throughput: None,
                stream_specification: table_info.stream_specification.clone(),
                latest_stream_arn: None,
                latest_stream_label: None,
                deletion_protection_enabled: table_info.deletion_protection_enabled,
            },
        })
    }
}

fn gsi_update_run_budget(interval_ms: JobIntervalMillis) -> Duration {
    Duration::from_millis(interval_ms.0.saturating_mul(95) / 100)
}
