use crate::{GsiPhysicalName, backends::turso::provider::storage_provider_impl::*};

impl TursoStorageProvider {
    pub(crate) async fn create_table_operation(
        &self,
        request: &CreateTableRequest,
    ) -> StorageResult<()> {
        validate_create_table_request(request)?;
        let table_name = request.table_name.clone();
        let _ddl_guard = self.ddl_lock.lock().await;

        let metadata = prepare_table_metadata(request)?;
        let stored_gsis = request
            .global_secondary_indexes
            .clone()
            .map(|indexes| indexes.into_iter().map(Into::into).collect::<Vec<_>>());

        let table_name_for_tx = table_name.clone();
        let this = self.clone();
        self.with_exclusive_transaction(true, |conn| {
            let this = this.clone();
            let metadata = metadata.clone();
            let request = request.clone();
            let stored_gsis = stored_gsis.clone();
            let table_name_for_tx = table_name_for_tx.clone();
            Box::pin(async move {
                if this.table_exists_conn(conn, &table_name_for_tx).await? {
                    return Err(StorageError::table_already_exists(&table_name_for_tx));
                }

                let table_id = uuid::Uuid::now_v7().to_string();
                let table_duration_plan = plan_table_stream_duration(
                    table_name_for_tx.clone(),
                    format!("turso-table:{table_id}"),
                    1,
                    metadata.table_stream_duration,
                    metadata.default_item_stream_duration,
                    metadata.created_at,
                );
                let insert_sql = sql_statements::insert_table();
                let insert_params = vec![
                    TursoValue::Text(table_id),
                    TursoValue::Text(table_name_for_tx.to_string()),
                    TursoValue::Text("CREATING".to_string()),
                    TursoValue::Integer(*metadata.created_at),
                    TursoValue::Text(metadata.attribute_definitions_json),
                    TursoValue::Text(metadata.key_schema_json),
                    TursoValue::Integer(i64::from(metadata.max_indexers.get())),
                    option_string_to_value(metadata.global_secondary_indexes_json),
                    TursoValue::Integer(0),
                    TursoValue::Integer(0),
                    option_string_to_value(metadata.stream_specification_json),
                    TursoValue::Integer(if metadata.deletion_protection_enabled {
                        1
                    } else {
                        0
                    }),
                    TursoValue::Integer(metadata.table_stream_duration.as_hours_wire_value()),
                    TursoValue::Integer(
                        metadata.default_item_stream_duration.as_hours_wire_value(),
                    ),
                ];
                let _ = this.execute(conn, insert_sql, insert_params).await?;
                this.write_stream_trim_state(
                    conn,
                    storage_provider::StreamTrimStateWrite {
                        state: table_duration_plan.trim_state,
                        next_marker: table_duration_plan.due_marker,
                    },
                )
                .await?;

                let rowid_mode = SqliteTableRowidMode::WithRowid;
                // TODO: Enable after Turso releases support for WITHOUT ROWID.
                // let rowid_mode = SqliteTableRowidMode::WithoutRowid;

                let create_sql = build_table_creation_sql(
                    &request.table_name,
                    &request.attribute_definitions,
                    &request.key_schema,
                    stored_gsis.as_deref(),
                    request.max_indexers,
                    rowid_mode,
                );
                let _ = this.execute(conn, &create_sql, Vec::new()).await?;

                if let Some(gsis) = stored_gsis.as_ref() {
                    for sql in build_gsi_creation_sqls(
                        &request.table_name,
                        &request.attribute_definitions,
                        &request.key_schema,
                        gsis,
                        request.max_indexers,
                        rowid_mode,
                    ) {
                        let _ = this.execute(conn, &sql, Vec::new()).await?;
                    }
                }

                let _ = this
                    .execute(
                        conn,
                        sql_statements::update_table_status(),
                        vec![
                            TursoValue::Text("ACTIVE".to_string()),
                            TursoValue::Text(table_name_for_tx.to_string()),
                        ],
                    )
                    .await?;

                Ok(())
            })
        })
        .await?;

        self.invalidate_table_cache(&table_name).await;
        Ok(())
    }

    pub(crate) async fn delete_table_operation(&self, table_name: &TableName) -> StorageResult<()> {
        let _ddl_guard = self.ddl_lock.lock().await;
        let table_name_clone = table_name.clone();
        let this = self.clone();

        self.with_exclusive_transaction(true, |conn| {
            let this = this.clone();
            let table_name_clone = table_name_clone.clone();
            Box::pin(async move {
                let table_info = this
                    .load_table_info_uncached(conn, &table_name_clone)
                    .await?;
                if table_info.deletion_protection_enabled {
                    return Err(StorageError::deletion_protection_enabled(&table_name_clone));
                }

                let _ = this
                    .execute(
                        conn,
                        sql_statements::delete_table_metadata(),
                        vec![TursoValue::Text(table_name_clone.to_string())],
                    )
                    .await?;
                let _ = this
                    .execute(
                        conn,
                        &sql_statements::drop_table(&table_name_clone.sanitized_name()),
                        Vec::new(),
                    )
                    .await?;

                if let Some(gsis) = table_info.global_secondary_indexes.as_ref() {
                    for gsi in gsis {
                        let gsi_table = gsi_table_name(&table_info.table_name, &gsi.index_name);
                        let _ = this
                            .execute(
                                conn,
                                &sql_statements::drop_named_table(&gsi_table),
                                Vec::new(),
                            )
                            .await?;
                    }
                }
                Ok(())
            })
        })
        .await?;

        self.invalidate_table_cache(table_name).await;
        Ok(())
    }

    pub(crate) async fn update_table_operation(
        &self,
        request: storage_types::UpdateTableRequest,
    ) -> StorageResult<storage_types::UpdateTableResponse> {
        let mut table_info = self.get_table_info(&request.table_name).await?;
        let capacity_increase = crate::provider_core::table_lifecycle::requested_capacity_increase(
            table_info.max_indexers,
            request.max_indexers,
        )?;
        if let Some(target) = capacity_increase {
            let _ddl_guard = self.ddl_lock.lock().await;
            let this = self.clone();
            let capacity_table_info = table_info.clone();
            self.with_exclusive_transaction(true, |conn| {
                let this = this.clone();
                let table_info = capacity_table_info.clone();
                Box::pin(async move {
                    let _ = this
                        .execute(
                            conn,
                            sql_statements::update_table_status(),
                            vec![
                                TursoValue::Text("UPDATING".to_string()),
                                TursoValue::Text(table_info.table_name.to_string()),
                            ],
                        )
                        .await?;
                    let mut physical_tables = Vec::with_capacity(
                        1 + table_info
                            .global_secondary_indexes
                            .as_ref()
                            .map_or(0, Vec::len),
                    );
                    physical_tables
                        .push(format!("table_{}", table_info.table_name.sanitized_name()));
                    if let Some(gsis) = table_info.global_secondary_indexes.as_ref() {
                        physical_tables.extend(gsis.iter().map(|gsi| {
                            GsiPhysicalName::compose(
                                &table_info.table_name.sanitized_name(),
                                &gsi.index_name.sanitized_name(),
                            )
                            .to_string()
                        }));
                    }
                    for ordinal in table_info.max_indexers.as_usize()..target.as_usize() {
                        let column = crate::utils::indexer_column_name(ordinal);
                        for physical_table in &physical_tables {
                            let _ = this
                                .execute(
                                    conn,
                                    &format!(
                                        "ALTER TABLE \"{physical_table}\" ADD COLUMN \"{column}\" \
                                         TEXT"
                                    ),
                                    Vec::new(),
                                )
                                .await?;
                        }
                    }
                    let changed = this
                        .execute(
                            conn,
                            "UPDATE tables SET max_indexers = ?1, table_status = 'ACTIVE' WHERE \
                             table_name = ?2",
                            vec![
                                TursoValue::Integer(i64::from(target.get())),
                                TursoValue::Text(table_info.table_name.to_string()),
                            ],
                        )
                        .await?;
                    if changed != 1 {
                        return Err(StorageError::internal(
                            "max indexer metadata update did not affect one table",
                        ));
                    }
                    Ok(())
                })
            })
            .await?;
            table_info.max_indexers = target;
            table_info.table_status = TableStatus::Active;
            self.invalidate_table_cache(&request.table_name).await;
        }
        if let Some(deletion_protection_enabled) = request.deletion_protection_enabled {
            let conn = self.connect().await?;
            let _ = self
                .execute(
                    &conn,
                    sql_statements::update_deletion_protection(),
                    vec![
                        TursoValue::Integer(if deletion_protection_enabled { 1 } else { 0 }),
                        TursoValue::Text(request.table_name.to_string()),
                    ],
                )
                .await?;
            self.invalidate_table_cache(&request.table_name).await;
            table_info.deletion_protection_enabled = deletion_protection_enabled;
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
            let table_name = request.table_name.clone();
            let this = self.clone();
            let table_stream_duration = table_info.table_stream_duration;
            let default_item_stream_duration = table_info.default_item_stream_duration;
            self.with_exclusive_transaction(true, |conn| {
                let this = this.clone();
                let table_name = table_name.clone();
                Box::pin(async move {
                    let table_scope_id = this.load_table_scope_id(conn, &table_name).await?;
                    let policy_version = this
                        .next_table_policy_version(conn, &table_scope_id)
                        .await?;
                    let table_duration_plan = plan_table_stream_duration(
                        table_name.clone(),
                        table_scope_id,
                        policy_version,
                        table_stream_duration,
                        default_item_stream_duration,
                        TimestampMillis::now(),
                    );
                    let _ = this
                        .execute(
                            conn,
                            sql_statements::update_stream_durations(),
                            vec![
                                TursoValue::Integer(table_stream_duration.as_hours_wire_value()),
                                TursoValue::Integer(
                                    default_item_stream_duration.as_hours_wire_value(),
                                ),
                                TursoValue::Text(table_name.to_string()),
                            ],
                        )
                        .await?;
                    this.write_stream_trim_state(
                        conn,
                        storage_provider::StreamTrimStateWrite {
                            state: table_duration_plan.trim_state,
                            next_marker: table_duration_plan.due_marker,
                        },
                    )
                    .await?;
                    Ok(())
                })
            })
            .await?;
            self.invalidate_table_cache(&request.table_name).await;
        }

        Ok(storage_types::UpdateTableResponse {
            table_description: storage_types::TableDescription {
                table_name: table_info.table_name.clone(),
                table_status: table_info.table_status,
                created_at: table_info.created_at.into(),
                attribute_definitions: table_info.attribute_definitions,
                key_schema: table_info.key_schema,
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
                global_secondary_indexes: table_info.global_secondary_indexes.map(|indexes| {
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
                }),
                local_secondary_indexes: None,
                provisioned_throughput: None,
                stream_specification: table_info.stream_specification,
                latest_stream_arn: None,
                latest_stream_label: None,
                deletion_protection_enabled: table_info.deletion_protection_enabled,
            },
        })
    }
}
