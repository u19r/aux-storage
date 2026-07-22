use crate::backends::turso::provider::core::*;

impl TursoStorageProvider {
    pub(crate) async fn insert_change_index_marker<C>(
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

    pub(crate) async fn insert_stream_row<C>(
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
}
