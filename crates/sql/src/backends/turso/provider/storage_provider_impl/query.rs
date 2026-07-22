use crate::backends::turso::provider::storage_provider_impl::*;

impl TursoStorageProvider {
    pub(crate) async fn trim_change_index_markers_older_than(
        &self,
        cutoff_created_at_ms: i64,
    ) -> StorageResult<usize> {
        let conn = self.connect().await?;
        let deleted_markers = self
            .execute(
                &conn,
                sql_statements::trim_change_index_markers_older_than(),
                vec![TursoValue::Integer(cutoff_created_at_ms)],
            )
            .await?;
        usize::try_from(deleted_markers)
            .map_err(|_| StorageError::internal("turso deleted marker count exceeds usize"))
    }

    pub(crate) async fn query_table_with_connection<C>(
        &self,
        conn: &C,
        request: &QueryTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)>
    where
        C: TursoSqlConnection + ?Sized,
    {
        request.validate_for_dynamodb()?;
        let table_info = self.get_table_info(&request.table_name).await?;
        let effective_limit = calc_limit(request.limit, DEFAULT_QUERY_LIMIT, MAX_QUERY_LIMIT)?;
        let exclusive_start_key = decode_exclusive_start(
            &request.exclusive_start_key,
            &table_info,
            &request.index_name,
        )?;

        let (table_name_safe, key_schema, table_key_schema_for_index, origin_gsi) =
            if let Some(index_name) = &request.index_name {
                let gsi = table_info
                    .global_secondary_indexes
                    .as_ref()
                    .and_then(|indexes| {
                        indexes.iter().find(|index| &index.index_name == index_name)
                    })
                    .ok_or_else(|| missing_index_error(&table_info, index_name))?;
                (
                    gsi_table_name(&table_info.table_name, index_name),
                    gsi.key_schema.clone(),
                    Some(table_info.key_schema.as_slice()),
                    true,
                )
            } else {
                (
                    format!("table_{}", table_info.table_name.sanitized_name()),
                    table_info.key_schema.clone(),
                    None,
                    false,
                )
            };

        let conditions = parse_key_condition_expression(
            &request.key_condition_expression,
            &key_schema,
            request.expression_attribute_names.as_ref(),
            request.expression_attribute_values.as_ref(),
        )?;

        let (sql, values) = build_sql_query(
            &table_name_safe,
            &key_schema,
            Some(conditions),
            exclusive_start_key,
            effective_limit,
            request.scan_index_forward,
            table_key_schema_for_index,
        )?;

        let rows = self
            .query_row_set(
                conn,
                &sql,
                values.into_iter().map(TursoValue::Text).collect(),
            )
            .await?;

        let mut items = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            let wire = if origin_gsi {
                self.build_wire_item_from_gsi_row_view(row, &table_info, &key_schema)
                    .await?
            } else {
                self.build_wire_item_from_main_row_view(row, &table_info)
                    .await?
            };
            items.push(wire);
        }

        let has_more = items.len() > effective_limit as usize;
        if has_more {
            items.pop();
        }

        let last_evaluated_key = if has_more {
            items
                .last()
                .map(|item| item.last_evaluated_key(&table_info, &request.index_name))
                .transpose()?
                .flatten()
        } else {
            None
        };

        Ok((request.project_wire_items(items)?, last_evaluated_key))
    }

    pub(crate) async fn batch_get_item_with_connection<C>(
        &self,
        conn: &C,
        request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetWireItemResponse>
    where
        C: TursoSqlConnection + ?Sized,
    {
        let mut responses = HashMap::new();
        for (table_name, keys_and_attributes) in request.request_items {
            let table_info = self.get_table_info(&table_name).await?;
            let mut table_items = Vec::new();
            for key in keys_and_attributes.keys {
                if let Some(item) = self.get_item_map_by_key(conn, &table_info, &key).await? {
                    table_items.push(WireItem::from_attribute_map(&item)?);
                }
            }
            responses.insert(table_name, table_items);
        }

        Ok(BatchGetWireItemResponse {
            responses: Some(responses),
            unprocessed_keys: None,
            consumed_capacity: None,
        })
    }
}
