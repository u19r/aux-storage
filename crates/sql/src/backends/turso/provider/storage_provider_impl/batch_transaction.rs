use crate::backends::turso::provider::storage_provider_impl::*;

impl TursoStorageProvider {
    pub(crate) async fn scan_table_operation(
        &self,
        request: &storage_types::ScanTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        let table_info = self.get_table_info(&request.table_name).await?;
        let effective_limit = calc_limit(request.limit, DEFAULT_SCAN_LIMIT, MAX_SCAN_LIMIT)?;
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

        let (sql, values) = build_sql_query(
            &table_name_safe,
            &key_schema,
            None,
            exclusive_start_key,
            effective_limit,
            Some(true),
            table_key_schema_for_index,
        )?;

        let conn = self.connect().await?;
        let rows = self
            .query_row_set(
                &conn,
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

        Ok((items, last_evaluated_key))
    }

    pub(crate) async fn batch_write_item_operation(
        &self,
        request: BatchWriteItemRequest,
        _should_write_to_stream: bool,
    ) -> StorageResult<BatchWriteItemResponse> {
        apply_gsi_write_pressure(self).await?;
        let mut prepared_ops: Vec<PreparedBatchOperation> = Vec::new();
        for (table_name, writes) in request.request_items {
            let table_info = self.get_table_info(&table_name).await?;
            for write in writes {
                prepared_ops.push(prepare_batch_operation(&table_info, write)?);
            }
        }

        let this = self.clone();
        self.with_transaction(true, move |conn| {
            let this = this.clone();
            let prepared_ops = prepared_ops.clone();
            Box::pin(async move {
                this.execute_prepared_batch_operations(conn, &prepared_ops)
                    .await
            })
        })
        .await?;

        Ok(BatchWriteItemResponse {
            unprocessed_items: None,
            item_collection_metrics: None,
            consumed_capacity: None,
        })
    }

    pub(crate) async fn transact_write_items_operation(
        &self,
        request: TransactWriteItemsRequest,
    ) -> StorageResult<TransactWriteItemsResponse> {
        apply_gsi_write_pressure(self).await?;
        let this = self.clone();
        self.with_transaction(true, |conn| {
            let this = this.clone();
            let request = request.clone();
            Box::pin(async move {
                let mut preflights = Vec::with_capacity(request.transact_items.len());
                for item in &request.transact_items {
                    preflights.push(this.preflight_transact_item_key(item).await?);
                }
                if let Some(error) = transaction_canceled_for_preflights(&preflights) {
                    return Err(error);
                }
                validate_no_duplicate_transact_item_keys(&preflights)?;

                let item_count = request.transact_items.len();
                let mut cancellation_reasons = vec![None; item_count];
                for (index, item) in request.transact_items.into_iter().enumerate() {
                    let result = async {
                        if let Some(put) = item.put {
                            let table_info = this.get_table_info(&put.table_name).await?;
                            validate_transact_put_item_key(&table_info, &put.item)?;
                            let old_item = if put.condition_expression.is_some() {
                                let split_item = split_item_into_key_and_attributes_sync(
                                    put.item.clone(),
                                    &table_info,
                                )?;
                                this.get_item_map_by_key(
                                    conn,
                                    &table_info,
                                    &split_item.key_attributes,
                                )
                                .await?
                            } else {
                                None
                            };
                            let condition = this
                                .parse_condition(
                                    put.condition_expression.clone(),
                                    &put.expression_attribute_names,
                                    &put.expression_attribute_values,
                                )
                                .await?;
                            if let Some(condition) = condition.as_ref()
                                && !evaluate_condition(
                                    condition_item_ref(old_item.as_ref()),
                                    condition,
                                )
                            {
                                return Err(transaction_canceled_for_reason(
                                    index,
                                    conditional_check_failed_reason(
                                        all_old(
                                            put.return_values_on_condition_check_failure.as_ref(),
                                        )
                                        .then_some(old_item.as_ref())
                                        .flatten(),
                                    )?,
                                ));
                            }
                            let _ = this
                                .put_item_txn(
                                    conn,
                                    &table_info,
                                    &put.item,
                                    None,
                                    false,
                                    put.aux_item_stream_ttl_hours,
                                )
                                .await?;
                        }

                        if let Some(delete) = item.delete {
                            let table_info = this.get_table_info(&delete.table_name).await?;
                            validate_transact_key(&table_info, &delete.key)?;
                            let old_item = if delete.condition_expression.is_some() {
                                this.get_item_map_by_key(conn, &table_info, &delete.key)
                                    .await?
                            } else {
                                None
                            };
                            let condition = this
                                .parse_condition(
                                    delete.condition_expression.clone(),
                                    &delete.expression_attribute_names,
                                    &delete.expression_attribute_values,
                                )
                                .await?;
                            if let Some(condition) = condition.as_ref()
                                && !evaluate_condition(
                                    condition_item_ref(old_item.as_ref()),
                                    condition,
                                )
                            {
                                return Err(transaction_canceled_for_reason(
                                    index,
                                    conditional_check_failed_reason(
                                        all_old(
                                            delete
                                                .return_values_on_condition_check_failure
                                                .as_ref(),
                                        )
                                        .then_some(old_item.as_ref())
                                        .flatten(),
                                    )?,
                                ));
                            }
                            let _ = this
                                .delete_item_txn_with_replication(
                                    conn,
                                    TursoDeleteItemInput {
                                        table_info: &table_info,
                                        key: &delete.key,
                                        condition: None,
                                        return_old_on_condition_failure: false,
                                        replication: None,
                                        item_stream_ttl_hours: delete.aux_item_stream_ttl_hours,
                                    },
                                )
                                .await?;
                        }

                        if let Some(update) = item.update {
                            let table_info = this.get_table_info(&update.table_name).await?;
                            let (operations, condition) = before_update_item(
                                update.update_expression.as_str(),
                                update.condition_expression.as_deref(),
                                update.expression_attribute_names.as_ref(),
                                update.expression_attribute_values.as_ref(),
                            )?;
                            let existing_item = this
                                .get_item_map_by_key(conn, &table_info, &update.key)
                                .await?;

                            if let Some(condition) = condition.as_ref()
                                && !evaluate_condition(
                                    condition_item_ref(existing_item.as_ref()),
                                    condition,
                                )
                            {
                                return Err(transaction_canceled_for_reason(
                                    index,
                                    conditional_check_failed_reason(
                                        all_old(
                                            update
                                                .return_values_on_condition_check_failure
                                                .as_ref(),
                                        )
                                        .then_some(existing_item.as_ref())
                                        .flatten(),
                                    )?,
                                ));
                            }

                            let item_to_update =
                                existing_item.unwrap_or_else(|| update.key.to_attribute_map());
                            let updated_item =
                                apply_bound_update_operations(item_to_update, &operations)?;
                            let _ = this
                                .put_item_txn(
                                    conn,
                                    &table_info,
                                    &updated_item,
                                    None,
                                    false,
                                    update.aux_item_stream_ttl_hours,
                                )
                                .await?;
                        }

                        if let Some(condition_check) = item.condition_check {
                            let table_info =
                                this.get_table_info(&condition_check.table_name).await?;
                            validate_transact_key(&table_info, &condition_check.key)?;
                            let existing = this
                                .get_item_map_by_key(conn, &table_info, &condition_check.key)
                                .await?;
                            let parsed = parse_condition_expression(
                                &condition_check.condition_expression,
                                condition_check.expression_attribute_names.as_ref(),
                                condition_check.expression_attribute_values.as_ref(),
                            )
                            .map_err(StorageError::validation)?;
                            if !evaluate_condition(condition_item_ref(existing.as_ref()), &parsed) {
                                return Err(transaction_canceled_for_reason(
                                    index,
                                    conditional_check_failed_reason(
                                        all_old(
                                            condition_check
                                                .return_values_on_condition_check_failure
                                                .as_ref(),
                                        )
                                        .then_some(existing.as_ref())
                                        .flatten(),
                                    )?,
                                ));
                            }
                        }
                        Ok(())
                    }
                    .await;
                    if let Err(error) = result {
                        let error =
                            transaction_canceled_for_item_error_with_len(index, item_count, error);
                        let Some(reason) = transaction_cancellation_reason_at(&error, index) else {
                            return Err(error);
                        };
                        cancellation_reasons[index] = Some(reason);
                    }
                }
                if let Some(error) = transaction_canceled_for_indexed_reasons(cancellation_reasons)
                {
                    return Err(error);
                }

                Ok(TransactWriteItemsResponse {
                    consumed_capacity: None,
                    item_collection_metrics: None,
                })
            })
        })
        .await
    }
}
