use crate::backends::turso::provider::{
    TursoOverwriteItemInput, TursoPutItemTxnInput, storage_provider_impl::*,
};

impl TursoStorageProvider {
    pub(crate) async fn put_item_request_operation(
        &self,
        request: PutItemRequest,
    ) -> StorageResult<PutItemResponse> {
        let return_old_on_condition_failure =
            storage_types::return_values_on_condition_check_failure_all_old(
                request.return_values_on_condition_check_failure.as_ref(),
            );
        let PutItemRequest {
            table_name,
            item,
            indexers,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            aux_item_stream_ttl_hours,
            ..
        } = request;
        apply_gsi_write_pressure(self).await?;
        let table_info = self.get_table_info(&table_name).await?;
        let condition = self
            .parse_condition(
                condition_expression,
                &expression_attribute_names,
                &expression_attribute_values,
            )
            .await?;
        let this = self.clone();

        let old_item = self
            .with_transaction(true, |conn| {
                let this = this.clone();
                let table_info = table_info.clone();
                let item = item.clone();
                let indexers = indexers.clone();
                let condition = condition.clone();
                Box::pin(async move {
                    this.put_item_txn(
                        conn,
                        &table_info,
                        TursoPutItemTxnInput {
                            item: &item,
                            indexers: indexers.as_deref(),
                            condition: condition.as_ref(),
                            return_old_on_condition_failure,
                            item_stream_ttl_hours: aux_item_stream_ttl_hours,
                        },
                    )
                    .await
                })
            })
            .await?;

        let attributes = if matches!(return_values, Some(AllOld::AllOld)) {
            old_item
        } else {
            None
        };

        Ok(PutItemResponse {
            attributes: attributes.map(Into::into),
        })
    }

    pub(crate) async fn delete_item_request_operation(
        &self,
        request: DeleteItemRequest,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        let return_old_on_condition_failure =
            storage_types::return_values_on_condition_check_failure_all_old(
                request.return_values_on_condition_check_failure.as_ref(),
            );
        let DeleteItemRequest {
            table_name,
            key,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            aux_item_stream_ttl_hours,
            ..
        } = request;
        apply_gsi_write_pressure(self).await?;
        let table_info = self.get_table_info(&table_name).await?;
        let condition = self
            .parse_condition(
                condition_expression,
                &expression_attribute_names,
                &expression_attribute_values,
            )
            .await?;
        let this = self.clone();

        self.with_transaction(true, |conn| {
            let this = this.clone();
            let table_info = table_info.clone();
            let key = key.clone();
            let condition = condition.clone();
            Box::pin(async move {
                this.delete_item_txn_with_replication(
                    conn,
                    TursoDeleteItemInput {
                        table_info: &table_info,
                        key: &key,
                        condition: condition.as_ref(),
                        return_old_on_condition_failure,
                        replication: None,
                        old_indexers: None,
                        item_stream_ttl_hours: aux_item_stream_ttl_hours,
                    },
                )
                .await
            })
        })
        .await
    }

    pub(crate) async fn guarded_put_item_operation(
        &self,
        request: GuardedPutItemRequest,
    ) -> StorageResult<PutItemResponse> {
        apply_gsi_write_pressure(self).await?;
        let GuardedPutItemRequest {
            table_name,
            item,
            guard,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            indexers,
        } = request;
        let table_info = self.get_table_info(&table_name).await?;
        let key_attributes =
            StorageProvider::get_key_attributes(self, &item, &table_info.key_schema)?;
        let condition = self
            .parse_condition(
                condition_expression,
                &expression_attribute_names,
                &expression_attribute_values,
            )
            .await?;
        let this = self.clone();

        let old_item = self
            .with_transaction(true, |conn| {
                let this = this.clone();
                let table_info = table_info.clone();
                let item = item.clone();
                let guard = guard.clone();
                let key_attributes = key_attributes.clone();
                let condition = condition.clone();
                let indexers = indexers.clone();
                Box::pin(async move {
                    this.validate_durable_guard(
                        conn,
                        &table_info.table_name,
                        &key_attributes,
                        &guard,
                    )
                    .await?;
                    this.put_item_txn(
                        conn,
                        &table_info,
                        TursoPutItemTxnInput {
                            item: &item,
                            indexers: Some(&indexers),
                            condition: condition.as_ref(),
                            return_old_on_condition_failure: false,
                            item_stream_ttl_hours: None,
                        },
                    )
                    .await
                })
            })
            .await?;

        let attributes = if matches!(return_values, Some(AllOld::AllOld)) {
            old_item
        } else {
            None
        };

        Ok(PutItemResponse {
            attributes: attributes.map(Into::into),
        })
    }

    pub(crate) async fn guarded_delete_item_operation(
        &self,
        request: GuardedDeleteItemRequest,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        apply_gsi_write_pressure(self).await?;
        let GuardedDeleteItemRequest {
            table_name,
            key,
            guard,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
        } = request;
        let table_info = self.get_table_info(&table_name).await?;
        let condition = self
            .parse_condition(
                condition_expression,
                &expression_attribute_names,
                &expression_attribute_values,
            )
            .await?;
        let this = self.clone();

        self.with_transaction(true, |conn| {
            let this = this.clone();
            let table_info = table_info.clone();
            let key = key.clone();
            let guard = guard.clone();
            let condition = condition.clone();
            Box::pin(async move {
                this.validate_durable_guard(conn, &table_info.table_name, &key, &guard)
                    .await?;
                this.delete_item_txn_with_replication(
                    conn,
                    TursoDeleteItemInput {
                        table_info: &table_info,
                        key: &key,
                        condition: condition.as_ref(),
                        return_old_on_condition_failure: false,
                        replication: None,
                        old_indexers: None,
                        item_stream_ttl_hours: None,
                    },
                )
                .await
            })
        })
        .await
    }

    pub(crate) async fn apply_replication_mutation_operation(
        &self,
        mutation: ReplicationMutation,
    ) -> StorageResult<()> {
        let table_info = self.get_table_info(&mutation.table_name).await?;
        let metadata = mutation.metadata.clone();
        let new_indexers = mutation.new_indexers.clone();
        let old_indexers = mutation.old_indexers.clone();
        let this = self.clone();
        if let Some(new_image) = mutation.new_image {
            self.with_transaction(true, |conn| {
                let this = this.clone();
                let table_info = table_info.clone();
                let new_image = new_image.clone();
                let metadata = metadata.clone();
                let new_indexers = new_indexers.clone();
                let old_indexers = old_indexers.clone();
                Box::pin(async move {
                    let split =
                        split_item_into_key_and_attributes_sync(new_image.clone(), &table_info)?;
                    let (old_item, stored_indexers) = this
                        .get_item_map_with_indexers_by_key(conn, &table_info, &split.key_attributes)
                        .await?
                        .map_or_else(
                            || (None, Vec::new()),
                            |(item, indexers)| (Some(item), indexers),
                        );
                    this.overwrite_item_txn(
                        conn,
                        &table_info,
                        TursoOverwriteItemInput {
                            item: &new_image,
                            old_item: old_item.as_ref(),
                            indexers: new_indexers.as_deref().unwrap_or_default(),
                            old_indexers: old_indexers
                                .as_deref()
                                .or_else(|| old_item.as_ref().map(|_| stored_indexers.as_slice())),
                            replication: Some(&metadata),
                            item_stream_ttl_hours: None,
                        },
                    )
                    .await
                })
            })
            .await?;
            return Ok(());
        }

        self.with_transaction(true, |conn| {
            let this = this.clone();
            let table_info = table_info.clone();
            let key = mutation.key.clone();
            let metadata = metadata.clone();
            let old_indexers = old_indexers.clone();
            Box::pin(async move {
                this.delete_item_txn_with_replication(
                    conn,
                    TursoDeleteItemInput {
                        table_info: &table_info,
                        key: &key,
                        condition: None,
                        return_old_on_condition_failure: false,
                        replication: Some(&metadata),
                        old_indexers: old_indexers.as_deref(),
                        item_stream_ttl_hours: None,
                    },
                )
                .await
                .map(|_| ())
            })
        })
        .await
    }

    pub(crate) async fn update_item_operation(
        &self,
        request: UpdateItemRequest,
    ) -> StorageResult<UpdateItemResponse> {
        apply_gsi_write_pressure(self).await?;
        let table_info = self.get_table_info(&request.table_name).await?;
        let UpdateItemRequest {
            table_name,
            key,
            indexers,
            update_expression,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            return_values_on_condition_check_failure,
            aux_item_stream_ttl_hours,
            ..
        } = request;
        let this = self.clone();
        let collect_response_fields = return_values_need_updated_fields(return_values.as_ref());
        let return_old_on_condition_failure =
            storage_types::return_values_on_condition_check_failure_all_old(
                return_values_on_condition_check_failure.as_ref(),
            );

        let (old_item, new_item, response_fields) = self
            .with_transaction(true, |conn| {
                let this = this.clone();
                let table_info = table_info.clone();
                let key = key.clone();
                let update_expression = update_expression.clone();
                let condition_expression = condition_expression.clone();
                let expression_attribute_names = expression_attribute_names.clone();
                let expression_attribute_values = expression_attribute_values.clone();
                let requested_indexers = indexers.clone();
                Box::pin(async move {
                    let (operations, condition) = before_update_item_optional(
                        update_expression.as_deref(),
                        condition_expression.as_deref(),
                        expression_attribute_names.as_ref(),
                        expression_attribute_values.as_ref(),
                    )?;
                    let response_fields = if collect_response_fields {
                        operations
                            .iter()
                            .map(|operation| operation.field_name_arc())
                            .collect::<Vec<_>>()
                    } else {
                        Vec::new()
                    };
                    let (existing_item, stored_indexers) = this
                        .get_item_map_with_indexers_by_key(conn, &table_info, &key)
                        .await?
                        .map_or_else(
                            || (None, Vec::new()),
                            |(item, indexers)| (Some(item), indexers),
                        );
                    let (item_to_update, updated_item) = plan_update_from_existing_item(
                        existing_item,
                        &key,
                        &operations,
                        condition.as_ref(),
                        return_old_on_condition_failure,
                    )?;
                    let effective_indexers = requested_indexers
                        .as_deref()
                        .unwrap_or(stored_indexers.as_slice());

                    this.overwrite_item_txn(
                        conn,
                        &table_info,
                        TursoOverwriteItemInput {
                            item: &updated_item,
                            old_item: Some(&item_to_update),
                            indexers: effective_indexers,
                            old_indexers: Some(&stored_indexers),
                            replication: None,
                            item_stream_ttl_hours: aux_item_stream_ttl_hours,
                        },
                    )
                    .await?;

                    Ok((item_to_update, updated_item, response_fields))
                })
            })
            .await?;

        let response = update_item_response(
            &response_fields,
            Some(old_item),
            Some(new_item),
            return_values.as_ref(),
        )?;

        self.invalidate_table_cache(&table_name).await;
        Ok(response)
    }

    pub(crate) async fn guarded_update_item_operation(
        &self,
        request: GuardedUpdateItemRequest,
    ) -> StorageResult<UpdateItemResponse> {
        apply_gsi_write_pressure(self).await?;
        let GuardedUpdateItemRequest { request, guard } = request;
        let table_info = self.get_table_info(&request.table_name).await?;
        let UpdateItemRequest {
            table_name,
            key,
            indexers,
            update_expression,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            aux_item_stream_ttl_hours,
            ..
        } = request;
        let this = self.clone();
        let collect_response_fields = return_values_need_updated_fields(return_values.as_ref());

        let (old_item, new_item, response_fields) = self
            .with_transaction(true, |conn| {
                let this = this.clone();
                let table_info = table_info.clone();
                let key = key.clone();
                let guard = guard.clone();
                let update_expression = update_expression.clone();
                let condition_expression = condition_expression.clone();
                let expression_attribute_names = expression_attribute_names.clone();
                let expression_attribute_values = expression_attribute_values.clone();
                let requested_indexers = indexers.clone();
                Box::pin(async move {
                    let (operations, condition) = before_update_item_optional(
                        update_expression.as_deref(),
                        condition_expression.as_deref(),
                        expression_attribute_names.as_ref(),
                        expression_attribute_values.as_ref(),
                    )?;
                    let response_fields = if collect_response_fields {
                        operations
                            .iter()
                            .map(|operation| operation.field_name_arc())
                            .collect::<Vec<_>>()
                    } else {
                        Vec::new()
                    };
                    this.validate_durable_guard(conn, &table_info.table_name, &key, &guard)
                        .await?;
                    let (existing_item, stored_indexers) = this
                        .get_item_map_with_indexers_by_key(conn, &table_info, &key)
                        .await?
                        .map_or_else(
                            || (None, Vec::new()),
                            |(item, indexers)| (Some(item), indexers),
                        );
                    let (item_to_update, updated_item) = plan_update_from_existing_item(
                        existing_item,
                        &key,
                        &operations,
                        condition.as_ref(),
                        false,
                    )?;
                    let effective_indexers = requested_indexers
                        .as_deref()
                        .unwrap_or(stored_indexers.as_slice());

                    this.overwrite_item_txn(
                        conn,
                        &table_info,
                        TursoOverwriteItemInput {
                            item: &updated_item,
                            old_item: Some(&item_to_update),
                            indexers: effective_indexers,
                            old_indexers: Some(&stored_indexers),
                            replication: None,
                            item_stream_ttl_hours: aux_item_stream_ttl_hours,
                        },
                    )
                    .await?;

                    Ok((item_to_update, updated_item, response_fields))
                })
            })
            .await?;

        let response = update_item_response(
            &response_fields,
            Some(old_item),
            Some(new_item),
            return_values.as_ref(),
        )?;

        self.invalidate_table_cache(&table_name).await;
        Ok(response)
    }
}
