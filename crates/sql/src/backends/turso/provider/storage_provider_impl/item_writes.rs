use crate::backends::turso::provider::storage_provider_impl::*;

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
                let condition = condition.clone();
                Box::pin(async move {
                    this.put_item_txn(
                        conn,
                        &table_info,
                        &item,
                        condition.as_ref(),
                        return_old_on_condition_failure,
                        aux_item_stream_ttl_hours,
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
                Box::pin(async move {
                    this.validate_durable_guard(
                        conn,
                        &table_info.table_name,
                        &key_attributes,
                        &guard,
                    )
                    .await?;
                    this.put_item_txn(conn, &table_info, &item, condition.as_ref(), false, None)
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
        let this = self.clone();
        if let Some(new_image) = mutation.new_image {
            self.with_transaction(true, |conn| {
                let this = this.clone();
                let table_info = table_info.clone();
                let new_image = new_image.clone();
                let metadata = metadata.clone();
                Box::pin(async move {
                    let split =
                        split_item_into_key_and_attributes_sync(new_image.clone(), &table_info)?;
                    let old_item = this
                        .get_item_map_by_key(conn, &table_info, &split.key_attributes)
                        .await?;
                    this.overwrite_item_txn_with_replication(
                        conn,
                        &table_info,
                        &new_image,
                        old_item.as_ref(),
                        Some(&metadata),
                        None,
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
            Box::pin(async move {
                this.delete_item_txn_with_replication(
                    conn,
                    TursoDeleteItemInput {
                        table_info: &table_info,
                        key: &key,
                        condition: None,
                        return_old_on_condition_failure: false,
                        replication: Some(&metadata),
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
                    let existing_item = this.get_item_map_by_key(conn, &table_info, &key).await?;
                    let (item_to_update, updated_item) = plan_update_from_existing_item(
                        existing_item,
                        &key,
                        &operations,
                        condition.as_ref(),
                        return_old_on_condition_failure,
                    )?;

                    this.overwrite_item_txn(
                        conn,
                        &table_info,
                        &updated_item,
                        Some(&item_to_update),
                        aux_item_stream_ttl_hours,
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
                    let existing_item = this.get_item_map_by_key(conn, &table_info, &key).await?;
                    let (item_to_update, updated_item) = plan_update_from_existing_item(
                        existing_item,
                        &key,
                        &operations,
                        condition.as_ref(),
                        false,
                    )?;

                    this.overwrite_item_txn(
                        conn,
                        &table_info,
                        &updated_item,
                        Some(&item_to_update),
                        aux_item_stream_ttl_hours,
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
