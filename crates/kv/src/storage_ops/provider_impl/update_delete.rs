use crate::storage_ops::provider_impl::*;

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub(super) async fn update_item_impl(
        &self,
        request: UpdateItemRequest,
    ) -> StorageResult<UpdateItemResponse> {
        apply_gsi_write_pressure(self).await?;
        let billed_bytes = serializable_payload_bytes(&request);
        let UpdateItemRequest {
            table_name,
            key,
            update_expression,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            return_values_on_condition_check_failure,
            aux_item_stream_ttl_hours: request_item_stream_ttl_hours,
            ..
        } = request;
        let table_metadata = self
            .get_table_identity_from_name(&table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(&table_name.clone()))?;
        let table_info = table_metadata.table_info.clone();
        let ttl_config = self.load_ttl_config(&table_name).await?;
        let preserve_old_item = return_values_need_old_item(return_values.as_ref())
            || ttl_tracking_enabled(ttl_config.as_ref())
            || request_item_stream_ttl_hours.is_some();
        let operations = update_operations(
            update_expression.as_deref(),
            expression_attribute_names.as_ref(),
            expression_attribute_values.as_ref(),
        )?;
        let condition = update_condition(
            condition_expression.as_deref(),
            expression_attribute_names.as_ref(),
            expression_attribute_values.as_ref(),
        )?;

        let result = self
            .retry_update_item(UpdateItemExecution {
                table_metadata,
                table_info,
                key,
                operations,
                condition,
                return_values_on_condition_check_failure,
                request_item_stream_ttl_hours,
                preserve_old_item,
                ttl_config,
            })
            .await?;

        record_write(result.items_updated, result.bytes_written);
        record_write_cost("update_item", "update", result.items_updated, billed_bytes);
        update_item_response(
            &result.operations,
            result.old_item,
            result.new_item,
            return_values.as_ref(),
        )
    }

    pub(super) async fn execute_delete_item(
        &self,
        request: DeleteItemRequest,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        let return_old_on_condition_failure = return_values_on_condition_check_failure_all_old(
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
        if key.is_empty() {
            record_write(0, 0);
            return Ok(None);
        }
        apply_gsi_write_pressure(self).await?;
        let billed_bytes = attr_map_payload_bytes(&key);
        let table_metadata = self
            .get_table_identity_from_name(&table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(&table_name.clone()))?;
        let table_info = table_metadata.table_info.clone();
        let ttl_config = self.load_ttl_config(&table_name).await?;
        let condition = delete_condition(
            condition_expression,
            expression_attribute_names.as_ref(),
            expression_attribute_values.as_ref(),
        )?;

        let mut old_new_items = self
            .kv_store
            .transact_write_table(
                vec![TransactWriteTableOperation::Delete {
                    table_identity: table_metadata.identity.clone(),
                    table_info,
                    key,
                    item_stream_ttl_hours: aux_item_stream_ttl_hours,
                    use_key_attributes_for_missing_item_condition: true,
                    condition,
                    return_values_on_condition_check_failure: return_old_on_condition_failure
                        .then(|| "ALL_OLD".to_string()),
                    replication: None,
                    ttl_config,
                }],
                self.immediate_gsi_consistency,
            )
            .await
            .map_err(normalize_conditional_transaction_error)?;

        let Some((old_item, _)) = old_new_items.pop() else {
            record_write(0, 0);
            record_write_cost("delete_item", "delete", 1, billed_bytes);
            return Ok(None);
        };
        record_write(usize::from(old_item.is_some()), 0);
        record_write_cost("delete_item", "delete", 1, billed_bytes);

        Ok(old_item)
    }

    async fn retry_update_item(
        &self,
        execution: UpdateItemExecution,
    ) -> StorageResult<UpdateItemExecutionResult> {
        let mut last_retryable_error: Option<StorageError> = None;

        for _ in 0..10 {
            match self.try_update_item_once(&execution).await {
                Ok(result) => return Ok(result),
                Err(error) if is_terminal_update_error(error.as_ref()) => return Err(error),
                Err(error) => last_retryable_error = Some(error),
            }
        }

        Err(last_retryable_error.unwrap_or_else(|| {
            StorageEnum::TransactionConflict {
                message: "TransactionConflict".to_string(),
            }
            .into()
        }))
    }

    async fn try_update_item_once(
        &self,
        execution: &UpdateItemExecution,
    ) -> StorageResult<UpdateItemExecutionResult> {
        let mut old_new_items = self
            .kv_store
            .transact_write_table(
                vec![TransactWriteTableOperation::Update {
                    table_identity: execution.table_metadata.identity.clone(),
                    table_info: execution.table_info.clone(),
                    key: execution.key.clone(),
                    operations: Arc::clone(&execution.operations),
                    item_stream_ttl_hours: execution.request_item_stream_ttl_hours,
                    condition: execution.condition.clone(),
                    return_values_on_condition_check_failure: execution
                        .return_values_on_condition_check_failure
                        .clone(),
                    replication: None,
                    preserve_old_item: execution.preserve_old_item,
                    transaction_validation: false,
                    ttl_config: execution.ttl_config.clone(),
                }],
                self.immediate_gsi_consistency,
            )
            .await
            .map_err(normalize_conditional_transaction_error)?;

        let (old_item, new_item) = old_new_items.pop().unwrap_or((None, None));
        let items_updated = new_item.as_ref().map_or(0, |_| 1);
        let bytes_written = new_item
            .as_ref()
            .map(|item| compute_items_bytes(std::slice::from_ref(item)))
            .transpose()?
            .unwrap_or(0);

        Ok(UpdateItemExecutionResult {
            operations: Arc::clone(&execution.operations),
            old_item,
            new_item,
            items_updated,
            bytes_written,
        })
    }
}

struct UpdateItemExecution {
    table_metadata: Arc<StoredTableMetadata>,
    table_info: StoredTableInfo,
    key: KeyAttributes,
    operations: Arc<[storage_provider::UpdateOperation]>,
    condition: Option<storage_condition::Condition>,
    return_values_on_condition_check_failure: Option<String>,
    request_item_stream_ttl_hours: Option<StreamRetentionDuration>,
    preserve_old_item: bool,
    ttl_config: Option<TtlConfigRecord>,
}

struct UpdateItemExecutionResult {
    operations: Arc<[storage_provider::UpdateOperation]>,
    old_item: Option<HashMap<String, AttributeValue>>,
    new_item: Option<HashMap<String, AttributeValue>>,
    items_updated: usize,
    bytes_written: usize,
}

fn update_operations(
    update_expression: Option<&str>,
    expression_attribute_names: Option<&HashMap<String, String>>,
    expression_attribute_values: Option<&HashMap<String, AttributeValue>>,
) -> StorageResult<Arc<[storage_provider::UpdateOperation]>> {
    let operations = if let Some(update_expression) = update_expression {
        storage_provider::parse_update_expression(
            update_expression,
            expression_attribute_names,
            expression_attribute_values,
        )?
    } else {
        Vec::new()
    };

    Ok(Arc::from(operations))
}

fn update_condition(
    condition_expression: Option<&str>,
    expression_attribute_names: Option<&HashMap<String, String>>,
    expression_attribute_values: Option<&HashMap<String, AttributeValue>>,
) -> StorageResult<Option<storage_condition::Condition>> {
    condition_expression
        .map(|condition_expression| {
            parse_condition_expression(
                condition_expression,
                expression_attribute_names,
                expression_attribute_values,
            )
            .map_err(StorageError::validation)
        })
        .transpose()
}

fn delete_condition(
    condition_expression: Option<String>,
    expression_attribute_names: Option<&HashMap<String, String>>,
    expression_attribute_values: Option<&HashMap<String, AttributeValue>>,
) -> StorageResult<Option<storage_condition::Condition>> {
    condition_expression
        .map(|condition_expression| {
            parse_condition_expression(
                &condition_expression,
                expression_attribute_names,
                expression_attribute_values,
            )
            .map_err(|error| {
                warn!(error);
                StorageError::validation(StorageValidationKind::InvalidConditionExpression)
            })
        })
        .transpose()
}

fn is_terminal_update_error(error: &StorageEnum) -> bool {
    matches!(
        error,
        StorageEnum::ConditionalCheckFailed
            | StorageEnum::ConditionalCheckFailedWithItem { .. }
            | StorageEnum::TransactionCanceled { .. }
            | StorageEnum::InternalServerError { .. }
    )
}
