use crate::storage_ops::provider_impl::*;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct TimestampedIdempotencyResponse {
    response: TransactWriteItemsResponse,
    created_at: TimestampMillis,
    expires_at: TimestampMillis,
}

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub(super) async fn transact_write_items_impl(
        &self,
        request: TransactWriteItemsRequest,
    ) -> StorageResult<TransactWriteItemsResponse> {
        apply_gsi_write_pressure(self).await?;
        self.transact_write_items_after_pressure(request).await
    }

    pub(super) async fn transact_write_items_after_pressure(
        &self,
        request: TransactWriteItemsRequest,
    ) -> StorageResult<TransactWriteItemsResponse> {
        if let Some(response) = self
            .load_cached_transact_write_response(request.client_request_token.as_deref())
            .await?
        {
            return Ok(response);
        }

        let client_request_token = request.client_request_token.clone();
        let mut plan = TransactWritePlan::new(&request);
        let operations = self
            .transact_write_table_operations(request.transact_items, &mut plan)
            .await?;

        let response = TransactWriteItemsResponse {
            consumed_capacity: None,
            item_collection_metrics: None,
        };
        let idempotency_writes =
            self.idempotency_writes(client_request_token.as_deref(), &response)?;
        match self
            .kv_store
            .transact_write_table_with_direct_writes(
                operations,
                idempotency_writes,
                self.immediate_gsi_consistency,
            )
            .await
        {
            Ok(_) => {}
            Err(error)
                if matches!(error.to_enum(), StorageEnum::ConditionalCheckFailed)
                    && client_request_token.is_some() =>
            {
                if let Some(response) = self
                    .load_cached_transact_write_response(client_request_token.as_deref())
                    .await?
                {
                    return Ok(response);
                }
                return Err(error);
            }
            Err(error) => return Err(error),
        }
        record_write(plan.total_items_updated, plan.total_bytes_written);
        plan.billed_tally.emit("transact_write_items");
        Ok(response)
    }

    async fn transact_write_table_operations(
        &self,
        transact_items: Vec<TransactWriteItem>,
        plan: &mut TransactWritePlan,
    ) -> StorageResult<Vec<TransactWriteTableOperation>> {
        let mut operations = Vec::new();
        let mut caches = TransactWriteCaches::default();

        for item in transact_items {
            operations.push(
                self.transact_write_table_operation(item, plan, &mut caches)
                    .await?,
            );
        }

        Ok(operations)
    }

    async fn transact_write_table_operation(
        &self,
        item: TransactWriteItem,
        plan: &mut TransactWritePlan,
        caches: &mut TransactWriteCaches,
    ) -> StorageResult<TransactWriteTableOperation> {
        match item {
            TransactWriteItem {
                put: Some(mut request),
                ..
            } => {
                self.transact_put_operation(&mut request, plan, caches)
                    .await
            }
            TransactWriteItem {
                delete: Some(request),
                ..
            } => self.transact_delete_operation(request, plan, caches).await,
            TransactWriteItem {
                update: Some(request),
                ..
            } => self.transact_update_operation(request, plan, caches).await,
            TransactWriteItem {
                condition_check: Some(request),
                ..
            } => self.transact_check_operation(request, caches).await,
            _ => Err(StorageError::validation(
                "Invalid Transact Write Item request",
            )),
        }
    }

    async fn transact_put_operation(
        &self,
        request: &mut storage_types::TransactPutRequest,
        plan: &mut TransactWritePlan,
        caches: &mut TransactWriteCaches,
    ) -> StorageResult<TransactWriteTableOperation> {
        let table_metadata = self
            .cached_transact_table_identity(caches, &request.table_name)
            .await?;
        let table_info = table_metadata.table_info.clone();
        let ttl_config = self
            .cached_transact_ttl_config(caches, &request.table_name)
            .await?;

        plan.record_put(&request.item)?;
        normalize_attribute_map_numbers_for_write(&mut request.item);

        Ok(TransactWriteTableOperation::Put {
            table_identity: table_metadata.identity.clone(),
            table_info,
            item: std::mem::take(&mut request.item),
            indexers: request.indexers.take(),
            item_stream_ttl_hours: request.aux_item_stream_ttl_hours,
            condition: cached_transact_condition_binding(
                &mut caches.condition_binding_cache,
                request.condition_expression.take(),
                request.expression_attribute_names.take(),
                request.expression_attribute_values.take(),
            )?,
            return_values_on_condition_check_failure: request
                .return_values_on_condition_check_failure
                .take(),
            replication: None,
            ttl_config,
        })
    }

    async fn transact_delete_operation(
        &self,
        request: storage_types::TransactDeleteRequest,
        plan: &mut TransactWritePlan,
        caches: &mut TransactWriteCaches,
    ) -> StorageResult<TransactWriteTableOperation> {
        let table_metadata = self
            .cached_transact_table_identity(caches, &request.table_name)
            .await?;
        let table_info = table_metadata.table_info.clone();
        let ttl_config = self
            .cached_transact_ttl_config(caches, &request.table_name)
            .await?;

        plan.record_delete();
        Ok(TransactWriteTableOperation::Delete {
            table_identity: table_metadata.identity.clone(),
            table_info,
            key: request.key,
            item_stream_ttl_hours: request.aux_item_stream_ttl_hours,
            use_key_attributes_for_missing_item_condition: false,
            condition: cached_transact_condition_binding(
                &mut caches.condition_binding_cache,
                request.condition_expression,
                request.expression_attribute_names,
                request.expression_attribute_values,
            )?,
            return_values_on_condition_check_failure: request
                .return_values_on_condition_check_failure,
            replication: None,
            ttl_config,
        })
    }

    async fn transact_update_operation(
        &self,
        request: storage_types::TransactUpdateRequest,
        plan: &mut TransactWritePlan,
        caches: &mut TransactWriteCaches,
    ) -> StorageResult<TransactWriteTableOperation> {
        let storage_types::TransactUpdateRequest {
            table_name,
            key,
            update_expression,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            indexers,
            return_values_on_condition_check_failure,
            aux_item_stream_ttl_hours,
            ..
        } = request;
        let table_metadata = self
            .cached_transact_table_identity(caches, &table_name)
            .await?;
        let table_info = table_metadata.table_info.clone();
        let ttl_config = self.cached_transact_ttl_config(caches, &table_name).await?;
        let preserve_old_item =
            ttl_tracking_enabled(ttl_config.as_ref()) || aux_item_stream_ttl_hours.is_some();
        let (operations, condition) = cached_transact_update_binding(
            &mut caches.update_binding_cache,
            update_expression,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
        )?;

        plan.record_update();
        Ok(TransactWriteTableOperation::Update {
            table_identity: table_metadata.identity.clone(),
            table_info,
            key,
            operations,
            indexers,
            item_stream_ttl_hours: aux_item_stream_ttl_hours,
            condition,
            return_values_on_condition_check_failure,
            replication: None,
            preserve_old_item,
            transaction_validation: true,
            ttl_config,
        })
    }

    async fn transact_check_operation(
        &self,
        request: storage_types::TransactConditionCheckRequest,
        caches: &mut TransactWriteCaches,
    ) -> StorageResult<TransactWriteTableOperation> {
        let table_metadata = self
            .cached_transact_table_identity(caches, &request.table_name)
            .await?;
        let table_info = table_metadata.table_info.clone();
        let condition = cached_transact_condition_binding(
            &mut caches.condition_binding_cache,
            Some(request.condition_expression),
            request.expression_attribute_names,
            request.expression_attribute_values,
        )?
        .ok_or(StorageError::validation(
            StorageValidationKind::InvalidConditionExpression,
        ))?;

        Ok(TransactWriteTableOperation::Check {
            table_identity: table_metadata.identity.clone(),
            table_info,
            key: request.key,
            condition,
            return_values_on_condition_check_failure: request
                .return_values_on_condition_check_failure,
        })
    }

    async fn load_cached_transact_write_response(
        &self,
        token: Option<&str>,
    ) -> StorageResult<Option<TransactWriteItemsResponse>> {
        let Some(token) = token else {
            return Ok(None);
        };
        let token_key = compact::idempotency_token_key(token);
        let Some(cached_data) = self.kv_store.get(&token_key, true).await? else {
            return Ok(None);
        };
        let Ok(timestamped_response) = storage_types::storage_serde::from_bytes::<
            TimestampedIdempotencyResponse,
        >(&cached_data) else {
            return Ok(None);
        };

        let current_time = TimestampMillis::now();
        if current_time < timestamped_response.expires_at {
            return Ok(Some(timestamped_response.response));
        }

        let _ = self.kv_store.delete(&token_key).await;
        Ok(None)
    }

    fn idempotency_writes(
        &self,
        token: Option<&str>,
        response: &TransactWriteItemsResponse,
    ) -> StorageResult<Vec<DirectWriteOperation>> {
        let Some(token) = token else {
            return Ok(Vec::new());
        };
        let current_time = TimestampMillis::now();
        let timestamped_response = TimestampedIdempotencyResponse {
            response: response.clone(),
            created_at: current_time,
            expires_at: current_time + IDEMPOTENCY_TOKEN_TTL_MS,
        };
        let response_bytes = storage_types::storage_serde::to_bytes(&timestamped_response)?;
        let key = compact::idempotency_token_key(token);
        Ok(vec![
            DirectWriteOperation::CheckValue {
                key: key.clone(),
                expected_value: None,
            },
            DirectWriteOperation::Put {
                key,
                value: response_bytes,
            },
        ])
    }

    async fn cached_transact_table_identity(
        &self,
        caches: &mut TransactWriteCaches,
        table_name: &TableName,
    ) -> StorageResult<Arc<StoredTableMetadata>> {
        if let Some(metadata) = caches.table_identity_cache.get(table_name) {
            return Ok(Arc::clone(metadata));
        }

        let metadata = self
            .get_table_identity_from_name(table_name)
            .await?
            .ok_or(StorageError::table_not_found(table_name))?;
        caches
            .table_identity_cache
            .insert(table_name.clone(), Arc::clone(&metadata));
        Ok(metadata)
    }

    async fn cached_transact_ttl_config(
        &self,
        caches: &mut TransactWriteCaches,
        table_name: &TableName,
    ) -> StorageResult<Option<TtlConfigRecord>> {
        if let Some(config) = caches.ttl_config_cache.get(table_name) {
            return Ok(config.clone());
        }

        let config = self.load_ttl_config(table_name).await?;
        caches
            .ttl_config_cache
            .insert(table_name.clone(), config.clone());
        Ok(config)
    }
}

#[derive(Default)]
struct TransactWriteCaches {
    table_identity_cache: HashMap<TableName, Arc<StoredTableMetadata>>,
    ttl_config_cache: HashMap<TableName, Option<TtlConfigRecord>>,
    condition_binding_cache: Vec<TransactConditionBindingCacheEntry>,
    update_binding_cache: Vec<TransactUpdateBindingCacheEntry>,
}

struct TransactWritePlan {
    billed_tally: WriteCostTally,
    total_items_updated: usize,
    total_bytes_written: usize,
}

impl TransactWritePlan {
    fn new(request: &TransactWriteItemsRequest) -> Self {
        let mut billed_tally = WriteCostTally::default();
        for item in &request.transact_items {
            billed_tally.record_transact_item(item);
        }

        Self {
            billed_tally,
            total_items_updated: 0,
            total_bytes_written: 0,
        }
    }

    fn record_put(&mut self, item: &HashMap<String, AttributeValue>) -> StorageResult<()> {
        self.total_items_updated += 1;
        self.total_bytes_written += compute_items_bytes(std::slice::from_ref(item))?;
        Ok(())
    }

    fn record_delete(&mut self) {
        self.total_items_updated += 1;
    }

    fn record_update(&mut self) {
        self.total_items_updated += 1;
    }
}
