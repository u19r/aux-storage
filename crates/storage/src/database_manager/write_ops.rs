use std::collections::HashMap;

use storage_provider::{
    StorageProvider, apply_bound_update_operations, before_update_item_optional,
    return_values_need_old_item, update_item_response,
};
use storage_sync::SyncWriteRequest;
use storage_types::{
    AllOld, AttributeMap, AttributeValue, ExprNameRef, ExprValueRef, GuardedDeleteItemRequest,
    GuardedPutItemRequest, GuardedUpdateItemRequest, IndexedWireItem, IndexerDeclaration,
    KeyAttributes, KeyRef, PutItemEncodeRequest, PutItemRequest, PutItemResponse,
    ReturnValuesOldNewUpdated, StorageEnum, StorageError, StorageResult, TableName, TableNamespace,
    UpdateItemRequest, UpdateItemResponse, WireItem, WriteRetryPolicy, context::WrappedError as _,
    expr_names_to_map, expr_values_to_map, validate_expression_attribute_usage,
};

use crate::{
    AuthoritativePointReadPurpose, PointReadGetRequest,
    database_manager::{
        DatabaseManager, DeleteItemInput, PreparedCacheWrite, PutItemInput, PutItemPayload,
        ResolvedStorageOperation, UpdateItemInput, WriteTargetSet,
        guarded_write_coordinator as guarded_write, record_storage_operation,
        record_storage_operation_for_target, refresh_existing_updated_at_on_put_payload,
        stamp_updated_at_on_put_payload, update_item_return_values_rewritable_from_post_image,
        validate_update_expression_usage,
    },
    namespace_routing::{NamespaceRequestRewriter, NamespaceRoute, NamespaceStorageMode},
    updated_at_apply::inject_updated_at_into_update_expression,
};

struct PreparedPutItem {
    operation: ResolvedStorageOperation,
    item: PutItemPayload,
    logical_item: HashMap<String, AttributeValue>,
    indexers: Option<Vec<String>>,
    condition_expression: Option<String>,
    expression_attribute_names: Option<HashMap<String, String>>,
    expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    return_values: Option<AllOld>,
    return_old_on_condition_failure: bool,
    aux_item_stream_ttl_hours: Option<storage_types::StreamRetentionDuration>,
    cache_effects: storage_cache::RuntimeWriteEffects,
}

struct PreparedDeleteItem {
    operation: ResolvedStorageOperation,
    key: KeyAttributes,
    logical_key: KeyAttributes,
    condition_expression: Option<String>,
    expression_attribute_names: Option<HashMap<String, String>>,
    expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    return_old_on_condition_failure: bool,
    aux_item_stream_ttl_hours: Option<storage_types::StreamRetentionDuration>,
    cache_effects: storage_cache::RuntimeWriteEffects,
}

struct PreparedUpdateItem {
    operation: ResolvedStorageOperation,
    request: UpdateItemRequest,
    cache_enabled: bool,
    customer_return_values: Option<ReturnValuesOldNewUpdated>,
    response_operations: Option<Vec<String>>,
    prepared_cache_write: Option<storage_cache::RuntimePreparedUpdateCacheWrite>,
}

fn condition_failure_from_preimage(
    item: Option<HashMap<String, AttributeValue>>,
    return_old: bool,
) -> StorageError {
    if return_old && let Some(item) = item {
        return StorageEnum::ConditionalCheckFailedWithItem { item: item.into() }.into();
    }
    StorageEnum::ConditionalCheckFailed.into()
}

fn normalize_routed_condition_failure(
    error: StorageError,
    rewriter: &NamespaceRequestRewriter,
    namespace: &TableNamespace,
) -> StorageError {
    let StorageEnum::ConditionalCheckFailedWithItem { item } = error.to_enum() else {
        return error;
    };
    let mut item = item.to_hashmap();
    if let Err(error) = rewriter.normalize_item_from_shared_table(namespace, &mut item) {
        return error;
    }
    StorageEnum::ConditionalCheckFailedWithItem { item: item.into() }.into()
}

impl DatabaseManager {
    pub async fn put_item(&self, input: PutItemInput) -> StorageResult<PutItemResponse> {
        self.put_item_with_retry(input, WriteRetryPolicy::no_retry())
            .await
    }

    pub async fn put_item_with_retry(
        &self,
        input: PutItemInput,
        retry_policy: WriteRetryPolicy,
    ) -> StorageResult<PutItemResponse> {
        let operation = self
            .resolve_storage_operation(input.table_name.clone())
            .await?;
        self.put_item_with_resolved_operation_and_retry(operation, input, retry_policy)
            .await
    }

    pub async fn put_item_with_resolved_operation(
        &self,
        operation: ResolvedStorageOperation,
        input: PutItemInput,
    ) -> StorageResult<PutItemResponse> {
        self.put_item_with_resolved_operation_and_retry(
            operation,
            input,
            WriteRetryPolicy::no_retry(),
        )
        .await
    }

    async fn put_item_with_resolved_operation_and_retry(
        &self,
        operation: ResolvedStorageOperation,
        input: PutItemInput,
        retry_policy: WriteRetryPolicy,
    ) -> StorageResult<PutItemResponse> {
        operation.ensure_table(&input.table_name, "PutItem")?;
        let mut prepared = self.prepare_put_item(operation, input).await?;
        if self.single_node_sync_mode_enabled() {
            return self.put_item_single_node_sync(prepared).await;
        }

        let route = prepared.operation.route.take();
        match route {
            Some(route) => self.put_item_routed(prepared, route, retry_policy).await,
            None => self.put_item_unrouted(prepared, retry_policy).await,
        }
    }

    async fn prepare_put_item(
        &self,
        operation: ResolvedStorageOperation,
        input: PutItemInput,
    ) -> StorageResult<PreparedPutItem> {
        let PutItemInput {
            table_name,
            item,
            indexers,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            return_old_on_condition_failure,
            aux_item_stream_ttl_hours,
        } = input;
        let declaration_names = indexers.as_deref().unwrap_or_default();
        IndexerDeclaration::validate(declaration_names, operation.table_info.max_indexers)?;
        validate_expression_attribute_usage(
            expression_attribute_names.as_ref(),
            expression_attribute_values.as_ref(),
            condition_expression.as_deref(),
        )?;
        let mut item = item;
        if self.single_table_mode_enabled() {
            stamp_updated_at_on_put_payload(&mut item)?;
        } else {
            refresh_existing_updated_at_on_put_payload(&mut item)?;
        }
        let logical_item = item.clone().into_attribute_map()?;
        let declaration = IndexerDeclaration::try_new(
            declaration_names.to_vec(),
            operation.table_info.max_indexers,
        )?;
        IndexedWireItem::validate_logical_item(&logical_item, &declaration)?;
        storage_types::validate_item_key_attributes_for_schema(
            &operation.table_info.key_schema,
            &logical_item,
        )?;
        let cache_effects = self
            .plan_put_item_cache_effects(&table_name, &operation, &logical_item)
            .await?;
        Ok(PreparedPutItem {
            operation,
            item,
            logical_item,
            indexers,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            return_old_on_condition_failure,
            aux_item_stream_ttl_hours,
            cache_effects,
        })
    }

    #[cfg(feature = "cache-write-planner")]
    async fn plan_put_item_cache_effects(
        &self,
        table_name: &TableName,
        operation: &ResolvedStorageOperation,
        logical_item: &HashMap<String, AttributeValue>,
    ) -> StorageResult<storage_cache::RuntimeWriteEffects> {
        self.cache_write_planner()
            .plan_put_item_cache_effects(table_name, operation, logical_item)
            .await
    }

    #[cfg(not(feature = "cache-write-planner"))]
    async fn plan_put_item_cache_effects(
        &self,
        _table_name: &TableName,
        _operation: &ResolvedStorageOperation,
        _logical_item: &HashMap<String, AttributeValue>,
    ) -> StorageResult<storage_cache::RuntimeWriteEffects> {
        Ok(self.empty_cache_write_effects())
    }

    async fn put_item_single_node_sync(
        &self,
        prepared: PreparedPutItem,
    ) -> StorageResult<PutItemResponse> {
        let PreparedPutItem {
            operation,
            logical_item,
            indexers,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            return_old_on_condition_failure,
            aux_item_stream_ttl_hours,
            cache_effects,
            ..
        } = prepared;
        let table_name = operation.logical_table_name;
        let request = PutItemRequest {
            table_name,
            item: logical_item,
            indexers,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            expected: None,
            conditional_operator: None,
            return_values,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
            return_values_on_condition_check_failure: return_old_on_condition_failure
                .then(|| "ALL_OLD".to_string()),
            aux_item_stream_ttl_hours,
        };
        self.execute_with_cache_effects(
            PreparedCacheWrite::Effects(cache_effects.clone()),
            || async {
                let response = self
                    .run_single_node_sync_write_request(
                        "put_item",
                        SyncWriteRequest::PutItem(request),
                    )
                    .await?;
                self.maybe_pause_after_storage_write_for_test().await;
                self.maybe_run_gsi_maintenance().await;
                sync_response_at(&response, 0, PutItemResponse { attributes: None })
            },
            |response| async { Ok((response, cache_effects)) },
        )
        .await
    }

    async fn put_item_unrouted(
        &self,
        prepared: PreparedPutItem,
        retry_policy: WriteRetryPolicy,
    ) -> StorageResult<PutItemResponse> {
        if retry_policy.max_attempts() == 1
            && prepared.aux_item_stream_ttl_hours.is_none()
            && let Some(response) = self.try_cached_guarded_put_item(&prepared).await?
        {
            return Ok(response);
        }
        let PreparedPutItem {
            operation,
            item,
            indexers,
            logical_item: _,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            return_old_on_condition_failure,
            aux_item_stream_ttl_hours,
            cache_effects,
        } = prepared;
        let table_name = operation.logical_table_name;
        self.execute_with_cache_effects(
            PreparedCacheWrite::Effects(cache_effects.clone()),
            || async {
                let response = self
                    .execute_put_item_payload(
                        PutItemInput {
                            table_name,
                            item,
                            indexers,
                            condition_expression,
                            expression_attribute_names,
                            expression_attribute_values,
                            return_values,
                            return_old_on_condition_failure,
                            aux_item_stream_ttl_hours,
                        },
                        retry_policy,
                    )
                    .await?;
                self.maybe_pause_after_storage_write_for_test().await;
                self.maybe_run_gsi_maintenance().await;
                Ok(response)
            },
            |response| async { Ok((response, cache_effects)) },
        )
        .await
    }

    async fn execute_put_item_payload(
        &self,
        input: PutItemInput,
        retry_policy: WriteRetryPolicy,
    ) -> StorageResult<PutItemResponse> {
        let PutItemInput {
            table_name,
            item,
            indexers,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            return_old_on_condition_failure,
            aux_item_stream_ttl_hours,
        } = input;
        match item {
            PutItemPayload::AttributeMap(item) => {
                self.run_default_admitted(
                    crate::admission::AdmissionClass::Write,
                    |provider| async move {
                        record_storage_operation(
                            "put_item",
                            provider.put_item_request_with_retry(
                                PutItemRequest {
                                    table_name,
                                    item,
                                    indexers,
                                    condition_expression,
                                    expression_attribute_names,
                                    expression_attribute_values,
                                    expected: None,
                                    conditional_operator: None,
                                    return_values,
                                    return_consumed_capacity: None,
                                    return_item_collection_metrics: None,
                                    return_values_on_condition_check_failure:
                                        return_old_on_condition_failure
                                            .then(|| "ALL_OLD".to_string()),
                                    aux_item_stream_ttl_hours,
                                },
                                retry_policy,
                            ),
                        )
                        .await
                    },
                )
                .await
            }
            PutItemPayload::WireItem(item) => {
                self.run_default_admitted(
                    crate::admission::AdmissionClass::Write,
                    |provider| async move {
                        record_storage_operation(
                            "put_item",
                            provider.put_item_encode_with_retry(
                                PutItemEncodeRequest {
                                    table_name,
                                    item: *item,
                                    indexers,
                                    condition_expression,
                                    expression_attribute_names,
                                    expression_attribute_values,
                                    return_values,
                                    return_old_on_condition_failure,
                                    aux_item_stream_ttl_hours,
                                },
                                retry_policy,
                            ),
                        )
                        .await
                    },
                )
                .await
            }
        }
    }

    async fn put_item_routed(
        &self,
        prepared: PreparedPutItem,
        route: NamespaceRoute,
        retry_policy: WriteRetryPolicy,
    ) -> StorageResult<PutItemResponse> {
        let PreparedPutItem {
            item,
            indexers,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            return_old_on_condition_failure,
            aux_item_stream_ttl_hours,
            cache_effects,
            ..
        } = prepared;

        let mut item_map = item.into_attribute_map()?;
        if route.storage_mode == NamespaceStorageMode::SharedTable {
            self.request_rewriter
                .rewrite_item_for_shared_table(&route.namespace, &mut item_map)?;
        }

        let target_count = route.write_targets.len();
        let mut routed_item_map = WriteTargetSet::new(target_count, item_map, "put_item.item")?;
        let mut routed_indexers = WriteTargetSet::new(target_count, indexers, "put_item.indexers")?;
        let mut routed_condition_expression = WriteTargetSet::new(
            target_count,
            condition_expression,
            "put_item.condition_expression",
        )?;
        let mut routed_expression_attribute_names = WriteTargetSet::new(
            target_count,
            expression_attribute_names,
            "put_item.expression_attribute_names",
        )?;
        let mut routed_expression_attribute_values = WriteTargetSet::new(
            target_count,
            expression_attribute_values,
            "put_item.expression_attribute_values",
        )?;
        let mut routed_return_values =
            WriteTargetSet::new(target_count, return_values, "put_item.return_values")?;
        let mut routed_aux_item_stream_ttl_hours = WriteTargetSet::new(
            target_count,
            aux_item_stream_ttl_hours,
            "put_item.aux_item_stream_ttl_hours",
        )?;
        self.execute_with_cache_effects(
            PreparedCacheWrite::Effects(cache_effects.clone()),
            || async {
                let mut response = self
                    .execute_routed_write_targets(
                        &route,
                        crate::database_manager::core::AdmissionLane::Foreground(
                            crate::admission::AdmissionClass::Write,
                        ),
                        "put_item routing produced no write targets",
                        |provider, target, target_index, target_role| {
                            let table_name = target.table_name.clone();
                            let item = routed_item_map.take(target_index);
                            let indexers = routed_indexers.take(target_index);
                            let condition_expression =
                                routed_condition_expression.take(target_index);
                            let expression_attribute_names =
                                routed_expression_attribute_names.take(target_index);
                            let expression_attribute_values =
                                routed_expression_attribute_values.take(target_index);
                            let return_values = routed_return_values.take(target_index);
                            let aux_item_stream_ttl_hours =
                                routed_aux_item_stream_ttl_hours.take(target_index);
                            async move {
                                record_storage_operation_for_target(
                                    "put_item",
                                    target_role,
                                    provider.put_item_request_with_retry(
                                        PutItemRequest {
                                            table_name,
                                            item: item?,
                                            indexers: indexers?,
                                            condition_expression: condition_expression?,
                                            expression_attribute_names: expression_attribute_names?,
                                            expression_attribute_values:
                                                expression_attribute_values?,
                                            expected: None,
                                            conditional_operator: None,
                                            return_values: return_values?,
                                            return_consumed_capacity: None,
                                            return_item_collection_metrics: None,
                                            return_values_on_condition_check_failure:
                                                return_old_on_condition_failure
                                                    .then(|| "ALL_OLD".to_string()),
                                            aux_item_stream_ttl_hours: aux_item_stream_ttl_hours?,
                                        },
                                        retry_policy,
                                    ),
                                )
                                .await
                            }
                        },
                    )
                    .await
                    .map_err(|error| {
                        if route.storage_mode == NamespaceStorageMode::SharedTable {
                            normalize_routed_condition_failure(
                                error,
                                &self.request_rewriter,
                                &route.namespace,
                            )
                        } else {
                            error
                        }
                    })?;
                if route.storage_mode == NamespaceStorageMode::SharedTable {
                    normalize_routed_response_attributes(
                        &self.request_rewriter,
                        &route.namespace,
                        &mut response.attributes,
                    )?;
                }
                Ok(response)
            },
            |response| async { Ok((response, cache_effects)) },
        )
        .await
    }

    pub async fn delete_item(
        &self,
        input: DeleteItemInput,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        let operation = self
            .resolve_storage_operation(input.table_name.clone())
            .await?;
        self.delete_item_with_resolved_operation(operation, input)
            .await
    }

    pub async fn delete_item_with_resolved_operation(
        &self,
        operation: ResolvedStorageOperation,
        input: DeleteItemInput,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        operation.ensure_table(&input.table_name, "DeleteItem")?;
        let mut prepared = self.prepare_delete_item(operation, input).await?;
        if self.single_node_sync_mode_enabled() {
            return self.delete_item_single_node_sync(prepared).await;
        }

        let route = prepared.operation.route.take();
        match route {
            Some(route) => self.delete_item_routed(prepared, route).await,
            None => self.delete_item_unrouted(prepared).await,
        }
    }

    async fn prepare_delete_item(
        &self,
        operation: ResolvedStorageOperation,
        input: DeleteItemInput,
    ) -> StorageResult<PreparedDeleteItem> {
        validate_expression_attribute_usage(
            input.expression_attribute_names.as_ref(),
            input.expression_attribute_values.as_ref(),
            input.condition_expression.as_deref(),
        )?;
        let table_name = input.table_name;
        let key = input.key;
        storage_types::validate_key_attributes_for_schema(&operation.table_info.key_schema, &key)?;
        let logical_key = key.clone();
        let cache_effects = self
            .plan_delete_item_cache_effects(&table_name, &operation, &logical_key)
            .await?;
        Ok(PreparedDeleteItem {
            operation,
            key,
            logical_key,
            condition_expression: input.condition_expression,
            expression_attribute_names: input.expression_attribute_names,
            expression_attribute_values: input.expression_attribute_values,
            return_old_on_condition_failure: input.return_old_on_condition_failure,
            aux_item_stream_ttl_hours: input.aux_item_stream_ttl_hours,
            cache_effects,
        })
    }

    #[cfg(feature = "cache-write-planner")]
    async fn plan_delete_item_cache_effects(
        &self,
        table_name: &TableName,
        operation: &ResolvedStorageOperation,
        logical_key: &KeyAttributes,
    ) -> StorageResult<storage_cache::RuntimeWriteEffects> {
        self.cache_write_planner()
            .plan_delete_item_cache_effects(table_name, operation, logical_key)
            .await
    }

    #[cfg(not(feature = "cache-write-planner"))]
    async fn plan_delete_item_cache_effects(
        &self,
        _table_name: &TableName,
        _operation: &ResolvedStorageOperation,
        _logical_key: &KeyAttributes,
    ) -> StorageResult<storage_cache::RuntimeWriteEffects> {
        Ok(self.empty_cache_write_effects())
    }

    async fn delete_item_single_node_sync(
        &self,
        prepared: PreparedDeleteItem,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        let PreparedDeleteItem {
            operation,
            logical_key,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_old_on_condition_failure,
            aux_item_stream_ttl_hours,
            cache_effects,
            ..
        } = prepared;
        let table_name = operation.logical_table_name;
        let request = storage_types::DeleteItemRequest {
            table_name,
            key: logical_key,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            expected: None,
            conditional_operator: None,
            return_values: None,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
            return_values_on_condition_check_failure: return_old_on_condition_failure
                .then(|| "ALL_OLD".to_string()),
            aux_item_stream_ttl_hours,
        };
        self.execute_with_cache_effects(
            PreparedCacheWrite::Effects(cache_effects.clone()),
            || async {
                self.run_single_node_sync_write_request(
                    "delete_item",
                    SyncWriteRequest::DeleteItem(request),
                )
                .await?;
                self.maybe_pause_after_storage_write_for_test().await;
                self.maybe_run_gsi_maintenance().await;
                Ok(None)
            },
            |response| async { Ok((response, cache_effects)) },
        )
        .await
    }

    async fn delete_item_unrouted(
        &self,
        prepared: PreparedDeleteItem,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        let PreparedDeleteItem {
            operation,
            key,
            logical_key,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_old_on_condition_failure,
            aux_item_stream_ttl_hours,
            cache_effects,
        } = prepared;
        let table_name = operation.logical_table_name;
        if aux_item_stream_ttl_hours.is_none()
            && let Some(response) = self
                .try_cached_guarded_delete_item(
                    DeleteItemInput {
                        table_name: table_name.clone(),
                        key: logical_key.clone(),
                        condition_expression: condition_expression.clone(),
                        expression_attribute_names: expression_attribute_names.clone(),
                        expression_attribute_values: expression_attribute_values.clone(),
                        return_old_on_condition_failure,
                        aux_item_stream_ttl_hours: None,
                    },
                    cache_effects.clone(),
                )
                .await?
        {
            return Ok(response);
        }
        let request = storage_types::DeleteItemRequest {
            table_name,
            key,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            expected: None,
            conditional_operator: None,
            return_values: None,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
            return_values_on_condition_check_failure: return_old_on_condition_failure
                .then(|| "ALL_OLD".to_string()),
            aux_item_stream_ttl_hours,
        };
        self.execute_with_cache_effects(
            PreparedCacheWrite::Effects(cache_effects.clone()),
            || async {
                let response = self
                    .run_default_admitted(
                        crate::admission::AdmissionClass::Write,
                        |provider| async move {
                            record_storage_operation(
                                "delete_item",
                                provider.delete_item_request(request),
                            )
                            .await
                        },
                    )
                    .await?;
                self.maybe_pause_after_storage_write_for_test().await;
                self.maybe_run_gsi_maintenance().await;
                Ok(response)
            },
            |response| async { Ok((response, cache_effects)) },
        )
        .await
    }

    async fn delete_item_routed(
        &self,
        prepared: PreparedDeleteItem,
        route: NamespaceRoute,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        let PreparedDeleteItem {
            mut key,
            condition_expression,
            expression_attribute_names,
            mut expression_attribute_values,
            return_old_on_condition_failure,
            aux_item_stream_ttl_hours,
            cache_effects,
            ..
        } = prepared;

        if route.storage_mode == NamespaceStorageMode::SharedTable {
            self.request_rewriter
                .rewrite_key_for_shared_table(&route.namespace, &mut key)?;
            self.request_rewriter.rewrite_condition_for_shared_table(
                &route.namespace,
                condition_expression.as_deref(),
                expression_attribute_names.as_ref(),
                expression_attribute_values.as_mut(),
            )?;
        }

        let target_count = route.write_targets.len();
        let mut routed_key = WriteTargetSet::new(target_count, key, "delete_item.key")?;
        let mut routed_condition_expression = WriteTargetSet::new(
            target_count,
            condition_expression,
            "delete_item.condition_expression",
        )?;
        let mut routed_expression_attribute_names = WriteTargetSet::new(
            target_count,
            expression_attribute_names,
            "delete_item.expression_attribute_names",
        )?;
        let mut routed_expression_attribute_values = WriteTargetSet::new(
            target_count,
            expression_attribute_values,
            "delete_item.expression_attribute_values",
        )?;
        self.execute_with_cache_effects(
            PreparedCacheWrite::Effects(cache_effects.clone()),
            || async {
                let mut response = self
                    .execute_routed_write_targets(
                        &route,
                        crate::database_manager::core::AdmissionLane::Foreground(
                            crate::admission::AdmissionClass::Write,
                        ),
                        "delete_item routing produced no write targets",
                        |provider, target, target_index, target_role| {
                            let table_name = target.table_name.clone();
                            let key = routed_key.take(target_index);
                            let condition_expression =
                                routed_condition_expression.take(target_index);
                            let expression_attribute_names =
                                routed_expression_attribute_names.take(target_index);
                            let expression_attribute_values =
                                routed_expression_attribute_values.take(target_index);
                            async move {
                                record_storage_operation_for_target(
                                    "delete_item",
                                    target_role,
                                    provider.delete_item_request(
                                        storage_types::DeleteItemRequest {
                                            table_name,
                                            key: key?,
                                            condition_expression: condition_expression?,
                                            expression_attribute_names: expression_attribute_names?,
                                            expression_attribute_values:
                                                expression_attribute_values?,
                                            expected: None,
                                            conditional_operator: None,
                                            return_values: None,
                                            return_consumed_capacity: None,
                                            return_item_collection_metrics: None,
                                            return_values_on_condition_check_failure:
                                                return_old_on_condition_failure
                                                    .then(|| "ALL_OLD".to_string()),
                                            aux_item_stream_ttl_hours,
                                        },
                                    ),
                                )
                                .await
                            }
                        },
                    )
                    .await
                    .map_err(|error| {
                        if route.storage_mode == NamespaceStorageMode::SharedTable {
                            normalize_routed_condition_failure(
                                error,
                                &self.request_rewriter,
                                &route.namespace,
                            )
                        } else {
                            error
                        }
                    })?;
                if route.storage_mode == NamespaceStorageMode::SharedTable
                    && let Some(attributes) = response.as_mut()
                {
                    self.request_rewriter
                        .normalize_item_from_shared_table(&route.namespace, attributes)?;
                }
                Ok(response)
            },
            |response| async { Ok((response, cache_effects)) },
        )
        .await
    }

    pub async fn update_item(&self, input: UpdateItemInput) -> StorageResult<UpdateItemResponse> {
        let operation = self
            .resolve_storage_operation(input.table_name.clone())
            .await?;
        self.update_item_with_resolved_operation(operation, input)
            .await
    }

    pub async fn update_item_with_resolved_operation(
        &self,
        operation: ResolvedStorageOperation,
        input: UpdateItemInput,
    ) -> StorageResult<UpdateItemResponse> {
        operation.ensure_table(&input.table_name, "UpdateItem")?;
        let mut prepared = self.prepare_update_item(operation, input).await?;
        if self.single_node_sync_mode_enabled() {
            return self.update_item_single_node_sync(prepared).await;
        }

        let route = prepared.operation.route.take();
        match route {
            Some(route) => self.update_item_routed(prepared, route).await,
            None => self.update_item_unrouted(prepared).await,
        }
    }

    async fn prepare_update_item(
        &self,
        operation: ResolvedStorageOperation,
        input: UpdateItemInput,
    ) -> StorageResult<PreparedUpdateItem> {
        let UpdateItemInput {
            table_name,
            key,
            update_expression,
            indexers,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            return_old_on_condition_failure,
            aux_item_stream_ttl_hours,
        } = input;
        if let Some(indexers) = indexers.as_deref() {
            IndexerDeclaration::validate(indexers, operation.table_info.max_indexers)?;
        }
        let mut update_expression = update_expression;
        let mut expression_attribute_names = expression_attribute_names;
        let mut expression_attribute_values = expression_attribute_values;
        if self.single_table_mode_enabled() && !update_expression.trim().is_empty() {
            inject_updated_at_into_update_expression(
                &mut update_expression,
                &mut expression_attribute_names,
                &mut expression_attribute_values,
            )?;
        }
        let update_expression_for_operations =
            (!update_expression.trim().is_empty()).then_some(update_expression.as_str());
        validate_update_expression_usage(
            update_expression_for_operations,
            condition_expression.as_deref(),
            expression_attribute_names.as_ref(),
            expression_attribute_values.as_ref(),
        )?;
        storage_types::validate_key_attributes_for_schema(&operation.table_info.key_schema, &key)?;

        let cache_enabled = self.cache_services.point_read_enabled();
        let customer_return_values = return_values;
        let rewrite_return_values_for_cache = cache_enabled
            && update_item_return_values_rewritable_from_post_image(
                customer_return_values.as_ref(),
            );
        let response_operations = if rewrite_return_values_for_cache {
            let (operations, _) = before_update_item_optional(
                update_expression_for_operations,
                condition_expression.as_deref(),
                expression_attribute_names.as_ref(),
                expression_attribute_values.as_ref(),
            )?;
            Some(
                operations
                    .into_iter()
                    .map(|operation| operation.field_name().to_string())
                    .collect::<Vec<_>>(),
            )
        } else {
            None
        };
        let provider_return_values = if self.single_node_sync_mode_enabled() {
            customer_return_values.clone()
        } else if rewrite_return_values_for_cache {
            Some(ReturnValuesOldNewUpdated::AllNew)
        } else {
            customer_return_values.clone()
        };

        let request = if update_expression.trim().is_empty() {
            UpdateItemRequest::builder()
                .table_name(table_name)
                .key(key)
                .indexers(indexers)
                .condition_expression(condition_expression)
                .expression_attribute_names(expression_attribute_names)
                .expression_attribute_values(expression_attribute_values)
                .return_values(provider_return_values)
                .return_values_on_condition_check_failure(
                    return_old_on_condition_failure.then(|| "ALL_OLD".to_string()),
                )
                .aux_item_stream_ttl_hours(aux_item_stream_ttl_hours)
                .build()
        } else {
            UpdateItemRequest::builder()
                .table_name(table_name)
                .key(key)
                .update_expression(update_expression)
                .indexers(indexers)
                .condition_expression(condition_expression)
                .expression_attribute_names(expression_attribute_names)
                .expression_attribute_values(expression_attribute_values)
                .return_values(provider_return_values)
                .return_values_on_condition_check_failure(
                    return_old_on_condition_failure.then(|| "ALL_OLD".to_string()),
                )
                .aux_item_stream_ttl_hours(aux_item_stream_ttl_hours)
                .build()
        };
        let prepared_cache_write = self
            .prepare_update_item_cache_write(&request.table_name, &operation, &request.key)
            .await?;
        Ok(PreparedUpdateItem {
            operation,
            request,
            cache_enabled,
            customer_return_values,
            response_operations,
            prepared_cache_write,
        })
    }

    #[cfg(feature = "cache-write-planner")]
    async fn prepare_update_item_cache_write(
        &self,
        table_name: &TableName,
        operation: &ResolvedStorageOperation,
        key: &KeyAttributes,
    ) -> StorageResult<Option<storage_cache::RuntimePreparedUpdateCacheWrite>> {
        self.cache_write_planner()
            .prepare_update_item_cache_write(table_name, operation, key)
            .await
            .map(Some)
    }

    #[cfg(not(feature = "cache-write-planner"))]
    async fn prepare_update_item_cache_write(
        &self,
        _table_name: &TableName,
        _operation: &ResolvedStorageOperation,
        _key: &KeyAttributes,
    ) -> StorageResult<Option<storage_cache::RuntimePreparedUpdateCacheWrite>> {
        Ok(None)
    }

    async fn update_item_single_node_sync(
        &self,
        prepared: PreparedUpdateItem,
    ) -> StorageResult<UpdateItemResponse> {
        let PreparedUpdateItem {
            operation: _,
            request,
            prepared_cache_write,
            ..
        } = prepared;
        let prepared_cache = prepared_update_cache(prepared_cache_write);
        self.execute_with_cache_effects(
            prepared_cache,
            || async {
                let response = self
                    .run_single_node_sync_write_request(
                        "update_item",
                        SyncWriteRequest::UpdateItem(request),
                    )
                    .await?;
                self.maybe_pause_after_storage_write_for_test().await;
                self.maybe_run_gsi_maintenance().await;
                sync_response_at(&response, 0, UpdateItemResponse { attributes: None })
            },
            |response| async { Ok((response, self.empty_cache_write_effects())) },
        )
        .await
    }

    async fn update_item_unrouted(
        &self,
        prepared: PreparedUpdateItem,
    ) -> StorageResult<UpdateItemResponse> {
        if let Some(response) = self
            .try_cached_guarded_update_item_for_prepared(&prepared)
            .await?
        {
            return Ok(response);
        }
        let PreparedUpdateItem {
            operation: _,
            request,
            cache_enabled,
            customer_return_values,
            response_operations,
            prepared_cache_write,
        } = prepared;
        let prepared_cache = prepared_update_cache(prepared_cache_write.clone());
        self.execute_with_cache_effects(
            prepared_cache,
            || async {
                let response = self
                    .run_default_admitted(
                        crate::admission::AdmissionClass::Write,
                        |provider| async move {
                            record_storage_operation(
                                "update_item",
                                provider.update_item(request.clone()),
                            )
                            .await
                        },
                    )
                    .await?;
                self.maybe_pause_after_storage_write_for_test().await;
                self.maybe_run_gsi_maintenance().await;
                Ok(response)
            },
            |response| async move {
                self.finalize_update_item_cache_response(
                    response,
                    cache_enabled,
                    prepared_cache_write,
                    response_operations,
                    customer_return_values,
                )
                .await
            },
        )
        .await
    }

    async fn try_cached_guarded_update_item_for_prepared(
        &self,
        prepared: &PreparedUpdateItem,
    ) -> StorageResult<Option<UpdateItemResponse>> {
        let Some(prepared_cache_write) = prepared.prepared_cache_write.as_ref() else {
            return Ok(None);
        };
        if prepared.request.aux_item_stream_ttl_hours.is_some() {
            return Ok(None);
        }
        self.try_cached_guarded_update_item(
            &prepared.request,
            prepared_cache_write.clone(),
            prepared.response_operations.clone(),
            prepared.customer_return_values.clone(),
        )
        .await
    }

    async fn update_item_routed(
        &self,
        prepared: PreparedUpdateItem,
        route: NamespaceRoute,
    ) -> StorageResult<UpdateItemResponse> {
        let PreparedUpdateItem {
            operation: _,
            mut request,
            cache_enabled,
            customer_return_values,
            response_operations,
            prepared_cache_write,
        } = prepared;
        if route.storage_mode == NamespaceStorageMode::SharedTable {
            self.request_rewriter
                .rewrite_update_for_shared_table(&route.namespace, &mut request)?;
        }

        let prepared_cache = prepared_update_cache(prepared_cache_write.clone());
        self.execute_with_cache_effects(
            prepared_cache,
            || async {
                let mut routed_requests =
                    WriteTargetSet::new(route.write_targets.len(), request, "update_item.request")?;
                let mut response = self
                    .execute_routed_write_targets(
                        &route,
                        crate::database_manager::core::AdmissionLane::Foreground(
                            crate::admission::AdmissionClass::Write,
                        ),
                        "update_item routing produced no write targets",
                        |provider, target, target_index, target_role| {
                            let request_for_target = routed_requests.take(target_index);
                            let table_name = target.table_name.clone();
                            async move {
                                let mut request_for_target = request_for_target?;
                                request_for_target.table_name = table_name;
                                record_storage_operation_for_target(
                                    "update_item",
                                    target_role,
                                    provider.update_item(request_for_target),
                                )
                                .await
                            }
                        },
                    )
                    .await
                    .map_err(|error| {
                        if route.storage_mode == NamespaceStorageMode::SharedTable {
                            normalize_routed_condition_failure(
                                error,
                                &self.request_rewriter,
                                &route.namespace,
                            )
                        } else {
                            error
                        }
                    })?;
                if route.storage_mode == NamespaceStorageMode::SharedTable {
                    normalize_routed_response_attributes(
                        &self.request_rewriter,
                        &route.namespace,
                        &mut response.attributes,
                    )?;
                }
                Ok(response)
            },
            |response| async move {
                self.finalize_update_item_cache_response(
                    response,
                    cache_enabled,
                    prepared_cache_write,
                    response_operations,
                    customer_return_values,
                )
                .await
            },
        )
        .await
    }

    async fn finalize_update_item_cache_response(
        &self,
        mut response: UpdateItemResponse,
        cache_enabled: bool,
        prepared_update_cache_write: Option<storage_cache::RuntimePreparedUpdateCacheWrite>,
        response_operations: Option<Vec<String>>,
        customer_return_values: Option<ReturnValuesOldNewUpdated>,
    ) -> StorageResult<(UpdateItemResponse, storage_cache::RuntimeWriteEffects)> {
        let post_image = if cache_enabled {
            if let Some(prepared_update_cache_write) = prepared_update_cache_write.as_ref() {
                match self
                    .get_item_with_consistent_read(
                        prepared_update_cache_write.table_name.clone(),
                        prepared_update_cache_write.key.clone(),
                        true,
                    )
                    .await
                {
                    Ok(item) => item,
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            table_name = %prepared_update_cache_write.table_name,
                            "update_item post-image read failed, invalidating point-read cache entry"
                        );
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        };

        if let Some(operations) = response_operations.as_ref() {
            let new_item = match post_image.as_ref() {
                Some(item) => Some(item.clone().into_attribute_map()?),
                None => response.attributes.clone().map(Into::into),
            };
            response =
                update_item_response(operations, None, new_item, customer_return_values.as_ref())?;
        }

        let cache_effects =
            self.finalize_update_item_cache_effects(prepared_update_cache_write, post_image)?;
        Ok((response, cache_effects))
    }

    #[cfg(feature = "cache-write-planner")]
    fn finalize_update_item_cache_effects(
        &self,
        prepared_update_cache_write: Option<storage_cache::RuntimePreparedUpdateCacheWrite>,
        post_image: Option<WireItem>,
    ) -> StorageResult<storage_cache::RuntimeWriteEffects> {
        if let Some(prepared_update_cache_write) = prepared_update_cache_write {
            self.cache_write_planner()
                .finalize_update_item_cache_effects(prepared_update_cache_write, post_image)
        } else {
            Ok(self.empty_cache_write_effects())
        }
    }

    #[cfg(not(feature = "cache-write-planner"))]
    fn finalize_update_item_cache_effects(
        &self,
        _prepared_update_cache_write: Option<storage_cache::RuntimePreparedUpdateCacheWrite>,
        _post_image: Option<WireItem>,
    ) -> StorageResult<storage_cache::RuntimeWriteEffects> {
        Ok(self.empty_cache_write_effects())
    }

    async fn try_cached_guarded_put_item(
        &self,
        prepared: &PreparedPutItem,
    ) -> StorageResult<Option<PutItemResponse>> {
        let PreparedPutItem {
            operation,
            logical_item,
            indexers,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            return_old_on_condition_failure,
            cache_effects,
            ..
        } = prepared;
        let table_name = &operation.logical_table_name;
        if !self.default_supports_guarded_writes() {
            guarded_write::record_unsupported_fallback("put_item");
            return Ok(None);
        }
        let table_info = self.get_table_info_arc(table_name).await?;
        let key = StorageProvider::get_key_attributes(
            self.storage.as_ref(),
            logical_item,
            &table_info.key_schema,
        )?;
        let request = PointReadGetRequest {
            table_name: table_name.clone(),
            key,
        };
        let Some(preimage) = guarded_write::authoritative_preimage(
            self,
            &request,
            AuthoritativePointReadPurpose::ConditionalPutPreImage,
        )
        .await?
        else {
            return Ok(None);
        };
        if !guarded_write::condition_matches(
            condition_expression.as_ref(),
            expression_attribute_names.as_ref(),
            expression_attribute_values.as_ref(),
            &preimage.item,
        )? {
            self.cache_services
                .prepare_write_intents(cache_effects)
                .await?;
            self.cache_services
                .release_write_intents(cache_effects)
                .await?;
            return Err(condition_failure_from_preimage(
                preimage.item,
                *return_old_on_condition_failure,
            ));
        }

        self.cache_services
            .prepare_write_intents(cache_effects)
            .await?;
        let guarded = GuardedPutItemRequest {
            table_name: table_name.clone(),
            item: logical_item.clone(),
            indexers: indexers.clone().unwrap_or_default(),
            guard: preimage.guard,
            condition_expression: condition_expression.clone(),
            expression_attribute_names: expression_attribute_names.clone(),
            expression_attribute_values: expression_attribute_values.clone(),
            return_values: return_values.clone(),
        };
        let response = match self
            .run_default_admitted(
                crate::admission::AdmissionClass::Write,
                |provider| async move {
                    record_storage_operation("guarded_put_item", provider.guarded_put_item(guarded))
                        .await
                },
            )
            .await
        {
            Ok(response) => response,
            Err(error) if guarded_write::should_fallback(&error) => {
                guarded_write::record_fallback("put_item", &error);
                self.cache_services
                    .release_write_intents(cache_effects)
                    .await?;
                return Ok(None);
            }
            Err(error) => {
                self.cache_services
                    .release_write_intents(cache_effects)
                    .await?;
                return Err(error);
            }
        };
        self.maybe_pause_after_storage_write_for_test().await;
        self.maybe_run_gsi_maintenance().await;
        self.cache_services
            .apply_write_effects(cache_effects)
            .await?;
        Ok(Some(response))
    }

    async fn try_cached_guarded_delete_item(
        &self,
        input: DeleteItemInput,
        cache_effects: storage_cache::RuntimeWriteEffects,
    ) -> StorageResult<Option<Option<HashMap<String, AttributeValue>>>> {
        let DeleteItemInput {
            table_name,
            key: logical_key,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_old_on_condition_failure,
            ..
        } = input;
        if !self.default_supports_guarded_writes() {
            guarded_write::record_unsupported_fallback("delete_item");
            return Ok(None);
        }
        let request = PointReadGetRequest {
            table_name: table_name.clone(),
            key: logical_key.clone(),
        };
        let Some(preimage) = guarded_write::authoritative_preimage(
            self,
            &request,
            AuthoritativePointReadPurpose::ConditionalDeletePreImage,
        )
        .await?
        else {
            return Ok(None);
        };
        if !guarded_write::condition_matches(
            condition_expression.as_ref(),
            expression_attribute_names.as_ref(),
            expression_attribute_values.as_ref(),
            &preimage.item,
        )? {
            self.cache_services
                .prepare_write_intents(&cache_effects)
                .await?;
            self.cache_services
                .release_write_intents(&cache_effects)
                .await?;
            return Err(condition_failure_from_preimage(
                preimage.item,
                return_old_on_condition_failure,
            ));
        }

        self.cache_services
            .prepare_write_intents(&cache_effects)
            .await?;
        let guarded = GuardedDeleteItemRequest {
            table_name,
            key: logical_key,
            guard: preimage.guard,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
        };
        let response = match self
            .run_default_admitted(
                crate::admission::AdmissionClass::Write,
                |provider| async move {
                    record_storage_operation(
                        "guarded_delete_item",
                        provider.guarded_delete_item(guarded),
                    )
                    .await
                },
            )
            .await
        {
            Ok(response) => response,
            Err(error) if guarded_write::should_fallback(&error) => {
                guarded_write::record_fallback("delete_item", &error);
                self.cache_services
                    .release_write_intents(&cache_effects)
                    .await?;
                return Ok(None);
            }
            Err(error) => {
                self.cache_services
                    .release_write_intents(&cache_effects)
                    .await?;
                return Err(error);
            }
        };
        self.maybe_pause_after_storage_write_for_test().await;
        self.maybe_run_gsi_maintenance().await;
        self.cache_services
            .apply_write_effects(&cache_effects)
            .await?;
        Ok(Some(response))
    }

    async fn try_cached_guarded_update_item(
        &self,
        request: &UpdateItemRequest,
        prepared_update_cache_write: storage_cache::RuntimePreparedUpdateCacheWrite,
        response_operations: Option<Vec<String>>,
        customer_return_values: Option<ReturnValuesOldNewUpdated>,
    ) -> StorageResult<Option<UpdateItemResponse>> {
        if !self.default_supports_guarded_writes() {
            guarded_write::record_unsupported_fallback("update_item");
            return Ok(None);
        }
        let point_request = PointReadGetRequest {
            table_name: request.table_name.clone(),
            key: request.key.clone(),
        };
        let Some(preimage) = guarded_write::authoritative_preimage(
            self,
            &point_request,
            AuthoritativePointReadPurpose::UpdatePreImage,
        )
        .await?
        else {
            return Ok(None);
        };
        if !guarded_write::condition_matches(
            request.condition_expression.as_ref(),
            request.expression_attribute_names.as_ref(),
            request.expression_attribute_values.as_ref(),
            &preimage.item,
        )? {
            self.cache_services
                .prepare_update_write_intent(&prepared_update_cache_write)
                .await?;
            self.cache_services
                .release_update_write_intent(&prepared_update_cache_write)
                .await?;
            return Err(condition_failure_from_preimage(
                preimage.item,
                storage_types::return_values_on_condition_check_failure_all_old(
                    request.return_values_on_condition_check_failure.as_ref(),
                ),
            ));
        }

        let (operations, _) = before_update_item_optional(
            request.update_expression.as_deref(),
            request.condition_expression.as_deref(),
            request.expression_attribute_names.as_ref(),
            request.expression_attribute_values.as_ref(),
        )?;
        let guarded_write::CachedGuardedPreImage {
            item: preimage_item,
            guard,
        } = preimage;
        let item_to_update = preimage_item.unwrap_or_else(|| request.key.to_attribute_map());
        let old_item_for_response = return_values_need_old_item(customer_return_values.as_ref())
            .then(|| item_to_update.clone());
        let updated_item = apply_bound_update_operations(item_to_update, &operations)?;

        self.cache_services
            .prepare_update_write_intent(&prepared_update_cache_write)
            .await?;
        let guarded = GuardedUpdateItemRequest {
            request: request.clone(),
            guard,
        };
        let provider_response = match self
            .run_default_admitted(
                crate::admission::AdmissionClass::Write,
                |provider| async move {
                    record_storage_operation(
                        "guarded_update_item",
                        provider.guarded_update_item(guarded),
                    )
                    .await
                },
            )
            .await
        {
            Ok(response) => response,
            Err(error) if guarded_write::should_fallback(&error) => {
                guarded_write::record_fallback("update_item", &error);
                self.cache_services
                    .release_update_write_intent(&prepared_update_cache_write)
                    .await?;
                return Ok(None);
            }
            Err(error) => {
                self.cache_services
                    .release_update_write_intent(&prepared_update_cache_write)
                    .await?;
                return Err(error);
            }
        };
        self.maybe_pause_after_storage_write_for_test().await;
        self.maybe_run_gsi_maintenance().await;

        let post_image = Some(WireItem::from_attribute_map(&updated_item)?);
        let cache_effects =
            self.finalize_update_item_cache_effects(Some(prepared_update_cache_write), post_image)?;
        self.cache_services
            .apply_write_effects(&cache_effects)
            .await?;

        let response = if let Some(operations) = response_operations.as_ref() {
            update_item_response(
                operations,
                old_item_for_response,
                Some(updated_item),
                customer_return_values.as_ref(),
            )?
        } else {
            provider_response
        };
        Ok(Some(response))
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "API shape follows update_item request fields"
    )]
    pub async fn update_item_ref(
        &self,
        table_name: TableName,
        key: KeyRef<'_>,
        update_expression: String,
        condition_expression: Option<String>,
        expression_attribute_names: Option<&[ExprNameRef<'_>]>,
        expression_attribute_values: Option<&[ExprValueRef<'_>]>,
        return_values: Option<ReturnValuesOldNewUpdated>,
    ) -> StorageResult<UpdateItemResponse> {
        let expression_attribute_names = expression_attribute_names.map(expr_names_to_map);
        let expression_attribute_values = expression_attribute_values.map(expr_values_to_map);
        validate_update_expression_usage(
            Some(&update_expression),
            condition_expression.as_deref(),
            expression_attribute_names.as_ref(),
            expression_attribute_values.as_ref(),
        )?;
        self.update_item(UpdateItemInput {
            table_name,
            key: key.to_map().into(),
            update_expression,
            indexers: None,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            return_old_on_condition_failure: false,
            aux_item_stream_ttl_hours: None,
        })
        .await
    }
}

fn normalize_routed_response_attributes(
    request_rewriter: &NamespaceRequestRewriter,
    namespace: &TableNamespace,
    attributes: &mut Option<AttributeMap>,
) -> StorageResult<()> {
    let Some(attribute_map) = attributes.take() else {
        return Ok(());
    };
    let mut item = attribute_map.into_hashmap();
    request_rewriter.normalize_item_from_shared_table(namespace, &mut item)?;
    *attributes = Some(item.into());
    Ok(())
}

fn prepared_update_cache(
    prepared: Option<storage_cache::RuntimePreparedUpdateCacheWrite>,
) -> PreparedCacheWrite {
    prepared
        .map(|prepared| PreparedCacheWrite::Update(Box::new(prepared)))
        .unwrap_or_else(|| PreparedCacheWrite::Effects(empty_write_effects()))
}

fn empty_write_effects() -> storage_cache::RuntimeWriteEffects {
    storage_cache::RuntimeWriteEffects {
        point_read: Vec::new(),
        query_proof: Vec::new(),
    }
}

fn sync_response_at<T>(
    response: &storage_sync::SyncProposalResponse,
    index: usize,
    default: T,
) -> StorageResult<T>
where
    T: serde::de::DeserializeOwned,
{
    let Some(response) = response.responses.get(index) else {
        return Ok(default);
    };
    let Some(response_json) = response.response_json.as_ref() else {
        return Ok(default);
    };
    serde_json::from_str(response_json).map_err(|error| StorageError::internal(&error.to_string()))
}
