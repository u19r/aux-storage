use std::collections::HashMap;

use storage_provider::{
    StorageProvider, apply_bound_update_operations, before_update_item,
    return_values_need_old_item, update_item_response,
};
use storage_sync::SyncWriteRequest;
use storage_types::{
    AllOld, AttributeMap, AttributeValue, ExprNameRef, ExprValueRef, GuardedDeleteItemRequest,
    GuardedPutItemRequest, GuardedUpdateItemRequest, KeyAttributes, KeyRef, PutItemRequest,
    PutItemResponse, ReturnValuesOldNewUpdated, StorageEnum, StorageError, StorageResult,
    TableName, TableNamespace, UpdateItemRequest, UpdateItemResponse, WireItem, expr_names_to_map,
    expr_values_to_map, validate_expression_attribute_usage,
};

use crate::{
    AuthoritativePointReadPurpose, PointReadGetRequest,
    database_manager::{
        DatabaseManager, DeleteItemInput, PreparedCacheWrite, PutItemInput, PutItemPayload,
        UpdateItemInput, WriteTargetSet, guarded_write_coordinator as guarded_write,
        record_storage_operation, record_storage_operation_for_target,
        refresh_existing_updated_at_on_put_payload, stamp_updated_at_on_put_payload,
        update_item_return_values_rewritable_from_post_image, validate_update_expression_usage,
    },
    namespace_routing::{NamespaceRequestRewriter, NamespaceStorageMode},
    updated_at_apply::inject_updated_at_into_update_expression,
};

impl DatabaseManager {
    pub async fn put_item(&self, input: PutItemInput) -> StorageResult<PutItemResponse> {
        let PutItemInput {
            table_name,
            item,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
        } = input;
        validate_expression_attribute_usage(
            expression_attribute_names.as_ref(),
            expression_attribute_values.as_ref(),
            condition_expression.as_deref().into_iter(),
        )?;
        let mut item = item;
        if self.single_table_mode_enabled() {
            stamp_updated_at_on_put_payload(&mut item)?;
        } else {
            refresh_existing_updated_at_on_put_payload(&mut item)?;
        }
        let logical_item = item.clone().into_attribute_map()?;
        let table_info = self.get_table_info_arc(&table_name).await?;
        storage_types::validate_item_key_attributes_for_schema(
            &table_info.key_schema,
            &logical_item,
        )?;
        let cache_write_planner = self.cache_write_planner();
        let cache_effects = cache_write_planner
            .plan_put_item_cache_effects(&table_name, &logical_item)
            .await?;
        if self.single_node_sync_mode_enabled() {
            let request = PutItemRequest {
                table_name: table_name.clone(),
                item: logical_item,
                condition_expression,
                expression_attribute_names,
                expression_attribute_values,
                expected: None,
                conditional_operator: None,
                return_values,
                return_consumed_capacity: None,
                return_item_collection_metrics: None,
                return_values_on_condition_check_failure: None,
            };
            return self
                .execute_with_cache_effects(
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
                .await;
        }

        let route = self.resolve_namespace_route_for_table(&table_name).await?;
        let Some(route) = route else {
            if let Some(response) = self
                .try_cached_guarded_put_item(
                    &table_name,
                    &logical_item,
                    cache_effects.clone(),
                    condition_expression.clone(),
                    expression_attribute_names.clone(),
                    expression_attribute_values.clone(),
                    return_values.clone(),
                )
                .await?
            {
                return Ok(response);
            }
            return self
                .execute_with_cache_effects(
                    PreparedCacheWrite::Effects(cache_effects.clone()),
                    || async {
                        let response = match item {
                            PutItemPayload::AttributeMap(item) => {
                                record_storage_operation(
                                    "put_item",
                                    self.storage.put_item(
                                        table_name.clone(),
                                        item,
                                        condition_expression,
                                        expression_attribute_names,
                                        expression_attribute_values,
                                        return_values,
                                    ),
                                )
                                .await?
                            }
                            PutItemPayload::WireItem(item) => {
                                record_storage_operation(
                                    "put_item",
                                    self.storage.put_item_encode(
                                        table_name.clone(),
                                        *item,
                                        condition_expression,
                                        expression_attribute_names,
                                        expression_attribute_values,
                                        return_values,
                                    ),
                                )
                                .await?
                            }
                        };
                        self.maybe_pause_after_storage_write_for_test().await;
                        self.maybe_run_gsi_maintenance().await;
                        Ok(response)
                    },
                    |response| async { Ok((response, cache_effects)) },
                )
                .await;
        };

        let mut item_map = item.into_attribute_map()?;
        if route.storage_mode == NamespaceStorageMode::SharedTable {
            self.request_rewriter
                .rewrite_item_for_shared_table(&route.namespace, &mut item_map)?;
        }

        let target_count = route.write_targets.len();
        let mut routed_item_map = WriteTargetSet::new(target_count, item_map, "put_item.item")?;
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
        self.execute_with_cache_effects(
            PreparedCacheWrite::Effects(cache_effects.clone()),
            || async {
                let mut response = self
                    .execute_routed_write_targets(
                        &route,
                        "put_item routing produced no write targets",
                        |provider, target, target_index, target_role| {
                            let table_name = target.table_name.clone();
                            let item = routed_item_map.take(target_index);
                            let condition_expression =
                                routed_condition_expression.take(target_index);
                            let expression_attribute_names =
                                routed_expression_attribute_names.take(target_index);
                            let expression_attribute_values =
                                routed_expression_attribute_values.take(target_index);
                            let return_values = routed_return_values.take(target_index);
                            async move {
                                record_storage_operation_for_target(
                                    "put_item",
                                    target_role,
                                    provider.put_item(
                                        table_name,
                                        item?,
                                        condition_expression?,
                                        expression_attribute_names?,
                                        expression_attribute_values?,
                                        return_values?,
                                    ),
                                )
                                .await
                            }
                        },
                    )
                    .await?;
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
        validate_expression_attribute_usage(
            input.expression_attribute_names.as_ref(),
            input.expression_attribute_values.as_ref(),
            input.condition_expression.as_deref().into_iter(),
        )?;
        let table_name = input.table_name;
        let mut key = input.key;
        let table_info = self.get_table_info_arc(&table_name).await?;
        storage_types::validate_key_attributes_for_schema(&table_info.key_schema, &key)?;
        let logical_key = key.clone();
        let cache_write_planner = self.cache_write_planner();
        let cache_effects = cache_write_planner
            .plan_delete_item_cache_effects(&table_name, &logical_key)
            .await?;
        let condition_expression = input.condition_expression;
        let expression_attribute_names = input.expression_attribute_names;
        let mut expression_attribute_values = input.expression_attribute_values;
        if self.single_node_sync_mode_enabled() {
            let request = storage_types::DeleteItemRequest {
                table_name: table_name.clone(),
                key: logical_key,
                condition_expression,
                expression_attribute_names,
                expression_attribute_values,
                expected: None,
                conditional_operator: None,
                return_values: None,
                return_consumed_capacity: None,
                return_item_collection_metrics: None,
                return_values_on_condition_check_failure: None,
            };
            return self
                .execute_with_cache_effects(
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
                .await;
        }
        let route = self.resolve_namespace_route_for_table(&table_name).await?;
        let Some(route) = route else {
            if let Some(response) = self
                .try_cached_guarded_delete_item(
                    &table_name,
                    &logical_key,
                    cache_effects.clone(),
                    condition_expression.clone(),
                    expression_attribute_names.clone(),
                    expression_attribute_values.clone(),
                )
                .await?
            {
                return Ok(response);
            }
            return self
                .execute_with_cache_effects(
                    PreparedCacheWrite::Effects(cache_effects.clone()),
                    || async {
                        let response = record_storage_operation(
                            "delete_item",
                            self.storage.delete_item(
                                table_name.clone(),
                                key,
                                condition_expression,
                                expression_attribute_names,
                                expression_attribute_values,
                            ),
                        )
                        .await?;
                        self.maybe_pause_after_storage_write_for_test().await;
                        self.maybe_run_gsi_maintenance().await;
                        Ok(response)
                    },
                    |response| async { Ok((response, cache_effects)) },
                )
                .await;
        };

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
                                    provider.delete_item(
                                        table_name,
                                        key?,
                                        condition_expression?,
                                        expression_attribute_names?,
                                        expression_attribute_values?,
                                    ),
                                )
                                .await
                            }
                        },
                    )
                    .await?;
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
        let UpdateItemInput {
            table_name,
            key,
            update_expression,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
        } = input;
        let mut update_expression = update_expression;
        let mut expression_attribute_names = expression_attribute_names;
        let mut expression_attribute_values = expression_attribute_values;
        if self.single_table_mode_enabled() {
            inject_updated_at_into_update_expression(
                &mut update_expression,
                &mut expression_attribute_names,
                &mut expression_attribute_values,
            )?;
        }
        validate_update_expression_usage(
            &update_expression,
            condition_expression.as_deref(),
            expression_attribute_names.as_ref(),
            expression_attribute_values.as_ref(),
        )?;
        let table_info = self.get_table_info_arc(&table_name).await?;
        storage_types::validate_key_attributes_for_schema(&table_info.key_schema, &key)?;

        let cache_enabled = self.cache_services.point_read_enabled();
        let customer_return_values = return_values;
        let rewrite_return_values_for_cache = cache_enabled
            && update_item_return_values_rewritable_from_post_image(
                customer_return_values.as_ref(),
            );
        let response_operations = if rewrite_return_values_for_cache {
            let (operations, _) = before_update_item(
                update_expression.as_str(),
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

        let request = UpdateItemRequest::builder()
            .table_name(table_name)
            .key(key)
            .update_expression(update_expression)
            .condition_expression(condition_expression)
            .expression_attribute_names(expression_attribute_names)
            .expression_attribute_values(expression_attribute_values)
            .return_values(provider_return_values)
            .build();
        let cache_write_planner = self.cache_write_planner();
        let prepared_update_cache_write = cache_write_planner
            .prepare_update_item_cache_write(&request.table_name, &request.key)
            .await?;
        if self.single_node_sync_mode_enabled() {
            return self
                .execute_with_cache_effects(
                    PreparedCacheWrite::Update(Box::new(prepared_update_cache_write)),
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
                .await;
        }

        let route = self
            .resolve_namespace_route_for_table(&request.table_name)
            .await?;
        let mut routed_request = request;
        if route.is_none()
            && let Some(response) = self
                .try_cached_guarded_update_item(
                    &routed_request,
                    prepared_update_cache_write.clone(),
                    response_operations.clone(),
                    customer_return_values.clone(),
                )
                .await?
        {
            return Ok(response);
        }
        if let Some(route) = route.as_ref()
            && route.storage_mode == NamespaceStorageMode::SharedTable
        {
            self.request_rewriter
                .rewrite_update_for_shared_table(&route.namespace, &mut routed_request)?;
        }

        let prepared_cache =
            PreparedCacheWrite::Update(Box::new(prepared_update_cache_write.clone()));
        let response_operations = response_operations.clone();
        self.execute_with_cache_effects(
            prepared_cache,
            || async {
                let response = if let Some(route) = route {
                    let mut routed_requests = WriteTargetSet::new(
                        route.write_targets.len(),
                        routed_request,
                        "update_item.request",
                    )?;
                    let mut response = self
                        .execute_routed_write_targets(
                            &route,
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
                        .await?;
                    if route.storage_mode == NamespaceStorageMode::SharedTable {
                        normalize_routed_response_attributes(
                            &self.request_rewriter,
                            &route.namespace,
                            &mut response.attributes,
                        )?;
                    }
                    response
                } else {
                    let response = record_storage_operation(
                        "update_item",
                        self.storage.update_item(routed_request),
                    )
                    .await?;
                    self.maybe_pause_after_storage_write_for_test().await;
                    self.maybe_run_gsi_maintenance().await;
                    response
                };
                Ok(response)
            },
            |mut response| async move {
                let post_image = if cache_enabled {
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
                };

                if let Some(operations) = response_operations.as_ref() {
                    let new_item = match post_image.as_ref() {
                        Some(item) => Some(item.clone().into_attribute_map()?),
                        None => response.attributes.clone().map(Into::into),
                    };
                    response = update_item_response(
                        operations,
                        None,
                        new_item,
                        customer_return_values.as_ref(),
                    )?;
                }

                let cache_effects = cache_write_planner
                    .finalize_update_item_cache_effects(prepared_update_cache_write, post_image)?;
                Ok((response, cache_effects))
            },
        )
        .await
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "helper mirrors guarded put request and cache effect inputs"
    )]
    async fn try_cached_guarded_put_item(
        &self,
        table_name: &TableName,
        logical_item: &HashMap<String, AttributeValue>,
        cache_effects: storage_cache::RuntimeWriteEffects,
        condition_expression: Option<String>,
        expression_attribute_names: Option<HashMap<String, String>>,
        expression_attribute_values: Option<HashMap<String, AttributeValue>>,
        return_values: Option<AllOld>,
    ) -> StorageResult<Option<PutItemResponse>> {
        if !self.storage.supports_guarded_writes() {
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
                .prepare_write_intents(&cache_effects)
                .await?;
            self.cache_services
                .release_write_intents(&cache_effects)
                .await?;
            return Err(StorageEnum::ConditionalCheckFailed.into());
        }

        self.cache_services
            .prepare_write_intents(&cache_effects)
            .await?;
        let guarded = GuardedPutItemRequest {
            table_name: table_name.clone(),
            item: logical_item.clone(),
            guard: preimage.guard,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
        };
        let response = match record_storage_operation(
            "guarded_put_item",
            self.storage.guarded_put_item(guarded),
        )
        .await
        {
            Ok(response) => response,
            Err(error) if guarded_write::should_fallback(&error) => {
                guarded_write::record_fallback("put_item", &error);
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

    async fn try_cached_guarded_delete_item(
        &self,
        table_name: &TableName,
        logical_key: &KeyAttributes,
        cache_effects: storage_cache::RuntimeWriteEffects,
        condition_expression: Option<String>,
        expression_attribute_names: Option<HashMap<String, String>>,
        expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    ) -> StorageResult<Option<Option<HashMap<String, AttributeValue>>>> {
        if !self.storage.supports_guarded_writes() {
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
            return Err(StorageEnum::ConditionalCheckFailed.into());
        }

        self.cache_services
            .prepare_write_intents(&cache_effects)
            .await?;
        let guarded = GuardedDeleteItemRequest {
            table_name: table_name.clone(),
            key: logical_key.clone(),
            guard: preimage.guard,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
        };
        let response = match record_storage_operation(
            "guarded_delete_item",
            self.storage.guarded_delete_item(guarded),
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
        if !self.storage.supports_guarded_writes() {
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
            return Err(StorageEnum::ConditionalCheckFailed.into());
        }

        let (operations, _) = before_update_item(
            request.update_expression.as_str(),
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
        let provider_response = match record_storage_operation(
            "guarded_update_item",
            self.storage.guarded_update_item(guarded),
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
        let cache_effects = self
            .cache_write_planner()
            .finalize_update_item_cache_effects(prepared_update_cache_write, post_image)?;
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
            &update_expression,
            condition_expression.as_deref(),
            expression_attribute_names.as_ref(),
            expression_attribute_values.as_ref(),
        )?;
        self.update_item(UpdateItemInput {
            table_name,
            key: key.to_map().into(),
            update_expression,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
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
