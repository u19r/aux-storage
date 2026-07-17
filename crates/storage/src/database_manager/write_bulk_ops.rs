use std::collections::{BTreeMap, HashMap};

use storage_cache::{
    PhysicalToLogicalWriteTableMap, RoutedBatchWriteTarget,
    insert_routed_batch_write_encode_request, insert_routed_batch_write_request,
    merge_unprocessed_batch_write_items,
};
use storage_sync::SyncWriteRequest;
use storage_types::{
    AttributeValue, AttributeValueLookup, BatchWriteItemEncodeRequest, BatchWriteItemRequest,
    BatchWriteItemResponse, EncodeWriteRequest, KeySchemaElement, StorageEnum, StorageError,
    StorageResult, TableName, TableNamespace, TransactEncodeItem, TransactWriteItemsEncodeRequest,
    TransactWriteItemsResponse, WriteRequest, WriteRetryPolicy, context::WrappedError,
    validate_item_key_attributes_for_schema, validate_key_attributes_for_schema,
};

use crate::{
    database_manager::{
        DatabaseManager, PreparedCacheWrite, ROUTED_DEFAULT_CONNECTION_ID, RoutedWriteTargetRole,
        ensure_route_writes_not_paused, fan_out_route_write_payload, record_storage_operation,
        record_storage_operation_for_target, set_transact_encode_item_table_name,
        transact_encode_item_table_name, validate_transact_encode_item_expression_usage,
    },
    namespace_routing::{NamespaceRouteRecord, NamespaceStorageMode},
    updated_at_apply::{
        refresh_existing_batch_write_encode_timestamps, refresh_existing_batch_write_timestamps,
        refresh_existing_transact_encode_item_timestamp, stamp_batch_write_encode_request,
        stamp_batch_write_request, stamp_transact_encode_item,
    },
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct RoutedWriteDispatchKey {
    pub(crate) connection_id: String,
    pub(crate) target_role: RoutedWriteTargetRole,
}

struct PreparedBatchWriteItem {
    request: BatchWriteItemRequest,
    cache_effects: storage_cache::RuntimeWriteEffects,
}

struct PreparedBatchWriteItemEncode {
    request: BatchWriteItemEncodeRequest,
    cache_effects: storage_cache::RuntimeWriteEffects,
}

struct PreparedTransactWriteItemsEncode {
    request: TransactWriteItemsEncodeRequest,
    client_request_token: Option<String>,
    return_consumed_capacity: Option<String>,
    return_item_collection_metrics: Option<String>,
    pending_routes: HashMap<TableNamespace, NamespaceRouteRecord>,
    cache_effects: storage_cache::RuntimeWriteEffects,
}

impl DatabaseManager {
    pub async fn batch_write_item(
        &self,
        request: BatchWriteItemRequest,
    ) -> StorageResult<BatchWriteItemResponse> {
        let prepared = self.prepare_batch_write_item(request).await?;
        if self.single_node_sync_mode_enabled() {
            return self.batch_write_item_single_node_sync(prepared).await;
        }
        if self.route_resolver.is_none() {
            return self.batch_write_item_unrouted(prepared).await;
        }
        self.batch_write_item_routed(prepared).await
    }

    async fn prepare_batch_write_item(
        &self,
        request: BatchWriteItemRequest,
    ) -> StorageResult<PreparedBatchWriteItem> {
        let mut request = request;
        self.validate_batch_write_unique_keys(&request).await?;
        if self.single_table_mode_enabled() {
            stamp_batch_write_request(&mut request)?;
        } else {
            refresh_existing_batch_write_timestamps(&mut request)?;
        }
        let cache_effects = self.plan_batch_write_cache_effects(&request).await?;
        Ok(PreparedBatchWriteItem {
            request,
            cache_effects,
        })
    }

    #[cfg(feature = "cache-write-planner")]
    async fn plan_batch_write_cache_effects(
        &self,
        request: &BatchWriteItemRequest,
    ) -> StorageResult<storage_cache::RuntimeWriteEffects> {
        self.cache_write_planner()
            .plan_batch_write_cache_effects(request)
            .await
    }

    #[cfg(not(feature = "cache-write-planner"))]
    async fn plan_batch_write_cache_effects(
        &self,
        _request: &BatchWriteItemRequest,
    ) -> StorageResult<storage_cache::RuntimeWriteEffects> {
        Ok(self.empty_cache_write_effects())
    }

    async fn batch_write_item_single_node_sync(
        &self,
        prepared: PreparedBatchWriteItem,
    ) -> StorageResult<BatchWriteItemResponse> {
        let PreparedBatchWriteItem {
            request,
            cache_effects,
        } = prepared;
        self.execute_with_cache_effects(
            PreparedCacheWrite::Effects(cache_effects.clone()),
            || async {
                self.run_single_node_sync_write_request(
                    "batch_write_item",
                    SyncWriteRequest::BatchWriteItem(request),
                )
                .await?;
                self.maybe_pause_after_storage_write_for_test().await;
                self.maybe_run_gsi_maintenance().await;
                Ok(BatchWriteItemResponse {
                    unprocessed_items: None,
                    item_collection_metrics: None,
                    consumed_capacity: None,
                })
            },
            |response| async { Ok((response, cache_effects)) },
        )
        .await
    }

    async fn batch_write_item_unrouted(
        &self,
        prepared: PreparedBatchWriteItem,
    ) -> StorageResult<BatchWriteItemResponse> {
        let PreparedBatchWriteItem {
            request,
            cache_effects,
        } = prepared;
        self.execute_with_cache_effects(
            PreparedCacheWrite::Effects(cache_effects.clone()),
            || async {
                let response = record_storage_operation(
                    "batch_write_item",
                    self.storage.batch_write_item(request, true),
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

    async fn batch_write_item_routed(
        &self,
        prepared: PreparedBatchWriteItem,
    ) -> StorageResult<BatchWriteItemResponse> {
        let PreparedBatchWriteItem {
            request,
            cache_effects,
        } = prepared;
        let (per_connection, physical_to_logical) = self
            .plan_routed_batch_write_requests::<WriteRequest>(
                request.request_items,
                request.return_consumed_capacity,
                request.return_item_collection_metrics,
            )
            .await?;

        self.execute_with_cache_effects(
            PreparedCacheWrite::Effects(cache_effects.clone()),
            || async {
                let mut merged_unprocessed: HashMap<TableName, Vec<storage_types::WriteRequest>> =
                    HashMap::new();
                for (dispatch_key, request) in per_connection {
                    let provider =
                        self.provider_for_request_connection(&dispatch_key.connection_id)?;
                    let response = record_storage_operation_for_target(
                        "batch_write_item",
                        dispatch_key.target_role,
                        provider.batch_write_item(request, true),
                    )
                    .await?;
                    if let Some(unprocessed) = response.unprocessed_items {
                        merge_unprocessed_batch_write_items(
                            &mut merged_unprocessed,
                            &physical_to_logical,
                            &dispatch_key.connection_id,
                            unprocessed,
                        );
                    }
                    self.maybe_run_gsi_maintenance_for_connection(&dispatch_key.connection_id)
                        .await?;
                }

                Ok(BatchWriteItemResponse {
                    unprocessed_items: (!merged_unprocessed.is_empty())
                        .then_some(merged_unprocessed),
                    item_collection_metrics: None,
                    consumed_capacity: None,
                })
            },
            |response| async { Ok((response, cache_effects)) },
        )
        .await
    }

    async fn validate_batch_write_unique_keys(
        &self,
        request: &BatchWriteItemRequest,
    ) -> StorageResult<()> {
        let mut table_infos = HashMap::with_capacity(request.request_items.len());
        for table_name in request.request_items.keys() {
            table_infos.insert(
                table_name.clone(),
                self.get_table_info_arc(table_name)
                    .await
                    .map_err(batch_write_table_not_found_as_resource_not_found)?,
            );
        }
        for (table_name, write_requests) in &request.request_items {
            let Some(table_info) = table_infos.get(table_name) else {
                return Err(StorageError::table_not_found(table_name));
            };
            let mut seen_keys = Vec::with_capacity(write_requests.len());
            for write_request in write_requests {
                let key = batch_write_request_primary_key(write_request, &table_info.key_schema)
                    .map_err(batch_write_key_error)?;
                if seen_keys.contains(&key) {
                    return Err(StorageError::validation(
                        "Provided list of item keys contains duplicates",
                    ));
                }
                seen_keys.push(key);
            }
        }
        Ok(())
    }

    pub async fn batch_write_item_encode(
        &self,
        request: BatchWriteItemEncodeRequest,
    ) -> StorageResult<BatchWriteItemResponse> {
        let prepared = self.prepare_batch_write_item_encode(request).await?;
        if self.single_node_sync_mode_enabled() {
            return Err(StorageError::unsupported(
                "BatchWriteItem encode single-node sync routing is not implemented yet",
            ));
        }
        if self.route_resolver.is_none() {
            return self.batch_write_item_encode_unrouted(prepared).await;
        }
        self.batch_write_item_encode_routed(prepared).await
    }

    async fn prepare_batch_write_item_encode(
        &self,
        request: BatchWriteItemEncodeRequest,
    ) -> StorageResult<PreparedBatchWriteItemEncode> {
        let mut request = request;
        if self.single_table_mode_enabled() {
            stamp_batch_write_encode_request(&mut request)?;
        } else {
            refresh_existing_batch_write_encode_timestamps(&mut request)?;
        }
        let cache_effects = self.plan_batch_write_encode_cache_effects(&request).await?;
        Ok(PreparedBatchWriteItemEncode {
            request,
            cache_effects,
        })
    }

    #[cfg(feature = "cache-write-planner")]
    async fn plan_batch_write_encode_cache_effects(
        &self,
        request: &BatchWriteItemEncodeRequest,
    ) -> StorageResult<storage_cache::RuntimeWriteEffects> {
        self.cache_write_planner()
            .plan_batch_write_encode_cache_effects(request)
            .await
    }

    #[cfg(not(feature = "cache-write-planner"))]
    async fn plan_batch_write_encode_cache_effects(
        &self,
        _request: &BatchWriteItemEncodeRequest,
    ) -> StorageResult<storage_cache::RuntimeWriteEffects> {
        Ok(self.empty_cache_write_effects())
    }

    async fn batch_write_item_encode_unrouted(
        &self,
        prepared: PreparedBatchWriteItemEncode,
    ) -> StorageResult<BatchWriteItemResponse> {
        let PreparedBatchWriteItemEncode {
            request,
            cache_effects,
        } = prepared;
        self.execute_with_cache_effects(
            PreparedCacheWrite::Effects(cache_effects.clone()),
            || async {
                let response = record_storage_operation(
                    "batch_write_item",
                    self.storage.batch_write_item_encode(request, true),
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

    async fn batch_write_item_encode_routed(
        &self,
        prepared: PreparedBatchWriteItemEncode,
    ) -> StorageResult<BatchWriteItemResponse> {
        let PreparedBatchWriteItemEncode {
            request,
            cache_effects,
        } = prepared;
        let (per_connection, physical_to_logical) = self
            .plan_routed_batch_write_requests::<EncodeWriteRequest>(
                request.request_items,
                request.return_consumed_capacity,
                request.return_item_collection_metrics,
            )
            .await?;

        self.execute_with_cache_effects(
            PreparedCacheWrite::Effects(cache_effects.clone()),
            || async {
                let mut merged_unprocessed: HashMap<TableName, Vec<storage_types::WriteRequest>> =
                    HashMap::new();
                for (dispatch_key, request) in per_connection {
                    let provider =
                        self.provider_for_request_connection(&dispatch_key.connection_id)?;
                    let response = record_storage_operation_for_target(
                        "batch_write_item",
                        dispatch_key.target_role,
                        provider.batch_write_item_encode(request, true),
                    )
                    .await?;
                    if let Some(unprocessed) = response.unprocessed_items {
                        merge_unprocessed_batch_write_items(
                            &mut merged_unprocessed,
                            &physical_to_logical,
                            &dispatch_key.connection_id,
                            unprocessed,
                        );
                    }
                    self.maybe_run_gsi_maintenance_for_connection(&dispatch_key.connection_id)
                        .await?;
                }

                Ok(BatchWriteItemResponse {
                    unprocessed_items: (!merged_unprocessed.is_empty())
                        .then_some(merged_unprocessed),
                    item_collection_metrics: None,
                    consumed_capacity: None,
                })
            },
            |response| async { Ok((response, cache_effects)) },
        )
        .await
    }

    async fn plan_routed_batch_write_requests<W>(
        &self,
        request_items: HashMap<TableName, Vec<W>>,
        return_consumed_capacity: Option<String>,
        return_item_collection_metrics: Option<String>,
    ) -> StorageResult<(
        BTreeMap<RoutedWriteDispatchKey, W::Request>,
        PhysicalToLogicalWriteTableMap,
    )>
    where
        W: BatchWriteRoutePayload,
    {
        let default_connection_id = ROUTED_DEFAULT_CONNECTION_ID.to_string();
        let mut per_connection = BTreeMap::new();
        let mut physical_to_logical = PhysicalToLogicalWriteTableMap::default();

        for (logical_table, write_requests) in request_items {
            let route = self
                .resolve_namespace_route_for_table(&logical_table)
                .await?;
            if let Some(route) = route {
                ensure_route_writes_not_paused(&route)?;
                let mut rewritten = write_requests;
                if route.storage_mode == NamespaceStorageMode::SharedTable {
                    for write_request in &mut rewritten {
                        W::rewrite_for_shared_table(self, &route.namespace, write_request)?;
                    }
                }
                fan_out_route_write_payload(
                    &route,
                    rewritten,
                    W::PAYLOAD_NAME,
                    |target, target_role, rewritten_for_target| {
                        W::insert_request(
                            &mut per_connection,
                            &mut physical_to_logical,
                            &return_consumed_capacity,
                            &return_item_collection_metrics,
                            RoutedBatchWriteTarget {
                                connection_id: target.connection_id.clone(),
                                physical_table: target.table_name.clone(),
                                logical_table: logical_table.clone(),
                            },
                            RoutedWriteDispatchKey {
                                connection_id: target.connection_id.clone(),
                                target_role,
                            },
                            rewritten_for_target,
                        );
                        Ok(())
                    },
                )?;
            } else {
                W::insert_request(
                    &mut per_connection,
                    &mut physical_to_logical,
                    &return_consumed_capacity,
                    &return_item_collection_metrics,
                    RoutedBatchWriteTarget {
                        connection_id: default_connection_id.clone(),
                        physical_table: logical_table.clone(),
                        logical_table,
                    },
                    RoutedWriteDispatchKey {
                        connection_id: default_connection_id.clone(),
                        target_role: RoutedWriteTargetRole::Primary,
                    },
                    write_requests,
                );
            }
        }

        Ok((per_connection, physical_to_logical))
    }

    pub async fn transact_write_items_encode(
        &self,
        request: TransactWriteItemsEncodeRequest,
    ) -> StorageResult<TransactWriteItemsResponse> {
        self.transact_write_items_encode_with_retry(request, WriteRetryPolicy::no_retry())
            .await
    }

    pub async fn transact_write_items_encode_with_retry(
        &self,
        request: TransactWriteItemsEncodeRequest,
        retry_policy: WriteRetryPolicy,
    ) -> StorageResult<TransactWriteItemsResponse> {
        let prepared = self.prepare_transact_write_items_encode(request).await?;
        if self.single_node_sync_mode_enabled() {
            return Err(StorageError::unsupported(
                "TransactWriteItems encode single-node sync routing is not implemented yet",
            ));
        }
        if self.route_resolver.is_none() {
            return self
                .transact_write_items_encode_unrouted(prepared, retry_policy)
                .await;
        }
        self.transact_write_items_encode_routed(prepared, retry_policy)
            .await
    }

    async fn prepare_transact_write_items_encode(
        &self,
        request: TransactWriteItemsEncodeRequest,
    ) -> StorageResult<PreparedTransactWriteItemsEncode> {
        let mut request = request;
        let client_request_token = request.client_request_token.clone();
        let return_consumed_capacity = request.return_consumed_capacity.clone();
        let return_item_collection_metrics = request.return_item_collection_metrics.clone();

        for item in &mut request.transact_items {
            if self.single_table_mode_enabled() {
                stamp_transact_encode_item(item)?;
            } else {
                refresh_existing_transact_encode_item_timestamp(item)?;
            }
            validate_transact_encode_item_expression_usage(item)?;
        }
        let pending_routes =
            Self::pending_namespace_routes_from_transact_items(&request.transact_items)?;
        let cache_effects = self
            .plan_transact_write_encode_cache_effects(&request.transact_items, &pending_routes)
            .await?;
        Ok(PreparedTransactWriteItemsEncode {
            request,
            client_request_token,
            return_consumed_capacity,
            return_item_collection_metrics,
            pending_routes,
            cache_effects,
        })
    }

    #[cfg(feature = "cache-write-planner")]
    async fn plan_transact_write_encode_cache_effects(
        &self,
        transact_items: &[TransactEncodeItem],
        pending_routes: &HashMap<TableNamespace, NamespaceRouteRecord>,
    ) -> StorageResult<storage_cache::RuntimeWriteEffects> {
        if !self.cache_write_effects_enabled() {
            return Ok(self.empty_cache_write_effects());
        }
        self.cache_write_planner()
            .plan_transact_write_encode_cache_effects(transact_items, pending_routes)
            .await
    }

    #[cfg(not(feature = "cache-write-planner"))]
    async fn plan_transact_write_encode_cache_effects(
        &self,
        _transact_items: &[TransactEncodeItem],
        _pending_routes: &HashMap<TableNamespace, NamespaceRouteRecord>,
    ) -> StorageResult<storage_cache::RuntimeWriteEffects> {
        Ok(self.empty_cache_write_effects())
    }

    async fn transact_write_items_encode_unrouted(
        &self,
        prepared: PreparedTransactWriteItemsEncode,
        retry_policy: WriteRetryPolicy,
    ) -> StorageResult<TransactWriteItemsResponse> {
        let PreparedTransactWriteItemsEncode {
            request,
            cache_effects,
            ..
        } = prepared;
        self.execute_with_cache_effects(
            PreparedCacheWrite::Effects(cache_effects.clone()),
            || async {
                let response = record_storage_operation(
                    "transact_write_items",
                    self.storage
                        .transact_write_items_encode_with_retry(request, retry_policy),
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

    async fn transact_write_items_encode_routed(
        &self,
        prepared: PreparedTransactWriteItemsEncode,
        retry_policy: WriteRetryPolicy,
    ) -> StorageResult<TransactWriteItemsResponse> {
        let PreparedTransactWriteItemsEncode {
            request,
            client_request_token,
            return_consumed_capacity,
            return_item_collection_metrics,
            pending_routes,
            cache_effects,
        } = prepared;
        let per_connection = self
            .plan_routed_transact_write_items_encode(request.transact_items, &pending_routes)
            .await?;
        self.execute_with_cache_effects(
            PreparedCacheWrite::Effects(cache_effects.clone()),
            || async {
                self.execute_routed_transact_write_items_encode(
                    per_connection,
                    client_request_token,
                    return_consumed_capacity,
                    return_item_collection_metrics,
                    retry_policy,
                )
                .await
            },
            |response| async { Ok((response, cache_effects)) },
        )
        .await
    }

    async fn plan_routed_transact_write_items_encode(
        &self,
        transact_items: Vec<TransactEncodeItem>,
        pending_routes: &HashMap<TableNamespace, NamespaceRouteRecord>,
    ) -> StorageResult<BTreeMap<RoutedWriteDispatchKey, TransactWriteItemsEncodeRequest>> {
        let default_connection_id = ROUTED_DEFAULT_CONNECTION_ID.to_string();
        let mut per_connection: BTreeMap<RoutedWriteDispatchKey, TransactWriteItemsEncodeRequest> =
            BTreeMap::new();
        for item in transact_items {
            let logical_table = transact_encode_item_table_name(&item)?;
            let route = self
                .resolve_namespace_route_for_table_with_pending(&logical_table, pending_routes)
                .await?;
            if let Some(route) = route {
                ensure_route_writes_not_paused(&route)?;
                let mut routed_item = item;
                if route.storage_mode == NamespaceStorageMode::SharedTable {
                    self.request_rewriter
                        .rewrite_transact_encode_item_for_shared_table(
                            &route.namespace,
                            &mut routed_item,
                        )?;
                }

                fan_out_route_write_payload(
                    &route,
                    routed_item,
                    "transact_write_items_encode.item",
                    |target, target_role, mut routed_item_for_target| {
                        set_transact_encode_item_table_name(
                            &mut routed_item_for_target,
                            target.table_name.clone(),
                        );
                        per_connection
                            .entry(RoutedWriteDispatchKey {
                                connection_id: target.connection_id.clone(),
                                target_role,
                            })
                            .or_default()
                            .transact_items
                            .push(routed_item_for_target);
                        Ok(())
                    },
                )?;
            } else {
                per_connection
                    .entry(RoutedWriteDispatchKey {
                        connection_id: default_connection_id.clone(),
                        target_role: RoutedWriteTargetRole::Primary,
                    })
                    .or_default()
                    .transact_items
                    .push(item);
            }
        }
        Ok(per_connection)
    }

    async fn execute_routed_transact_write_items_encode(
        &self,
        per_connection: BTreeMap<RoutedWriteDispatchKey, TransactWriteItemsEncodeRequest>,
        client_request_token: Option<String>,
        return_consumed_capacity: Option<String>,
        return_item_collection_metrics: Option<String>,
        retry_policy: WriteRetryPolicy,
    ) -> StorageResult<TransactWriteItemsResponse> {
        let mut primary_response: Option<TransactWriteItemsResponse> = None;
        for (dispatch_key, mut request_for_connection) in per_connection {
            request_for_connection.client_request_token = client_request_token.clone();
            request_for_connection.return_consumed_capacity = return_consumed_capacity.clone();
            request_for_connection.return_item_collection_metrics =
                return_item_collection_metrics.clone();

            let provider = self.provider_for_request_connection(&dispatch_key.connection_id)?;
            let response = record_storage_operation_for_target(
                "transact_write_items",
                dispatch_key.target_role,
                provider
                    .transact_write_items_encode_with_retry(request_for_connection, retry_policy),
            )
            .await?;
            if primary_response.is_none() {
                primary_response = Some(response);
            }
            self.maybe_run_gsi_maintenance_for_connection(&dispatch_key.connection_id)
                .await?;
        }

        primary_response.ok_or_else(|| {
            StorageError::internal("transact_write_items_encode routing produced no write targets")
        })
    }
}

trait BatchWriteRoutePayload: Clone {
    type Request;

    const PAYLOAD_NAME: &'static str;

    fn rewrite_for_shared_table(
        manager: &DatabaseManager,
        namespace: &TableNamespace,
        write_request: &mut Self,
    ) -> StorageResult<()>;

    fn insert_request(
        per_connection: &mut BTreeMap<RoutedWriteDispatchKey, Self::Request>,
        physical_to_logical: &mut PhysicalToLogicalWriteTableMap,
        return_consumed_capacity: &Option<String>,
        return_item_collection_metrics: &Option<String>,
        target: RoutedBatchWriteTarget,
        dispatch_key: RoutedWriteDispatchKey,
        write_requests: Vec<Self>,
    );
}

impl BatchWriteRoutePayload for WriteRequest {
    type Request = BatchWriteItemRequest;

    const PAYLOAD_NAME: &'static str = "batch_write_item.write_requests";

    fn rewrite_for_shared_table(
        manager: &DatabaseManager,
        namespace: &TableNamespace,
        write_request: &mut Self,
    ) -> StorageResult<()> {
        manager
            .request_rewriter
            .rewrite_write_request_for_shared_table(namespace, write_request)
    }

    fn insert_request(
        per_connection: &mut BTreeMap<RoutedWriteDispatchKey, Self::Request>,
        physical_to_logical: &mut PhysicalToLogicalWriteTableMap,
        return_consumed_capacity: &Option<String>,
        return_item_collection_metrics: &Option<String>,
        target: RoutedBatchWriteTarget,
        dispatch_key: RoutedWriteDispatchKey,
        write_requests: Vec<Self>,
    ) {
        insert_routed_batch_write_request(
            per_connection,
            physical_to_logical,
            return_consumed_capacity,
            return_item_collection_metrics,
            target,
            dispatch_key,
            write_requests,
        );
    }
}

impl BatchWriteRoutePayload for EncodeWriteRequest {
    type Request = BatchWriteItemEncodeRequest;

    const PAYLOAD_NAME: &'static str = "batch_write_item_encode.write_requests";

    fn rewrite_for_shared_table(
        manager: &DatabaseManager,
        namespace: &TableNamespace,
        write_request: &mut Self,
    ) -> StorageResult<()> {
        manager
            .request_rewriter
            .rewrite_encode_write_request_for_shared_table(namespace, write_request)
    }

    fn insert_request(
        per_connection: &mut BTreeMap<RoutedWriteDispatchKey, Self::Request>,
        physical_to_logical: &mut PhysicalToLogicalWriteTableMap,
        return_consumed_capacity: &Option<String>,
        return_item_collection_metrics: &Option<String>,
        target: RoutedBatchWriteTarget,
        dispatch_key: RoutedWriteDispatchKey,
        write_requests: Vec<Self>,
    ) {
        insert_routed_batch_write_encode_request(
            per_connection,
            physical_to_logical,
            return_consumed_capacity,
            return_item_collection_metrics,
            target,
            dispatch_key,
            write_requests,
        );
    }
}

fn batch_write_table_not_found_as_resource_not_found(error: StorageError) -> StorageError {
    if let StorageEnum::TableNotFound { name, .. } = error.to_enum() {
        return StorageError::Base(StorageEnum::ResourceNotFound {
            resource_type: "table",
            resource_id: name.clone(),
        });
    }
    error
}

fn batch_write_request_primary_key(
    write_request: &WriteRequest,
    key_schema: &[KeySchemaElement],
) -> StorageResult<(AttributeValue, Option<AttributeValue>)> {
    if let Some(put) = &write_request.put_request {
        validate_item_key_attributes_for_schema(key_schema, &put.item)?;
        return key_values_from_attributes(key_schema, &put.item);
    }
    if let Some(delete) = &write_request.delete_request {
        validate_key_attributes_for_schema(key_schema, &delete.key)?;
        return key_values_from_attributes(key_schema, &delete.key);
    }
    Err(StorageError::validation(
        "WriteRequest must contain exactly one of PutRequest or DeleteRequest",
    ))
}

fn batch_write_key_error(error: StorageError) -> StorageError {
    let StorageEnum::Validation { message } = error.to_enum() else {
        return error;
    };
    if message == "The parameter cannot be converted to a numeric value"
        || message == "Attempting to store more than 38 significant digits in a Number"
        || message
            == "Number underflow. Attempting to store a number with magnitude smaller than \
                supported range"
    {
        return StorageError::raw_validation(message.clone());
    }
    error
}

fn key_values_from_attributes(
    key_schema: &[KeySchemaElement],
    attributes: &impl AttributeValueLookup,
) -> StorageResult<(AttributeValue, Option<AttributeValue>)> {
    let mut hash_key = None;
    let mut range_key = None;
    for element in key_schema {
        let value = attributes
            .get_attribute_value(&element.attribute_name)
            .ok_or_else(StorageError::invalid_or_missing_key)?;
        match element.key_type {
            storage_types::KeyType::Hash => hash_key = Some(value.clone()),
            storage_types::KeyType::Range => range_key = Some(value.clone()),
        }
    }
    Ok((
        hash_key.ok_or_else(StorageError::invalid_or_missing_key)?,
        range_key,
    ))
}
