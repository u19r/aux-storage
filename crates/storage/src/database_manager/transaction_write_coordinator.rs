use std::collections::{BTreeMap, HashMap};

use storage_sync::SyncWriteRequest;
use storage_types::{
    AttributeValue, DurableTransactWriteGuard, GuardedTransactWriteItemsRequest, StorageEnum,
    StorageError, StorageResult, TableNamespace, TransactWriteItem, TransactWriteItemsRequest,
    TransactWriteItemsResponse, context::WrappedError as _,
};

use crate::{
    database_manager::{
        DatabaseManager, PreparedCacheWrite, ROUTED_DEFAULT_CONNECTION_ID, RoutedWriteTargetRole,
        ensure_route_writes_not_paused, fan_out_route_write_payload,
        guarded_write_coordinator as guarded_write, record_storage_operation,
        record_storage_operation_for_target, set_transact_item_table_name,
        transact_item_table_name, validate_transact_write_item_expression_usage,
        write_bulk_ops::RoutedWriteDispatchKey,
    },
    namespace_routing::{NamespaceRouteRecord, NamespaceStorageMode},
    point_read_cache::{AuthoritativePointReadPurpose, PointReadGetRequest},
    updated_at_apply::{refresh_existing_transact_write_item_timestamp, stamp_transact_write_item},
};

struct PreparedTransactWriteItems {
    request: TransactWriteItemsRequest,
    client_request_token: Option<String>,
    return_consumed_capacity: Option<String>,
    return_item_collection_metrics: Option<String>,
    pending_routes: HashMap<TableNamespace, NamespaceRouteRecord>,
    cache_effects: storage_cache::RuntimeWriteEffects,
}

#[derive(Default)]
struct RoutedTransactWriteItems {
    request: TransactWriteItemsRequest,
    shared_table_namespaces: Vec<Option<TableNamespace>>,
}

impl DatabaseManager {
    pub async fn transact_write_items(
        &self,
        request: TransactWriteItemsRequest,
    ) -> StorageResult<TransactWriteItemsResponse> {
        let prepared = self.prepare_transact_write_items(request).await?;
        if self.single_node_sync_mode_enabled() {
            return self.transact_write_items_single_node_sync(prepared).await;
        }
        if self.route_resolver.is_none() {
            return self.transact_write_items_unrouted(prepared).await;
        }
        self.transact_write_items_routed(prepared).await
    }

    async fn prepare_transact_write_items(
        &self,
        request: TransactWriteItemsRequest,
    ) -> StorageResult<PreparedTransactWriteItems> {
        let mut request = request;
        let client_request_token = request.client_request_token.clone();
        let return_consumed_capacity = request.return_consumed_capacity.clone();
        let return_item_collection_metrics = request.return_item_collection_metrics.clone();

        for item in &mut request.transact_items {
            if self.single_table_mode_enabled() {
                stamp_transact_write_item(item)?;
            } else {
                refresh_existing_transact_write_item_timestamp(item)?;
            }
            validate_transact_write_item_expression_usage(item)?;
        }
        let pending_routes =
            Self::pending_namespace_routes_from_transact_write_items(&request.transact_items)?;
        let cache_effects = self
            .plan_transact_write_cache_effects(&request.transact_items, &pending_routes)
            .await?;
        Ok(PreparedTransactWriteItems {
            request,
            client_request_token,
            return_consumed_capacity,
            return_item_collection_metrics,
            pending_routes,
            cache_effects,
        })
    }

    #[cfg(feature = "cache-write-planner")]
    async fn plan_transact_write_cache_effects(
        &self,
        transact_items: &[TransactWriteItem],
        pending_routes: &HashMap<TableNamespace, NamespaceRouteRecord>,
    ) -> StorageResult<storage_cache::RuntimeWriteEffects> {
        if !self.cache_write_effects_enabled() {
            return Ok(self.empty_cache_write_effects());
        }
        self.cache_write_planner()
            .plan_transact_write_cache_effects(transact_items, pending_routes)
            .await
    }

    #[cfg(not(feature = "cache-write-planner"))]
    async fn plan_transact_write_cache_effects(
        &self,
        _transact_items: &[TransactWriteItem],
        _pending_routes: &HashMap<TableNamespace, NamespaceRouteRecord>,
    ) -> StorageResult<storage_cache::RuntimeWriteEffects> {
        Ok(self.empty_cache_write_effects())
    }

    async fn transact_write_items_single_node_sync(
        &self,
        prepared: PreparedTransactWriteItems,
    ) -> StorageResult<TransactWriteItemsResponse> {
        let PreparedTransactWriteItems {
            request,
            client_request_token,
            cache_effects,
            ..
        } = prepared;
        if client_request_token.is_some() {
            return Err(StorageError::unsupported(
                "TransactWriteItems client request tokens are not implemented for single-node \
                 sync routing yet",
            ));
        }
        self.execute_with_cache_effects(
            PreparedCacheWrite::Effects(cache_effects.clone()),
            || async {
                self.run_single_node_sync_write_request(
                    "transact_write_items",
                    SyncWriteRequest::TransactWriteItems(request),
                )
                .await?;
                self.maybe_pause_after_storage_write_for_test().await;
                self.maybe_run_gsi_maintenance().await;
                Ok(TransactWriteItemsResponse {
                    consumed_capacity: None,
                    item_collection_metrics: None,
                })
            },
            |response| async { Ok((response, cache_effects)) },
        )
        .await
    }

    async fn transact_write_items_unrouted(
        &self,
        prepared: PreparedTransactWriteItems,
    ) -> StorageResult<TransactWriteItemsResponse> {
        if let Some(response) = self
            .try_cached_guarded_transact_write_items(
                &prepared.request,
                prepared.cache_effects.clone(),
            )
            .await?
        {
            return Ok(response);
        }
        let PreparedTransactWriteItems {
            request,
            cache_effects,
            ..
        } = prepared;
        self.execute_with_cache_effects(
            PreparedCacheWrite::Effects(cache_effects.clone()),
            || async {
                let response = record_storage_operation(
                    "transact_write_items",
                    self.storage.transact_write_items(request),
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

    async fn transact_write_items_routed(
        &self,
        prepared: PreparedTransactWriteItems,
    ) -> StorageResult<TransactWriteItemsResponse> {
        let PreparedTransactWriteItems {
            request,
            client_request_token,
            return_consumed_capacity,
            return_item_collection_metrics,
            pending_routes,
            cache_effects,
        } = prepared;
        let per_connection = self
            .plan_routed_transact_write_items(request.transact_items, &pending_routes)
            .await?;
        self.execute_with_cache_effects(
            PreparedCacheWrite::Effects(cache_effects.clone()),
            || async {
                self.execute_routed_transact_write_items(
                    per_connection,
                    client_request_token,
                    return_consumed_capacity,
                    return_item_collection_metrics,
                )
                .await
            },
            |response| async { Ok((response, cache_effects)) },
        )
        .await
    }

    async fn plan_routed_transact_write_items(
        &self,
        transact_items: Vec<TransactWriteItem>,
        pending_routes: &HashMap<TableNamespace, NamespaceRouteRecord>,
    ) -> StorageResult<BTreeMap<RoutedWriteDispatchKey, RoutedTransactWriteItems>> {
        let default_connection_id = ROUTED_DEFAULT_CONNECTION_ID.to_string();
        let mut per_connection: BTreeMap<RoutedWriteDispatchKey, RoutedTransactWriteItems> =
            BTreeMap::new();
        for item in transact_items {
            let logical_table = transact_item_table_name(&item)?;
            let route = self
                .resolve_namespace_route_for_table_with_pending(&logical_table, pending_routes)
                .await?;
            if let Some(route) = route {
                ensure_route_writes_not_paused(&route)?;
                let mut routed_item = item;
                let shared_table_namespace =
                    (route.storage_mode == NamespaceStorageMode::SharedTable)
                        .then(|| route.namespace.clone());
                if route.storage_mode == NamespaceStorageMode::SharedTable {
                    self.request_rewriter
                        .rewrite_transact_item_for_shared_table(
                            &route.namespace,
                            &mut routed_item,
                        )?;
                }

                fan_out_route_write_payload(
                    &route,
                    routed_item,
                    "transact_write_items.item",
                    |target, target_role, mut routed_item_for_target| {
                        set_transact_item_table_name(
                            &mut routed_item_for_target,
                            target.table_name.clone(),
                        );
                        let batch = per_connection
                            .entry(RoutedWriteDispatchKey {
                                connection_id: target.connection_id.clone(),
                                target_role,
                            })
                            .or_default();
                        batch
                            .request
                            .transact_items
                            .push(routed_item_for_target);
                        batch
                            .shared_table_namespaces
                            .push(shared_table_namespace.clone());
                        Ok(())
                    },
                )?;
            } else {
                let batch = per_connection
                    .entry(RoutedWriteDispatchKey {
                        connection_id: default_connection_id.clone(),
                        target_role: RoutedWriteTargetRole::Primary,
                    })
                    .or_default();
                batch.request.transact_items.push(item);
                batch.shared_table_namespaces.push(None);
            }
        }
        Ok(per_connection)
    }

    async fn execute_routed_transact_write_items(
        &self,
        per_connection: BTreeMap<RoutedWriteDispatchKey, RoutedTransactWriteItems>,
        client_request_token: Option<String>,
        return_consumed_capacity: Option<String>,
        return_item_collection_metrics: Option<String>,
    ) -> StorageResult<TransactWriteItemsResponse> {
        let mut primary_response: Option<TransactWriteItemsResponse> = None;
        for (dispatch_key, mut batch) in per_connection {
            let request_for_connection = &mut batch.request;
            request_for_connection.client_request_token = client_request_token.clone();
            request_for_connection.return_consumed_capacity = return_consumed_capacity.clone();
            request_for_connection.return_item_collection_metrics =
                return_item_collection_metrics.clone();

            let provider = self.provider_for_request_connection(&dispatch_key.connection_id)?;
            let response = record_storage_operation_for_target(
                "transact_write_items",
                dispatch_key.target_role,
                provider.transact_write_items(batch.request),
            )
            .await
            .map_err(|error| {
                self.normalize_routed_transaction_error(
                    error,
                    &batch.shared_table_namespaces,
                )
            })?;
            if primary_response.is_none() {
                primary_response = Some(response);
            }
            self.maybe_run_gsi_maintenance_for_connection(&dispatch_key.connection_id)
                .await?;
        }

        primary_response.ok_or_else(|| {
            StorageError::internal("transact_write_items routing produced no write targets")
        })
    }

    fn normalize_routed_transaction_error(
        &self,
        error: StorageError,
        shared_table_namespaces: &[Option<TableNamespace>],
    ) -> StorageError {
        let StorageEnum::TransactionCanceled { reasons } = error.to_enum() else {
            return error;
        };
        let mut reasons = reasons.clone();
        for (reason, namespace) in reasons.iter_mut().zip(shared_table_namespaces) {
            let Some(namespace) = namespace else {
                continue;
            };
            let mut parts = reason.splitn(3, '\t');
            if parts.next() != Some("ConditionalCheckFailed") {
                continue;
            }
            let (Some(message), Some(item)) = (parts.next(), parts.next()) else {
                continue;
            };
            let mut item: HashMap<String, AttributeValue> = match serde_json::from_str(item) {
                Ok(item) => item,
                Err(error) => return error.into(),
            };
            if let Err(error) = self
                .request_rewriter
                .normalize_item_from_shared_table(namespace, &mut item)
            {
                return error;
            }
            let item = match serde_json::to_string(&item) {
                Ok(item) => item,
                Err(error) => return error.into(),
            };
            *reason = format!("ConditionalCheckFailed\t{message}\t{item}");
        }
        StorageEnum::TransactionCanceled { reasons }.into()
    }

    async fn try_cached_guarded_transact_write_items(
        &self,
        request: &TransactWriteItemsRequest,
        cache_effects: storage_cache::RuntimeWriteEffects,
    ) -> StorageResult<Option<TransactWriteItemsResponse>> {
        if !self.cache_services.authoritative_write_preimages_enabled() {
            return Ok(None);
        }
        if !self.storage.supports_guarded_transaction_writes() {
            guarded_write::record_unsupported_fallback("transact_write_items");
            return Ok(None);
        }
        let guards = self
            .cached_transaction_guards(&request.transact_items)
            .await?;
        if guards.is_empty() {
            return Ok(None);
        }

        self.cache_services
            .prepare_write_intents(&cache_effects)
            .await?;
        let guarded_request = GuardedTransactWriteItemsRequest {
            request: request.clone(),
            guards,
        };
        let response = match record_storage_operation(
            "guarded_transact_write_items",
            self.storage.guarded_transact_write_items(guarded_request),
        )
        .await
        {
            Ok(response) => response,
            Err(error) if guarded_write::should_fallback(&error) => {
                guarded_write::record_fallback("transact_write_items", &error);
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

    async fn cached_transaction_guards(
        &self,
        items: &[TransactWriteItem],
    ) -> StorageResult<Vec<DurableTransactWriteGuard>> {
        let mut guards = Vec::new();
        for item in items {
            let Some(request) = self.transact_item_point_read_request(item).await? else {
                continue;
            };
            let Some(preimage) = guarded_write::authoritative_preimage(
                self,
                &request,
                AuthoritativePointReadPurpose::TransactionPreImage,
            )
            .await?
            else {
                continue;
            };
            guards.push(preimage.into_transaction_guard(&request));
        }
        Ok(guards)
    }

    async fn transact_item_point_read_request(
        &self,
        item: &TransactWriteItem,
    ) -> StorageResult<Option<PointReadGetRequest>> {
        if let Some(put) = &item.put {
            let table_info = self.get_table_info_arc(&put.table_name).await?;
            let key = storage_provider::StorageProvider::get_key_attributes(
                self.storage.as_ref(),
                &put.item,
                &table_info.key_schema,
            )?;
            return Ok(Some(PointReadGetRequest {
                table_name: put.table_name.clone(),
                key,
            }));
        }
        if let Some(update) = &item.update {
            return Ok(Some(PointReadGetRequest {
                table_name: update.table_name.clone(),
                key: update.key.clone(),
            }));
        }
        if let Some(delete) = &item.delete {
            return Ok(Some(PointReadGetRequest {
                table_name: delete.table_name.clone(),
                key: delete.key.clone(),
            }));
        }
        if let Some(condition_check) = &item.condition_check {
            return Ok(Some(PointReadGetRequest {
                table_name: condition_check.table_name.clone(),
                key: condition_check.key.clone(),
            }));
        }
        Ok(None)
    }
}
