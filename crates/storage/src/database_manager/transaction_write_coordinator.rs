use std::{collections::BTreeMap, time::Instant};

use storage_sync::SyncWriteRequest;
use storage_types::{
    DurableTransactWriteGuard, GuardedTransactWriteItemsRequest, StorageError, StorageResult,
    TransactWriteItem, TransactWriteItemsRequest, TransactWriteItemsResponse,
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
    namespace_routing::NamespaceStorageMode,
    point_read_cache::{AuthoritativePointReadPurpose, PointReadGetRequest},
    updated_at_apply::{refresh_existing_transact_write_item_timestamp, stamp_transact_write_item},
};

impl DatabaseManager {
    pub async fn transact_write_items(
        &self,
        request: TransactWriteItemsRequest,
    ) -> StorageResult<TransactWriteItemsResponse> {
        let started = Instant::now();
        let mut request = request;
        let client_request_token = request.client_request_token.clone();
        let return_consumed_capacity = request.return_consumed_capacity.clone();
        let return_item_collection_metrics = request.return_item_collection_metrics.clone();
        let operation_count = request.transact_items.len();

        for item in &mut request.transact_items {
            if self.single_table_mode_enabled() {
                stamp_transact_write_item(item)?;
            } else {
                refresh_existing_transact_write_item_timestamp(item)?;
            }
            validate_transact_write_item_expression_usage(item)?;
        }
        let validation_ms = started.elapsed().as_secs_f64() * 1000.0;
        let route_started = Instant::now();
        let pending_routes =
            Self::pending_namespace_routes_from_transact_write_items(&request.transact_items)?;
        let routing_prepare_ms = route_started.elapsed().as_secs_f64() * 1000.0;
        let cache_plan_started = Instant::now();
        let cache_effects = if self.cache_write_effects_enabled() {
            let cache_write_planner = self.cache_write_planner();
            cache_write_planner
                .plan_transact_write_cache_effects(&request.transact_items, &pending_routes)
                .await?
        } else {
            self.empty_cache_write_effects()
        };
        let cache_plan_ms = cache_plan_started.elapsed().as_secs_f64() * 1000.0;
        if self.single_node_sync_mode_enabled() {
            if client_request_token.is_some() {
                return Err(StorageError::unsupported(
                    "TransactWriteItems client request tokens are not implemented for single-node \
                     sync routing yet",
                ));
            }
            return self
                .execute_with_cache_effects(
                    PreparedCacheWrite::Effects(cache_effects.clone()),
                    || async {
                        let storage_started = Instant::now();
                        self.run_single_node_sync_write_request(
                            "transact_write_items",
                            SyncWriteRequest::TransactWriteItems(request),
                        )
                        .await?;
                        let storage_ms = storage_started.elapsed().as_secs_f64() * 1000.0;
                        self.maybe_pause_after_storage_write_for_test().await;
                        self.maybe_run_gsi_maintenance().await;
                        tracing::debug!(
                            operation_count,
                            validation_ms,
                            routing_prepare_ms,
                            cache_plan_ms,
                            storage_ms,
                            total_ms = started.elapsed().as_secs_f64() * 1000.0,
                            "storage sync transact_write_items phase timing"
                        );
                        Ok(TransactWriteItemsResponse {
                            consumed_capacity: None,
                            item_collection_metrics: None,
                        })
                    },
                    |response| async { Ok((response, cache_effects)) },
                )
                .await;
        }
        if self.route_resolver.is_none() {
            if let Some(response) = self
                .try_cached_guarded_transact_write_items(
                    &request,
                    cache_effects.clone(),
                    TransactionTiming {
                        operation_count,
                        validation_ms,
                        routing_prepare_ms,
                        cache_plan_ms,
                        started,
                    },
                )
                .await?
            {
                return Ok(response);
            }
            return self
                .execute_with_cache_effects(
                    PreparedCacheWrite::Effects(cache_effects.clone()),
                    || async {
                        let storage_started = Instant::now();
                        let response = record_storage_operation(
                            "transact_write_items",
                            self.storage.transact_write_items(request),
                        )
                        .await?;
                        let storage_ms = storage_started.elapsed().as_secs_f64() * 1000.0;
                        self.maybe_pause_after_storage_write_for_test().await;
                        self.maybe_run_gsi_maintenance().await;
                        tracing::debug!(
                            operation_count,
                            validation_ms,
                            routing_prepare_ms,
                            cache_plan_ms,
                            storage_ms,
                            total_ms = started.elapsed().as_secs_f64() * 1000.0,
                            "storage transact_write_items phase timing"
                        );
                        Ok(response)
                    },
                    |response| async { Ok((response, cache_effects)) },
                )
                .await;
        }

        let default_connection_id = ROUTED_DEFAULT_CONNECTION_ID.to_string();
        let mut per_connection: BTreeMap<RoutedWriteDispatchKey, TransactWriteItemsRequest> =
            BTreeMap::new();
        for item in request.transact_items {
            let logical_table = transact_item_table_name(&item)?;
            let route = self
                .resolve_namespace_route_for_table(&logical_table)
                .await?;
            if let Some(route) = route {
                ensure_route_writes_not_paused(&route)?;
                let mut routed_item = item;
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

        self.execute_with_cache_effects(
            PreparedCacheWrite::Effects(cache_effects.clone()),
            || async {
                let storage_started = Instant::now();
                let mut primary_response: Option<TransactWriteItemsResponse> = None;
                let mut dispatch_count = 0usize;
                for (dispatch_key, mut request_for_connection) in per_connection {
                    request_for_connection.client_request_token = client_request_token.clone();
                    request_for_connection.return_consumed_capacity =
                        return_consumed_capacity.clone();
                    request_for_connection.return_item_collection_metrics =
                        return_item_collection_metrics.clone();

                    let provider =
                        self.provider_for_request_connection(&dispatch_key.connection_id)?;
                    let response = record_storage_operation_for_target(
                        "transact_write_items",
                        dispatch_key.target_role,
                        provider.transact_write_items(request_for_connection),
                    )
                    .await?;
                    if primary_response.is_none() {
                        primary_response = Some(response);
                    }
                    dispatch_count += 1;
                    self.maybe_run_gsi_maintenance_for_connection(&dispatch_key.connection_id)
                        .await?;
                }

                let response = primary_response.ok_or_else(|| {
                    StorageError::internal("transact_write_items routing produced no write targets")
                })?;
                tracing::debug!(
                    operation_count,
                    validation_ms,
                    routing_prepare_ms,
                    cache_plan_ms,
                    storage_ms = storage_started.elapsed().as_secs_f64() * 1000.0,
                    dispatch_count,
                    total_ms = started.elapsed().as_secs_f64() * 1000.0,
                    "storage transact_write_items phase timing"
                );
                Ok(response)
            },
            |response| async { Ok((response, cache_effects)) },
        )
        .await
    }

    async fn try_cached_guarded_transact_write_items(
        &self,
        request: &TransactWriteItemsRequest,
        cache_effects: storage_cache::RuntimeWriteEffects,
        timing: TransactionTiming,
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
        let storage_started = Instant::now();
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
        tracing::debug!(
            operation_count = timing.operation_count,
            validation_ms = timing.validation_ms,
            routing_prepare_ms = timing.routing_prepare_ms,
            cache_plan_ms = timing.cache_plan_ms,
            storage_ms = storage_started.elapsed().as_secs_f64() * 1000.0,
            total_ms = timing.started.elapsed().as_secs_f64() * 1000.0,
            "storage guarded transact_write_items phase timing"
        );
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

struct TransactionTiming {
    operation_count: usize,
    validation_ms: f64,
    routing_prepare_ms: f64,
    cache_plan_ms: f64,
    started: Instant,
}
