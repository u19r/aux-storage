use std::collections::HashMap;

use storage_cache::{
    RuntimeIndexTransitionTarget, RuntimePendingIndexTransition,
    RuntimePendingIndexTransitionTarget, collect_base_writes_for_batch_write,
    collect_base_writes_for_batch_write_encode, collect_base_writes_for_transact_write_items,
    collect_base_writes_for_transact_write_items_encode,
    collect_pending_index_transition_update_lookups,
    collect_pending_query_proof_targets_for_transact_write_items,
    collect_pending_query_proof_targets_for_transact_write_items_encode,
    collect_point_read_mutations_for_batch_write,
    collect_point_read_mutations_for_batch_write_encode,
    collect_point_read_mutations_for_transact_write_items,
    collect_point_read_mutations_for_transact_write_items_encode,
    collect_query_proof_targets_for_batch_write,
    collect_query_proof_targets_for_batch_write_encode, collect_transact_write_encode_table_names,
    collect_transact_write_table_names, compose_write_effects, finalize_pending_index_transitions,
    maybe_indexed_table_info,
};
use storage_types::{
    BatchWriteItemEncodeRequest, BatchWriteItemRequest, StorageResult, StoredTableInfo, TableName,
    TableNamespace, TransactEncodeItem, TransactWriteItem,
};

use crate::{
    cache_write_planner::{
        PointReadCacheMutation, QueryProofIndexTransition, StorageCachePlannerLoad,
        StorageCacheWriteEffects, StorageCacheWritePlanner,
    },
    namespace_routing::NamespaceRouteRecord,
};

#[cfg_attr(not(feature = "cache-write-planner"), allow(dead_code))]
impl<'a, L> StorageCacheWritePlanner<'a, L>
where L: StorageCachePlannerLoad
{
    fn filter_indexed_table_infos(
        &self,
        table_infos: HashMap<TableName, StoredTableInfo>,
    ) -> HashMap<TableName, StoredTableInfo> {
        table_infos
            .into_iter()
            .filter_map(|(table_name, table_info)| {
                maybe_indexed_table_info(self.query_proof_enabled, table_info)
                    .map(|table_info| (table_name, table_info))
            })
            .collect()
    }

    async fn resolve_index_transition_targets(
        &self,
        targets: Vec<RuntimeIndexTransitionTarget>,
    ) -> StorageResult<Vec<QueryProofIndexTransition>> {
        let mut transitions = Vec::with_capacity(targets.len());
        for target in targets {
            let old_item = self
                .loader
                .get_item_map_with_consistent_read_for_cache(
                    target.table_name.clone(),
                    target.old_item_lookup_key.clone(),
                    true,
                )
                .await?;
            transitions.push(target.build(old_item));
        }
        Ok(transitions)
    }

    async fn resolve_pending_index_transition_targets(
        &self,
        targets: Vec<RuntimePendingIndexTransitionTarget>,
        pending_routes: &HashMap<TableNamespace, NamespaceRouteRecord>,
    ) -> StorageResult<Vec<RuntimePendingIndexTransition>> {
        let mut transitions = Vec::with_capacity(targets.len());
        for target in targets {
            let old_item = self
                .loader
                .get_item_map_with_consistent_read_with_pending_for_cache(
                    target.table_name.clone(),
                    target.old_item_lookup_key.clone(),
                    true,
                    pending_routes,
                )
                .await?;
            transitions.push(target.build(old_item));
        }
        Ok(transitions)
    }

    async fn table_infos_for_tables<'b, I>(
        &self,
        table_names: I,
    ) -> StorageResult<HashMap<TableName, StoredTableInfo>>
    where
        I: IntoIterator<Item = &'b TableName>,
    {
        let mut table_infos = HashMap::new();
        for table_name in table_names {
            if table_infos.contains_key(table_name) {
                continue;
            }
            table_infos.insert(
                table_name.clone(),
                self.loader.get_table_info_for_cache(table_name).await?,
            );
        }
        Ok(table_infos)
    }

    async fn table_infos_with_pending_for_tables<'b, I>(
        &self,
        table_names: I,
        pending_routes: &HashMap<TableNamespace, NamespaceRouteRecord>,
    ) -> StorageResult<HashMap<TableName, StoredTableInfo>>
    where
        I: IntoIterator<Item = &'b TableName>,
    {
        let mut table_infos = HashMap::new();
        for table_name in table_names {
            if table_infos.contains_key(table_name) {
                continue;
            }
            table_infos.insert(
                table_name.clone(),
                self.loader
                    .get_table_info_with_pending_for_cache(table_name, pending_routes)
                    .await?,
            );
        }
        Ok(table_infos)
    }

    pub(crate) async fn cache_effects_for_batch_write(
        &self,
        request: &BatchWriteItemRequest,
        index_transitions: Vec<QueryProofIndexTransition>,
    ) -> StorageResult<StorageCacheWriteEffects> {
        let table_infos = self
            .table_infos_for_tables(request.request_items.keys())
            .await?;
        Ok(compose_write_effects(
            collect_point_read_mutations_for_batch_write(request, &table_infos)?,
            collect_base_writes_for_batch_write(request, &table_infos),
            index_transitions,
            self.query_proof_enabled,
        ))
    }

    pub(crate) async fn plan_batch_write_cache_effects(
        &self,
        request: &BatchWriteItemRequest,
    ) -> StorageResult<StorageCacheWriteEffects> {
        if !cfg!(feature = "cache-write-planner") {
            return Ok(StorageCacheWriteEffects {
                point_read: Vec::new(),
                query_proof: Vec::new(),
            });
        }
        self.cache_effects_for_batch_write(
            request,
            self.query_proof_cache_index_transitions_for_batch_write(request)
                .await?,
        )
        .await
    }

    pub(crate) async fn cache_effects_for_batch_write_encode(
        &self,
        request: &BatchWriteItemEncodeRequest,
        index_transitions: Vec<QueryProofIndexTransition>,
    ) -> StorageResult<StorageCacheWriteEffects> {
        let table_infos = self
            .table_infos_for_tables(request.request_items.keys())
            .await?;
        Ok(compose_write_effects(
            collect_point_read_mutations_for_batch_write_encode(request, &table_infos)?,
            collect_base_writes_for_batch_write_encode(request, &table_infos)?,
            index_transitions,
            self.query_proof_enabled,
        ))
    }

    pub(crate) async fn plan_batch_write_encode_cache_effects(
        &self,
        request: &BatchWriteItemEncodeRequest,
    ) -> StorageResult<StorageCacheWriteEffects> {
        if !cfg!(feature = "cache-write-planner") {
            return Ok(StorageCacheWriteEffects {
                point_read: Vec::new(),
                query_proof: Vec::new(),
            });
        }
        self.cache_effects_for_batch_write_encode(
            request,
            self.query_proof_cache_index_transitions_for_batch_write_encode(request)
                .await?,
        )
        .await
    }

    pub(crate) async fn cache_effects_for_transact_write_items(
        &self,
        transact_items: &[TransactWriteItem],
        index_transitions: Vec<QueryProofIndexTransition>,
        point_read: Vec<PointReadCacheMutation>,
    ) -> StorageResult<StorageCacheWriteEffects> {
        let table_infos = self
            .table_infos_for_tables(collect_transact_write_table_names(transact_items).iter())
            .await?;
        Ok(compose_write_effects(
            point_read,
            collect_base_writes_for_transact_write_items(transact_items, &table_infos),
            index_transitions,
            self.query_proof_enabled,
        ))
    }

    pub(crate) async fn plan_transact_write_cache_effects(
        &self,
        transact_items: &[TransactWriteItem],
        pending_routes: &HashMap<TableNamespace, NamespaceRouteRecord>,
    ) -> StorageResult<StorageCacheWriteEffects> {
        if !cfg!(feature = "cache-write-planner") {
            return Ok(StorageCacheWriteEffects {
                point_read: Vec::new(),
                query_proof: Vec::new(),
            });
        }
        self.cache_effects_for_transact_write_items(
            transact_items,
            self.query_proof_index_transitions_for_transact_write_items(
                transact_items,
                pending_routes,
            )
            .await?,
            self.point_read_cache_mutations_for_transact_write_items(
                transact_items,
                pending_routes,
            )
            .await?,
        )
        .await
    }

    pub(crate) async fn cache_effects_for_transact_write_items_encode(
        &self,
        transact_items: &[TransactEncodeItem],
        index_transitions: Vec<QueryProofIndexTransition>,
        point_read: Vec<PointReadCacheMutation>,
    ) -> StorageResult<StorageCacheWriteEffects> {
        let table_infos = self
            .table_infos_for_tables(
                collect_transact_write_encode_table_names(transact_items).iter(),
            )
            .await?;
        Ok(compose_write_effects(
            point_read,
            collect_base_writes_for_transact_write_items_encode(transact_items, &table_infos)?,
            index_transitions,
            self.query_proof_enabled,
        ))
    }

    pub(crate) async fn plan_transact_write_encode_cache_effects(
        &self,
        transact_items: &[TransactEncodeItem],
        pending_routes: &HashMap<TableNamespace, NamespaceRouteRecord>,
    ) -> StorageResult<StorageCacheWriteEffects> {
        if !cfg!(feature = "cache-write-planner") {
            return Ok(StorageCacheWriteEffects {
                point_read: Vec::new(),
                query_proof: Vec::new(),
            });
        }
        self.cache_effects_for_transact_write_items_encode(
            transact_items,
            self.query_proof_index_transitions_for_transact_write_items_encode(
                transact_items,
                pending_routes,
            )
            .await?,
            self.point_read_cache_mutations_for_transact_write_items_encode(
                transact_items,
                pending_routes,
            )
            .await?,
        )
        .await
    }

    pub(crate) async fn query_proof_cache_index_transitions_for_batch_write(
        &self,
        request: &BatchWriteItemRequest,
    ) -> StorageResult<Vec<QueryProofIndexTransition>> {
        let indexed_table_infos = self.filter_indexed_table_infos(
            self.table_infos_for_tables(request.request_items.keys())
                .await?,
        );
        self.resolve_index_transition_targets(collect_query_proof_targets_for_batch_write(
            request,
            &indexed_table_infos,
        )?)
        .await
    }

    pub(crate) async fn query_proof_cache_index_transitions_for_batch_write_encode(
        &self,
        request: &BatchWriteItemEncodeRequest,
    ) -> StorageResult<Vec<QueryProofIndexTransition>> {
        let indexed_table_infos = self.filter_indexed_table_infos(
            self.table_infos_for_tables(request.request_items.keys())
                .await?,
        );
        self.resolve_index_transition_targets(collect_query_proof_targets_for_batch_write_encode(
            request,
            &indexed_table_infos,
        )?)
        .await
    }

    pub(crate) async fn query_proof_index_transitions_for_transact_write_items(
        &self,
        transact_items: &[TransactWriteItem],
        pending_routes: &HashMap<TableNamespace, NamespaceRouteRecord>,
    ) -> StorageResult<Vec<QueryProofIndexTransition>> {
        self.resolve_pending_query_proof_index_transitions(
            self.pending_query_proof_index_transitions_for_transact_write_items(
                transact_items,
                pending_routes,
            )
            .await?,
        )
        .await
    }

    async fn pending_query_proof_index_transitions_for_transact_write_items(
        &self,
        transact_items: &[TransactWriteItem],
        pending_routes: &HashMap<TableNamespace, NamespaceRouteRecord>,
    ) -> StorageResult<Vec<RuntimePendingIndexTransition>> {
        let indexed_table_infos = self.filter_indexed_table_infos(
            self.table_infos_with_pending_for_tables(
                collect_transact_write_table_names(transact_items).iter(),
                pending_routes,
            )
            .await?,
        );
        self.resolve_pending_index_transition_targets(
            collect_pending_query_proof_targets_for_transact_write_items(
                transact_items,
                &indexed_table_infos,
            )?,
            pending_routes,
        )
        .await
    }

    pub(crate) async fn query_proof_index_transitions_for_transact_write_items_encode(
        &self,
        transact_items: &[TransactEncodeItem],
        pending_routes: &HashMap<TableNamespace, NamespaceRouteRecord>,
    ) -> StorageResult<Vec<QueryProofIndexTransition>> {
        self.resolve_pending_query_proof_index_transitions(
            self.pending_query_proof_index_transitions_for_transact_write_items_encode(
                transact_items,
                pending_routes,
            )
            .await?,
        )
        .await
    }

    async fn pending_query_proof_index_transitions_for_transact_write_items_encode(
        &self,
        transact_items: &[TransactEncodeItem],
        pending_routes: &HashMap<TableNamespace, NamespaceRouteRecord>,
    ) -> StorageResult<Vec<RuntimePendingIndexTransition>> {
        let indexed_table_infos = self.filter_indexed_table_infos(
            self.table_infos_with_pending_for_tables(
                collect_transact_write_encode_table_names(transact_items).iter(),
                pending_routes,
            )
            .await?,
        );
        self.resolve_pending_index_transition_targets(
            collect_pending_query_proof_targets_for_transact_write_items_encode(
                transact_items,
                &indexed_table_infos,
            )?,
            pending_routes,
        )
        .await
    }

    async fn resolve_pending_query_proof_index_transitions(
        &self,
        transitions: Vec<RuntimePendingIndexTransition>,
    ) -> StorageResult<Vec<QueryProofIndexTransition>> {
        let mut resolved_update_items = Vec::with_capacity(transitions.len());
        for update_lookup in collect_pending_index_transition_update_lookups(&transitions) {
            let new_item = match update_lookup {
                Some((table_name, key)) => {
                    self.loader
                        .get_item_map_with_consistent_read_for_cache(table_name, key, true)
                        .await?
                }
                None => None,
            };
            resolved_update_items.push(new_item);
        }
        finalize_pending_index_transitions(transitions, resolved_update_items)
    }

    pub(crate) async fn point_read_cache_mutations_for_transact_write_items(
        &self,
        transact_items: &[TransactWriteItem],
        pending_routes: &HashMap<TableNamespace, NamespaceRouteRecord>,
    ) -> StorageResult<Vec<PointReadCacheMutation>> {
        let table_infos = self
            .table_infos_with_pending_for_tables(
                collect_transact_write_table_names(transact_items).iter(),
                pending_routes,
            )
            .await?;
        collect_point_read_mutations_for_transact_write_items(transact_items, &table_infos)
    }

    pub(crate) async fn point_read_cache_mutations_for_transact_write_items_encode(
        &self,
        transact_items: &[TransactEncodeItem],
        pending_routes: &HashMap<TableNamespace, NamespaceRouteRecord>,
    ) -> StorageResult<Vec<PointReadCacheMutation>> {
        let table_infos = self
            .table_infos_with_pending_for_tables(
                collect_transact_write_encode_table_names(transact_items).iter(),
                pending_routes,
            )
            .await?;
        collect_point_read_mutations_for_transact_write_items_encode(transact_items, &table_infos)
    }
}
