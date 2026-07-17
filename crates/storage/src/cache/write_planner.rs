use std::collections::HashMap;

use storage_cache::{
    RuntimeIndexTransition, RuntimePreparedIndexPrewrite, RuntimePreparedUpdateCacheWrite,
    RuntimeWriteEffects, build_delete_item_cache_effects, build_put_item_cache_effects,
    extract_primary_key_from_item, finalize_update_cache_effects, maybe_indexed_table_info,
    maybe_prepare_index_prewrite, prepare_update_cache_write,
};
use storage_types::{
    AttributeValue, KeyAttributes, StorageResult, StoredTableInfo, TableName, TableNamespace,
    WireItem,
};

use crate::{database_manager::ResolvedStorageOperation, namespace_routing::NamespaceRouteRecord};

pub(crate) type QueryProofIndexTransition = RuntimeIndexTransition;
pub(crate) type PointReadCacheMutation = storage_cache::RuntimePointReadMutation;
pub(crate) type StorageCacheWriteEffects = RuntimeWriteEffects;
pub(crate) type PreparedQueryProofPrewriteImage = RuntimePreparedIndexPrewrite;
pub(crate) type PreparedUpdateCacheWrite = RuntimePreparedUpdateCacheWrite;

pub(crate) trait StorageCachePlannerLoad {
    async fn get_table_info_for_cache(
        &self,
        table_name: &TableName,
    ) -> StorageResult<StoredTableInfo>;

    async fn get_table_info_with_pending_for_cache(
        &self,
        table_name: &TableName,
        pending_routes: &HashMap<TableNamespace, NamespaceRouteRecord>,
    ) -> StorageResult<StoredTableInfo>;

    async fn get_item_map_with_resolved_operation_for_cache(
        &self,
        operation: &ResolvedStorageOperation,
        key: KeyAttributes,
        consistent_read: bool,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>>;

    async fn get_item_map_with_consistent_read_for_cache(
        &self,
        table_name: TableName,
        key: KeyAttributes,
        consistent_read: bool,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>>;

    async fn get_item_map_with_consistent_read_with_pending_for_cache(
        &self,
        table_name: TableName,
        key: KeyAttributes,
        consistent_read: bool,
        pending_routes: &HashMap<TableNamespace, NamespaceRouteRecord>,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>>;
}

pub(crate) struct StorageCacheWritePlanner<'a, L> {
    pub(crate) loader: &'a L,
    pub(crate) query_proof_enabled: bool,
}

#[cfg_attr(not(feature = "cache-write-planner"), allow(dead_code))]
impl<'a, L> StorageCacheWritePlanner<'a, L>
where L: StorageCachePlannerLoad
{
    pub(crate) fn new(loader: &'a L, query_proof_enabled: bool) -> Self {
        Self {
            loader,
            query_proof_enabled,
        }
    }

    #[cfg_attr(not(feature = "cache-write-planner"), allow(dead_code))]
    pub(crate) async fn plan_put_item_cache_effects(
        &self,
        table_name: &TableName,
        operation: &ResolvedStorageOperation,
        logical_item: &HashMap<String, AttributeValue>,
    ) -> StorageResult<StorageCacheWriteEffects> {
        if !cfg!(feature = "cache-write-planner") {
            return Ok(RuntimeWriteEffects {
                point_read: Vec::new(),
                query_proof: Vec::new(),
            });
        }
        let table_info = operation.table_info();
        let query_proof_key = extract_primary_key_from_item(&table_info.key_schema, logical_item)?;
        let query_proof_prewrite = self
            .query_proof_cache_maybe_load_gsi_prewrite_image_with_table_info(
                &query_proof_key,
                operation,
            )
            .await?;
        build_put_item_cache_effects(
            table_name,
            table_info.clone(),
            logical_item,
            query_proof_prewrite,
            self.query_proof_enabled,
        )
    }

    async fn query_proof_cache_maybe_load_gsi_prewrite_image_with_table_info(
        &self,
        key: &KeyAttributes,
        operation: &ResolvedStorageOperation,
    ) -> StorageResult<Option<PreparedQueryProofPrewriteImage>> {
        let table_info = operation.table_info();
        let Some(table_info) =
            maybe_indexed_table_info(self.query_proof_enabled, table_info.clone())
        else {
            return Ok(None);
        };
        let current_item = self
            .loader
            .get_item_map_with_resolved_operation_for_cache(operation, key.clone(), true)
            .await?;
        Ok(maybe_prepare_index_prewrite(
            self.query_proof_enabled,
            table_info,
            current_item,
        ))
    }

    pub(crate) async fn plan_delete_item_cache_effects(
        &self,
        table_name: &TableName,
        operation: &ResolvedStorageOperation,
        logical_key: &KeyAttributes,
    ) -> StorageResult<StorageCacheWriteEffects> {
        if !cfg!(feature = "cache-write-planner") {
            return Ok(RuntimeWriteEffects {
                point_read: Vec::new(),
                query_proof: Vec::new(),
            });
        }
        let table_info = operation.table_info();
        let query_proof_prewrite = self
            .query_proof_cache_maybe_load_gsi_prewrite_image_with_table_info(logical_key, operation)
            .await?;
        Ok(build_delete_item_cache_effects(
            table_name,
            table_info.clone(),
            logical_key,
            query_proof_prewrite,
            self.query_proof_enabled,
        ))
    }

    pub(crate) async fn prepare_update_item_cache_write(
        &self,
        table_name: &TableName,
        operation: &ResolvedStorageOperation,
        key: &KeyAttributes,
    ) -> StorageResult<PreparedUpdateCacheWrite> {
        let table_info = operation.table_info();
        Ok(prepare_update_cache_write(
            table_name,
            table_info.clone(),
            key,
            self.query_proof_cache_maybe_load_gsi_prewrite_image_with_table_info(key, operation)
                .await?,
        ))
    }

    pub(crate) fn finalize_update_item_cache_effects(
        &self,
        prepared: PreparedUpdateCacheWrite,
        post_image: Option<WireItem>,
    ) -> StorageResult<StorageCacheWriteEffects> {
        finalize_update_cache_effects(prepared, post_image, self.query_proof_enabled)
    }
}
