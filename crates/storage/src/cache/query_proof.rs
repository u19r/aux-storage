use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use storage_cache::runtime_query_proof::RuntimeQueryReadBlockReason;
pub type QueryProofReadPlan = storage_cache::RuntimeQueryProofReadPlan;
use storage_types::{QueryTableRequest, StorageResult, StoredTableInfo, TableName, WireItem};

pub use crate::query_proof_store::{PreparedQueryProofRead, QueryProofMaterializedPage};
use crate::{
    query_proof_request::{DerivedQueryManifestEntry, DerivedQueryPage},
    query_proof_store::InMemoryQueryProofCacheState,
    query_proof_types::{InMemoryQueryProofCacheConfig, QueryManifestKey, QueryManifestSnapshot},
};

#[async_trait]
pub trait QueryProofCache: Send + Sync {
    fn is_enabled(&self) -> bool {
        false
    }

    async fn record_base_put(
        &self,
        table_name: &TableName,
        table_info: &StoredTableInfo,
        item: &WireItem,
    ) -> StorageResult<()>;

    async fn record_base_delete(
        &self,
        table_name: &TableName,
        table_info: &StoredTableInfo,
        key: &WireItem,
    ) -> StorageResult<()>;

    async fn invalidate_base_coverage(
        &self,
        table_name: &TableName,
        table_info: &StoredTableInfo,
        key: &WireItem,
    ) -> StorageResult<()>;

    async fn record_index_transition(
        &self,
        table_name: &TableName,
        table_info: &StoredTableInfo,
        old_item: Option<&WireItem>,
        new_item: Option<&WireItem>,
    ) -> StorageResult<()>;

    async fn invalidate_index_query_spaces(&self, table_name: &TableName) -> StorageResult<()>;

    async fn invalidate_table(&self, table_name: &TableName) -> StorageResult<()>;

    async fn record_query_page(
        &self,
        table_name: &TableName,
        table_info: &StoredTableInfo,
        request: &QueryTableRequest,
        items: &[WireItem],
        has_more: bool,
    ) -> StorageResult<()>;

    async fn prepare_query_read(
        &self,
        table_name: &TableName,
        table_info: &StoredTableInfo,
        request: &QueryTableRequest,
    ) -> StorageResult<PreparedQueryProofRead>;

    async fn plan_query_read(
        &self,
        table_name: &TableName,
        table_info: &StoredTableInfo,
        request: &QueryTableRequest,
    ) -> StorageResult<QueryProofReadPlan> {
        Ok(self
            .prepare_query_read(table_name, table_info, request)
            .await?
            .plan)
    }

    async fn materialize_query_read(
        &self,
        table_name: &TableName,
        table_info: &StoredTableInfo,
        request: &QueryTableRequest,
    ) -> StorageResult<Option<QueryProofMaterializedPage>> {
        Ok(self
            .prepare_query_read(table_name, table_info, request)
            .await?
            .materialized_page)
    }
}

#[derive(Debug, Default)]
pub struct NoopQueryProofCache;

#[async_trait]
impl QueryProofCache for NoopQueryProofCache {
    async fn record_base_put(
        &self,
        _table_name: &TableName,
        _table_info: &StoredTableInfo,
        _item: &WireItem,
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn record_base_delete(
        &self,
        _table_name: &TableName,
        _table_info: &StoredTableInfo,
        _key: &WireItem,
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn invalidate_base_coverage(
        &self,
        _table_name: &TableName,
        _table_info: &StoredTableInfo,
        _key: &WireItem,
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn record_index_transition(
        &self,
        _table_name: &TableName,
        _table_info: &StoredTableInfo,
        _old_item: Option<&WireItem>,
        _new_item: Option<&WireItem>,
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn invalidate_index_query_spaces(&self, _table_name: &TableName) -> StorageResult<()> {
        Ok(())
    }

    async fn invalidate_table(&self, _table_name: &TableName) -> StorageResult<()> {
        Ok(())
    }

    async fn record_query_page(
        &self,
        _table_name: &TableName,
        _table_info: &StoredTableInfo,
        _request: &QueryTableRequest,
        _items: &[WireItem],
        _has_more: bool,
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn prepare_query_read(
        &self,
        _table_name: &TableName,
        _table_info: &StoredTableInfo,
        request: &QueryTableRequest,
    ) -> StorageResult<PreparedQueryProofRead> {
        Ok(storage_cache::blocked_runtime_query_proof_read(
            if request.consistent_read {
                RuntimeQueryReadBlockReason::StrongReadBypass
            } else {
                RuntimeQueryReadBlockReason::CacheDisabled
            },
        ))
    }
}

#[derive(Clone)]
pub struct InMemoryQueryProofCache {
    state: Arc<Mutex<InMemoryQueryProofCacheState>>,
}

impl InMemoryQueryProofCache {
    #[must_use]
    pub fn new(config: InMemoryQueryProofCacheConfig) -> Self {
        Self {
            state: Arc::new(Mutex::new(InMemoryQueryProofCacheState::new(config))),
        }
    }

    pub fn snapshot_base_partition(&self, key: &QueryManifestKey) -> Option<QueryManifestSnapshot> {
        self.lock_state().snapshot_base_partition(key)
    }

    fn lock_state(&self) -> MutexGuard<'_, InMemoryQueryProofCacheState> {
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                tracing::warn!("query proof cache mutex poisoned, recovering inner state");
                poisoned.into_inner()
            }
        }
    }
}

#[async_trait]
impl QueryProofCache for InMemoryQueryProofCache {
    fn is_enabled(&self) -> bool {
        self.lock_state().is_enabled()
    }

    async fn record_base_put(
        &self,
        table_name: &TableName,
        table_info: &StoredTableInfo,
        item: &WireItem,
    ) -> StorageResult<()> {
        let derived = DerivedQueryManifestEntry::for_put(table_name, table_info, None, item)?;
        self.lock_state().record_put(derived);
        Ok(())
    }

    async fn record_base_delete(
        &self,
        table_name: &TableName,
        table_info: &StoredTableInfo,
        key: &WireItem,
    ) -> StorageResult<()> {
        let derived = DerivedQueryManifestEntry::for_put(table_name, table_info, None, key)?;
        self.lock_state().record_delete(derived);
        Ok(())
    }

    async fn invalidate_base_coverage(
        &self,
        table_name: &TableName,
        table_info: &StoredTableInfo,
        key: &WireItem,
    ) -> StorageResult<()> {
        let derived = DerivedQueryManifestEntry::for_put(table_name, table_info, None, key)?;
        self.lock_state()
            .invalidate_partition_coverage(derived.manifest_key, derived.schema_fingerprint);
        Ok(())
    }

    async fn record_index_transition(
        &self,
        table_name: &TableName,
        table_info: &StoredTableInfo,
        old_item: Option<&WireItem>,
        new_item: Option<&WireItem>,
    ) -> StorageResult<()> {
        let index_names = table_info
            .global_secondary_indexes
            .as_ref()
            .map(|indexes| {
                indexes
                    .iter()
                    .map(|index| index.index_name.as_ref().to_string())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if index_names.is_empty() {
            return Ok(());
        }

        let mut state = self.lock_state();
        for index_name in index_names {
            let old_entry = DerivedQueryManifestEntry::for_sparse_item(
                table_name,
                table_info,
                Some(index_name.as_str()),
                old_item,
            )?;
            let new_entry = DerivedQueryManifestEntry::for_sparse_item(
                table_name,
                table_info,
                Some(index_name.as_str()),
                new_item,
            )?;
            state.record_index_transition(old_entry, new_entry);
        }
        Ok(())
    }

    async fn invalidate_index_query_spaces(&self, table_name: &TableName) -> StorageResult<()> {
        self.lock_state().invalidate_index_query_spaces(table_name);
        Ok(())
    }

    async fn invalidate_table(&self, table_name: &TableName) -> StorageResult<()> {
        self.lock_state().invalidate_table(table_name);
        Ok(())
    }

    async fn record_query_page(
        &self,
        table_name: &TableName,
        table_info: &StoredTableInfo,
        request: &QueryTableRequest,
        items: &[WireItem],
        has_more: bool,
    ) -> StorageResult<()> {
        let Some(page) =
            DerivedQueryPage::for_query_page(table_name, table_info, request, items, has_more)?
        else {
            return Ok(());
        };
        self.lock_state().record_query_page(page);
        Ok(())
    }

    async fn prepare_query_read(
        &self,
        table_name: &TableName,
        table_info: &StoredTableInfo,
        request: &QueryTableRequest,
    ) -> StorageResult<PreparedQueryProofRead> {
        self.lock_state()
            .prepare_query_read(table_name, table_info, request)
    }
}

#[must_use]
pub fn noop_query_proof_cache() -> Arc<dyn QueryProofCache> {
    Arc::new(NoopQueryProofCache)
}
