use storage_cache::{
    RuntimePreparedQueryExecution, RuntimePreparedQueryRead, RuntimePreparedQueryReadOutcome,
    extract_primary_key_from_item, plan_runtime_query_execution, prepare_runtime_query_read,
};
use storage_types::{
    KeyAttributes, QueryTableRequest, StorageResult, StoredTableInfo, TableName, WireItem,
};

use crate::{
    cache_coordinator::StorageCacheServices,
    cache_read_observability::{
        StorageCacheReadOperation, StorageCacheReadOutcome, record_storage_cache_read_outcome,
    },
    point_read_cache::{PointReadGetRequest, PointReadGetResult},
};

pub(crate) trait StorageCacheQueryRuntimeLoad {
    async fn get_table_info_for_cache_query(
        &self,
        table_name: &TableName,
    ) -> StorageResult<StoredTableInfo>;

    async fn get_item_with_consistent_read_for_cache_query(
        &self,
        table_name: TableName,
        key: KeyAttributes,
        consistent_read: bool,
    ) -> StorageResult<Option<WireItem>>;
}

pub(crate) struct MaterializedQueryPagePayloads {
    pub(crate) items: Vec<WireItem>,
    pub(crate) had_point_read_cache_miss: bool,
}

pub(crate) type PreparedQueryCacheExecution = RuntimePreparedQueryExecution<WireItem>;

pub(crate) struct StorageCacheQueryRuntime<'a, L> {
    services: &'a StorageCacheServices,
    loader: &'a L,
}

impl<'a, L> StorageCacheQueryRuntime<'a, L>
where L: StorageCacheQueryRuntimeLoad
{
    pub(crate) fn new(services: &'a StorageCacheServices, loader: &'a L) -> Self {
        Self { services, loader }
    }

    pub(crate) async fn invalidate_table(&self, table_name: &TableName) -> StorageResult<()> {
        if !self.services.query_proof_enabled() {
            return Ok(());
        }
        self.services.invalidate_table(table_name).await
    }

    pub(crate) async fn materialize_query_page_payloads(
        &self,
        table_name: &TableName,
        primary_keys: &[KeyAttributes],
    ) -> StorageResult<Option<MaterializedQueryPagePayloads>> {
        if primary_keys.is_empty() {
            return Ok(Some(MaterializedQueryPagePayloads {
                items: Vec::new(),
                had_point_read_cache_miss: false,
            }));
        }

        let mut items = Vec::with_capacity(primary_keys.len());
        let mut had_point_read_cache_miss = false;
        for key in primary_keys {
            let request = PointReadGetRequest {
                table_name: table_name.clone(),
                key: key.clone(),
            };
            match self.services.get_eventual_point_read(&request).await? {
                PointReadGetResult::Hit(item) => {
                    let Some(item) = *item else {
                        return Ok(None);
                    };
                    items.push(item);
                }
                PointReadGetResult::Miss => {
                    had_point_read_cache_miss = true;
                    let write_version = self.services.claim_point_read_write_version();
                    let Some(item) = self
                        .loader
                        .get_item_with_consistent_read_for_cache_query(
                            table_name.clone(),
                            key.clone(),
                            true,
                        )
                        .await?
                    else {
                        return Ok(None);
                    };
                    self.services
                        .write_point_read_put(&request, &item, write_version)
                        .await?;
                    items.push(item);
                }
            }
        }

        Ok(Some(MaterializedQueryPagePayloads {
            items,
            had_point_read_cache_miss,
        }))
    }

    pub(crate) async fn prepare_query_execution(
        &self,
        request: &QueryTableRequest,
    ) -> StorageResult<PreparedQueryCacheExecution> {
        if request.projection_expression.is_some() {
            return Ok(RuntimePreparedQueryExecution::None);
        }
        let table_info = self
            .loader
            .get_table_info_for_cache_query(&request.table_name)
            .await?;
        let prepared_query_proof = self
            .services
            .prepare_query_read(&request.table_name, &table_info, request)
            .await?;
        let Some(materialized_page) = prepared_query_proof.materialized_page else {
            return Ok(RuntimePreparedQueryExecution::None);
        };
        let Some(materialized_payloads) = self
            .materialize_query_page_payloads(&request.table_name, &materialized_page.primary_keys)
            .await?
        else {
            return Ok(RuntimePreparedQueryExecution::None);
        };

        let prepared = prepare_runtime_query_read(
            &prepared_query_proof.plan,
            materialized_page,
            materialized_payloads.items,
            materialized_payloads.had_point_read_cache_miss,
        );
        if let RuntimePreparedQueryRead::WholePage { outcome, .. } = &prepared {
            let outcome = match outcome {
                RuntimePreparedQueryReadOutcome::Hit => StorageCacheReadOutcome::Hit,
                RuntimePreparedQueryReadOutcome::HitPartial => StorageCacheReadOutcome::HitPartial,
            };
            record_storage_cache_read_outcome(StorageCacheReadOperation::Query, outcome);
        }

        Ok(plan_runtime_query_execution(prepared, request.limit))
    }

    pub(crate) fn record_partial_hit(&self) {
        record_storage_cache_read_outcome(
            StorageCacheReadOperation::Query,
            StorageCacheReadOutcome::HitPartial,
        );
    }

    pub(crate) fn record_miss_if_eventual(&self, consistent_read: bool) {
        if !consistent_read {
            record_storage_cache_read_outcome(
                StorageCacheReadOperation::Query,
                StorageCacheReadOutcome::Miss,
            );
        }
    }

    pub(crate) async fn observe_db_query_result(
        &self,
        request: &QueryTableRequest,
        items: &[WireItem],
        has_more: bool,
    ) -> StorageResult<()> {
        if request.projection_expression.is_some() {
            return Ok(());
        }
        let should_record_query_page = self.services.query_proof_enabled();
        let should_record_query_items =
            self.services.point_read_enabled() && request.index_name.is_none() && !items.is_empty();
        if !should_record_query_page && !should_record_query_items {
            return Ok(());
        }

        let table_info = self
            .loader
            .get_table_info_for_cache_query(&request.table_name)
            .await?;
        if should_record_query_items {
            for item in items {
                let item_map = item.to_attribute_map()?;
                let point_read_request = PointReadGetRequest {
                    table_name: request.table_name.clone(),
                    key: extract_primary_key_from_item(&table_info.key_schema, &item_map)?,
                };
                let write_version = self.services.claim_point_read_write_version();
                self.services
                    .write_point_read_put(&point_read_request, item, write_version)
                    .await?;
            }
        }
        if should_record_query_page {
            self.services
                .record_query_page(&request.table_name, &table_info, request, items, has_more)
                .await?;
        }
        Ok(())
    }
}
