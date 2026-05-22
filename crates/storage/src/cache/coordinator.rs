use std::{collections::HashSet, sync::Arc};

use storage_cache::{
    RuntimePointReadMutation, RuntimePreparedUpdateCacheWrite, RuntimeQueryProofMutation,
    RuntimeWriteEffects,
};
use storage_types::{
    BatchGetItemRequest, DurableBatchPointReadProof, DurablePointReadProof, QueryTableRequest,
    StorageResult, StoredTableInfo, TableName, WireItem,
};

use crate::{
    point_read_cache::{
        AuthoritativePointReadPurpose, AuthoritativePointReadResult, PointReadBatchGetResult,
        PointReadCache, PointReadGetRequest, PointReadGetResult,
    },
    query_proof_cache::{PreparedQueryProofRead, QueryProofCache},
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StorageAuthoritativeCacheOptions {
    pub authoritative_strong_point_reads: bool,
    pub authoritative_write_preimages: bool,
    pub strong_read_through_warming: bool,
}

#[derive(Clone)]
pub(crate) struct StorageCacheServices {
    point_read_cache: Arc<dyn PointReadCache>,
    query_proof_cache: Arc<dyn QueryProofCache>,
    authoritative_options: StorageAuthoritativeCacheOptions,
}

impl StorageCacheServices {
    fn point_read_request(mutation: &RuntimePointReadMutation) -> PointReadGetRequest {
        match mutation {
            RuntimePointReadMutation::Put {
                table_name, key, ..
            }
            | RuntimePointReadMutation::Delete { table_name, key }
            | RuntimePointReadMutation::Invalidate { table_name, key } => PointReadGetRequest {
                table_name: table_name.clone(),
                key: key.clone(),
            },
        }
    }

    fn touched_query_proof_tables(effects: &RuntimeWriteEffects) -> HashSet<TableName> {
        effects
            .query_proof
            .iter()
            .map(|mutation| match mutation {
                RuntimeQueryProofMutation::RecordBasePut { table_name, .. }
                | RuntimeQueryProofMutation::RecordBaseDelete { table_name, .. }
                | RuntimeQueryProofMutation::InvalidateBaseCoverage { table_name, .. }
                | RuntimeQueryProofMutation::RecordIndexTransition { table_name, .. } => {
                    table_name.clone()
                }
            })
            .collect()
    }

    pub(crate) fn new(
        point_read_cache: Arc<dyn PointReadCache>,
        query_proof_cache: Arc<dyn QueryProofCache>,
        authoritative_options: StorageAuthoritativeCacheOptions,
    ) -> Self {
        Self {
            point_read_cache,
            query_proof_cache,
            authoritative_options,
        }
    }

    pub(crate) const fn authoritative_strong_point_reads_enabled(&self) -> bool {
        self.authoritative_options.authoritative_strong_point_reads
    }

    pub(crate) const fn strong_read_through_warming_enabled(&self) -> bool {
        self.authoritative_options.strong_read_through_warming
    }

    pub(crate) const fn authoritative_write_preimages_enabled(&self) -> bool {
        self.authoritative_options.authoritative_write_preimages
    }

    pub(crate) async fn get_eventual_point_read(
        &self,
        request: &PointReadGetRequest,
    ) -> StorageResult<PointReadGetResult> {
        self.point_read_cache.get_eventual(request).await
    }

    pub(crate) async fn get_authoritative_point_read(
        &self,
        request: &PointReadGetRequest,
        purpose: AuthoritativePointReadPurpose,
    ) -> StorageResult<AuthoritativePointReadResult> {
        self.point_read_cache
            .get_authoritative(request, purpose)
            .await
    }

    pub(crate) async fn batch_get_eventual_point_reads(
        &self,
        request: &BatchGetItemRequest,
    ) -> StorageResult<PointReadBatchGetResult> {
        self.point_read_cache.batch_get_eventual(request).await
    }

    pub(crate) async fn batch_get_authoritative_point_reads(
        &self,
        request: &BatchGetItemRequest,
        purpose: AuthoritativePointReadPurpose,
    ) -> StorageResult<PointReadBatchGetResult> {
        self.point_read_cache
            .batch_get_authoritative(request, purpose)
            .await
    }

    pub(crate) fn claim_point_read_write_version(&self) -> u64 {
        self.point_read_cache.claim_write_version()
    }

    /// Mark all keys in the write effects as in-flight. Reads for these keys
    /// will return `Miss` until the intents are released (via
    /// `apply_write_effects` or `release_write_intents`).
    pub(crate) async fn prepare_write_intents(
        &self,
        effects: &RuntimeWriteEffects,
    ) -> StorageResult<()> {
        for mutation in &effects.point_read {
            let request = Self::point_read_request(mutation);
            self.point_read_cache.prepare_write(&request).await?;
        }
        Ok(())
    }

    /// Release in-flight write marks without applying cache mutations.
    /// Call this on error paths when the DB write failed.
    pub(crate) async fn release_write_intents(
        &self,
        effects: &RuntimeWriteEffects,
    ) -> StorageResult<()> {
        for mutation in &effects.point_read {
            let request = Self::point_read_request(mutation);
            self.point_read_cache.complete_write(&request).await?;
        }
        Ok(())
    }

    /// Prepare a write intent for an update operation. The intent covers
    /// the primary key being updated and will be released by
    /// `apply_write_effects`.
    pub(crate) async fn prepare_update_write_intent(
        &self,
        prepared: &RuntimePreparedUpdateCacheWrite,
    ) -> StorageResult<()> {
        let request = PointReadGetRequest {
            table_name: prepared.table_name.clone(),
            key: prepared.key.clone(),
        };
        self.point_read_cache.prepare_write(&request).await
    }

    pub(crate) async fn release_update_write_intent(
        &self,
        prepared: &RuntimePreparedUpdateCacheWrite,
    ) -> StorageResult<()> {
        let request = PointReadGetRequest {
            table_name: prepared.table_name.clone(),
            key: prepared.key.clone(),
        };
        self.point_read_cache.complete_write(&request).await
    }

    pub(crate) async fn write_point_read_put(
        &self,
        request: &PointReadGetRequest,
        item: &WireItem,
        write_version: u64,
    ) -> StorageResult<()> {
        self.point_read_cache
            .write_put(request, item, write_version)
            .await
    }

    pub(crate) async fn warm_authoritative_point_read(
        &self,
        request: &PointReadGetRequest,
        proof: DurablePointReadProof,
    ) -> StorageResult<()> {
        let version = self.point_read_cache.claim_write_version();
        match proof {
            DurablePointReadProof::Present { item, revision } => {
                self.point_read_cache
                    .write_put_with_revision(request, &item, revision, version)
                    .await
            }
            DurablePointReadProof::Absent { proof } => {
                self.point_read_cache
                    .write_delete_with_absence_proof(request, proof, version)
                    .await
            }
        }
    }

    pub(crate) async fn warm_authoritative_batch_point_reads(
        &self,
        proof: DurableBatchPointReadProof,
    ) -> StorageResult<()> {
        for (table_name, entries) in proof.responses {
            for entry in entries {
                let request = PointReadGetRequest {
                    table_name: table_name.clone(),
                    key: entry.key,
                };
                self.warm_authoritative_point_read(&request, entry.proof)
                    .await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn apply_write_effects(
        &self,
        effects: &RuntimeWriteEffects,
    ) -> StorageResult<()> {
        let point_read_requests = effects
            .point_read
            .iter()
            .map(Self::point_read_request)
            .collect::<Vec<_>>();
        let query_proof_tables = Self::touched_query_proof_tables(effects);

        let apply_result = async {
            for mutation in &effects.query_proof {
                match mutation {
                    RuntimeQueryProofMutation::RecordBasePut {
                        table_name,
                        table_info,
                        item,
                    } => {
                        let item = WireItem::from_attribute_map(item)?;
                        self.query_proof_cache
                            .record_base_put(table_name, table_info, &item)
                            .await?;
                    }
                    RuntimeQueryProofMutation::RecordBaseDelete {
                        table_name,
                        table_info,
                        key,
                    } => {
                        let key = WireItem::from_attribute_map(&key.to_attribute_map())?;
                        self.query_proof_cache
                            .record_base_delete(table_name, table_info, &key)
                            .await?;
                    }
                    RuntimeQueryProofMutation::InvalidateBaseCoverage {
                        table_name,
                        table_info,
                        key,
                    } => {
                        let key = WireItem::from_attribute_map(&key.to_attribute_map())?;
                        self.query_proof_cache
                            .invalidate_base_coverage(table_name, table_info, &key)
                            .await?;
                    }
                    RuntimeQueryProofMutation::RecordIndexTransition {
                        table_name,
                        table_info,
                        old_item,
                        new_item,
                    } => {
                        let old_item = old_item
                            .as_ref()
                            .map(WireItem::from_attribute_map)
                            .transpose()?;
                        let new_item = new_item
                            .as_ref()
                            .map(WireItem::from_attribute_map)
                            .transpose()?;
                        self.query_proof_cache
                            .record_index_transition(
                                table_name,
                                table_info,
                                old_item.as_ref(),
                                new_item.as_ref(),
                            )
                            .await?;
                    }
                }
            }

            for mutation in &effects.point_read {
                match mutation {
                    RuntimePointReadMutation::Put {
                        table_name,
                        key,
                        item,
                    } => {
                        let request = PointReadGetRequest {
                            table_name: table_name.clone(),
                            key: key.clone(),
                        };
                        let version = self.point_read_cache.claim_write_version();
                        self.point_read_cache
                            .write_put(&request, item, version)
                            .await?;
                    }
                    RuntimePointReadMutation::Delete { table_name, key } => {
                        let request = PointReadGetRequest {
                            table_name: table_name.clone(),
                            key: key.clone(),
                        };
                        let version = self.point_read_cache.claim_write_version();
                        self.point_read_cache
                            .write_delete(&request, version)
                            .await?;
                    }
                    RuntimePointReadMutation::Invalidate { table_name, key } => {
                        let request = PointReadGetRequest {
                            table_name: table_name.clone(),
                            key: key.clone(),
                        };
                        self.point_read_cache.invalidate(&request).await?;
                    }
                }
            }

            for request in &point_read_requests {
                self.point_read_cache.complete_write(request).await?;
            }
            StorageResult::Ok(())
        }
        .await;

        if let Err(error) = apply_result {
            for table_name in &query_proof_tables {
                let _ = self.query_proof_cache.invalidate_table(table_name).await;
            }
            for request in &point_read_requests {
                let _ = self.point_read_cache.invalidate(request).await;
                let _ = self.point_read_cache.complete_write(request).await;
            }
            return Err(error);
        }

        Ok(())
    }

    pub(crate) async fn invalidate_table(&self, table_name: &TableName) -> StorageResult<()> {
        self.query_proof_cache.invalidate_table(table_name).await
    }

    pub(crate) async fn record_query_page(
        &self,
        table_name: &TableName,
        table_info: &StoredTableInfo,
        request: &QueryTableRequest,
        items: &[WireItem],
        has_more: bool,
    ) -> StorageResult<()> {
        self.query_proof_cache
            .record_query_page(table_name, table_info, request, items, has_more)
            .await
    }

    pub(crate) async fn prepare_query_read(
        &self,
        table_name: &TableName,
        table_info: &StoredTableInfo,
        request: &QueryTableRequest,
    ) -> StorageResult<PreparedQueryProofRead> {
        self.query_proof_cache
            .prepare_query_read(table_name, table_info, request)
            .await
    }

    pub(crate) fn point_read_enabled(&self) -> bool {
        self.point_read_cache.is_enabled()
    }

    pub(crate) fn query_proof_enabled(&self) -> bool {
        self.query_proof_cache.is_enabled()
    }
}
