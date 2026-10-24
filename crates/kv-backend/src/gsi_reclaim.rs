//! Exact native index identity observations and bounded retired family pages.

use storage_backfill::GsiBuildTarget;
use storage_provider::{GsiReclaimAttempt, TableReclaimStep};
use storage_types::{
    ReadProtection, StorageError, StorageResult, TableHistoryId, maintenance::TargetIncarnation,
};

use crate::{
    KvAttempt,
    catalog::{KvCatalogAttempt, decode_index_incarnation, metadata_key, table_id_from_history},
    constants::{
        GSI_AUTHORITY_FAMILY, GSI_BRANCH_FAMILY, GSI_PROJECTION_FAMILY, GSI_TOMBSTONE_FAMILY,
    },
};

impl<A: KvAttempt> GsiReclaimAttempt for KvCatalogAttempt<'_, A> {
    async fn load_index_incarnation(
        &mut self,
        history: TableHistoryId,
        target: &GsiBuildTarget,
    ) -> StorageResult<Option<TargetIncarnation>> {
        let bytes = self
            .attempt
            .get(
                &metadata_key(table_id_from_history(history)?),
                ReadProtection::Conflict,
            )
            .await?
            .ok_or_else(|| {
                StorageError::internal("KV active index binding has no table metadata")
            })?;
        decode_index_incarnation(&bytes, history, target)
    }

    async fn delete_index_page(
        &mut self,
        history: TableHistoryId,
        target: &GsiBuildTarget,
    ) -> StorageResult<TableReclaimStep> {
        let table_id = table_id_from_history(history)?;
        let scope = target.incarnation.as_ref();
        if scope.len() != 6 || scope[..4] != table_id.to_be_bytes() {
            return Err(StorageError::internal(
                "KV retired index scope differs from table history",
            ));
        }
        let prefixes = [
            GSI_PROJECTION_FAMILY,
            GSI_AUTHORITY_FAMILY,
            GSI_BRANCH_FAMILY,
            GSI_TOMBSTONE_FAMILY,
        ]
        .map(|family| {
            let mut prefix = vec![family];
            prefix.extend_from_slice(scope);
            prefix
        })
        .to_vec();
        crate::table_reclaim::delete_family_pages(self.attempt, prefixes).await
    }
}
