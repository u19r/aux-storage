//! KV physical effects for the shared create-table executor.

use std::future::Future;

use storage_backfill::GsiBuild;
use storage_provider::{
    CreateTableAttempt, CreateTableLayout, StreamTrimStateWrite, table_retention_scope_id,
};
use storage_types::{
    IndexName, StorageError, StorageResult, StoredTableInfo, SystemStreamCheckpoint,
    maintenance::TargetIncarnation,
};

use crate::{
    KvAttempt,
    catalog::{KvCatalogAttempt, KvIndexKind, KvTableIdentity},
    gsi::register_gsi_builds,
    retention::write_stream_retention,
    stream_history::capture_high,
};

impl<A: KvAttempt> CreateTableAttempt for KvCatalogAttempt<'_, A> {
    type Identity = KvTableIdentity;
    type LayoutSeed = (Option<Vec<u8>>, Option<Vec<u8>>);

    fn read_layout(
        &mut self,
        table: &StoredTableInfo,
    ) -> impl Future<Output = StorageResult<CreateTableLayout<Self::LayoutSeed>>> + Send {
        self.read_table_layout(table)
    }

    async fn insert_layout(
        &mut self,
        table: &StoredTableInfo,
        seed: Self::LayoutSeed,
    ) -> StorageResult<Option<Self::Identity>> {
        self.insert_table_layout(table, seed).map(Some)
    }

    fn retention_scope_id(&self, _: &StoredTableInfo, identity: &Self::Identity) -> String {
        table_retention_scope_id(identity.table_id.into())
    }

    fn write_retention(
        &mut self,
        identity: &Self::Identity,
        write: StreamTrimStateWrite,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        write_stream_retention(self.attempt, identity, write)
    }

    fn capture_high(
        &mut self,
    ) -> impl Future<Output = StorageResult<SystemStreamCheckpoint>> + Send {
        capture_high(self.attempt)
    }

    fn index_incarnation(
        &self,
        identity: &Self::Identity,
        index: &IndexName,
    ) -> StorageResult<TargetIncarnation> {
        let index = identity
            .indexes
            .iter()
            .find(|candidate| {
                candidate.index_name == *index && candidate.index_kind == KvIndexKind::UserGsi
            })
            .ok_or_else(|| StorageError::internal("KV GSI has no index identity"))?;
        Ok(TargetIncarnation::from(crate::gsi::index_incarnation(
            identity.table_id,
            index.index_id,
        )))
    }

    fn register_gsis(
        &mut self,
        identity: &Self::Identity,
        builds: Vec<GsiBuild<()>>,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        register_gsi_builds(self.attempt, identity, builds)
    }
}
