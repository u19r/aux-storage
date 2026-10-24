//! SQL physical effects for the shared create-table executor.

use std::future::Future;

use storage_backfill::GsiBuild;
use storage_provider::{
    CreateTableAttempt, CreateTableLayout, StreamTrimStateWrite, table_retention_scope_id,
};
use storage_types::{
    IndexName, StorageResult, StoredTableInfo, SystemStreamCheckpoint, TableHistoryId,
    maintenance::TargetIncarnation,
};

use crate::{
    SqlAttempt, SqlLayout, catalog::SqlCatalogAttempt, gsi::register_gsi_builds,
    physical_name::initial_gsi_incarnation, retention::write_stream_retention,
    stream_history::capture_high,
};

impl<A: SqlAttempt, L: SqlLayout> CreateTableAttempt for SqlCatalogAttempt<'_, A, L> {
    type Identity = TableHistoryId;
    type LayoutSeed = bool;

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
        let history_id = TableHistoryId::from(uuid::Uuid::now_v7());
        let inserted = self.insert_table_layout(table, history_id, seed).await?;
        Ok(inserted.then_some(history_id))
    }

    fn retention_scope_id(&self, _: &StoredTableInfo, identity: &Self::Identity) -> String {
        table_retention_scope_id(*identity)
    }

    fn write_retention(
        &mut self,
        _: &Self::Identity,
        write: StreamTrimStateWrite,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        write_stream_retention(self.attempt, self.engine, write)
    }

    fn capture_high(
        &mut self,
    ) -> impl Future<Output = StorageResult<SystemStreamCheckpoint>> + Send {
        capture_high(self.attempt, self.engine)
    }

    fn index_incarnation(
        &self,
        identity: &Self::Identity,
        index: &IndexName,
    ) -> StorageResult<TargetIncarnation> {
        Ok(initial_gsi_incarnation(*identity, index))
    }

    fn register_gsis(
        &mut self,
        identity: &Self::Identity,
        builds: Vec<GsiBuild<()>>,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        register_gsi_builds(self.attempt, self.engine, *identity, builds)
    }
}
