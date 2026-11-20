//! Read-only native observations for an operator-owned table archive.

use std::sync::Arc;

use storage_provider::{
    ItemWriteObservation, PrimaryStateScan, PrimaryStateScanAttempt, RequestTableBindings,
    TableMetadataCache, TtlPolicyAttempt,
};
use storage_types::{
    AttemptMode, AttemptPlan, DeadlineClass, KeyAttributes, StorageError, StorageResult,
    StoredTableInfo, StoredWireItem, SystemClock, TableHistoryId, TableName, TimeToLiveDescription,
};
use stream_provider::StreamOperations;

use crate::{KvBackend, KvStore};

/// An existing native store opened without initializing formats or jobs.
/// The caller must keep the database immutable throughout all observations.
pub struct RecoverySource<B> {
    store: KvStore<B>,
    bindings: RequestTableBindings,
    table: TableName,
    database_id: [u8; 16],
}

pub struct RecoverySourceRow {
    pub keys: KeyAttributes,
    pub item: StoredWireItem,
}

pub struct RecoverySourcePage {
    pub rows: Vec<RecoverySourceRow>,
    pub continuation: Option<String>,
}

impl<B: KvBackend + 'static> RecoverySource<B> {
    pub async fn open(backend: B, table: TableName) -> StorageResult<Self> {
        let identity = backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                    .start(backend.attempt_deadlines()),
                |attempt| {
                    Box::pin(async move {
                        crate::infrastructure::layout::load_identity(
                            attempt,
                            crate::ReadProtection::Snapshot,
                        )
                        .await
                    })
                },
            )
            .await?
            .ok_or_else(|| StorageError::validation("source database is not initialized"))?;
        let store = KvStore {
            backend: Arc::new(backend),
            metadata_cache: Arc::new(TableMetadataCache::new()),
            clock: Arc::new(SystemClock),
            queue_wake: None,
            queue_catalog_hints: Arc::default(),
            admission: storage_common::native_admission::AttemptAdmission::Caller,
        };
        Ok(Self {
            store,
            bindings: RequestTableBindings::default(),
            table,
            database_id: identity,
        })
    }

    pub fn database_id(&self) -> [u8; 16] {
        self.database_id
    }

    pub async fn system_stream_scope(&self) -> StorageResult<storage_types::SystemStreamScope> {
        self.store.system_stream_scope().await.map_err(|error| {
            StorageError::from(stream_provider::StreamError::into_storage_enum(error))
        })
    }

    pub async fn table(
        &self,
    ) -> StorageResult<(TableHistoryId, StoredTableInfo, TimeToLiveDescription)> {
        let bindings = self.bindings.clone();
        let name = self.table.clone();
        let cache = Arc::clone(&self.store.metadata_cache);
        self.store
            .with_catalog_attempt(
                AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                    .start(self.store.backend.attempt_deadlines()),
                move |attempt| {
                    Box::pin(async move {
                        let (table, _) = bindings.resolve_table(attempt, &cache, &name).await?;
                        table.require_data_readable()?;
                        if table.table_status != storage_types::TableStatus::Active
                            || table.binding.regional.is_some()
                            || table
                                .binding
                                .gsis
                                .iter()
                                .any(|index| index.phase != storage_types::GsiPhase::Active)
                        {
                            return Err(StorageError::unsupported(
                                "recovery source must be an active, unpaired table with active \
                                 indexes",
                            ));
                        }
                        let ttl = storage_common::TtlConfigRecord::describe(
                            attempt.read_ttl_config(&table).await?,
                        );
                        if matches!(
                            &ttl.time_to_live_status,
                            storage_types::TimeToLiveStatus::Enabling
                                | storage_types::TimeToLiveStatus::Disabling
                        ) {
                            return Err(StorageError::unsupported(
                                "recovery source TTL is changing",
                            ));
                        }
                        Ok((table.history_id, table.info.as_ref().clone(), ttl))
                    })
                },
            )
            .await
    }

    pub async fn page(
        &self,
        after: Option<String>,
        limit: u32,
    ) -> StorageResult<RecoverySourcePage> {
        let bindings = self.bindings.clone();
        let name = self.table.clone();
        let cache = Arc::clone(&self.store.metadata_cache);
        self.store
            .with_catalog_attempt(
                AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::RangeRead)
                    .start(self.store.backend.attempt_deadlines()),
                move |attempt| {
                    Box::pin(async move {
                        let (table, _) = bindings.resolve_table(attempt, &cache, &name).await?;
                        table.require_data_readable()?;
                        let scan = PrimaryStateScan::try_new(table, after, Some(limit))?;
                        let page = attempt.scan_primary_state(&scan).await?;
                        let rows = page
                            .items
                            .into_iter()
                            .map(|row| match row.state {
                                ItemWriteObservation::Live { stored, .. } => {
                                    Ok(RecoverySourceRow {
                                        keys: row.key,
                                        item: *stored,
                                    })
                                }
                                _ => Err(StorageError::internal(
                                    "primary recovery scan returned a non-live row",
                                )),
                            })
                            .collect::<StorageResult<Vec<_>>>()?;
                        Ok(RecoverySourcePage {
                            rows,
                            continuation: page.continuation,
                        })
                    })
                },
            )
            .await
    }
}
