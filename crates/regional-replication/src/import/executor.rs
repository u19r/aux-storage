use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec,
    regional_application::{
        application::PreparedImportApplication, import_admission::BoundedImport,
    },
};
use storage_provider::DynamoPort;
use storage_types::{AttemptMode, StorageError, StorageResult};

use super::store::NativeImportStore;
use crate::{import_receipt::CommittedImportPages, journal::JournalStore};

pub struct ImportExecution {
    pub import: BoundedImport,
    pub codec: Arc<CanonicalJsonCodec>,
}

pub async fn execute_import<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    execution: ImportExecution,
) -> StorageResult<CommittedImportPages>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    let import = execution.import;
    let cache = port.metadata_cache();
    let publication_cache = Arc::clone(&cache);
    let (pending, publications, completed) = Arc::clone(&journal)
        .with_admitted_attempt(
            (
                port,
                storage_provider::native_admission::NativeWork::Recovery,
            ),
            crate::journal::NativeAttempt::Items(AttemptMode::ReadWriteItems),
            move |attempt, timestamp| {
                Box::pin(async move {
                    let prepared =
                        PreparedImportApplication::try_from(&import).map_err(StorageError::from)?;
                    let mut store = NativeImportStore {
                        journal: &journal,
                        codec: &execution.codec,
                        cache: &cache,
                        import: import.as_ref(),
                        timestamp,
                        publications: Vec::new(),
                        tables: Vec::new(),
                        completed: None,
                        events: Default::default(),
                    };
                    store.load_configuration(attempt).await?;
                    store.prepare_events(attempt).await?;
                    let pending = prepared
                        .apply(&mut store, attempt)
                        .await
                        .map_err(StorageError::from)?;
                    Ok((pending, store.publications, store.completed))
                })
            },
        )
        .await?;
    publication_cache.publish(publications);
    Ok(CommittedImportPages::after_commit(pending.pages, completed))
}
