use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec,
    regional_application::{
        import_admission::BoundedImport,
        observation::{ImportPageScope, TablePageAcceptance},
    },
};
use storage_provider::DynamoPort;
use storage_types::{AttemptMode, StorageResult};

use crate::journal::JournalStore;

/// Evidence read through a completed native transaction, never an uncommitted
/// PendingImport. Construction stays at this native boundary.
pub struct CommittedImportPages {
    pages: Vec<TablePageAcceptance>,
    completed: Option<Vec<TablePageAcceptance>>,
}

pub enum ImportReceiptObservation {
    Durable(CommittedImportPages),
    Unresolved,
}

impl CommittedImportPages {
    pub(crate) fn after_commit(
        pages: Vec<TablePageAcceptance>,
        completed: Option<Vec<TablePageAcceptance>>,
    ) -> Self {
        Self { pages, completed }
    }
    pub fn completed(&self) -> Option<&[TablePageAcceptance]> {
        self.completed.as_deref()
    }
    pub fn pages(&self) -> &[TablePageAcceptance] {
        &self.pages
    }
}

/// Missing evidence after an unknown commit does not establish an abort.
pub async fn reconcile_import_pages<P: DynamoPort>(
    port: &P,
    journal: Arc<JournalStore>,
    input: (BoundedImport, Arc<CanonicalJsonCodec>),
) -> StorageResult<ImportReceiptObservation>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    let (import, codec) = input;
    let (pages, completed) = Arc::clone(&journal)
        .with_admitted_attempt(
            (
                port,
                storage_provider::native_admission::NativeWork::Recovery,
            ),
            crate::journal::NativeAttempt::Items(AttemptMode::ReadOnly),
            move |attempt, _| {
                Box::pin(async move {
                    let pages = journal
                        .load_import_pages(
                            attempt,
                            import.as_ref(),
                            &codec,
                            ImportPageScope::Selected,
                        )
                        .await?;
                    let completed = if import.as_ref().fragment().is_some() {
                        journal
                            .load_import_pages(
                                attempt,
                                import.as_ref(),
                                &codec,
                                ImportPageScope::Complete,
                            )
                            .await?
                    } else {
                        pages.clone()
                    };
                    Ok((pages, completed))
                })
            },
        )
        .await?;
    Ok(match pages {
        Some(pages) => ImportReceiptObservation::Durable(CommittedImportPages { pages, completed }),
        None => ImportReceiptObservation::Unresolved,
    })
}
