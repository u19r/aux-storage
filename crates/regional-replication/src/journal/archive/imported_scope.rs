//! Flatten imported metadata custody without recursively reading public
//! history.
use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::DurableRecord};
use replication_provider::regional_application::observation::TablePageAcceptance;
use storage_types::{ReadProtection, StorageResult};

use super::publication_scope::ArchiveEventDependency;
use crate::{
    journal::{JournalStore, retained_links::RetainedSourceLink},
    native_record::corrupt_record,
};

pub(super) struct ImportedArchiveScope {
    pub events: Vec<ArchiveEventDependency>,
    pub metadata: Vec<DurableRecord>,
    pub links: Vec<RetainedSourceLink>,
}

impl JournalStore {
    pub(super) async fn load_imported_page_scope<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: (TablePageAcceptance, Vec<DurableRecord>),
        protection: ReadProtection,
    ) -> StorageResult<ImportedArchiveScope> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (page, mut metadata) = input;
            let key = self.identity_key(
                crate::constants::IMPORT_MANIFEST_SUFFIX,
                &page.page.manifest,
            )?;
            let record = self
                .load_archive_dependency_metadata(attempt, &key, protection)
                .await?;
            let manifest = self.decode_import_manifest_identity(&record)?;
            let selected = page
                .require_manifest(&manifest)
                .map_err(|_| corrupt_record())?;
            let mut events = Vec::with_capacity(selected.len());
            let mut links = Vec::with_capacity(selected.len());
            for (link, receipt) in selected.into_iter().zip(&page.events) {
                events.push(ArchiveEventDependency {
                    event: link.event.clone(),
                    digest: link.digest,
                    lineage: link.lineage,
                    delivery: None,
                    receipt: Some(receipt.clone()),
                });
                links.push(RetainedSourceLink {
                    origin: manifest.origin.clone(),
                    link: link.clone(),
                });
            }
            metadata.push(record);
            Ok(ImportedArchiveScope {
                events,
                metadata,
                links,
            })
        }
        .await;
        settlement.complete(result)
    }
}
