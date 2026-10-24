use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::RecordChange};
use replication_types::{configuration::proposal::ControlProposal, protocol::CanonicalDigest};
use storage_types::{ReadProtection, Revision, StorageResult};

use crate::{
    constants::{COLLECTED_HISTORY_DEFINITION_SUFFIX, HISTORY_COLLECTION_SUFFIX},
    history_collection::{HistoryCollectionChange, HistoryCollectionRecord},
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn accept_history_collection<
        A: PhysicalRecordAttempt + storage_provider::CatalogAttempt,
    >(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        input: (Revision, &HistoryCollectionChange),
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let observation = self
                .observe_history_collection(attempt, proposal, ReadProtection::Conflict)
                .await?
                .ok_or_else(corrupt_record)?;
            // Another worker or an unknown commit may already have advanced
            // this cursor.
            if observation.revision != input.0 {
                return Ok(());
            }
            self.require_control_revision(attempt, proposal, storage_types::ReadProtection::Shared)
                .await?;
            let request = crate::history_capture::HistoryCaptureRequest {
                proposal: proposal.clone(),
                region: self.region.clone(),
            };
            self.require_ready_history_capture(
                attempt,
                &request,
                storage_types::ReadProtection::Conflict,
            )
            .await?;
            let mut record = observation.record;
            self.require_native_collection_fact(attempt, &record, input.1)
                .await?;
            if let HistoryCollectionChange::RetainedPage(page) = input.1 {
                let history = record.adopt_retained_cut(page)?;
                let local = &proposal
                    .participant(&self.region)
                    .map_err(|_| corrupt_record())?
                    .proposed;
                self.store_retained_catalog_cut(
                    attempt,
                    local,
                    (proposal.operation().id, &history),
                )
                .await?;
            }
            let fact = self.prepare_collected_fact(&mut record, input.1)?;
            record.validate()?;
            self.require_declared_collection(attempt, &record, ReadProtection::Shared)
                .await?;
            let key = self.identity_key(HISTORY_COLLECTION_SUFFIX, &proposal.operation().id)?;
            let root = self
                .records
                .prepare(key, Some(observation.revision), &record)?;
            self.records.store(attempt, &[fact, root]).await
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) fn prepare_collected_fact(
        &self,
        record: &mut HistoryCollectionRecord,
        change: &HistoryCollectionChange,
    ) -> StorageResult<RecordChange> {
        if matches!(
            record.ancestry,
            crate::history_collection::AncestryCollectionStage::RetainedDefinition
                | crate::history_collection::AncestryCollectionStage::RetainedPages { .. }
        ) {
            return self.prepare_collected_retained(record, change);
        }
        if !matches!(
            record.ancestry,
            crate::history_collection::AncestryCollectionStage::Active
        ) {
            return self.prepare_collected_ancestry(record, change);
        }
        let request = record.request()?.ok_or_else(corrupt_record)?;
        match change {
            HistoryCollectionChange::Definition(definition) => {
                definition.validate()?;
                if record.current.is_some()
                    || definition.request().content()? != request.content()?
                    || definition.policy_digest()? != record.policy
                {
                    return Err(corrupt_record());
                }
                let key = self.identity_key(
                    COLLECTED_HISTORY_DEFINITION_SUFFIX,
                    &(request.proposal.operation().id, &request.region),
                )?;
                record.vector = definition.vector_seed()?;
                record.current = Some(definition.clone());
                self.records.prepare(key, None, definition)
            }
            HistoryCollectionChange::Page(page) => {
                let definition = record.current.as_ref().ok_or_else(corrupt_record)?;
                let fact =
                    self.prepare_collected_history_page(definition, record.next_partition, page)?;
                for cut in &page.cuts {
                    record.vector = cut.fold_vector(record.vector)?;
                }
                if let Some(next) = page.next_partition {
                    record.next_partition = next;
                } else {
                    Self::finish_collected_source(record)?;
                }
                Ok(fact)
            }
            _ => Err(corrupt_record()),
        }
    }

    fn finish_collected_source(record: &mut HistoryCollectionRecord) -> StorageResult<()> {
        let definition = record.current.take().ok_or_else(corrupt_record)?;
        if record.vector != definition.vector() {
            return Err(corrupt_record());
        }
        record.content = CanonicalDigest::of_json(&(record.content, definition.content()))
            .map_err(|_| corrupt_record())?;
        record.ancestry =
            crate::history_collection::AncestryCollectionStage::Catalog { after: None };
        record.next_partition = 0;
        Ok(())
    }
}
