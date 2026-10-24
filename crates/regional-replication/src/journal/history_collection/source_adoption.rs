use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_provider::CatalogAttempt;
use storage_types::{ReadProtection, StorageResult};

use crate::{
    constants::{HISTORY_COLLECTION_SUFFIX, SOURCE_COLLECTION_DECLARATION_SUFFIX},
    history_capture::HistoryCaptureRequest,
    history_collection::{
        HistoryAdoptionDefinition, HistoryCollectionChange, HistoryCollectionRecord,
    },
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn declare_source_collection<A: PhysicalRecordAttempt + CatalogAttempt>(
        &self,
        attempt: &mut A,
        request: &HistoryCaptureRequest,
        definition: &HistoryAdoptionDefinition,
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            request.validate()?;
            definition.validate()?;
            if request.region != self.region
                || self.region == request.proposal.operation().home_region
                || definition.proposal() != &request.proposal
            {
                return Err(corrupt_record());
            }
            self.require_control_revision(
                attempt,
                &request.proposal,
                storage_types::ReadProtection::Shared,
            )
            .await?;
            let current = self
                .require_ready_history_capture(
                    attempt,
                    request,
                    storage_types::ReadProtection::Conflict,
                )
                .await?;
            if current.policy_digest()? != definition.current().policy_digest()? {
                return Err(corrupt_record());
            }
            let initial = self
                .load_retained_baseline_definition(
                    attempt,
                    request,
                    storage_types::ReadProtection::Shared,
                )
                .await?;
            if initial.initial().content() != definition.initial().content() {
                return Err(corrupt_record());
            }
            let key = self.identity_key(
                SOURCE_COLLECTION_DECLARATION_SUFFIX,
                &request.proposal.operation().id,
            )?;
            if let Some(row) = self
                .load_immutable_waves(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                )
                .await?
                .pop()
                .flatten()
            {
                let old: HistoryAdoptionDefinition =
                    self.decode_source_collection_declaration(&row)?;
                if old.proposal() != &request.proposal || old.content() != definition.content() {
                    return Err(corrupt_record());
                }
                self.observe_history_collection(attempt, &request.proposal, ReadProtection::Shared)
                    .await?
                    .ok_or_else(corrupt_record)?;
                return Ok(());
            }
            let root_key =
                self.identity_key(HISTORY_COLLECTION_SUFFIX, &request.proposal.operation().id)?;
            if self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&root_key),
                    ReadProtection::Conflict,
                )
                .await?
                .pop()
                .flatten()
                .is_some()
            {
                return Err(corrupt_record());
            }
            let root = HistoryCollectionRecord::from_primary_definition(definition.current())?;
            self.records
                .store(
                    attempt,
                    &[
                        self.records.prepare(key, None, definition)?,
                        self.records.prepare(root_key, None, &root)?,
                    ],
                )
                .await
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(in crate::journal) fn decode_source_collection_declaration(
        &self,
        row: &infrastructure_provider::record::DurableRecord,
    ) -> StorageResult<HistoryAdoptionDefinition> {
        let definition: HistoryAdoptionDefinition = self.records.decode(row)?;
        definition.validate()?;
        self.require_collection_request(definition.current().request())?;
        let proposal = definition.proposal();
        if self.region == proposal.operation().home_region
            || row.revision != storage_types::Revision::INITIAL
            || row.key
                != self.identity_key(
                    SOURCE_COLLECTION_DECLARATION_SUFFIX,
                    &proposal.operation().id,
                )?
        {
            return Err(corrupt_record());
        }
        Ok(definition)
    }

    pub(super) async fn require_declared_collection<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &HistoryCollectionRecord,
        protection: ReadProtection,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if self.region == record.proposal.operation().home_region {
                return Ok(());
            }
            let key = self.identity_key(
                SOURCE_COLLECTION_DECLARATION_SUFFIX,
                &record.proposal.operation().id,
            )?;
            let row = self
                .load_immutable_waves(attempt, std::slice::from_ref(&key), protection)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let definition: HistoryAdoptionDefinition =
                self.decode_source_collection_declaration(&row)?;
            definition.require_collection(record)
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn require_native_collection_fact<
        A: PhysicalRecordAttempt + CatalogAttempt,
    >(
        &self,
        attempt: &mut A,
        record: &HistoryCollectionRecord,
        change: &HistoryCollectionChange,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if record.request()?.is_none() {
                return self
                    .require_native_retained_collection(attempt, record, change)
                    .await;
            }
            let request = record.request()?.ok_or_else(corrupt_record)?;
            if request.region != self.region {
                return Ok(());
            }
            let native = self
                .require_ready_history_capture(
                    attempt,
                    &request,
                    storage_types::ReadProtection::Conflict,
                )
                .await?;
            match change {
                HistoryCollectionChange::Definition(definition)
                    if definition.content() != native.content() =>
                {
                    Err(corrupt_record())
                }
                HistoryCollectionChange::Page(page) => {
                    native.require_page(page, record.next_partition)?;
                    Ok(())
                }
                _ => Ok(()),
            }
        }
        .await;
        settlement.complete(result)
    }
}
