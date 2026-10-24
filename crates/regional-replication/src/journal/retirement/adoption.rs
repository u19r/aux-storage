use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecordKey, RecordChange},
};
use replication_types::identity::{
    origin::OriginId,
    resource::{ControlOperationId, TableIdentity, TableLineage},
};
use storage_types::{ReadProtection, StorageResult};

use super::{catalog::RetiredSourceReference, record::RetiredOrigin};
use crate::{
    constants::{
        ADOPTED_RETIRED_CUT_SUFFIX, COLLECTED_RETIRED_PAGE_SUFFIX, CONTROL_OPERATION_SUFFIX,
    },
    control::ControlDecision,
    history_capture::{HistoryCaptureRequest, RetiredHistoryDefinition, RetiredHistoryPage},
    history_collection::HistoryCollectionChange,
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(in crate::journal) async fn prepare_adopted_retirement<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: (
            ControlOperationId,
            (TableIdentity, TableLineage),
            &HistoryCollectionChange,
        ),
    ) -> StorageResult<Vec<RecordChange>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (operation, local, change) = input;
            match change {
                HistoryCollectionChange::Catalog(page) => {
                    let mut changes = Vec::with_capacity(page.entries.len());
                    for definition in &page.entries {
                        if let Some(change) = self
                            .prepare_adopted_catalog_entry(attempt, (operation, local), definition)
                            .await?
                        {
                            changes.push(change);
                        }
                    }
                    if !changes.is_empty() {
                        self.retain_history_epoch(attempt, local.1).await?;
                    }
                    Ok(changes)
                }
                HistoryCollectionChange::Retired(page) => {
                    self.prepare_adopted_retired_cuts(attempt, local, page)
                        .await
                }
                _ => Ok(Vec::new()),
            }
        }
        .await;
        settlement.complete(result)
    }

    async fn prepare_adopted_catalog_entry<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: (ControlOperationId, (TableIdentity, TableLineage)),
        definition: &RetiredHistoryDefinition,
    ) -> StorageResult<Option<RecordChange>> {
        let (operation, local) = input;
        let key = self.retired_source_catalog_key(local, definition.source()?)?;
        let row = self
            .records
            .load(
                attempt,
                std::slice::from_ref(&key),
                ReadProtection::Conflict,
            )
            .await?
            .pop()
            .flatten();
        let reference = RetiredSourceReference::Adoption {
            operation,
            supplier: definition.request().region.clone(),
            definition: definition.content(),
        };
        if let Some(row) = row {
            let old = self.decode_retired_source_reference(&row)?;
            if old != reference {
                let (removal, vector) = match &old {
                    RetiredSourceReference::Removal { .. } => {
                        self.load_received_retirement(attempt, &old, ReadProtection::Shared)
                            .await?
                    }
                    RetiredSourceReference::Adoption { .. } => {
                        let retained = self
                            .load_adopted_retired_fact(attempt, &old, ReadProtection::Shared)
                            .await?;
                        let vector = retained.definition.vector();
                        (retained.definition.into_original_seal(), vector)
                    }
                };
                if removal.source()? != definition.source()?
                    || removal.content()? != definition.removal_content()?
                    || vector != definition.vector()
                {
                    return Err(corrupt_record());
                }
            }
            return Ok(None);
        }
        self.records.prepare(key, None, &reference).map(Some)
    }

    async fn prepare_adopted_retired_cuts<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        local: (TableIdentity, TableLineage),
        page: &RetiredHistoryPage,
    ) -> StorageResult<Vec<RecordChange>> {
        let source = page.definition.source()?;
        let mut changes = Vec::with_capacity(page.cuts.len());
        let keys = page
            .cuts
            .iter()
            .map(|cut| self.adopted_retired_cut_key((local.0, source.table, local.1), &cut.origin))
            .collect::<StorageResult<Vec<_>>>()?;
        let rows = self
            .records
            .load(attempt, &keys, ReadProtection::Conflict)
            .await?;
        for ((cut, key), row) in page.cuts.iter().zip(keys).zip(rows) {
            let retired = RetiredOrigin {
                operation: page.definition.removal().operation().id,
                region: source.region.clone(),
                content: page.definition.removal_content()?,
                cut: cut.clone(),
            };
            if let Some(row) = row {
                if row.revision != storage_types::Revision::INITIAL {
                    return Err(corrupt_record());
                }
                let old: RetiredOrigin = self.records.decode(&row)?;
                if old.operation != retired.operation
                    || old.region != retired.region
                    || old.content != retired.content
                    || old.cut != retired.cut
                {
                    return Err(corrupt_record());
                }
            } else {
                changes.push(self.records.prepare(key, None, &retired)?);
            }
        }
        Ok(changes)
    }

    pub(super) fn adopted_retired_cut_key(
        &self,
        local: (TableIdentity, TableIdentity, TableLineage),
        origin: &OriginId,
    ) -> StorageResult<DurableRecordKey> {
        self.identity_key(
            ADOPTED_RETIRED_CUT_SUFFIX,
            &(local.0, local.1, local.2, origin),
        )
    }

    async fn load_adopted_retired_fact<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        reference: &RetiredSourceReference,
        protection: ReadProtection,
    ) -> StorageResult<RetiredHistoryPage> {
        let RetiredSourceReference::Adoption {
            operation,
            supplier,
            definition,
        } = reference
        else {
            return Err(corrupt_record());
        };
        let key = self.identity_key(
            COLLECTED_RETIRED_PAGE_SUFFIX,
            &(operation, supplier, definition, 0u32),
        )?;
        let row = self
            .records
            .load(attempt, std::slice::from_ref(&key), protection)
            .await?
            .pop()
            .flatten()
            .ok_or_else(corrupt_record)?;
        if row.revision != storage_types::Revision::INITIAL {
            return Err(corrupt_record());
        }
        let page: RetiredHistoryPage = self.records.decode(&row)?;
        page.definition.require_page(&page, 0)?;
        if page.definition.content() != *definition
            || page.definition.request().region != *supplier
            || page.definition.request().proposal.operation().id != *operation
        {
            return Err(corrupt_record());
        }
        Ok(page)
    }

    pub(super) async fn load_adopted_retired_definition<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &HistoryCaptureRequest,
        reference: &RetiredSourceReference,
    ) -> StorageResult<RetiredHistoryDefinition> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (original, proposal, bootstrap) = self
                .load_adopted_retirement(attempt, reference, ReadProtection::Shared)
                .await?;
            RetiredHistoryDefinition::from_adoption(request.clone(), &original, proposal, bootstrap)
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn load_adopted_retirement<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        reference: &RetiredSourceReference,
        protection: ReadProtection,
    ) -> StorageResult<(
        RetiredHistoryDefinition,
        replication_types::configuration::proposal::ControlProposal,
        replication_types::protocol::CanonicalDigest,
    )> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let RetiredSourceReference::Adoption { operation, .. } = reference else { return Err(corrupt_record()); };
            let key = self.identity_key(CONTROL_OPERATION_SUFFIX, operation)?;
            let row = self.records.load(attempt, &[key], protection).await?.pop().flatten().ok_or_else(corrupt_record)?;
            let control = self.decode_control_record(&row)?;
            if control.proposal.operation().id != *operation || !matches!(control.decision, ControlDecision::Installed | ControlDecision::Completed) { return Err(corrupt_record()); }
            replication_provider::control::validate_addition(&control.proposal).map_err(|_| corrupt_record())?;
            if !matches!(&control.proposal.operation().change, replication_types::configuration::control::ConfigurationChange::AddRegion { region } if region == &self.region) { return Err(corrupt_record()); }
            let definition = self.load_retained_bootstrap_definition(attempt, &control.proposal, protection).await?;
            let crate::bootstrap::TargetBootstrapDefinition::Addition(adoption) = definition else { return Err(corrupt_record()); };
            if control.bootstrap != Some(adoption.content()) { return Err(corrupt_record()); }
            let original = self.load_adopted_retired_fact(attempt, reference, protection).await?;
            Ok((original.definition, control.proposal, adoption.content()))

        }.await;
        settlement.complete(result)
    }
}
