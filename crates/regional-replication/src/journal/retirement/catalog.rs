use infrastructure_provider::{
    page::{BoundedRecordRange, RecordPageRequest},
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecordKey, RecordAddress},
};
use replication_types::{
    configuration::{control::ConfigurationChange, installed::RegionalSourceBinding},
    protocol::CanonicalDigest,
};
use serde::{Deserialize, Serialize};
use storage_types::{ReadProtection, StorageResult};

use super::record::ReceivedSeal;
use crate::{
    constants::{RECEIVED_SEAL_SUFFIX, RETIRED_SOURCE_CATALOG_SUFFIX},
    control::{ControlDecision, ControlRecord},
    history_capture::{
        HistoryCaptureRequest, HistoryCatalogScope, RetiredHistoryCatalogPage,
        RetiredHistoryDefinition,
    },
    journal::JournalStore,
    lifecycle::SealRequest,
    native_record::corrupt_record,
};

#[derive(PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) enum RetiredSourceReference {
    Removal {
        operation: replication_types::identity::resource::ControlOperationId,
        region: replication_types::identity::replica::RegionId,
        vector: CanonicalDigest,
    },
    Adoption {
        operation: replication_types::identity::resource::ControlOperationId,
        supplier: replication_types::identity::replica::RegionId,
        definition: CanonicalDigest,
    },
}

impl JournalStore {
    /// The catalog becomes visible atomically with the installed removal
    /// decision.
    pub(in crate::journal) async fn retain_retired_source_catalog<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        control: &ControlRecord,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if !matches!(
                control.proposal.operation().change,
                ConfigurationChange::RemoveRegion { .. } | ConfigurationChange::DisableGlobal
            ) {
                return Ok(());
            }
            for evidence in &control.retirement {
                let request =
                    SealRequest::try_new(control.proposal.clone(), evidence.region.clone())?;
                if !request.survivors()?.contains(&self.region) {
                    continue;
                }
                if self.load_received_seal_vector(attempt, &request).await? != evidence.vector {
                    return Err(corrupt_record());
                }
                let local = &request
                    .proposal
                    .participant(&self.region)
                    .map_err(|_| corrupt_record())?
                    .proposed
                    .configuration;
                let key = self
                    .retired_source_catalog_key((local.table, local.lineage), request.source()?)?;
                let rows = self
                    .records
                    .load(
                        attempt,
                        std::slice::from_ref(&key),
                        ReadProtection::Conflict,
                    )
                    .await?;
                let reference = RetiredSourceReference::Removal {
                    operation: request.proposal.operation().id,
                    region: request.region.clone(),
                    vector: evidence.vector,
                };
                if let Some(row) = &rows[0] {
                    let old = self.decode_retired_source_reference(row)?;
                    if old != reference {
                        return Err(corrupt_record());
                    }
                } else {
                    self.retain_history_epoch(attempt, local.lineage).await?;
                    self.records
                        .store(attempt, &[self.records.prepare(key, None, &reference)?])
                        .await?;
                }
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn load_retired_source_catalog<
        A: PhysicalRecordAttempt + storage_provider::CatalogAttempt,
    >(
        &self,
        attempt: &mut A,
        request: &HistoryCaptureRequest,
        scope: &HistoryCatalogScope,
    ) -> StorageResult<RetiredHistoryCatalogPage> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            request.validate()?;
            if request.region != self.region {
                return Err(corrupt_record());
            }
            self.records
                .limits
                .check_batch(scope.max_records.get(), scope.max_bytes.get())?;
            self.require_control_revision(
                attempt,
                &request.proposal,
                storage_types::ReadProtection::Shared,
            )
            .await?;
            self.require_ready_history_capture(
                attempt,
                request,
                storage_types::ReadProtection::Conflict,
            )
            .await?;
            let state = replication_provider::control::borrow_addition_source(
                &request.proposal,
                &request.region,
            )
            .map_err(|_| corrupt_record())?;
            let first = self.identity_key(
                RETIRED_SOURCE_CATALOG_SUFFIX,
                &(state.configuration.table, state.configuration.lineage),
            )?;
            let original = self.record_prefix_range(&first)?;
            let bounds = original.bounds();
            let range =
                BoundedRecordRange::try_new(bounds.0.clone(), bounds.1.clone(), scope.max_records)?;
            if scope
                .after
                .as_ref()
                .is_some_and(|after| !range.contains(after))
            {
                return Err(corrupt_record());
            }
            let page = self
                .records
                .scan(
                    attempt,
                    RecordPageRequest {
                        range,
                        after: scope.after.clone(),
                        max_bytes: self.records.limits.max_bytes(),
                        protection: ReadProtection::Snapshot,
                    },
                )
                .await?;
            let mut entries = Vec::with_capacity(page.records.len());
            for row in page.records {
                let reference = self.decode_retired_source_reference(&row)?;
                let definition = self
                    .load_catalog_retired_definition(attempt, request, &reference)
                    .await?;
                if row.key
                    != self.retired_source_catalog_key(
                        (state.configuration.table, state.configuration.lineage),
                        definition.source()?,
                    )?
                {
                    return Err(corrupt_record());
                }
                entries.push(definition);
            }
            RetiredHistoryCatalogPage::from_native(request, scope, entries, page.continuation)
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn load_catalog_retired_source<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &HistoryCaptureRequest,
        source: &RegionalSourceBinding,
    ) -> StorageResult<RetiredHistoryDefinition> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let state = replication_provider::control::borrow_addition_source(
                &request.proposal,
                &request.region,
            )
            .map_err(|_| corrupt_record())?;
            let key = self.retired_source_catalog_key(
                (state.configuration.table, state.configuration.lineage),
                source,
            )?;
            let row = self
                .records
                .load(attempt, std::slice::from_ref(&key), ReadProtection::Shared)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let reference = self.decode_retired_source_reference(&row)?;
            let definition = self
                .load_catalog_retired_definition(attempt, request, &reference)
                .await?;
            if definition.source()? != source {
                return Err(corrupt_record());
            }
            Ok(definition)
        }
        .await;
        settlement.complete(result)
    }

    async fn load_catalog_retired_definition<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &HistoryCaptureRequest,
        reference: &RetiredSourceReference,
    ) -> StorageResult<RetiredHistoryDefinition> {
        if matches!(reference, RetiredSourceReference::Adoption { .. }) {
            return self
                .load_adopted_retired_definition(attempt, request, reference)
                .await;
        }
        let (removal, vector) = self
            .load_received_retirement(attempt, reference, ReadProtection::Shared)
            .await?;
        RetiredHistoryDefinition::from_native(request.clone(), removal, vector)
    }

    pub(super) async fn load_received_retirement<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        reference: &RetiredSourceReference,
        protection: ReadProtection,
    ) -> StorageResult<(SealRequest, CanonicalDigest)> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let RetiredSourceReference::Removal {
                operation,
                region,
                vector,
            } = reference
            else {
                return Err(corrupt_record());
            };
            let key = self.identity_key(RECEIVED_SEAL_SUFFIX, &(operation, region))?;
            let row = self
                .records
                .load(attempt, std::slice::from_ref(&key), protection)
                .await?
                .pop()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let root: ReceivedSeal = self.records.decode(&row)?;
            self.require_seal_survivor(&root.request)?;
            if root.request.proposal.operation().id != *operation
                || root.request.region != *region
                || root.vector != *vector
                || !root
                    .progress(u32::from(root.request.source()?.partitions))
                    .complete
            {
                return Err(corrupt_record());
            }
            let row = self
                .load_control(attempt, &root.request.proposal, protection)
                .await?
                .ok_or_else(corrupt_record)?;
            let control = self.decode_control(&row, &root.request.proposal)?;
            if !matches!(
                control.decision,
                ControlDecision::Installed | ControlDecision::Completed
            ) || !control
                .retirement
                .iter()
                .any(|evidence| evidence.region == *region && evidence.vector == *vector)
            {
                return Err(corrupt_record());
            }
            Ok((root.request, root.vector))
        }
        .await;
        settlement.complete(result)
    }

    pub(super) fn decode_retired_source_reference(
        &self,
        row: &infrastructure_provider::record::DurableRecord,
    ) -> StorageResult<RetiredSourceReference> {
        if row.revision != storage_types::Revision::INITIAL {
            return Err(corrupt_record());
        }
        self.records.decode(row)
    }

    pub(in crate::journal) fn retired_source_catalog_key(
        &self,
        local: (
            replication_types::identity::resource::TableIdentity,
            replication_types::identity::resource::TableLineage,
        ),
        source: &RegionalSourceBinding,
    ) -> StorageResult<DurableRecordKey> {
        let mut key: RecordAddress = self
            .identity_key(RETIRED_SOURCE_CATALOG_SUFFIX, &local)?
            .into();
        let serialized_digest = CanonicalDigest::of_json(&(
            source.table,
            &source.region,
            source.database_group,
            source.generation,
            source.partitions,
        ))
        .map_err(|_| corrupt_record())?;
        key.suffix.extend_from_slice(serialized_digest.as_ref());
        key.try_into()
    }
}
