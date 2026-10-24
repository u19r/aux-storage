//! Exact original transfer and durable peer coverage for one frozen retained
//! cut.
use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, delivery::coverage::DeliveryCoverage,
    runtime_configuration::limits::WorkLimits,
};
use replication_types::regional_import::RegionalImport;
use storage_provider::CatalogAttempt;
use storage_types::{ReadProtection, StorageResult};

use crate::{
    bootstrap::{BaselineCopyScope, ReenableHistoryPage},
    delivery::DeliveryScope,
    journal::JournalStore,
    native_record::corrupt_record,
    transport::RemoteImportReceipt,
};

impl JournalStore {
    pub(in crate::journal) async fn require_reenable_delivery<
        A: PhysicalRecordAttempt + CatalogAttempt,
    >(
        &self,
        attempt: &mut A,
        scope: &BaselineCopyScope,
        ordinal: u64,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<(ReenableHistoryPage, DeliveryScope)> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (definition, page) = self
                .load_retained_history_export(attempt, &scope.proposal, ordinal, protection)
                .await?;
            scope.require_definition(&definition)?;
            let cut = &page.cut.cut;
            let delivery = DeliveryScope {
                origin: cut.origin.clone(),
                table: cut.source.table,
                local_table: scope.proposal.operation().table,
                lineage: cut.lineage,
                membership: cut.membership,
                destination: scope.destination.clone(),
            };
            Ok((page, delivery))
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn require_reenable_original_coverage<
        A: PhysicalRecordAttempt + CatalogAttempt,
    >(
        &self,
        attempt: &mut A,
        scope: &BaselineCopyScope,
        ordinal: u64,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<(ReenableHistoryPage, DeliveryCoverage)> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (page, delivery) = self
                .require_reenable_delivery(attempt, scope, ordinal, protection)
                .await?;
            let coverage = self
                .observe_delivery_progress(attempt, &delivery, protection)
                .await?
                .0;
            if coverage.through() > page.cut.cut.through {
                return Err(corrupt_record());
            }
            Ok((page, coverage))
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn plan_reenable_original<
        A: stream_provider::retained_image::RetainedOwnershipAttempt + CatalogAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (&BaselineCopyScope, u64),
        encoding: (&CanonicalJsonCodec, &WorkLimits),
    ) -> StorageResult<Option<RegionalImport>> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            self.require_reenable_copy_complete(
                attempt,
                input.0,
                storage_types::ReadProtection::Snapshot,
            )
            .await?;
            let (page, delivery) = self
                .require_reenable_baseline_complete(
                    attempt,
                    input,
                    storage_types::ReadProtection::Snapshot,
                )
                .await?;
            let coverage = self
                .observe_delivery_progress(attempt, &delivery, ReadProtection::Snapshot)
                .await?
                .0;
            if coverage.through() > page.cut.cut.through {
                return Err(corrupt_record());
            }
            if coverage.through() == page.cut.cut.through {
                if !coverage.pending().is_empty() {
                    return Err(corrupt_record());
                }
                return Ok(None);
            }
            let range = replication_types::event::interval::ClosedSourceInterval::try_from((
                coverage.through(),
                page.cut.cut.through,
            ))
            .map_err(|_| corrupt_record())?;
            let frame = crate::delivery::load_bounded_frame(
                self,
                attempt,
                delivery.source_request(range),
                encoding,
            )
            .await
            .map_err(storage_types::StorageError::from)?;
            let (manifest, events) = frame.into_parts();
            RegionalImport::try_new((vec![delivery.binding()], manifest, events), encoding.0)
                .map(Some)
                .map_err(|_| corrupt_record())
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(crate) async fn record_reenable_original<
        A: stream_provider::retained_image::RetainedOwnershipAttempt + CatalogAttempt,
    >(
        &self,
        attempt: &mut A,
        scope: (&BaselineCopyScope, u64),
        input: (&RegionalImport, &RemoteImportReceipt),
        encoding: (&CanonicalJsonCodec, &WorkLimits),
    ) -> StorageResult<bool> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            self.require_reenable_copy_complete(
                attempt,
                scope.0,
                storage_types::ReadProtection::Shared,
            )
            .await?;
            let (page, delivery) = self
                .require_reenable_baseline_complete(
                    attempt,
                    scope,
                    storage_types::ReadProtection::Shared,
                )
                .await?;
            delivery.require_import(input.0)?;
            if input.0.manifest().range.through() > page.cut.cut.through {
                return Err(corrupt_record());
            }
            self.require_original_frame(attempt, (&delivery, input.0), encoding)
                .await?;
            let coverage = self
                .acknowledge_delivery(attempt, (&delivery, input.0, input.1))
                .await?;
            Ok(coverage.through() == page.cut.cut.through && coverage.pending().is_empty())
        }
        .await;
        settlement_guard.complete(result)
    }
}
