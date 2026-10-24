use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, delivery::coverage::DeliveryCoverage,
    runtime_configuration::limits::WorkLimits,
};
use storage_provider::CatalogAttempt;
use storage_types::{ReadProtection, StorageResult};

use crate::{
    bootstrap::{
        AdoptedOriginalPage, AdoptedOriginalScope, SourceCaptureCut, TargetBootstrapDefinition,
    },
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(in crate::journal) async fn require_reenable_copy_complete<
        A: PhysicalRecordAttempt + CatalogAttempt,
    >(
        &self,
        attempt: &mut A,
        scope: &crate::bootstrap::BaselineCopyScope,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<()> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let definition = self
                .load_source_bootstrap_definition(attempt, &scope.proposal, protection)
                .await?;
            scope.require_definition(&definition)?;
            if definition.retained_inventory()?.is_none() {
                return Err(corrupt_record());
            }
            let (_, copied) = self
                .load_source_copy(attempt, (scope, &definition), protection)
                .await?;
            if !copied.complete {
                return Err(corrupt_record());
            }
            Ok(())
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(crate) async fn require_adopted_original<A: PhysicalRecordAttempt + CatalogAttempt>(
        &self,
        attempt: &mut A,
        scope: &AdoptedOriginalScope,
    ) -> StorageResult<DeliveryCoverage> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let delivery = scope.delivery()?;
            let definition = self
                .load_source_bootstrap_definition(
                    attempt,
                    &scope.transfer.proposal,
                    storage_types::ReadProtection::Shared,
                )
                .await?;
            let TargetBootstrapDefinition::Addition(current) = &definition else {
                return Err(corrupt_record());
            };
            if current.content() != scope.definition.content() {
                return Err(corrupt_record());
            }
            let (_, copied) = self
                .load_source_copy(
                    attempt,
                    (&scope.transfer, &definition),
                    ReadProtection::Shared,
                )
                .await?;
            if !copied.complete {
                return Err(corrupt_record());
            }
            self.require_collected_original(attempt, scope).await?;
            Ok(self
                .observe_delivery_progress(attempt, &delivery, ReadProtection::Shared)
                .await?
                .0)
        }
        .await;
        settlement_guard.complete(result)
    }

    async fn require_collected_original<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        scope: &AdoptedOriginalScope,
    ) -> StorageResult<()> {
        let proposal = &scope.transfer.proposal;
        match &scope.page {
            AdoptedOriginalPage::Active { supplier, after } => {
                let definition = self
                    .load_collected_history_definition(
                        attempt,
                        proposal,
                        supplier,
                        ReadProtection::Shared,
                    )
                    .await?;
                let before =
                    replication_provider::control::borrow_addition_source(proposal, supplier)
                        .map_err(|_| corrupt_record())?;
                let source = before
                    .sources
                    .iter()
                    .find(|source| &source.region == supplier)
                    .ok_or_else(corrupt_record)?;
                let page = self
                    .load_collected_history_page(
                        attempt,
                        proposal,
                        (supplier, *after),
                        ReadProtection::Shared,
                    )
                    .await?;
                definition.require_page(&page, *after)?;
                if source != &scope.source || !page.cuts.contains(&scope.cut) {
                    return Err(corrupt_record());
                }
            }
            AdoptedOriginalPage::Retired {
                supplier,
                definition,
                after,
            } => {
                let page = self
                    .load_collected_retired_page(
                        attempt,
                        proposal,
                        (supplier, *definition, *after),
                        ReadProtection::Shared,
                    )
                    .await?;
                if page.definition.source()? != &scope.source
                    || !page.cuts.iter().any(|cut| {
                        SourceCaptureCut {
                            origin: cut.origin.clone(),
                            through: cut.through,
                        } == scope.cut
                    })
                {
                    return Err(corrupt_record());
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn record_adopted_original<
        A: stream_provider::retained_image::RetainedOwnershipAttempt + CatalogAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (
            &AdoptedOriginalScope,
            &replication_types::regional_import::RegionalImport,
            &crate::transport::RemoteImportReceipt,
        ),
        configuration: (&CanonicalJsonCodec, &WorkLimits),
    ) -> StorageResult<DeliveryCoverage> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (scope, import, receipt) = input;
            let delivery = scope.delivery()?;
            delivery.require_import(import)?;
            if import.manifest().range.through() > scope.cut.through {
                return Err(corrupt_record());
            }
            self.require_adopted_original(attempt, scope).await?;
            self.require_original_frame(attempt, (&delivery, import), configuration)
                .await?;
            self.acknowledge_delivery(attempt, (&delivery, import, receipt))
                .await
        }
        .await;
        settlement_guard.complete(result)
    }
}
