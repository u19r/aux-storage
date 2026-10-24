use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, delivery::coverage::DeliveryCoverage,
    runtime_configuration::limits::WorkLimits,
};
use storage_provider::CatalogAttempt;
use storage_types::{ReadProtection, StorageResult};

use crate::{bootstrap::BootstrapTailScope, journal::JournalStore, native_record::corrupt_record};

impl JournalStore {
    pub(crate) async fn require_bootstrap_tail<A: PhysicalRecordAttempt + CatalogAttempt>(
        &self,
        attempt: &mut A,
        scope: &BootstrapTailScope,
        protection: ReadProtection,
    ) -> StorageResult<DeliveryCoverage> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let delivery = scope.delivery()?;
            let definition = self
                .load_enable_bootstrap_definition(attempt, &scope.transfer.proposal, protection)
                .await?;
            let (_, copied) = self
                .load_source_copy(attempt, (&scope.transfer, &definition), protection)
                .await?;
            if !copied.complete {
                return Err(corrupt_record());
            }
            self.require_source_capture_cut(
                attempt,
                &scope.transfer.proposal,
                (&scope.cut, protection),
            )
            .await?;
            let expected =
                replication_provider::control::source_capture_state(&scope.transfer.proposal)
                    .map_err(|_| corrupt_record())?;
            let current = self
                .observe_table_state(attempt, delivery.table, protection)
                .await?
                .ok_or_else(corrupt_record)?;
            if current.state() != &expected {
                return Err(corrupt_record());
            }
            let stamp = attempt
                .read_catalog_stamp(&expected.name)
                .await?
                .ok_or_else(corrupt_record)?;
            if stamp.history_id != expected.physical {
                return Err(corrupt_record());
            }
            Ok(self
                .observe_delivery_progress(attempt, &delivery, protection)
                .await?
                .0)
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(crate) async fn record_bootstrap_tail<
        A: stream_provider::retained_image::RetainedOwnershipAttempt + CatalogAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (
            &BootstrapTailScope,
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
            scope.require_range(import.manifest().range)?;
            self.require_bootstrap_tail(attempt, scope, ReadProtection::Shared)
                .await?;
            self.require_original_frame(attempt, (&delivery, import), configuration)
                .await?;
            self.acknowledge_delivery(attempt, (&delivery, import, receipt))
                .await
        }
        .await;
        settlement_guard.complete(result)
    }
}
