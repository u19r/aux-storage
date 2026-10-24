use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use storage_provider::CatalogAttempt;
use storage_types::StorageResult;

use crate::{
    bootstrap::copy_scope::BaselineCopyBudget,
    history_capture::{
        HistoryCaptureRequest, RetainedBaselineDefinition, RetainedBaselinePage,
        RetainedBaselineScope,
    },
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn load_retained_baseline_definition<
        A: PhysicalRecordAttempt + CatalogAttempt,
    >(
        &self,
        attempt: &mut A,
        request: &HistoryCaptureRequest,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<RetainedBaselineDefinition> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            request.validate()?;
            if request.region != self.region {
                return Err(corrupt_record());
            }
            self.require_control_revision(attempt, &request.proposal, protection)
                .await?;
            self.require_ready_history_capture(attempt, request, protection)
                .await?;
            let initial = self
                .load_initial_baseline_definition(attempt, request, protection)
                .await?;
            RetainedBaselineDefinition::from_native(request.clone(), initial)
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(crate) async fn load_retained_baseline_page<
        A: stream_provider::retained_image::RetainedOwnershipAttempt + CatalogAttempt,
    >(
        &self,
        attempt: &mut A,
        request: &HistoryCaptureRequest,
        input: (&CanonicalJsonCodec, &RetainedBaselineScope),
    ) -> StorageResult<RetainedBaselinePage> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            self.records
                .limits
                .check_batch(input.1.max_records.get(), input.1.max_bytes.get())?;
            let definition = self
                .load_retained_baseline_definition(
                    attempt,
                    request,
                    storage_types::ReadProtection::Snapshot,
                )
                .await?;
            let budget = BaselineCopyBudget {
                records: input.1.max_records,
                bytes: input.1.max_bytes,
            };
            let page = self
                .load_baseline_page(
                    attempt,
                    (request.proposal.operation().lineage, input.1.after.clone()),
                    (input.0, budget),
                )
                .await?;
            let source = definition.initial().proposal().operation().table;
            let schema = definition
                .initial()
                .proposal()
                .participant(&definition.initial().proposal().operation().home_region)
                .map_err(|_| corrupt_record())?
                .proposed
                .configuration
                .schema;
            if page.authorities.iter().any(|authority| {
                authority.item().source_table != source || authority.item().schema != schema
            }) {
                return Err(corrupt_record());
            }
            let page = RetainedBaselinePage::from_native(
                definition,
                input.1.after.clone(),
                page,
                self.records.limits.max_record_bytes(),
            )?;
            page.require_scope(request, input.1)?;
            Ok(page)
        }
        .await;
        settlement_guard.complete(result)
    }
}
