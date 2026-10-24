use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::identity::origin::OriginId;
use storage_provider::CatalogAttempt;
use storage_types::StorageResult;

use super::custody::RetiredSourceAuthority;
use crate::{
    history_capture::{HistoryCaptureRequest, RetiredHistoryPage},
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn load_retired_history_page<A: PhysicalRecordAttempt + CatalogAttempt>(
        &self,
        attempt: &mut A,
        request: &HistoryCaptureRequest,
        scope: (
            &replication_types::configuration::installed::RegionalSourceBinding,
            u32,
        ),
    ) -> StorageResult<RetiredHistoryPage> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            request.validate()?;
            if request.region != self.region {
                return Err(corrupt_record());
            }
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
            let definition = self
                .load_catalog_retired_source(attempt, request, scope.0)
                .await?;
            let source = definition.source()?.clone();
            let current = replication_provider::control::borrow_addition_source(
                &request.proposal,
                &request.region,
            )
            .map_err(|_| corrupt_record())?;
            let authority = RetiredSourceAuthority::from_capture(&definition, current);
            let count = u32::from(source.partitions);
            let after = scope.1;
            if after >= count {
                return Err(corrupt_record());
            }
            let width = u32::try_from(
                self.records
                    .limits
                    .max_records()
                    .get()
                    .min(crate::constants::HISTORY_CUT_PAGE_WIDTH),
            )
            .map_err(|_| corrupt_record())?;
            let end = after.saturating_add(width).min(count);
            let origins = (after..end)
                .map(|partition| {
                    Ok(OriginId {
                        region: source.region.clone(),
                        database_group: source.database_group,
                        journal_generation: source.generation,
                        journal_partition: u16::try_from(partition)
                            .map_err(|_| corrupt_record())?
                            .into(),
                    })
                })
                .collect::<StorageResult<Vec<_>>>()?;
            let cuts = self
                .load_retired_source_cuts(
                    attempt,
                    current,
                    (&authority, &origins),
                    storage_types::ReadProtection::Shared,
                )
                .await?;
            let page = RetiredHistoryPage {
                definition,
                cuts,
                next_partition: (end < count).then_some(end),
            };
            page.definition.require_page(&page, after)?;
            Ok(page)
        }
        .await;
        settlement.complete(result)
    }
}
