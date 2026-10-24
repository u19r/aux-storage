use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_provider::DynamoPort;

use super::{
    AncestryCollectionStage, HistoryCollectionChange, HistoryCollectionRecord, HistoryCollector,
};
use crate::{
    history_capture::{HistoryCaptureRequest, HistoryCaptureStatus},
    transport::TransportError,
};

impl<P: DynamoPort> HistoryCollector<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub(super) async fn fetch_collection_change(
        &self,
        request: Arc<HistoryCaptureRequest>,
        record: &HistoryCollectionRecord,
        source: &crate::transport::SourceReservation,
    ) -> Result<Option<HistoryCollectionChange>, TransportError> {
        source.require_live()?;
        let change = match &record.ancestry {
            AncestryCollectionStage::Active => {
                if record.current.is_some() {
                    HistoryCollectionChange::Page(
                        self.fetch_source_page(request, record.next_partition)
                            .await?,
                    )
                } else {
                    if self.advance_source(Arc::clone(&request)).await?
                        != HistoryCaptureStatus::Ready
                    {
                        return Ok(None);
                    }
                    source.require_live()?;
                    HistoryCollectionChange::Definition(
                        self.fetch_source_definition(request).await?,
                    )
                }
            }
            AncestryCollectionStage::Catalog { after } => {
                let scope = Arc::new(self.journal.collection_catalog_scope(after.clone()));
                HistoryCollectionChange::Catalog(
                    self.fetch_retired_source_catalog(request, scope).await?,
                )
            }
            AncestryCollectionStage::Retired {
                definition, next, ..
            } => HistoryCollectionChange::Retired(
                self.fetch_retired_source_page(request, (definition.source()?.clone(), *next))
                    .await?,
            ),
            AncestryCollectionStage::RetainedDefinition
            | AncestryCollectionStage::RetainedPages { .. } => {
                return self
                    .fetch_retained_collection_change(request, record)
                    .await
                    .map(Some);
            }
            AncestryCollectionStage::Complete => {
                return Err(crate::native_record::corrupt_record().into());
            }
            AncestryCollectionStage::Baseline => HistoryCollectionChange::Baseline(
                self.fetch_retained_baseline_definition(request).await?,
            ),
        };
        Ok(Some(change))
    }
}

impl<P: DynamoPort> HistoryCollector<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    async fn fetch_retained_collection_change(
        &self,
        request: Arc<HistoryCaptureRequest>,
        record: &HistoryCollectionRecord,
    ) -> Result<HistoryCollectionChange, TransportError> {
        if !self.is_local_source(&request)
            || request.region != request.proposal.operation().home_region
        {
            return Err(TransportError::Peer);
        }
        let next = match record.ancestry {
            AncestryCollectionStage::RetainedDefinition => None,
            AncestryCollectionStage::RetainedPages { next, .. } => Some(next),
            _ => return Err(crate::native_record::corrupt_record().into()),
        };
        let journal = Arc::clone(&self.journal);
        Ok(Arc::clone(&journal)
            .with_admitted_attempt(
                (
                    self.port.as_ref(),
                    storage_provider::native_admission::NativeWork::Recovery,
                ),
                crate::journal::NativeAttempt::Items(storage_types::AttemptMode::ReadOnly),
                move |attempt, _| {
                    Box::pin(async move {
                        match next {
                            Some(next) => journal
                                .load_capture_retained_page(
                                    attempt,
                                    &request,
                                    next,
                                    storage_types::ReadProtection::Snapshot,
                                )
                                .await
                                .map(|page| HistoryCollectionChange::RetainedPage(Box::new(page))),
                            None => journal
                                .load_capture_retained_inventory(
                                    attempt,
                                    &request,
                                    storage_types::ReadProtection::Snapshot,
                                )
                                .await
                                .map(HistoryCollectionChange::RetainedDefinition),
                        }
                    })
                },
            )
            .await?)
    }
}
