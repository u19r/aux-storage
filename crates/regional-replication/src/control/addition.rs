use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::configuration::proposal::ControlProposal;
use storage_provider::DynamoPort;

use super::runner::{ControlResult, ControlRunner};
use crate::history_capture::{self, HistoryCaptureRequest, HistoryCaptureStatus};

impl<P: DynamoPort + 'static> ControlRunner<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub(super) async fn release_addition_history(
        &self,
        proposal: Arc<ControlProposal>,
        source: &crate::transport::SourceReservation,
    ) -> ControlResult<bool> {
        for participant in proposal
            .participants()
            .iter()
            .filter(|participant| participant.before.is_some())
        {
            source.require_live()?;
            let request = Arc::new(HistoryCaptureRequest {
                proposal: proposal.as_ref().clone(),
                region: participant.region.clone(),
            });
            let status = if participant.region == *self.journal.region() {
                history_capture::release_history_capture(
                    self.port.as_ref(),
                    Arc::clone(&self.journal),
                    request,
                )
                .await?
            } else {
                self.peer(&proposal, &participant.region)?
                    .release_history_capture(request)
                    .await?
            };
            if status != HistoryCaptureStatus::Released {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) async fn cancel_addition_history(
        &self,
        proposal: Arc<ControlProposal>,
        source: &crate::transport::SourceReservation,
    ) -> ControlResult<bool> {
        for participant in proposal
            .participants()
            .iter()
            .filter(|participant| participant.before.is_some())
        {
            source.require_live()?;
            let request = Arc::new(HistoryCaptureRequest {
                proposal: proposal.as_ref().clone(),
                region: participant.region.clone(),
            });
            let status = if participant.region == *self.journal.region() {
                history_capture::observe_history_capture(
                    self.port.as_ref(),
                    Arc::clone(&self.journal),
                    Arc::clone(&request),
                )
                .await?
            } else {
                self.peer(&proposal, &participant.region)?
                    .observe_history_capture(Arc::clone(&request))
                    .await?
            };
            if status.is_none() || status == Some(HistoryCaptureStatus::Cancelled) {
                continue;
            }
            source.require_live()?;
            let status = if participant.region == *self.journal.region() {
                history_capture::cancel_history_capture(
                    self.port.as_ref(),
                    Arc::clone(&self.journal),
                    request,
                )
                .await?
            } else {
                self.peer(&proposal, &participant.region)?
                    .cancel_history_capture(request)
                    .await?
            };
            if status != HistoryCaptureStatus::Cancelled {
                return Ok(false);
            }
        }
        Ok(true)
    }
}
