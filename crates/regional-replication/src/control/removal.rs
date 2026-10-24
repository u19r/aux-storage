use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::{
    configuration::{control::ConfigurationChange, proposal::ControlProposal},
    identity::replica::RegionId,
};
use storage_provider::DynamoPort;

use super::{
    removal_transfer::{RemovalTransferChange, RemovalTransferPlan, execute_removal_transfer},
    runner::{ControlResult, ControlRunner},
};
use crate::lifecycle::{SealReceptionProgress, SealRequest};

impl<P: DynamoPort + 'static> ControlRunner<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    /// One pair and at most one page per turn. The primary cursor selects a
    /// pair; its actual survivor root decides page progress and completion.
    pub(super) async fn propagate_removal_seals(
        &self,
        proposal: Arc<ControlProposal>,
        source: &crate::transport::SourceReservation,
    ) -> ControlResult<bool> {
        if !matches!(
            proposal.operation().change,
            ConfigurationChange::RemoveRegion { .. } | ConfigurationChange::DisableGlobal
        ) {
            return Ok(true);
        }
        source.require_live()?;
        let position = execute_removal_transfer(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            Arc::clone(&proposal),
            RemovalTransferChange::Observe,
        )
        .await?;
        let Some(step) = RemovalTransferPlan::try_new(&proposal)?.select_step(position)? else {
            return Ok(true);
        };
        let request = Arc::new(step.request);
        let progress = self
            .observe_survivor_seal(Arc::clone(&request), &step.survivor, source)
            .await?;
        let complete = if progress.complete {
            true
        } else {
            source.require_live()?;
            let page = self
                .peer(&proposal, &request.region)?
                .fetch_sealed_table_cuts(Arc::clone(&request), progress.next_partition)
                .await?;
            source.require_live()?;
            let received = if &step.survivor == self.journal.region() {
                crate::lifecycle::receive_sealed_table_cuts(
                    self.port.as_ref(),
                    Arc::clone(&self.journal),
                    (request, Arc::new(page)),
                )
                .await?
            } else {
                self.peer(&proposal, &step.survivor)?
                    .receive_sealed_table_cuts(request, page)
                    .await?
            };
            received.complete
        };
        if complete {
            source.require_live()?;
            execute_removal_transfer(
                self.port.as_ref(),
                Arc::clone(&self.journal),
                proposal,
                RemovalTransferChange::Advance(position),
            )
            .await?;
        }
        Ok(false)
    }

    async fn observe_survivor_seal(
        &self,
        request: Arc<SealRequest>,
        survivor: &RegionId,
        source: &crate::transport::SourceReservation,
    ) -> ControlResult<SealReceptionProgress> {
        source.require_live()?;
        if survivor == self.journal.region() {
            return Ok(crate::lifecycle::observe_received_seal(
                self.port.as_ref(),
                Arc::clone(&self.journal),
                request,
            )
            .await?);
        }
        Ok(self
            .peer(&request.proposal, survivor)?
            .observe_received_seal(request)
            .await?)
    }
}
