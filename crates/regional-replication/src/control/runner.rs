use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::{configuration::proposal::ControlProposal, identity::replica::RegionId};
use storage_provider::DynamoPort;

use super::{
    CancelledControl, CompletedControl, ControlDecision, ControlOutcome, ControlRequest,
    DurableGrant, DurableInstallation, ParticipantEvidence, execute_control,
};
use crate::{
    journal::JournalStore,
    transport::{SourceReservation, TcpPeerClient, TcpTransport},
};

pub(super) type ControlResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// The durable regional records are the phase cursor. Losing a scheduler
/// checkpoint cannot move a control backward or manufacture a decision.
pub(crate) struct ControlRunner<P> {
    pub(crate) port: Arc<P>,
    pub(crate) journal: Arc<JournalStore>,
    pub(crate) transport: TcpTransport,
    pub(crate) codec: Arc<replication_provider::canonical_event_codec::CanonicalJsonCodec>,
}

impl<P: DynamoPort + 'static> ControlRunner<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub(super) async fn advance_pending(&self) -> ControlResult<bool> {
        let source = self
            .transport
            .reserve_source_budget(self.operation_timeout()?)
            .await?;
        let runner = self.clone();
        crate::transport::settle_source_with(source, move |source| async move {
            source.require_live()?;
            let Some(work) = super::recovery::select_pending_work(
                runner.port.as_ref(),
                Arc::clone(&runner.journal),
            )
            .await?
            else {
                return Ok(false);
            };
            let proposal = match work {
                super::pending::PendingControlWork::Configuration(proposal) => proposal,
                super::pending::PendingControlWork::Trim(proposal) => {
                    let work = runner.advance_trim(Arc::new(proposal), &source).await?;
                    return Ok(work == crate::trim::record::TrimWorkState::Ready);
                }
            };
            let proposal = Arc::new(proposal);
            if runner
                .advance_control(Arc::clone(&proposal), &source)
                .await?
            {
                source.require_live()?;
                super::recovery::retire_pending_control(
                    runner.port.as_ref(),
                    Arc::clone(&runner.journal),
                    proposal,
                )
                .await?;
            }
            Ok(true)
        })
        .await
    }

    pub(crate) async fn advance_control(
        &self,
        proposal: Arc<ControlProposal>,
        source: &SourceReservation,
    ) -> ControlResult<bool> {
        if &proposal.operation().home_region != self.journal.region() {
            return Err(storage_types::StorageError::unsupported(
                "regional control requires configuration primary",
            )
            .into());
        }
        let outcome = self
            .native(ControlRequest::Prepare(Arc::clone(&proposal)), source)
            .await?;
        match outcome {
            ControlOutcome::Prepared(grant) => self.commit(proposal, grant, source).await?,
            ControlOutcome::Committed(certificate) => {
                self.install_committed(certificate, source).await?
            }
            ControlOutcome::Installed(receipt) => self.finish(proposal, receipt, source).await?,
            ControlOutcome::Completed(certificate) => {
                return self
                    .finish_bootstrap_release(proposal, certificate, source)
                    .await;
            }
            ControlOutcome::Cancelled(certificate) => {
                return self
                    .finish_bootstrap_cancellation(proposal, certificate, source)
                    .await;
            }
        }
        Ok(false)
    }

    async fn finish_bootstrap_release(
        &self,
        proposal: Arc<ControlProposal>,
        certificate: CompletedControl,
        source: &SourceReservation,
    ) -> ControlResult<bool> {
        self.release(Arc::clone(&proposal), certificate, source)
            .await?;
        if matches!(
            proposal.operation().change,
            replication_types::configuration::control::ConfigurationChange::AddRegion { .. }
        ) {
            return self.release_addition_history(proposal, source).await;
        }
        if !matches!(
            proposal.operation().change,
            replication_types::configuration::control::ConfigurationChange::EnableGlobal { .. }
        ) {
            return Ok(true);
        }
        source.require_live()?;
        let status = crate::bootstrap::release_source_capture(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            proposal,
        )
        .await?;
        Ok(status == crate::bootstrap::SourceCaptureStatus::Released)
    }

    async fn finish_bootstrap_cancellation(
        &self,
        proposal: Arc<ControlProposal>,
        certificate: CancelledControl,
        source: &SourceReservation,
    ) -> ControlResult<bool> {
        self.release_cancelled(Arc::clone(&proposal), certificate, source)
            .await?;
        if matches!(
            proposal.operation().change,
            replication_types::configuration::control::ConfigurationChange::AddRegion { .. }
        ) {
            return self.cancel_addition_history(proposal, source).await;
        }
        if !matches!(
            proposal.operation().change,
            replication_types::configuration::control::ConfigurationChange::EnableGlobal { .. }
        ) {
            return Ok(true);
        }
        source.require_live()?;
        let status = crate::bootstrap::cancel_source_capture(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            proposal,
        )
        .await?;
        Ok(status == crate::bootstrap::SourceCaptureStatus::Cancelled)
    }

    async fn native(
        &self,
        request: ControlRequest,
        source: &SourceReservation,
    ) -> ControlResult<ControlOutcome> {
        source.require_live()?;
        Ok(execute_control(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            Arc::clone(&self.codec),
            Arc::new(request),
        )
        .await?)
    }

    pub(super) fn peer(
        &self,
        proposal: &ControlProposal,
        region: &RegionId,
    ) -> ControlResult<TcpPeerClient> {
        let target = proposal.participant(region)?;
        let source = target
            .proposed
            .sources
            .iter()
            .find(|source| &source.region == region)
            .ok_or_else(|| {
                storage_types::StorageError::unsupported("regional control destination missing")
            })?;
        Ok(self.transport.destination(source)?)
    }

    async fn commit(
        &self,
        proposal: Arc<ControlProposal>,
        local: DurableGrant,
        source: &SourceReservation,
    ) -> ControlResult<()> {
        let mut grants = Vec::with_capacity(proposal.participants().len());
        let mut local = Some(local);
        for region in &proposal.operation().participants {
            if region == self.journal.region() {
                grants.push(ParticipantEvidence::LocalGrant(
                    local.take().ok_or("duplicate local control participant")?,
                ));
            } else {
                source.require_live()?;
                let receipt = self
                    .peer(&proposal, region)?
                    .prepare_control(Arc::clone(&proposal))
                    .await?;
                grants.push(ParticipantEvidence::Remote(receipt));
            }
        }
        if !self
            .propagate_removal_seals(Arc::clone(&proposal), source)
            .await?
        {
            return Ok(());
        }
        self.native(
            ControlRequest::Commit {
                operation: proposal,
                grants,
            },
            source,
        )
        .await?;
        Ok(())
    }

    async fn install_committed(
        &self,
        certificate: super::CommittedControl,
        source: &SourceReservation,
    ) -> ControlResult<()> {
        if !self
            .propagate_removal_seals(Arc::new(certificate.proposal.clone()), source)
            .await?
        {
            return Ok(());
        }
        let proposal = &certificate.proposal;
        let peers = proposal
            .operation()
            .participants
            .iter()
            .filter(|region| *region != self.journal.region())
            .map(|region| self.peer(proposal, region))
            .collect::<ControlResult<Vec<_>>>()?;
        let local = self.native(ControlRequest::Install(certificate.clone()), source);
        let remote = futures::future::join_all(peers.iter().map(|peer| async {
            source.require_live()?;
            peer.install_control(certificate.clone()).await
        }));
        let (local, remote) = tokio::join!(local, remote);
        let mut failure = local.err();
        for result in remote {
            if let Err(error) = result {
                failure.get_or_insert_with(|| error.into());
            }
        }
        failure.map_or(Ok(()), Err)
    }

    async fn finish(
        &self,
        proposal: Arc<ControlProposal>,
        local: DurableInstallation,
        source: &SourceReservation,
    ) -> ControlResult<()> {
        if !self
            .propagate_removal_seals(Arc::clone(&proposal), source)
            .await?
        {
            return Ok(());
        }
        let certificate = local.committed_control();
        let peers = proposal
            .operation()
            .participants
            .iter()
            .filter(|region| *region != self.journal.region())
            .map(|region| self.peer(&proposal, region))
            .collect::<ControlResult<Vec<_>>>()?;
        let mut receipts = futures::future::join_all(peers.iter().map(|peer| async {
            source.require_live()?;
            peer.install_control(certificate.clone()).await
        }))
        .await
        .into_iter();
        let mut installations = Vec::with_capacity(proposal.participants().len());
        let mut local = Some(local);
        let mut failure = None;
        for region in &proposal.operation().participants {
            if region == self.journal.region() {
                installations.push(ParticipantEvidence::LocalInstallation(
                    local.take().ok_or("duplicate local control participant")?,
                ));
                continue;
            }
            match receipts
                .next()
                .ok_or("missing regional installation reply")?
            {
                Ok(receipt) => installations.push(ParticipantEvidence::Remote(receipt)),
                Err(error) => {
                    failure.get_or_insert(error);
                }
            }
        }
        if let Some(error) = failure {
            return Err(error.into());
        }
        self.native(
            ControlRequest::Finish {
                certificate,
                installations,
            },
            source,
        )
        .await?;
        Ok(())
    }

    async fn release(
        &self,
        proposal: Arc<ControlProposal>,
        certificate: CompletedControl,
        source: &SourceReservation,
    ) -> ControlResult<()> {
        for region in &proposal.operation().participants {
            if region != self.journal.region() {
                source.require_live()?;
                let receipt = self
                    .peer(&proposal, region)?
                    .release_control(certificate.clone())
                    .await?;
                if !receipt.matches(&proposal, ControlDecision::Completed, region) {
                    return Err(crate::transport::TransportError::Nack(
                        crate::transport::ImportNack::UnresolvedCommit,
                    )
                    .into());
                }
            }
        }
        Ok(())
    }

    async fn release_cancelled(
        &self,
        proposal: Arc<ControlProposal>,
        certificate: CancelledControl,
        source: &SourceReservation,
    ) -> ControlResult<()> {
        for region in &proposal.operation().participants {
            if region != self.journal.region() {
                source.require_live()?;
                let receipt = self
                    .peer(&proposal, region)?
                    .release_cancelled_control(certificate.clone())
                    .await?;
                if !receipt.matches(&proposal, ControlDecision::Cancelled, region) {
                    return Err(crate::transport::TransportError::Nack(
                        crate::transport::ImportNack::UnresolvedCommit,
                    )
                    .into());
                }
            }
        }
        Ok(())
    }
}

impl<P: DynamoPort> ControlRunner<P> {
    pub(crate) fn operation_timeout(
        &self,
    ) -> Result<std::time::Duration, crate::transport::TransportError> {
        let configuration = self.transport.configuration().as_ref().as_ref();
        let exchanges = u32::try_from(configuration.peers.len())
            .ok()
            .and_then(|count| count.checked_add(1))
            .and_then(|count| count.checked_mul(4))
            .ok_or(crate::transport::TransportError::Configuration)?;
        configuration
            .timing
            .request_budget(exchanges)
            .map_err(|_| crate::transport::TransportError::Configuration)
    }
}

impl<P> Clone for ControlRunner<P> {
    fn clone(&self) -> Self {
        Self {
            port: Arc::clone(&self.port),
            journal: Arc::clone(&self.journal),
            transport: self.transport.clone(),
            codec: Arc::clone(&self.codec),
        }
    }
}
