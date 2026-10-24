//! One shared job recovers primary coordination and local participant scans.
use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_provider::DynamoPort;

use super::runner::{ControlResult, ControlRunner};
use crate::{
    transport::{SourceReservation, TcpPeerClient},
    trim::{
        executor::{TrimRequest, execute_trim},
        proposal::PublicTrimProposal,
        record::{TrimDecision, TrimOutcome, TrimWorkState},
    },
};

impl<P: DynamoPort + 'static> ControlRunner<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub(super) async fn advance_trim(
        &self,
        proposal: Arc<PublicTrimProposal>,
        source: &SourceReservation,
    ) -> ControlResult<TrimWorkState> {
        let outcome = self
            .advance_qualified_trim(Arc::clone(&proposal), source)
            .await?;
        let decision = outcome
            .observation
            .as_ref()
            .ok_or_else(crate::native_record::corrupt_record)?
            .decision;
        if proposal.primary != *self.journal.region() {
            return Ok(outcome.work);
        }
        if matches!(
            decision,
            TrimDecision::Committed(_) | TrimDecision::Installed(_) | TrimDecision::Completed(_)
        ) {
            return self.advance_committed_trim(proposal, outcome, source).await;
        }
        if decision == TrimDecision::Cancelled && outcome.cancellation_released {
            return Ok(TrimWorkState::Waiting);
        }
        if decision == TrimDecision::Preparing
            && outcome.grant_turn.is_some()
            && outcome.work == TrimWorkState::Ready
        {
            return Ok(TrimWorkState::Ready);
        }
        if matches!(decision, TrimDecision::Granted(_))
            || (decision == TrimDecision::Preparing && outcome.participants_prepared)
        {
            return self
                .settle_ordered_trim_grants(
                    proposal,
                    outcome
                        .observation
                        .ok_or_else(crate::native_record::corrupt_record)?,
                    source,
                )
                .await;
        }
        self.settle_trim_preparation(
            proposal,
            outcome
                .observation
                .ok_or_else(crate::native_record::corrupt_record)?,
            source,
        )
        .await
    }

    async fn advance_committed_trim(
        &self,
        proposal: Arc<PublicTrimProposal>,
        outcome: TrimOutcome,
        source: &SourceReservation,
    ) -> ControlResult<TrimWorkState> {
        let decision = outcome
            .observation
            .as_ref()
            .ok_or_else(crate::native_record::corrupt_record)?
            .decision;
        match decision {
            TrimDecision::Committed(_) => {
                source.require_live()?;
                self.execute_trim_request(TrimRequest::InstallPrimary(proposal))
                    .await?;
                Ok(TrimWorkState::Ready)
            }
            TrimDecision::Installed(_) => {
                self.install_trim_participants(
                    proposal,
                    outcome
                        .certificate
                        .ok_or_else(crate::native_record::corrupt_record)?,
                    source,
                )
                .await
            }
            TrimDecision::Completed(_) if outcome.completion_released => Ok(TrimWorkState::Waiting),
            TrimDecision::Completed(_) => {
                self.finish_trim_participants(
                    proposal,
                    outcome
                        .completion
                        .ok_or_else(crate::native_record::corrupt_record)?,
                    source,
                )
                .await
            }
            _ => Err(crate::native_record::corrupt_record().into()),
        }
    }

    async fn settle_trim_preparation(
        &self,
        proposal: Arc<PublicTrimProposal>,
        record: crate::trim::record::TrimObservation,
        source: &SourceReservation,
    ) -> ControlResult<TrimWorkState> {
        let peers = self.trim_peers(&proposal)?;
        source.require_live()?;
        let decision = record.decision;
        let receipts = self
            .transport
            .settle_trim_participants(peers, record)
            .await?;
        source.require_live()?;
        match decision {
            TrimDecision::Cancelled => {
                self.execute_trim_request(TrimRequest::ReleaseCancellation { proposal, receipts })
                    .await?;
                Ok(TrimWorkState::Waiting)
            }
            TrimDecision::Preparing => {
                self.execute_trim_request(TrimRequest::RecordPreparation { proposal, receipts })
                    .await?;
                Ok(TrimWorkState::Ready)
            }
            _ => Err(crate::native_record::corrupt_record().into()),
        }
    }

    async fn install_trim_participants(
        &self,
        proposal: Arc<PublicTrimProposal>,
        certificate: crate::trim::commit::TrimCommitCertificate,
        source: &SourceReservation,
    ) -> ControlResult<TrimWorkState> {
        let peers = self.trim_peers(&proposal)?;
        source.require_live()?;
        let receipts = self
            .transport
            .install_trim_participants(peers, certificate)
            .await?;
        source.require_live()?;
        self.execute_trim_request(TrimRequest::Complete { proposal, receipts })
            .await?;
        Ok(TrimWorkState::Ready)
    }

    async fn finish_trim_participants(
        &self,
        proposal: Arc<PublicTrimProposal>,
        completion: crate::trim::completion::TrimCompletionCertificate,
        source: &SourceReservation,
    ) -> ControlResult<TrimWorkState> {
        let peers = self.trim_peers(&proposal)?;
        source.require_live()?;
        let receipts = self
            .transport
            .finish_trim_participants(peers, completion)
            .await?;
        source.require_live()?;
        self.execute_trim_request(TrimRequest::ReleaseCompletion { proposal, receipts })
            .await?;
        Ok(TrimWorkState::Waiting)
    }

    async fn settle_ordered_trim_grants(
        &self,
        proposal: Arc<PublicTrimProposal>,
        record: crate::trim::record::TrimObservation,
        source: &SourceReservation,
    ) -> ControlResult<TrimWorkState> {
        let peers = self.trim_peers(&proposal)?;
        source.require_live()?;
        let receipts = self
            .transport
            .observe_trim_participants(peers, Arc::clone(&proposal))
            .await?;
        source.require_live()?;
        if let Some(turn) = crate::trim::grant_turn::TrimGrantTurn::try_next(&record, &receipts)? {
            if turn.target()? == self.journal.region() {
                let authorization =
                    crate::trim::grant_turn::PrimaryTrimGrantTurn::try_from_primary(
                        turn,
                        &proposal.primary,
                    )?;
                self.execute_trim_request(TrimRequest::AuthorizeGrant(authorization))
                    .await?;
            } else {
                let target = turn.target()?;
                let participants = proposal
                    .participants
                    .iter()
                    .filter(|participant| &participant.region != self.journal.region());
                let peer = participants
                    .zip(self.trim_peers(&proposal)?)
                    .find(|(participant, _)| &participant.region == target)
                    .map(|(_, peer)| peer)
                    .ok_or_else(crate::native_record::corrupt_record)?;
                peer.authorize_trim_grant(turn).await?;
            }
            source.require_live()?;
            return Ok(TrimWorkState::Waiting);
        }
        self.execute_trim_request(TrimRequest::Commit { proposal, receipts })
            .await?;
        Ok(TrimWorkState::Waiting)
    }

    async fn advance_qualified_trim(
        &self,
        proposal: Arc<PublicTrimProposal>,
        source: &SourceReservation,
    ) -> ControlResult<TrimOutcome> {
        let qualification = self.journal.begin_trim_time(proposal.operation);
        source.require_live()?;
        let mut outcome = self
            .execute_trim_request(TrimRequest::Advance(Arc::clone(&proposal)))
            .await?;
        source.require_live()?;
        if outcome
            .observation
            .as_ref()
            .is_some_and(|record| record.decision != TrimDecision::Preparing)
        {
            return Ok(outcome);
        }
        if outcome.grant_turn.is_none() {
            return Ok(outcome);
        }
        let age = self
            .journal
            .observe_trim_time(
                (&proposal, source.require_time_deadline()?),
                outcome.prefix.take(),
            )
            .await;
        source.require_live()?;
        let Some(age) = age else {
            qualification.retain_anchor();
            return Ok(outcome);
        };
        let encoding = (self.transport.source_limits(), Arc::clone(&self.codec));
        let outcome = crate::trim::executor::execute_qualified_trim(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            (Arc::clone(&proposal), age),
            encoding,
        )
        .await?;
        source.require_live()?;
        if outcome.work == TrimWorkState::AwaitingQualification {
            qualification.retain_anchor();
        }
        Ok(outcome)
    }

    async fn execute_trim_request(&self, request: TrimRequest) -> ControlResult<TrimOutcome> {
        let encoding = (self.transport.source_limits(), Arc::clone(&self.codec));
        let request = Arc::new(request);
        Ok(execute_trim(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            request,
            encoding,
        )
        .await?)
    }

    fn trim_peers(&self, proposal: &PublicTrimProposal) -> ControlResult<Vec<TcpPeerClient>> {
        proposal
            .participants
            .iter()
            .filter(|participant| &participant.region != self.journal.region())
            .map(|participant| {
                let state = &participant.state;
                let source = state
                    .sources
                    .iter()
                    .find(|source| {
                        source.region == participant.region
                            && source.table == state.configuration.table
                            && source.config == state.configuration.config
                    })
                    .ok_or_else(crate::native_record::corrupt_record)?;
                Ok(self.transport.destination(source)?)
            })
            .collect()
    }
}
