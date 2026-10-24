//! A remote observation is bound to one configured destination and exact
//! intent.
use std::sync::Arc;

use replication_types::identity::replica::RegionId;

use super::{
    client::TcpPeerClient,
    error::{TransportError, TransportResult},
    message::WireIdentity,
    trim_message::{TrimCommand, TrimMessage, TrimReply, require_trim_participant},
};
use crate::{
    constants::{TRANSPORT_REPLY_KIND, TRANSPORT_TRIM_KIND},
    trim::{
        proposal::PublicTrimProposal,
        record::{TrimDecision, TrimObservation},
    },
};

pub struct RemoteTrimReceipt {
    destination: WireIdentity,
    record: Option<TrimObservation>,
    installation: Option<crate::trim::completion::TrimInstalledParticipant>,
}

impl RemoteTrimReceipt {
    pub(crate) fn require_grant(
        &self,
        proposal: &PublicTrimProposal,
        region: &RegionId,
    ) -> storage_types::StorageResult<Option<replication_types::protocol::CanonicalDigest>> {
        if self.destination.region != *region {
            return Err(crate::native_record::corrupt_record());
        }
        let Some(record) = &self.record else {
            return Ok(None);
        };
        record.require_proposal(proposal)?;
        match record.decision {
            TrimDecision::Granted(grant) => Ok(Some(grant)),
            TrimDecision::Preparing => Ok(None),
            _ => Err(crate::native_record::corrupt_record()),
        }
    }

    pub(crate) fn require_installation(
        &self,
        proposal: &PublicTrimProposal,
        region: &RegionId,
        digest: replication_types::protocol::CanonicalDigest,
    ) -> storage_types::StorageResult<&crate::trim::completion::TrimInstalledParticipant> {
        let record = self.record.as_ref().ok_or_else(|| {
            storage_types::StorageError::unsupported("Trim participant installation is unresolved")
        })?;
        record.require_proposal(proposal)?;
        if self.destination.region != *region
            || !(record.decision == TrimDecision::Installed(digest)
                || matches!(record.decision, TrimDecision::Completed(_)))
        {
            return Err(crate::native_record::corrupt_record());
        }
        let installed = self
            .installation
            .as_ref()
            .ok_or_else(crate::native_record::corrupt_record)?;
        if installed.region != *region || installed.certificate != digest {
            return Err(crate::native_record::corrupt_record());
        }
        Ok(installed)
    }

    pub(crate) fn require_decision(
        &self,
        proposal: &PublicTrimProposal,
        region: &RegionId,
        decision: TrimDecision,
    ) -> storage_types::StorageResult<()> {
        let record = self.record.as_ref().ok_or_else(|| {
            storage_types::StorageError::unsupported("Trim participant decision is unresolved")
        })?;
        if self.destination.region != *region || !record.decision.satisfies_observation(decision) {
            return Err(crate::native_record::corrupt_record());
        }
        record.require_proposal(proposal)
    }
}

impl TcpPeerClient {
    pub(crate) async fn authorize_trim_grant(
        &self,
        turn: crate::trim::grant_turn::TrimGrantTurn,
    ) -> TransportResult<RemoteTrimReceipt> {
        turn.validate()?;
        if turn.target()? != &self.peer.region {
            return Err(TransportError::Peer);
        }
        self.exchange_trim(TrimCommand::Authorize(turn)).await
    }
    pub async fn prepare_trim(
        &self,
        proposal: Arc<PublicTrimProposal>,
    ) -> TransportResult<RemoteTrimReceipt> {
        self.exchange_trim(TrimCommand::Prepare(proposal)).await
    }

    pub async fn observe_trim(
        &self,
        proposal: Arc<PublicTrimProposal>,
    ) -> TransportResult<RemoteTrimReceipt> {
        self.exchange_trim(TrimCommand::Observe(proposal)).await
    }

    async fn exchange_trim(&self, command: TrimCommand) -> TransportResult<RemoteTrimReceipt> {
        command.validate()?;
        let proposal = command.proposal().clone();
        self.settle_exchange(move |peer, deadline| async move {
            let from = peer.resources.identity.clone();
            let to = peer.destination_identity();
            require_trim_participant(&proposal, &from)?;
            require_trim_participant(&proposal, &to)?;
            if from.region != proposal.primary {
                return Err(TransportError::Peer);
            }
            let mut socket = peer.connect(deadline).await?;
            super::chunk_exchange::write_serialized(
                &mut socket,
                (
                    TRANSPORT_TRIM_KIND,
                    TrimMessage {
                        from: from.clone(),
                        to: to.clone(),
                        command,
                    },
                ),
                &peer.resources,
                (from.clone(), to.clone()),
            )
            .await?;
            let reply: TrimReply = super::chunk_exchange::read_message(
                &mut socket,
                TRANSPORT_REPLY_KIND,
                &peer.resources,
                (to.clone(), from.clone()),
            )
            .await?;
            if reply.from != to || reply.to != from || reply.proposal != proposal {
                return Err(TransportError::Peer);
            }
            let outcome = reply.result.map_err(TransportError::Nack)?;
            let record = outcome.observation;
            if let Some(record) = &record {
                record.require_proposal(&proposal)?;
            }
            Ok(RemoteTrimReceipt {
                destination: to,
                record,
                installation: outcome.installation,
            })
        })
        .await
    }
}

impl super::resources::TcpTransport {
    pub(crate) async fn observe_trim_participants(
        &self,
        peers: Vec<TcpPeerClient>,
        proposal: Arc<PublicTrimProposal>,
    ) -> TransportResult<Vec<RemoteTrimReceipt>> {
        self.exchange_trim_participants(peers, TrimCommand::Observe(proposal))
            .await
    }
    pub(crate) async fn settle_trim_participants(
        &self,
        peers: Vec<TcpPeerClient>,
        record: TrimObservation,
    ) -> TransportResult<Vec<RemoteTrimReceipt>> {
        let command = match record.decision {
            TrimDecision::Granted(_)
            | TrimDecision::Committed(_)
            | TrimDecision::Installed(_)
            | TrimDecision::Completed(_) => return Err(super::error::TransportError::Peer),
            TrimDecision::Preparing => TrimCommand::Prepare(Arc::new(record.proposal)),
            TrimDecision::Cancelled => TrimCommand::Cancel(record),
        };
        self.exchange_trim_participants(peers, command).await
    }

    pub(crate) async fn install_trim_participants(
        &self,
        peers: Vec<TcpPeerClient>,
        certificate: crate::trim::commit::TrimCommitCertificate,
    ) -> TransportResult<Vec<RemoteTrimReceipt>> {
        certificate.require_integrity()?;
        self.exchange_trim_participants(peers, TrimCommand::Install(certificate))
            .await
    }

    pub(crate) async fn finish_trim_participants(
        &self,
        peers: Vec<TcpPeerClient>,
        completion: crate::trim::completion::TrimCompletionCertificate,
    ) -> TransportResult<Vec<RemoteTrimReceipt>> {
        self.exchange_trim_participants(peers, TrimCommand::Finish(completion))
            .await
    }

    async fn exchange_trim_participants(
        &self,
        peers: Vec<TcpPeerClient>,
        command: TrimCommand,
    ) -> TransportResult<Vec<RemoteTrimReceipt>> {
        use futures::StreamExt;
        // Start each exchange deadline only when its bounded pool position is
        // admitted. Collect every result before selecting an error, preserving
        // native settlement.
        let results: Vec<_> = futures::stream::iter(peers)
            .map(|peer| {
                let command = command.clone();
                async move { peer.exchange_trim(command).await }
            })
            .buffered(self.resources.source_capacity as usize)
            .collect()
            .await;
        results.into_iter().collect()
    }
}
