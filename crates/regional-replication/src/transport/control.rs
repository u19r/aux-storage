use std::sync::Arc;

use replication_types::{configuration::proposal::ControlProposal, identity::replica::RegionId};

use super::{
    client::TcpPeerClient,
    control_message::{ControlCommand, ControlMessage, ControlReply},
    error::{TransportError, TransportResult},
    message::WireIdentity,
};
use crate::{
    constants::{TRANSPORT_CONTROL_KIND, TRANSPORT_REPLY_KIND},
    control::{
        CancelledControl, CommittedControl, CompletedControl, ControlDecision, ControlRecord,
    },
};

pub struct RemoteControlReceipt {
    destination: WireIdentity,
    record: ControlRecord,
}

impl RemoteControlReceipt {
    pub(crate) fn matches(
        &self,
        proposal: &ControlProposal,
        expected: ControlDecision,
        region: &RegionId,
    ) -> bool {
        self.destination.region == *region
            && &self.record.proposal == proposal
            && (self.record.decision == expected
                || (expected == ControlDecision::Installed
                    && self.record.decision == ControlDecision::Completed))
    }

    pub(crate) fn retirement(&self) -> &[crate::control::RetiredSealEvidence] {
        &self.record.retirement
    }

    pub(crate) fn bootstrap(&self) -> Option<replication_types::protocol::CanonicalDigest> {
        self.record.bootstrap
    }

    pub fn region(&self) -> &RegionId {
        &self.destination.region
    }
}

impl TcpPeerClient {
    pub async fn prepare_control(
        &self,
        proposal: Arc<ControlProposal>,
    ) -> TransportResult<RemoteControlReceipt> {
        self.control(ControlCommand::Prepare { proposal }).await
    }

    pub async fn observe_control(
        &self,
        proposal: Arc<ControlProposal>,
    ) -> TransportResult<RemoteControlReceipt> {
        self.control(ControlCommand::Observe { proposal }).await
    }

    pub async fn install_control(
        &self,
        certificate: CommittedControl,
    ) -> TransportResult<RemoteControlReceipt> {
        self.control(ControlCommand::Install {
            proposal: Arc::new(certificate.proposal),
            bootstrap: certificate.bootstrap,
            retirement: certificate.retirement,
        })
        .await
    }

    pub async fn release_control(
        &self,
        certificate: CompletedControl,
    ) -> TransportResult<RemoteControlReceipt> {
        self.control(ControlCommand::Release {
            proposal: Arc::new(certificate.proposal),
            bootstrap: certificate.bootstrap,
            retirement: certificate.retirement,
        })
        .await
    }

    pub async fn release_cancelled_control(
        &self,
        certificate: CancelledControl,
    ) -> TransportResult<RemoteControlReceipt> {
        self.control(ControlCommand::ReleaseCancelled {
            proposal: Arc::new(certificate.proposal),
            bootstrap: certificate.bootstrap,
        })
        .await
    }

    async fn control(&self, command: ControlCommand) -> TransportResult<RemoteControlReceipt> {
        command.validate()?;
        let proposal = Arc::clone(command.proposal());
        self.settle_exchange(move |peer, deadline| async move {
            let from = peer.resources.identity.clone();
            let to = peer.destination_identity();
            require_participant(&proposal, &to)?;
            let frame = ControlMessage {
                from: from.clone(),
                to: to.clone(),
                command,
            };
            let mut socket = peer.connect(deadline).await?;
            super::chunk_exchange::write_serialized(
                &mut socket,
                (TRANSPORT_CONTROL_KIND, frame),
                &peer.resources,
                (from.clone(), to.clone()),
            )
            .await?;
            let reply: ControlReply = super::chunk_exchange::read_message(
                &mut socket,
                TRANSPORT_REPLY_KIND,
                &peer.resources,
                (to.clone(), from.clone()),
            )
            .await?;
            if reply.from != to || reply.to != from {
                return Err(TransportError::Peer);
            }
            let record = reply.result.map_err(TransportError::Nack)?;
            if record.proposal != *proposal {
                return Err(TransportError::Peer);
            }
            record.validate_evidence()?;
            Ok(RemoteControlReceipt {
                destination: to,
                record,
            })
        })
        .await
    }
}

pub(super) fn require_participant(
    proposal: &ControlProposal,
    identity: &WireIdentity,
) -> TransportResult<()> {
    let target = proposal.participant(&identity.region)?;
    if !target.proposed.sources.iter().any(|source| {
        source.region == identity.region
            && source.database_group == identity.group
            && source.generation == identity.generation
    }) {
        return Err(TransportError::Peer);
    }
    Ok(())
}
