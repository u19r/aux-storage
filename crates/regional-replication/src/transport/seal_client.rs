use std::sync::Arc;

use super::{
    client::TcpPeerClient,
    control::require_participant,
    error::{TransportError, TransportResult},
    seal_message::{SealAction, SealMessage, SealOutcome, SealReply},
};
use crate::{
    constants::{TRANSPORT_REPLY_KIND, TRANSPORT_SEAL_KIND},
    lifecycle::{SealReceptionProgress, SealRequest, SealVectorPage},
};

impl TcpPeerClient {
    pub(crate) async fn fetch_sealed_table_cuts(
        &self,
        request: Arc<SealRequest>,
        after: u32,
    ) -> TransportResult<SealVectorPage> {
        match self
            .exchange_seal(request, SealAction::Fetch(after))
            .await?
        {
            SealOutcome::Page(page) => Ok(page),
            _ => Err(TransportError::Peer),
        }
    }

    pub(crate) async fn observe_received_seal(
        &self,
        request: Arc<SealRequest>,
    ) -> TransportResult<SealReceptionProgress> {
        match self.exchange_seal(request, SealAction::Observe).await? {
            SealOutcome::Progress(progress) => Ok(progress),
            _ => Err(TransportError::Peer),
        }
    }

    pub(crate) async fn receive_sealed_table_cuts(
        &self,
        request: Arc<SealRequest>,
        page: SealVectorPage,
    ) -> TransportResult<SealReceptionProgress> {
        match self
            .exchange_seal(request, SealAction::Receive(page))
            .await?
        {
            SealOutcome::Progress(progress) => Ok(progress),
            _ => Err(TransportError::Peer),
        }
    }

    async fn exchange_seal(
        &self,
        request: Arc<SealRequest>,
        action: SealAction,
    ) -> TransportResult<SealOutcome> {
        request.validate()?;
        let from = self.resources.identity.clone();
        let to = self.destination_identity();
        require_participant(&request.proposal, &from)?;
        require_participant(&request.proposal, &to)?;
        if from.region != request.proposal.operation().home_region {
            return Err(TransportError::Peer);
        }
        let content = request.content()?;
        self.settle_exchange(move |peer, deadline| async move {
            let mut socket = peer.connect(deadline).await?;
            super::chunk_exchange::write_serialized(
                &mut socket,
                (
                    TRANSPORT_SEAL_KIND,
                    SealMessage {
                        from: from.clone(),
                        to: to.clone(),
                        request,
                        action,
                    },
                ),
                &peer.resources,
                (from.clone(), to.clone()),
            )
            .await?;
            let reply: SealReply = super::chunk_exchange::read_message(
                &mut socket,
                TRANSPORT_REPLY_KIND,
                &peer.resources,
                (to.clone(), from.clone()),
            )
            .await?;
            if reply.from != to || reply.to != from || reply.content != content {
                return Err(TransportError::Peer);
            }
            reply.result.map_err(TransportError::Nack)
        })
        .await
    }
}
