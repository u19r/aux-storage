use std::sync::Arc;

use replication_provider::{
    regional_application::observation::{ImportPageScope, TablePageAcceptance},
    runtime_configuration::deployment::PeerConfiguration,
};
use replication_types::regional_import::RegionalImport;
use tokio::net::TcpStream;
use tracing::{Instrument as _, instrument::WithSubscriber as _};

use super::{
    codec,
    connection::TransportConnection,
    error::{TransportError, TransportResult},
    message::{ImportAcknowledgement, ImportReply, OriginalEvents, OutgoingPage, WireIdentity},
    resources::TransportResources,
};
use crate::constants::{TRANSPORT_DATA_KIND, TRANSPORT_REPLY_KIND};

#[derive(Clone)]
pub struct TcpPeerClient {
    pub(super) resources: Arc<TransportResources>,
    pub(super) peer: PeerConfiguration,
}

/// Validated declaration from a configured private peer. This is not a local
/// CommittedImportPages capability and cannot manufacture native commit
/// evidence.
pub struct RemoteImportReceipt {
    pub(super) destination: WireIdentity,
    pub(super) pages: Vec<TablePageAcceptance>,
    pub(super) bindings: Vec<replication_types::configuration::membership::RegionalTableBinding>,
}

impl RemoteImportReceipt {
    pub fn peer(&self) -> replication_types::identity::replica::ReplicaId {
        self.destination.replica
    }
    pub(crate) fn matches_import(&self, import: &RegionalImport) -> bool {
        self.bindings == import.bindings()
    }
    pub(crate) fn matches_destination(
        &self,
        destination: &replication_types::configuration::installed::RegionalSourceBinding,
    ) -> bool {
        self.destination.region == destination.region
            && self.destination.group == destination.database_group
            && self.destination.generation == destination.generation
    }
    pub fn pages(&self) -> &[TablePageAcceptance] {
        &self.pages
    }
}

impl TcpPeerClient {
    pub(super) async fn settle_exchange<R, F, W>(&self, exchange: F) -> TransportResult<R>
    where
        R: Send + 'static,
        F: FnOnce(Self, tokio::time::Instant) -> W + Send + 'static,
        W: std::future::Future<Output = TransportResult<R>> + Send + 'static,
    {
        let deadline = self.resources.request_deadline()?;
        let reservation =
            tokio::time::timeout_at(deadline, self.resources.outbound.clone().acquire_owned())
                .await
                .map_err(|_| TransportError::Timeout)?
                .map_err(|_| TransportError::Worker)?;
        let peer = self.clone();
        let settlement = async move {
            let _reservation = reservation;
            exchange(peer, deadline).await
        };
        // Timeout detaches the accepted task; its permit still covers live
        // encoding and exchange buffers.
        let accepted = tokio::spawn(
            metrics_facade::inherit_request_cost(settlement)
                .instrument(tracing::Span::current())
                .with_current_subscriber(),
        );
        match tokio::time::timeout_at(deadline, accepted)
            .await
            .map_err(|_| TransportError::Timeout)?
        {
            Ok(result) => result,
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(_) => Err(TransportError::Worker),
        }
    }

    pub(super) async fn deliver_fragment(
        &self,
        import: Arc<RegionalImport>,
    ) -> TransportResult<ImportAcknowledgement> {
        self.settle_exchange(move |peer, deadline| async move {
            let from = peer.resources.identity.clone();
            let to = peer.destination_identity();
            let encoding = Arc::clone(&import);
            let limit = peer.resources.semantic_limit;
            let peers = (from.clone(), to.clone());
            let frame = tokio::task::spawn_blocking(move || encode_page(&encoding, peers, limit))
                .await
                .map_err(|_| TransportError::Worker)??;
            peer.exchange_import(&import, frame, deadline).await
        })
        .await
    }

    pub(super) async fn exchange_import(
        &self,
        import: &RegionalImport,
        frame: codec::EncodedFrame,
        deadline: tokio::time::Instant,
    ) -> TransportResult<ImportAcknowledgement> {
        let reply = self.exchange(frame, deadline).await?;
        if reply.from != self.destination_identity() || reply.to != self.resources.identity {
            return Err(TransportError::Peer);
        }
        let acknowledgement = reply.result.map_err(TransportError::Nack)?;
        ImportPageScope::Selected.require_pages(import, &acknowledgement.pages)?;
        Ok(acknowledgement)
    }
    pub(super) fn destination_identity(&self) -> WireIdentity {
        WireIdentity {
            deployment: self.resources.identity.deployment,
            replica: self.peer.replica,
            region: self.peer.region.clone(),
            group: self.peer.group,
            generation: self.peer.generation,
        }
    }

    pub(super) async fn connect(
        &self,
        deadline: tokio::time::Instant,
    ) -> TransportResult<TransportConnection> {
        if tokio::time::Instant::now() >= deadline {
            return Err(TransportError::Timeout);
        }
        let socket = tokio::time::timeout_at(deadline, TcpStream::connect(self.peer.endpoint))
            .await
            .map_err(|_| TransportError::Timeout)??;
        socket.set_nodelay(true)?;
        Ok(TransportConnection::new(socket, deadline))
    }

    async fn exchange(
        &self,
        frame: codec::EncodedFrame,
        deadline: tokio::time::Instant,
    ) -> TransportResult<ImportReply> {
        let mut socket = self.connect(deadline).await?;
        super::chunk_exchange::write_message(
            &mut socket,
            (TRANSPORT_DATA_KIND, frame),
            &self.resources,
            (self.resources.identity.clone(), self.destination_identity()),
        )
        .await?;
        let reply: ImportReply = super::chunk_exchange::read_message(
            &mut socket,
            TRANSPORT_REPLY_KIND,
            &self.resources,
            (self.destination_identity(), self.resources.identity.clone()),
        )
        .await?;
        Ok(reply)
    }
}

pub(super) fn encode_page(
    import: &RegionalImport,
    peers: (WireIdentity, WireIdentity),
    limit: usize,
) -> TransportResult<codec::EncodedFrame> {
    codec::encode(
        &OutgoingPage {
            from: peers.0,
            to: peers.1,
            manifest: import.manifest(),
            bindings: import.bindings(),
            fragment: import.fragment(),
            events: OriginalEvents(import.events()),
        },
        limit,
    )
}
