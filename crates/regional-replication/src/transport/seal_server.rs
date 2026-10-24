use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_provider::DynamoPort;

use super::{
    connection::TransportConnection,
    control::require_participant,
    error::{ImportNack, TransportError, TransportResult},
    seal_message::{SealAction, SealMessage, SealOutcome, SealReply},
    server::{TcpImportServer, classify_native_error},
};
use crate::constants::TRANSPORT_REPLY_KIND;

impl<P: DynamoPort + 'static> TcpImportServer<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
        + storage_provider::PrimaryStateScanAttempt
        + storage_provider::ItemTombstoneScanAttempt
        + storage_provider::GsiProjectionScanAttempt
        + storage_provider::TtlProjectionScanAttempt
        + storage_provider::GsiProjectionMetadataAttempt
{
    pub(super) async fn receive_seal(
        &self,
        socket: &mut TransportConnection,
        message: SealMessage,
    ) -> TransportResult<()> {
        self.require_peer(&message.from, &message.to)?;
        message.request.validate()?;
        require_participant(&message.request.proposal, &message.from)?;
        require_participant(&message.request.proposal, &message.to)?;
        if message.from.region != message.request.proposal.operation().home_region {
            return Err(TransportError::Peer);
        }
        let content = message.request.content()?;
        socket.require_live()?;
        let result = self.apply_seal(message.request, message.action).await;
        super::chunk_exchange::write_serialized(
            socket,
            (
                TRANSPORT_REPLY_KIND,
                SealReply {
                    from: self.resources.identity.clone(),
                    to: message.from.clone(),
                    content,
                    result,
                },
            ),
            &self.resources,
            (self.resources.identity.clone(), message.from),
        )
        .await
    }

    async fn apply_seal(
        &self,
        request: Arc<crate::lifecycle::SealRequest>,
        action: SealAction,
    ) -> Result<SealOutcome, ImportNack> {
        match action {
            SealAction::Fetch(after) => {
                if &request.region != self.journal.region() {
                    return Err(ImportNack::InvalidPage);
                }
                crate::lifecycle::load_sealed_table_cuts(
                    self.port.as_ref(),
                    Arc::clone(&self.journal),
                    request,
                    after,
                )
                .await
                .map(SealOutcome::Page)
                .map_err(|error| classify_native_error(&error))
            }
            SealAction::Observe => crate::lifecycle::observe_received_seal(
                self.port.as_ref(),
                Arc::clone(&self.journal),
                request,
            )
            .await
            .map(SealOutcome::Progress)
            .map_err(|error| classify_native_error(&error)),
            SealAction::Receive(page) => crate::lifecycle::receive_sealed_table_cuts(
                self.port.as_ref(),
                Arc::clone(&self.journal),
                (request, Arc::new(page)),
            )
            .await
            .map(SealOutcome::Progress)
            .map_err(|error| classify_native_error(&error)),
        }
    }
}
