//! Only the configured primary can drive a participant's immutable trim
//! operation.
use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_provider::DynamoPort;

use super::{
    connection::TransportConnection,
    error::{ImportNack, TransportError, TransportResult},
    server::{TcpImportServer, classify_native_error},
    trim_message::{TrimCommand, TrimMessage, TrimReply, require_trim_participant},
};
use crate::{
    constants::TRANSPORT_REPLY_KIND,
    trim::{
        executor::{TrimRequest, execute_trim},
        record::TrimOutcome,
    },
};

impl<P: DynamoPort + 'static> TcpImportServer<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
        + storage_provider::PrimaryStateScanAttempt
        + storage_provider::ItemTombstoneScanAttempt
        + storage_provider::GsiProjectionScanAttempt
        + storage_provider::TtlProjectionScanAttempt
        + storage_provider::GsiProjectionMetadataAttempt
{
    pub(super) async fn receive_trim(
        &self,
        socket: &mut TransportConnection,
        message: TrimMessage,
    ) -> TransportResult<()> {
        self.require_peer(&message.from, &message.to)?;
        message.command.validate()?;
        let proposal = message.command.proposal().clone();
        require_trim_participant(&proposal, &message.from)?;
        require_trim_participant(&proposal, &message.to)?;
        if message.from.region != proposal.primary {
            return Err(TransportError::Peer);
        }
        socket.require_live()?;
        let result = self.apply_trim(message.command, &message.from.region).await;
        super::chunk_exchange::write_serialized(
            socket,
            (
                TRANSPORT_REPLY_KIND,
                TrimReply {
                    from: self.resources.identity.clone(),
                    to: message.from.clone(),
                    proposal,
                    result,
                },
            ),
            &self.resources,
            (self.resources.identity.clone(), message.from),
        )
        .await
    }

    async fn apply_trim(
        &self,
        command: TrimCommand,
        primary: &replication_types::identity::replica::RegionId,
    ) -> Result<TrimOutcome, ImportNack> {
        let proposal = Arc::new(command.proposal().clone());
        let request = match command {
            TrimCommand::Authorize(turn) => TrimRequest::AuthorizeGrant(
                crate::trim::grant_turn::PrimaryTrimGrantTurn::try_from_primary(turn, primary)
                    .map_err(|error| classify_native_error(&error))?,
            ),
            TrimCommand::Finish(completion) => TrimRequest::Finish(
                crate::trim::completion::PrimaryTrimCompletion::try_from_primary(
                    completion, primary,
                )
                .map_err(|error| classify_native_error(&error))?,
            ),
            TrimCommand::Install(certificate) => TrimRequest::InstallParticipant(
                crate::trim::installation::PrimaryTrimInstallation::try_from_primary(
                    certificate,
                    primary,
                )
                .map_err(|error| classify_native_error(&error))?,
            ),
            TrimCommand::Prepare(_) => TrimRequest::Prepare(Arc::clone(&proposal)),
            TrimCommand::Observe(_) => TrimRequest::Observe(Arc::clone(&proposal)),
            TrimCommand::Cancel(record) => TrimRequest::ReleaseParticipantCancellation(
                crate::trim::record::PrimaryTrimCancellation::try_from_primary(record, primary)
                    .map_err(|error| classify_native_error(&error))?,
            ),
        };
        let request = Arc::new(request);
        let encoding = (self.resources.source_limits(), Arc::clone(&self.codec));
        execute_trim(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            request,
            encoding,
        )
        .await
        .map_err(|error| classify_native_error(&error))
    }
}
