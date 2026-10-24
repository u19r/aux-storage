use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_provider::DynamoPort;

use super::{
    connection::TransportConnection,
    control::require_participant,
    error::{ImportNack, TransportError, TransportResult},
    history_message::{
        HistoryCaptureAction, HistoryCaptureMessage, HistoryCaptureOutcome, HistoryCaptureReply,
    },
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
    pub(super) async fn receive_history_capture(
        &self,
        socket: &mut TransportConnection,
        message: HistoryCaptureMessage,
    ) -> TransportResult<()> {
        self.require_peer(&message.from, &message.to)?;
        message.request.validate()?;
        require_participant(&message.request.proposal, &message.from)?;
        require_participant(&message.request.proposal, &message.to)?;
        if message.from.region != message.request.proposal.operation().home_region
            || message.to.region != message.request.region
        {
            return Err(TransportError::Peer);
        }
        let content = message.request.content()?;
        socket.require_live()?;
        let result = self
            .apply_history_capture(message.request, message.action)
            .await;
        super::chunk_exchange::write_serialized(
            socket,
            (
                TRANSPORT_REPLY_KIND,
                HistoryCaptureReply {
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

    async fn apply_history_capture(
        &self,
        request: Arc<crate::history_capture::HistoryCaptureRequest>,
        action: HistoryCaptureAction,
    ) -> Result<HistoryCaptureOutcome, ImportNack> {
        let result = match action {
            HistoryCaptureAction::DeclareCollection(definition) => {
                crate::history_collection::source_executor::declare_source_collection(
                    self.port.as_ref(),
                    Arc::clone(&self.journal),
                    (request, Arc::new(*definition)),
                )
                .await
                .map(|()| HistoryCaptureOutcome::CollectionAccepted)
            }
            HistoryCaptureAction::ObserveCollection => {
                crate::history_collection::executor::observe_collection(
                    self.port.as_ref(),
                    Arc::clone(&self.journal),
                    Arc::new(request.proposal.clone()),
                )
                .await
                .map(|observation| HistoryCaptureOutcome::Collection(observation.map(Box::new)))
            }
            HistoryCaptureAction::AcceptCollection { revision, change } => {
                crate::history_collection::executor::accept_collection(
                    self.port.as_ref(),
                    Arc::clone(&self.journal),
                    Arc::new(request.proposal.clone()),
                    (revision, Arc::new(*change)),
                )
                .await
                .map(|()| HistoryCaptureOutcome::CollectionAccepted)
            }
            HistoryCaptureAction::Advance => crate::history_capture::advance_history_capture(
                self.port.as_ref(),
                Arc::clone(&self.journal),
                request,
            )
            .await
            .map(|status| HistoryCaptureOutcome::Status(Some(status))),
            HistoryCaptureAction::Observe => crate::history_capture::observe_history_capture(
                self.port.as_ref(),
                Arc::clone(&self.journal),
                request,
            )
            .await
            .map(HistoryCaptureOutcome::Status),
            HistoryCaptureAction::Fetch(after) => {
                crate::history_capture::load_history_capture_cuts(
                    self.port.as_ref(),
                    Arc::clone(&self.journal),
                    request,
                    after,
                )
                .await
                .map(HistoryCaptureOutcome::Vector)
            }
            HistoryCaptureAction::Definition => {
                crate::history_capture::load_history_capture_definition(
                    self.port.as_ref(),
                    Arc::clone(&self.journal),
                    request,
                )
                .await
                .map(HistoryCaptureOutcome::Definition)
            }
            HistoryCaptureAction::BaselineDefinition => {
                crate::history_capture::load_retained_baseline_definition(
                    self.port.as_ref(),
                    Arc::clone(&self.journal),
                    request,
                )
                .await
                .map(HistoryCaptureOutcome::BaselineDefinition)
            }
            HistoryCaptureAction::Baselines(scope) => {
                crate::history_capture::load_retained_baseline_page(
                    self.port.as_ref(),
                    Arc::clone(&self.journal),
                    request,
                    (Arc::clone(&self.codec), Arc::new(scope)),
                )
                .await
                .map(|page| HistoryCaptureOutcome::Baselines(Box::new(page)))
            }
            HistoryCaptureAction::Catalog(scope) => {
                crate::history_capture::load_retired_source_catalog(
                    self.port.as_ref(),
                    Arc::clone(&self.journal),
                    request,
                    Arc::new(scope),
                )
                .await
                .map(HistoryCaptureOutcome::Catalog)
            }
            HistoryCaptureAction::Retired { source, after } => {
                crate::history_capture::load_retired_history_page(
                    self.port.as_ref(),
                    Arc::clone(&self.journal),
                    request,
                    (source, after),
                )
                .await
                .map(HistoryCaptureOutcome::Retired)
            }
            HistoryCaptureAction::Cancel => crate::history_capture::cancel_history_capture(
                self.port.as_ref(),
                Arc::clone(&self.journal),
                request,
            )
            .await
            .map(|status| HistoryCaptureOutcome::Status(Some(status))),
            HistoryCaptureAction::Release => crate::history_capture::release_history_capture(
                self.port.as_ref(),
                Arc::clone(&self.journal),
                request,
            )
            .await
            .map(|status| HistoryCaptureOutcome::Status(Some(status))),
        };
        result.map_err(|error| classify_native_error(&error))
    }
}
