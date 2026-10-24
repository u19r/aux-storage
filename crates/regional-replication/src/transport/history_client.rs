use std::sync::Arc;

use super::{
    client::TcpPeerClient,
    control::require_participant,
    error::{TransportError, TransportResult},
    history_message::{
        HistoryCaptureAction, HistoryCaptureMessage, HistoryCaptureOutcome, HistoryCaptureReply,
    },
};
use crate::{
    constants::{TRANSPORT_HISTORY_CAPTURE_KIND, TRANSPORT_REPLY_KIND},
    history_capture::{HistoryCaptureRequest, HistoryCaptureStatus, HistoryCaptureVectorPage},
};

impl TcpPeerClient {
    pub(crate) async fn declare_source_collection(
        &self,
        request: Arc<HistoryCaptureRequest>,
        definition: Arc<crate::history_collection::HistoryAdoptionDefinition>,
    ) -> TransportResult<()> {
        definition.validate()?;
        if definition.proposal() != &request.proposal {
            return Err(TransportError::Peer);
        }
        match self
            .exchange_history_capture(
                request,
                HistoryCaptureAction::DeclareCollection(Box::new(definition.as_ref().clone())),
            )
            .await?
        {
            HistoryCaptureOutcome::CollectionAccepted => Ok(()),
            _ => Err(TransportError::Peer),
        }
    }

    pub(crate) async fn observe_source_collection(
        &self,
        request: Arc<HistoryCaptureRequest>,
    ) -> TransportResult<Option<crate::history_collection::HistoryCollectionObservation>> {
        match self
            .exchange_history_capture(
                Arc::clone(&request),
                HistoryCaptureAction::ObserveCollection,
            )
            .await?
        {
            HistoryCaptureOutcome::Collection(observation) => {
                if let Some(observation) = &observation {
                    observation.record.validate()?;
                    if observation.record.proposal != request.proposal {
                        return Err(TransportError::Peer);
                    }
                }
                Ok(observation.map(|observation| *observation))
            }
            _ => Err(TransportError::Peer),
        }
    }

    pub(crate) async fn accept_source_collection(
        &self,
        request: Arc<HistoryCaptureRequest>,
        input: (
            storage_types::Revision,
            crate::history_collection::HistoryCollectionChange,
        ),
    ) -> TransportResult<()> {
        match self
            .exchange_history_capture(
                request,
                HistoryCaptureAction::AcceptCollection {
                    revision: input.0,
                    change: Box::new(input.1),
                },
            )
            .await?
        {
            HistoryCaptureOutcome::CollectionAccepted => Ok(()),
            _ => Err(TransportError::Peer),
        }
    }

    pub async fn advance_history_capture(
        &self,
        request: Arc<HistoryCaptureRequest>,
    ) -> TransportResult<HistoryCaptureStatus> {
        match self
            .exchange_history_capture(request, HistoryCaptureAction::Advance)
            .await?
        {
            HistoryCaptureOutcome::Status(Some(status)) => Ok(status),
            _ => Err(TransportError::Peer),
        }
    }

    pub async fn observe_history_capture(
        &self,
        request: Arc<HistoryCaptureRequest>,
    ) -> TransportResult<Option<HistoryCaptureStatus>> {
        match self
            .exchange_history_capture(request, HistoryCaptureAction::Observe)
            .await?
        {
            HistoryCaptureOutcome::Status(status) => Ok(status),
            _ => Err(TransportError::Peer),
        }
    }

    pub async fn fetch_history_capture_cuts(
        &self,
        request: Arc<HistoryCaptureRequest>,
        after: u32,
    ) -> TransportResult<HistoryCaptureVectorPage> {
        match self
            .exchange_history_capture(Arc::clone(&request), HistoryCaptureAction::Fetch(after))
            .await?
        {
            HistoryCaptureOutcome::Vector(page) => {
                request.require_page(&page, after)?;
                Ok(page)
            }
            _ => Err(TransportError::Peer),
        }
    }

    pub async fn fetch_history_capture_definition(
        &self,
        request: Arc<HistoryCaptureRequest>,
    ) -> TransportResult<crate::history_capture::HistoryCaptureDefinition> {
        match self
            .exchange_history_capture(Arc::clone(&request), HistoryCaptureAction::Definition)
            .await?
        {
            HistoryCaptureOutcome::Definition(definition) => {
                definition.validate()?;
                if definition.request().content()? != request.content()? {
                    return Err(TransportError::Peer);
                }
                Ok(definition)
            }
            _ => Err(TransportError::Peer),
        }
    }

    pub async fn fetch_retained_baseline_definition(
        &self,
        request: Arc<HistoryCaptureRequest>,
    ) -> TransportResult<crate::history_capture::RetainedBaselineDefinition> {
        match self
            .exchange_history_capture(
                Arc::clone(&request),
                HistoryCaptureAction::BaselineDefinition,
            )
            .await?
        {
            HistoryCaptureOutcome::BaselineDefinition(definition) => {
                definition.require_request(&request)?;
                Ok(definition)
            }
            _ => Err(TransportError::Peer),
        }
    }

    pub async fn fetch_retained_baseline_page(
        &self,
        request: Arc<HistoryCaptureRequest>,
        scope: Arc<crate::history_capture::RetainedBaselineScope>,
    ) -> TransportResult<crate::history_capture::RetainedBaselinePage> {
        match self
            .exchange_history_capture(
                Arc::clone(&request),
                HistoryCaptureAction::Baselines(scope.as_ref().clone()),
            )
            .await?
        {
            HistoryCaptureOutcome::Baselines(page) => {
                page.require_scope(&request, &scope)?;
                Ok(*page)
            }
            _ => Err(TransportError::Peer),
        }
    }

    pub async fn fetch_retired_source_catalog(
        &self,
        request: Arc<HistoryCaptureRequest>,
        scope: Arc<crate::history_capture::HistoryCatalogScope>,
    ) -> TransportResult<crate::history_capture::RetiredHistoryCatalogPage> {
        match self
            .exchange_history_capture(
                Arc::clone(&request),
                HistoryCaptureAction::Catalog(scope.as_ref().clone()),
            )
            .await?
        {
            HistoryCaptureOutcome::Catalog(page) => {
                page.require_scope(&request, &scope)?;
                Ok(page)
            }
            _ => Err(TransportError::Peer),
        }
    }

    pub async fn fetch_retired_history_page(
        &self,
        request: Arc<HistoryCaptureRequest>,
        scope: (
            replication_types::configuration::installed::RegionalSourceBinding,
            u32,
        ),
    ) -> TransportResult<crate::history_capture::RetiredHistoryPage> {
        match self
            .exchange_history_capture(
                Arc::clone(&request),
                HistoryCaptureAction::Retired {
                    source: scope.0.clone(),
                    after: scope.1,
                },
            )
            .await?
        {
            HistoryCaptureOutcome::Retired(page) => {
                page.definition.require_request(&request, &scope.0)?;
                page.definition.require_page(&page, scope.1)?;
                Ok(page)
            }
            _ => Err(TransportError::Peer),
        }
    }

    pub async fn cancel_history_capture(
        &self,
        request: Arc<HistoryCaptureRequest>,
    ) -> TransportResult<HistoryCaptureStatus> {
        match self
            .exchange_history_capture(request, HistoryCaptureAction::Cancel)
            .await?
        {
            HistoryCaptureOutcome::Status(Some(status)) => Ok(status),
            _ => Err(TransportError::Peer),
        }
    }

    pub async fn release_history_capture(
        &self,
        request: Arc<HistoryCaptureRequest>,
    ) -> TransportResult<HistoryCaptureStatus> {
        match self
            .exchange_history_capture(request, HistoryCaptureAction::Release)
            .await?
        {
            HistoryCaptureOutcome::Status(Some(status)) => Ok(status),
            _ => Err(TransportError::Peer),
        }
    }

    async fn exchange_history_capture(
        &self,
        request: Arc<HistoryCaptureRequest>,
        action: HistoryCaptureAction,
    ) -> TransportResult<HistoryCaptureOutcome> {
        request.validate()?;
        let from = self.resources.identity.clone();
        let to = self.destination_identity();
        require_participant(&request.proposal, &from)?;
        require_participant(&request.proposal, &to)?;
        if from.region != request.proposal.operation().home_region || to.region != request.region {
            return Err(TransportError::Peer);
        }
        let content = request.content()?;
        self.settle_exchange(move |peer, deadline| async move {
            let mut socket = peer.connect(deadline).await?;
            super::chunk_exchange::write_serialized(
                &mut socket,
                (
                    TRANSPORT_HISTORY_CAPTURE_KIND,
                    HistoryCaptureMessage {
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
            let reply: HistoryCaptureReply = super::chunk_exchange::read_message(
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
