use std::sync::Arc;

use storage_types::Revision;

use super::{
    bootstrap_message::{BootstrapAction, BootstrapOutcome},
    client::TcpPeerClient,
    error::{TransportError, TransportResult},
};
use crate::{
    bootstrap::HiddenTargetRequest,
    history_collection::{
        HistoryAdoptionDefinition, HistoryCollectionChange, HistoryCollectionObservation,
    },
};

impl TcpPeerClient {
    pub(crate) async fn begin_target_adoption(
        &self,
        target: Arc<HiddenTargetRequest>,
        definition: Arc<HistoryAdoptionDefinition>,
    ) -> TransportResult<()> {
        let action = BootstrapAction::BeginAdoption { target, definition };
        match self.bootstrap(action).await? {
            BootstrapOutcome::AdoptionStored => Ok(()),
            _ => Err(TransportError::Peer),
        }
    }

    pub(crate) async fn observe_target_adoption(
        &self,
        target: Arc<HiddenTargetRequest>,
        definition: Arc<HistoryAdoptionDefinition>,
    ) -> TransportResult<Option<HistoryCollectionObservation>> {
        let action = BootstrapAction::ObserveAdoption {
            target,
            definition: Arc::clone(&definition),
        };
        let BootstrapOutcome::Adoption(observation) = self.bootstrap(action).await? else {
            return Err(TransportError::Peer);
        };
        if let Some(observation) = &observation {
            definition.require_collection(&observation.record)?;
        }
        Ok(observation.map(|observation| *observation))
    }

    pub(crate) async fn accept_target_adoption(
        &self,
        target: Arc<HiddenTargetRequest>,
        definition: Arc<HistoryAdoptionDefinition>,
        input: (Revision, HistoryCollectionChange),
    ) -> TransportResult<()> {
        let action = BootstrapAction::AcceptAdoption {
            target,
            definition,
            revision: input.0,
            change: Box::new(input.1),
        };
        match self.bootstrap(action).await? {
            BootstrapOutcome::AdoptionStored => Ok(()),
            _ => Err(TransportError::Peer),
        }
    }
}
