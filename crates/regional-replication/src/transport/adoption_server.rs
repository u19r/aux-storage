use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_provider::DynamoPort;

use super::{
    bootstrap_message::BootstrapOutcome,
    error::ImportNack,
    server::{TcpImportServer, classify_native_error},
};
use crate::{
    bootstrap::HiddenTargetRequest,
    history_collection::{
        HistoryAdoptionDefinition,
        adoption_executor::{self, AdoptionAcceptance},
    },
};

impl<P: DynamoPort + 'static> TcpImportServer<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub(super) async fn begin_adoption(
        &self,
        target: Arc<HiddenTargetRequest>,
        definition: Arc<HistoryAdoptionDefinition>,
    ) -> Result<BootstrapOutcome, ImportNack> {
        adoption_executor::begin_adoption(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            target,
            definition,
        )
        .await
        .map(|()| BootstrapOutcome::AdoptionStored)
        .map_err(|error| classify_native_error(&error))
    }

    pub(super) async fn observe_adoption(
        &self,
        target: Arc<HiddenTargetRequest>,
        definition: Arc<HistoryAdoptionDefinition>,
    ) -> Result<BootstrapOutcome, ImportNack> {
        adoption_executor::observe_adoption(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            target,
            definition,
        )
        .await
        .map(|observation| BootstrapOutcome::Adoption(observation.map(Box::new)))
        .map_err(|error| classify_native_error(&error))
    }

    pub(super) async fn accept_adoption(
        &self,
        input: AdoptionAcceptance,
    ) -> Result<BootstrapOutcome, ImportNack> {
        adoption_executor::accept_adoption(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            Arc::new(input),
        )
        .await
        .map(|()| BootstrapOutcome::AdoptionStored)
        .map_err(|error| classify_native_error(&error))
    }
}
