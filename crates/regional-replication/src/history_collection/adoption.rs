use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_provider::DynamoPort;

use super::{HistoryAdoptionDefinition, HistoryCollector};
use crate::{
    bootstrap::{BaselineCopyScope, HiddenTargetRequest, TargetBootstrapDefinition},
    native_record::corrupt_record,
    transport::TransportError,
};

impl<P: DynamoPort + 'static> HistoryCollector<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    /// Relays one bounded source fact from the primary to the hidden native
    /// target. Completion proves declaration agreement, not imported data
    /// or activation.
    pub(crate) async fn relay_collected_adoption(
        &self,
        scope: Arc<BaselineCopyScope>,
        source: &crate::transport::SourceReservation,
    ) -> Result<bool, TransportError> {
        source.require_live()?;
        let definition = Arc::new(
            self.load_complete(Arc::new(scope.proposal.clone()))
                .await?
                .ok_or_else(corrupt_record)?,
        );
        scope.require_definition(&TargetBootstrapDefinition::Addition(Box::new(
            definition.as_ref().clone(),
        )))?;
        for participant in scope
            .proposal
            .participants()
            .iter()
            .filter(|participant| participant.before.is_some())
        {
            source.require_live()?;
            if !self
                .relay_source_adoption(Arc::clone(&definition), participant.region.clone(), source)
                .await?
            {
                return Ok(false);
            }
        }
        self.relay_target_adoption(Arc::new(scope.target.clone()), definition, source)
            .await
    }

    async fn relay_target_adoption(
        &self,
        request: Arc<HiddenTargetRequest>,
        definition: Arc<HistoryAdoptionDefinition>,
        reservation: &crate::transport::SourceReservation,
    ) -> Result<bool, TransportError> {
        definition.validate()?;
        let region = &self.transport.configuration().as_ref().as_ref().region;
        if region != &definition.proposal().operation().home_region {
            return Err(TransportError::Peer);
        }
        let replication_types::configuration::control::ConfigurationChange::AddRegion {
            region: target_region,
        } = &definition.proposal().operation().change
        else {
            return Err(TransportError::Peer);
        };
        let source = request
            .target
            .sources
            .iter()
            .find(|source| &source.region == target_region)
            .ok_or(TransportError::Peer)?;
        let peer = self.transport.destination(source)?;
        reservation.require_live()?;
        let Some(observation) = peer
            .observe_target_adoption(Arc::clone(&request), Arc::clone(&definition))
            .await?
        else {
            reservation.require_live()?;
            peer.begin_target_adoption(request, definition).await?;
            return Ok(false);
        };
        let Some(source) = observation.record.work_request()? else {
            return Ok(true);
        };
        reservation.require_live()?;
        let Some(change) = self
            .fetch_collection_change(Arc::new(source), &observation.record, reservation)
            .await?
        else {
            return Ok(false);
        };
        reservation.require_live()?;
        peer.accept_target_adoption(request, definition, (observation.revision, change))
            .await?;
        Ok(false)
    }
}
