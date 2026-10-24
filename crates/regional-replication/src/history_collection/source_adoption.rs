use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::identity::replica::RegionId;
use storage_provider::DynamoPort;

use super::{HistoryAdoptionDefinition, HistoryCollector};
use crate::{history_capture::HistoryCaptureRequest, transport::TransportError};

impl<P: DynamoPort + 'static> HistoryCollector<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub(super) async fn relay_source_adoption(
        &self,
        definition: Arc<HistoryAdoptionDefinition>,
        region: RegionId,
        reservation: &crate::transport::SourceReservation,
    ) -> Result<bool, TransportError> {
        definition.validate()?;
        if self.transport.configuration().as_ref().as_ref().region
            != definition.proposal().operation().home_region
        {
            return Err(TransportError::Peer);
        }
        let request = Arc::new(HistoryCaptureRequest {
            proposal: definition.proposal().clone(),
            region,
        });
        request.validate()?;
        if self.is_local_source(&request) {
            return Ok(true);
        }
        let peer = self.source_peer(&request)?;
        reservation.require_live()?;
        let Some(observation) = peer.observe_source_collection(Arc::clone(&request)).await? else {
            reservation.require_live()?;
            peer.declare_source_collection(request, definition).await?;
            return Ok(false);
        };
        definition.require_collection(&observation.record)?;
        let Some(source) = observation.record.work_request()? else {
            return Ok(true);
        };
        let Some(change) = self
            .fetch_collection_change(Arc::new(source), &observation.record, reservation)
            .await?
        else {
            return Ok(false);
        };
        reservation.require_live()?;
        peer.accept_source_collection(request, (observation.revision, change))
            .await?;
        Ok(false)
    }
}
