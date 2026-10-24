use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use storage_provider::DynamoPort;

use crate::{
    bootstrap::{
        BaselineCopyScope, BootstrapTransfer, HiddenVerificationStatus, TargetBootstrapDefinition,
    },
    history_collection::HistoryCollector,
    journal::JournalStore,
    transport::{TcpTransport, TransportError},
};

/// Bounded AddRegion data preparation. A verified target still requires the
/// lifecycle's participant grants before membership can become visible.
pub(crate) struct AdditionBootstrap<P> {
    collector: HistoryCollector<P>,
    transfer: BootstrapTransfer<P>,
    transport: TcpTransport,
}

impl<P: DynamoPort + 'static> AdditionBootstrap<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub fn try_new(
        port: Arc<P>,
        journal: Arc<JournalStore>,
        codec: Arc<CanonicalJsonCodec>,
        transport: TcpTransport,
    ) -> Result<Self, TransportError> {
        let collector =
            HistoryCollector::try_new(Arc::clone(&port), Arc::clone(&journal), transport.clone())?;
        let transfer = BootstrapTransfer::try_new(port, journal, codec, transport.clone())?;
        Ok(Self {
            collector,
            transfer,
            transport,
        })
    }

    pub(crate) async fn transfer_addition(
        &self,
        scope: Arc<BaselineCopyScope>,
        source: &crate::transport::SourceReservation,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        source.require_live()?;
        replication_provider::control::validate_addition(&scope.proposal)?;
        if !self
            .collector
            .collect_page(Arc::new(scope.proposal.clone()), source)
            .await?
        {
            return Ok(false);
        }
        if !self
            .collector
            .relay_collected_adoption(Arc::clone(&scope), source)
            .await?
        {
            return Ok(false);
        }
        if !self
            .transfer
            .transfer_copy(Arc::clone(&scope), source)
            .await?
        {
            return Ok(false);
        }
        if !self
            .transfer
            .transfer_adoption(Arc::clone(&scope), source)
            .await?
        {
            return Ok(false);
        }
        if !self
            .transfer
            .transfer_collected(Arc::clone(&scope), source)
            .await?
        {
            return Ok(false);
        }
        self.verify_target(scope, source).await
    }

    async fn verify_target(
        &self,
        scope: Arc<BaselineCopyScope>,
        source: &crate::transport::SourceReservation,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        source.require_live()?;
        let definition = self
            .collector
            .load_complete(Arc::new(scope.proposal.clone()))
            .await?
            .ok_or_else(crate::native_record::corrupt_record)?;
        let definition = TargetBootstrapDefinition::Addition(Box::new(definition));
        scope.require_definition(&definition)?;
        let peer = self.transport.destination(&scope.destination)?;
        source.require_live()?;
        let status = peer
            .advance_target_verification((Arc::new(scope.target.clone()), Arc::new(definition)))
            .await?;
        Ok(status == HiddenVerificationStatus::Verified)
    }
}

impl<P> Clone for AdditionBootstrap<P> {
    fn clone(&self) -> Self {
        Self {
            collector: self.collector.clone(),
            transfer: self.transfer.clone(),
            transport: self.transport.clone(),
        }
    }
}
