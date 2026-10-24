use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_provider::DynamoPort;

use super::{
    BootstrapTailScope, read_source_capture_definition,
    tail_executor::{plan_bootstrap_tail, record_bootstrap_tail},
    tail_scope::BootstrapTailExecution,
};

impl<P: DynamoPort + 'static> super::transfer::BootstrapTransfer<P> {
    pub(super) async fn transfer_tail(
        &self,
        scope: Arc<BootstrapTailScope>,
        source: &crate::transport::SourceReservation,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>>
    where
        for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
    {
        source.require_live()?;
        let import = plan_bootstrap_tail(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            Arc::clone(&scope),
            (Arc::clone(&self.codec), Arc::clone(&self.limits)),
        )
        .await?;
        source.require_live()?;
        let definition = read_source_capture_definition(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            Arc::new(scope.transfer.proposal.clone()),
        )
        .await?;
        let peer = self.transport.destination(&scope.transfer.destination)?;
        source.require_live()?;
        peer.declare_bootstrap_cut((
            Arc::new(scope.transfer.target.clone()),
            Arc::new(definition),
            Arc::new(scope.cut.clone()),
        ))
        .await?;
        let Some(import) = import else {
            return Ok(true);
        };
        let import = Arc::new(import);
        source.require_live()?;
        let receipt = Arc::new(peer.deliver(Arc::clone(&import)).await?);
        let execution = Arc::new(BootstrapTailExecution {
            scope: Arc::clone(&scope),
            import,
            receipt,
            codec: Arc::clone(&self.codec),
            limits: Arc::clone(&self.limits),
        });
        source.require_live()?;
        let progress =
            record_bootstrap_tail(self.port.as_ref(), Arc::clone(&self.journal), execution).await?;
        Ok(progress.through() >= scope.cut.through)
    }
}
