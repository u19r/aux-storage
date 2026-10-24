use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_provider::DynamoPort;

use super::{
    AdoptedOriginalScope,
    original_executor::{plan_adopted_original, record_adopted_original},
    original_scope::AdoptedOriginalExecution,
};

impl<P: DynamoPort + 'static> super::transfer::BootstrapTransfer<P> {
    /// Traverses the complete frozen collection. Cursor advancement requires
    /// native receipt coverage for every selected original interval.
    pub(crate) async fn transfer_collected(
        &self,
        scope: Arc<super::BaselineCopyScope>,
        source: &crate::transport::SourceReservation,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>>
    where
        for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
    {
        source.require_live()?;
        let work = super::original_executor::plan_original_traversal(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            scope,
        )
        .await?;
        match work {
            crate::journal::OriginalTraversalWork::Complete => Ok(true),
            crate::journal::OriginalTraversalWork::Pending => Ok(false),
            crate::journal::OriginalTraversalWork::Transfer(original) => {
                if self
                    .transfer_adopted_original(Arc::clone(&original), source)
                    .await?
                {
                    source.require_live()?;
                    super::original_executor::advance_original_traversal(
                        self.port.as_ref(),
                        Arc::clone(&self.journal),
                        original,
                    )
                    .await?;
                }
                Ok(false)
            }
        }
    }

    async fn transfer_adopted_original(
        &self,
        scope: Arc<AdoptedOriginalScope>,
        source: &crate::transport::SourceReservation,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>>
    where
        for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
    {
        source.require_live()?;
        let Some(import) = plan_adopted_original(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            Arc::clone(&scope),
            (Arc::clone(&self.codec), Arc::clone(&self.limits)),
        )
        .await?
        else {
            return Ok(true);
        };
        let peer = self.transport.destination(&scope.transfer.destination)?;
        let import = Arc::new(import);
        source.require_live()?;
        let receipt = Arc::new(peer.deliver(Arc::clone(&import)).await?);
        let execution = Arc::new(AdoptedOriginalExecution {
            scope: Arc::clone(&scope),
            import,
            receipt,
            codec: Arc::clone(&self.codec),
            limits: Arc::clone(&self.limits),
        });
        source.require_live()?;
        let progress =
            record_adopted_original(self.port.as_ref(), Arc::clone(&self.journal), execution)
                .await?;
        Ok(progress.through() >= scope.cut.through)
    }
}
