use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_provider::DynamoPort;

use super::{
    BaselineCopyScope,
    copy_executor::{acknowledge_baseline_copy, plan_baseline_copy},
};

impl<P: DynamoPort + 'static> super::transfer::BootstrapTransfer<P> {
    pub(crate) async fn transfer_copy(
        &self,
        scope: Arc<BaselineCopyScope>,
        source: &crate::transport::SourceReservation,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>>
    where
        for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
    {
        source.require_live()?;
        let definition = super::copy_executor::load_source_bootstrap_definition(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            Arc::new(scope.proposal.clone()),
        )
        .await?;
        scope.require_definition(&definition)?;
        let peer = self.transport.destination(&scope.destination)?;
        let budget = peer.baseline_copy_capacity(&scope.target, &definition)?;
        source.require_live()?;
        let Some(planned) = plan_baseline_copy(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            scope,
            (Arc::clone(&self.codec), budget),
        )
        .await?
        else {
            return Ok(true);
        };
        let planned = Arc::new(planned);
        source.require_live()?;
        let receipt = peer
            .copy_target_baselines((
                Arc::new(planned.scope.target.clone()),
                Arc::clone(&planned.definition),
                Arc::clone(&planned.page),
            ))
            .await?;
        source.require_live()?;
        Ok(acknowledge_baseline_copy(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            (planned, Arc::new(receipt)),
        )
        .await?)
    }
}
