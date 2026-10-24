//! Connected declaration and original-byte delivery for a re-enabled table.
use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_provider::DynamoPort;

use super::{
    BaselineCopyScope, TargetBootstrapDefinition,
    reenable_original_executor::{
        RetainedOriginalExecution, plan_reenable_original, record_reenable_original,
    },
};

impl<P: DynamoPort + 'static> super::transfer::BootstrapTransfer<P> {
    pub(crate) async fn declare_retained_cut(
        &self,
        scope: Arc<BaselineCopyScope>,
        ordinal: u64,
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
        let TargetBootstrapDefinition::Reenable(definition) = definition else {
            return Err("retained declaration requires re-enable".into());
        };
        let definition = Arc::new(*definition);
        source.require_live()?;
        let page = super::read_reenable_history_page(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            Arc::new(scope.proposal.clone()),
            ordinal,
        )
        .await?;
        let peer = self.transport.destination(&scope.destination)?;
        source.require_live()?;
        peer.begin_target_retained_history((
            Arc::new(scope.target.clone()),
            Arc::clone(&definition),
        ))
        .await?;
        source.require_live()?;
        peer.accept_target_retained_page((
            Arc::new(scope.target.clone()),
            definition,
            Arc::new(page),
        ))
        .await?;
        Ok(true)
    }

    pub(crate) async fn transfer_baselines(
        &self,
        scope: Arc<BaselineCopyScope>,
        ordinal: u64,
        source: &crate::transport::SourceReservation,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>>
    where
        for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
    {
        source.require_live()?;
        let peer = self.transport.destination(&scope.destination)?;
        let budget = peer.retained_baseline_capacity(&scope.target, ordinal)?;
        source.require_live()?;
        let Some(page) = super::reenable_baseline_executor::plan_reenable_baseline(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            (Arc::clone(&scope), ordinal),
            (Arc::clone(&self.codec), budget),
        )
        .await?
        else {
            return Ok(true);
        };
        let page = Arc::new(page);
        source.require_live()?;
        let receipt = Arc::new(
            peer.accept_target_retained_baselines((
                Arc::new(scope.target.clone()),
                ordinal,
                Arc::clone(&page),
            ))
            .await?,
        );
        let execution = Arc::new(
            super::reenable_baseline_executor::RetainedBaselineExecution {
                scope,
                ordinal,
                page,
                receipt,
                codec: Arc::clone(&self.codec),
            },
        );
        source.require_live()?;
        Ok(super::reenable_baseline_executor::record_reenable_baseline(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            execution,
        )
        .await?)
    }

    pub(super) async fn transfer_retained_original(
        &self,
        scope: Arc<BaselineCopyScope>,
        ordinal: u64,
        source: &crate::transport::SourceReservation,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>>
    where
        for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
    {
        source.require_live()?;
        let Some(import) = plan_reenable_original(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            (Arc::clone(&scope), ordinal),
            (Arc::clone(&self.codec), Arc::clone(&self.limits)),
        )
        .await?
        else {
            return Ok(true);
        };
        let peer = self.transport.destination(&scope.destination)?;
        let import = Arc::new(import);
        source.require_live()?;
        let receipt = Arc::new(peer.deliver(Arc::clone(&import)).await?);
        let execution = Arc::new(RetainedOriginalExecution {
            scope,
            ordinal,
            import,
            receipt,
            codec: Arc::clone(&self.codec),
            limits: Arc::clone(&self.limits),
        });
        source.require_live()?;
        Ok(
            record_reenable_original(self.port.as_ref(), Arc::clone(&self.journal), execution)
                .await?,
        )
    }
}

impl<P: DynamoPort + 'static> super::transfer::BootstrapTransfer<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub(crate) async fn transfer_adoption(
        &self,
        scope: Arc<BaselineCopyScope>,
        source: &crate::transport::SourceReservation,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        use super::retained_transfer_executor::{self, RetainedTransferStage};
        source.require_live()?;
        let stage = retained_transfer_executor::load_retained_transfer_stage(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            Arc::clone(&scope),
        )
        .await?;
        let complete = match stage {
            RetainedTransferStage::Baselines(ordinal) => {
                self.transfer_baselines(Arc::clone(&scope), ordinal, source)
                    .await?
            }
            RetainedTransferStage::Originals(ordinal) => {
                self.transfer_retained_original(Arc::clone(&scope), ordinal, source)
                    .await?
            }
            RetainedTransferStage::Complete => return Ok(true),
        };
        if complete {
            source.require_live()?;
            retained_transfer_executor::advance_retained_transfer_stage(
                self.port.as_ref(),
                Arc::clone(&self.journal),
                scope,
                stage,
            )
            .await?;
        }
        Ok(false)
    }
}
