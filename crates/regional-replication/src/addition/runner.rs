use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_provider::DynamoPort;

use super::{
    AddRegionPlan, AdditionBootstrap, AdditionChange, AdditionPhase, AdditionRecord,
    lifecycle_executor::execute_addition,
};
use crate::{
    bootstrap::BaselineCopyScope,
    control::{ControlOutcome, reconcile_control, runner::ControlRunner},
    journal::JournalStore,
    native_record::corrupt_record,
    transport::TcpTransport,
};

pub(super) struct AdditionRunner<P> {
    pub port: Arc<P>,
    pub journal: Arc<JournalStore>,
    pub transport: TcpTransport,
    pub bootstrap: AdditionBootstrap<P>,
    pub control: ControlRunner<P>,
}

impl<P: DynamoPort + 'static> AdditionRunner<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub(super) async fn advance_pending(
        &self,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let source = self
            .transport
            .reserve_source_budget(self.control.operation_timeout()?)
            .await?;
        let runner = self.clone();
        crate::transport::settle_source_with(source, move |source| async move {
            source.require_live()?;
            let Some(plan) = super::recovery::select_pending_addition(
                runner.port.as_ref(),
                Arc::clone(&runner.journal),
            )
            .await?
            else {
                return Ok(false);
            };
            runner.advance_addition(Arc::new(plan), &source).await?;
            Ok(true)
        })
        .await
    }

    async fn advance_addition(
        &self,
        plan: Arc<AddRegionPlan>,
        source: &crate::transport::SourceReservation,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        source.require_live()?;
        plan.require_deployment(self.transport.configuration().as_ref().as_ref())?;
        let record = self
            .native(Arc::clone(&plan), AdditionChange::Observe, source)
            .await?;
        match record.phase {
            AdditionPhase::Reserving => {
                let peer = self.transport.destination(plan.destination()?)?;
                source.require_live()?;
                let reserved = peer.reserve_target(Arc::new(plan.target.clone())).await?;
                self.native(
                    plan,
                    AdditionChange::Reserve(Box::new(reserved.proposed().clone())),
                    source,
                )
                .await?;
            }
            AdditionPhase::Bootstrapping => {
                let proposal = record.proposal.ok_or_else(corrupt_record)?;
                let scope = BaselineCopyScope::try_new(
                    proposal,
                    plan.target.clone(),
                    plan.destination()?.clone(),
                )?;
                if self
                    .bootstrap
                    .transfer_addition(Arc::new(scope), source)
                    .await?
                {
                    self.native(plan, AdditionChange::Advance(record.phase), source)
                        .await?;
                }
            }
            AdditionPhase::Controlling | AdditionPhase::Cancelling => {
                self.advance_control(plan, record, source).await?
            }
            AdditionPhase::Completed | AdditionPhase::Cancelled => return Ok(true),
        }
        Ok(false)
    }

    async fn advance_control(
        &self,
        plan: Arc<AddRegionPlan>,
        record: AdditionRecord,
        source: &crate::transport::SourceReservation,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let Some(proposal) = record.proposal else {
            if record.phase != AdditionPhase::Cancelling {
                return Err(corrupt_record().into());
            }
            let peer = self.transport.destination(plan.destination()?)?;
            source.require_live()?;
            peer.cancel_target(Arc::new(plan.target.clone())).await?;
            self.native(plan, AdditionChange::Advance(record.phase), source)
                .await?;
            return Ok(());
        };
        let proposal = Arc::new(proposal);
        if !self
            .control
            .advance_control(Arc::clone(&proposal), source)
            .await?
        {
            return Ok(());
        }
        source.require_live()?;
        let decision = reconcile_control(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            Arc::clone(&self.control.codec),
            proposal,
        )
        .await?
        .ok_or_else(corrupt_record)?;
        if matches!(decision, ControlOutcome::Cancelled(_)) {
            let peer = self.transport.destination(plan.destination()?)?;
            source.require_live()?;
            peer.cancel_target(Arc::new(plan.target.clone())).await?;
        }
        self.native(plan, AdditionChange::Advance(record.phase), source)
            .await?;
        Ok(())
    }

    async fn native(
        &self,
        plan: Arc<AddRegionPlan>,
        change: AdditionChange,
        source: &crate::transport::SourceReservation,
    ) -> Result<AdditionRecord, Box<dyn std::error::Error + Send + Sync>> {
        source.require_live()?;
        Ok(execute_addition(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            plan,
            Arc::new(change),
        )
        .await?)
    }
}

impl<P> Clone for AdditionRunner<P> {
    fn clone(&self) -> Self {
        Self {
            port: Arc::clone(&self.port),
            journal: Arc::clone(&self.journal),
            transport: self.transport.clone(),
            bootstrap: self.bootstrap.clone(),
            control: self.control.clone(),
        }
    }
}
