use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_provider::{DynamoPort, ItemTombstoneScanAttempt, PrimaryStateScanAttempt};

use super::{
    executor::{EnableChange, execute_enable},
    plan::EnablePlan,
    record::EnablePhase,
};
use crate::{
    bootstrap::{BaselineCopyScope, BootstrapTransferJob},
    control::runner::ControlRunner,
    journal::JournalStore,
    native_record::corrupt_record,
    transport::TcpTransport,
};

pub(super) struct EnableRunner<P> {
    pub port: Arc<P>,
    pub journal: Arc<JournalStore>,
    pub transport: TcpTransport,
    pub bootstrap: BootstrapTransferJob<P>,
    pub control: ControlRunner<P>,
}

impl<P: DynamoPort + 'static> EnableRunner<P>
where for<'native> P::Attempt<'native>:
        PhysicalRecordAttempt + PrimaryStateScanAttempt + ItemTombstoneScanAttempt
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
            let Some(plan) = super::recovery::select_pending_enable(
                runner.port.as_ref(),
                Arc::clone(&runner.journal),
            )
            .await?
            else {
                return Ok(false);
            };
            runner.advance_enable(Arc::new(plan), &source).await?;
            Ok(true)
        })
        .await
    }

    async fn advance_enable(
        &self,
        plan: Arc<EnablePlan>,
        source: &crate::transport::SourceReservation,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        source.require_live()?;
        for target in &plan.targets {
            self.transport.destination(plan.destination(target)?)?;
        }
        let record = self
            .native(Arc::clone(&plan), EnableChange::Observe, source)
            .await?;
        match record.phase {
            EnablePhase::Reserving => {
                let target = plan
                    .targets
                    .get(record.reservations.len())
                    .ok_or_else(corrupt_record)?;
                let peer = self.transport.destination(plan.destination(target)?)?;
                source.require_live()?;
                let reserved = peer.reserve_target(Arc::new(target.clone())).await?;
                self.native(
                    plan,
                    EnableChange::Reserve(Box::new(reserved.proposed().clone())),
                    source,
                )
                .await?;
            }
            EnablePhase::Bootstrapping(index) => {
                let target = plan
                    .targets
                    .get(index as usize)
                    .ok_or_else(corrupt_record)?;
                let proposal = record.proposal.ok_or_else(corrupt_record)?;
                let scope = BaselineCopyScope::try_new(
                    proposal,
                    target.clone(),
                    plan.destination(target)?.clone(),
                )?;
                if self
                    .bootstrap
                    .transfer_bootstrap(Arc::new(scope), source)
                    .await?
                {
                    self.native(plan, EnableChange::Advance(record.phase), source)
                        .await?;
                }
            }
            EnablePhase::Controlling => {
                if self
                    .control
                    .advance_control(
                        Arc::new(record.proposal.ok_or_else(corrupt_record)?),
                        source,
                    )
                    .await?
                {
                    self.native(plan, EnableChange::Advance(record.phase), source)
                        .await?;
                }
            }
            EnablePhase::Cancelling(index) => {
                self.advance_cancellation(plan, record, index, source)
                    .await?
            }
            EnablePhase::Completed | EnablePhase::Cancelled => return Ok(true),
        }
        Ok(false)
    }

    async fn advance_cancellation(
        &self,
        plan: Arc<EnablePlan>,
        record: super::record::EnableRecord,
        index: u32,
        source: &crate::transport::SourceReservation,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if index == 0
            && let Some(proposal) = &record.proposal
            && !self
                .control
                .advance_control(Arc::new(proposal.clone()), source)
                .await?
        {
            return Ok(());
        }
        if let Some(target) = plan.targets.get(index as usize) {
            let peer = self.transport.destination(plan.destination(target)?)?;
            source.require_live()?;
            peer.cancel_target(Arc::new(target.clone())).await?;
        }
        self.native(plan, EnableChange::Advance(record.phase), source)
            .await?;
        Ok(())
    }

    async fn native(
        &self,
        plan: Arc<EnablePlan>,
        change: EnableChange,
        source: &crate::transport::SourceReservation,
    ) -> Result<super::record::EnableRecord, Box<dyn std::error::Error + Send + Sync>> {
        source.require_live()?;
        Ok(execute_enable(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            plan,
            Arc::new(change),
        )
        .await?)
    }
}

impl<P> Clone for EnableRunner<P> {
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
