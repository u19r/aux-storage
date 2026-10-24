use std::{num::NonZeroU16, sync::Arc, time::Duration};

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use scheduled_bg_jobs::{
    EffectMode, ExecutionContext, FairnessPolicy, HandlerFuture, JobHandler, JobKind, JobOutcome,
    JobSpec, OutcomeCode, QueueName, RetryPreset, RetrySelection, ValidationError,
};
use storage_provider::{DynamoPort, ItemTombstoneScanAttempt, PrimaryStateScanAttempt};

use super::{
    BaselineCopyScope, BootstrapTailScope, SourceCaptureJob, SourceCaptureStatus,
    begin_source_capture, read_source_capture_cut,
    transfer_executor::{advance_transfer_phase, load_transfer_phase},
};
use crate::{
    constants::BOOTSTRAP_TRANSFER_JOB_KIND,
    journal::{JournalStore, TransferPhase},
    transport::{TcpTransport, TransportError},
};

/// One shared scheduler handler drives capture, baseline, and every source
/// partition. Native workflow records survive lost scheduler checkpoints.
pub struct BootstrapTransferJob<P> {
    capture: SourceCaptureJob<P>,
    transfer: super::BootstrapTransfer<P>,
    timeout: Duration,
}

impl<P: DynamoPort + 'static> BootstrapTransferJob<P> {
    pub fn try_new(
        port: Arc<P>,
        journal: Arc<JournalStore>,
        codec: Arc<CanonicalJsonCodec>,
        transport: TcpTransport,
    ) -> Result<Self, TransportError> {
        let timeout = transport
            .configuration()
            .as_ref()
            .as_ref()
            .timing
            .request_budget(4)
            .map_err(|_| TransportError::Configuration)?;
        let transfer = super::BootstrapTransfer::try_new(
            Arc::clone(&port),
            Arc::clone(&journal),
            Arc::clone(&codec),
            transport.clone(),
        )?;
        let capture = SourceCaptureJob::try_new(
            Arc::clone(&port),
            Arc::clone(&journal),
            codec,
            transport.clone(),
        )?;
        Ok(Self {
            capture,
            transfer,
            timeout,
        })
    }

    pub fn specification(self) -> Result<JobSpec<BaselineCopyScope, Self>, ValidationError> {
        let timeout = self.timeout;
        Ok(JobSpec::new(
            JobKind::try_new(BOOTSTRAP_TRANSFER_JOB_KIND)?,
            NonZeroU16::MIN,
            QueueName::try_new(BOOTSTRAP_TRANSFER_JOB_KIND)?,
            self,
            |scope: &BaselineCopyScope| {
                replication_provider::control::source_capture_state(&scope.proposal).map_err(
                    |_| ValidationError::InvalidValue {
                        field: "initial regional bootstrap transfer",
                    },
                )?;
                scope
                    .validate()
                    .map_err(|_| ValidationError::InvalidValue {
                        field: "regional bootstrap transfer",
                    })?;
                serde_json::to_vec(scope).map(Into::into).map_err(|_| {
                    ValidationError::InvalidValue {
                        field: "regional bootstrap transfer",
                    }
                })
            },
        )
        .policy(
            EffectMode::Idempotent,
            FairnessPolicy::low_volume(),
            RetrySelection::Preset(RetryPreset::Maintenance),
            timeout,
        ))
    }

    pub(crate) async fn advance_transfer(
        &self,
        scope: Arc<BaselineCopyScope>,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>>
    where
        for<'native> P::Attempt<'native>:
            PhysicalRecordAttempt + PrimaryStateScanAttempt + ItemTombstoneScanAttempt,
    {
        let source = self
            .transfer
            .transport
            .reserve_source_budget(self.timeout)
            .await?;
        let transfer = self.clone();
        crate::transport::settle_source_with(source, move |source| async move {
            transfer.transfer_bootstrap(scope, &source).await
        })
        .await
    }

    pub(crate) async fn transfer_bootstrap(
        &self,
        scope: Arc<BaselineCopyScope>,
        source: &crate::transport::SourceReservation,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>>
    where
        for<'native> P::Attempt<'native>:
            PhysicalRecordAttempt + PrimaryStateScanAttempt + ItemTombstoneScanAttempt,
    {
        source.require_live()?;
        replication_provider::control::source_capture_state(&scope.proposal)?;
        let phase = load_transfer_phase(
            self.transfer.port.as_ref(),
            Arc::clone(&self.transfer.journal),
            Arc::clone(&scope),
        )
        .await?;
        let completed = match phase {
            TransferPhase::Capturing => self.advance_capture(&scope, source).await?,
            TransferPhase::DeclaringRetained(ordinal) => {
                self.transfer
                    .declare_retained_cut(Arc::clone(&scope), ordinal, source)
                    .await?
            }
            TransferPhase::RetainedBaselines(ordinal) => {
                self.transfer
                    .transfer_baselines(Arc::clone(&scope), ordinal, source)
                    .await?
            }
            TransferPhase::RetainedOriginals(ordinal) => {
                self.transfer
                    .transfer_retained_original(Arc::clone(&scope), ordinal, source)
                    .await?
            }
            TransferPhase::Copying => {
                self.transfer
                    .transfer_copy(Arc::clone(&scope), source)
                    .await?
            }
            TransferPhase::Tailing(partition) => {
                source.require_live()?;
                let cut = read_source_capture_cut(
                    self.transfer.port.as_ref(),
                    Arc::clone(&self.transfer.journal),
                    Arc::new(scope.proposal.clone()),
                    partition,
                )
                .await?;
                self.transfer
                    .transfer_tail(
                        Arc::new(BootstrapTailScope {
                            transfer: scope.as_ref().clone(),
                            cut,
                        }),
                        source,
                    )
                    .await?
            }
            TransferPhase::Verifying => self.advance_verification(&scope, source).await?,
            TransferPhase::Verified => return Ok(true),
        };
        if !completed {
            return Ok(false);
        }
        source.require_live()?;
        let next = advance_transfer_phase(
            self.transfer.port.as_ref(),
            Arc::clone(&self.transfer.journal),
            scope,
            phase,
        )
        .await?;
        Ok(next == TransferPhase::Verified)
    }

    async fn advance_verification(
        &self,
        scope: &BaselineCopyScope,
        source: &crate::transport::SourceReservation,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>>
    where
        for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
    {
        source.require_live()?;
        let definition = super::copy_executor::load_source_bootstrap_definition(
            self.transfer.port.as_ref(),
            Arc::clone(&self.transfer.journal),
            Arc::new(scope.proposal.clone()),
        )
        .await?;
        scope.require_definition(&definition)?;
        let peer = self.transfer.transport.destination(&scope.destination)?;
        source.require_live()?;
        let status = peer
            .advance_target_verification((Arc::new(scope.target.clone()), Arc::new(definition)))
            .await?;
        Ok(status == super::HiddenVerificationStatus::Verified)
    }

    async fn advance_capture(
        &self,
        scope: &BaselineCopyScope,
        source: &crate::transport::SourceReservation,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>>
    where
        for<'native> P::Attempt<'native>:
            PhysicalRecordAttempt + PrimaryStateScanAttempt + ItemTombstoneScanAttempt,
    {
        let proposal = Arc::new(scope.proposal.clone());
        source.require_live()?;
        let current = begin_source_capture(
            self.transfer.port.as_ref(),
            Arc::clone(&self.transfer.journal),
            Arc::clone(&proposal),
        )
        .await?;
        if current == SourceCaptureStatus::Cancelled {
            return Err("source capture was cancelled".into());
        }
        let next = self.capture.capture_source(proposal, source).await?;
        Ok(matches!(
            next,
            SourceCaptureStatus::BaselineReady | SourceCaptureStatus::Released
        ))
    }
}

impl<P: DynamoPort + 'static> JobHandler for BootstrapTransferJob<P>
where for<'native> P::Attempt<'native>:
        PhysicalRecordAttempt + PrimaryStateScanAttempt + ItemTombstoneScanAttempt
{
    fn execute<'a>(
        &'a self,
        context: ExecutionContext<'a>,
        payload: &'a [u8],
    ) -> HandlerFuture<'a> {
        Box::pin(async move {
            if context.is_cancelled() {
                return Ok(JobOutcome::Cancelled);
            }
            let scope = Arc::new(serde_json::from_slice(payload)?);
            if self.advance_transfer(scope).await? {
                return Ok(JobOutcome::Complete(OutcomeCode::try_new(
                    "regional-bootstrap-verified",
                )?));
            }
            Ok(JobOutcome::Continue {
                not_before: context.wall_now(),
                checkpoint: Default::default(),
            })
        })
    }
}

impl<P> Clone for BootstrapTransferJob<P> {
    fn clone(&self) -> Self {
        Self {
            capture: self.capture.clone(),
            transfer: self.transfer.clone(),
            timeout: self.timeout,
        }
    }
}
