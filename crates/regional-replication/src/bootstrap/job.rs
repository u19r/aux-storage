use std::{num::NonZeroU16, sync::Arc, time::Duration};

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use replication_types::configuration::proposal::ControlProposal;
use scheduled_bg_jobs::{
    EffectMode, ExecutionContext, FairnessPolicy, HandlerFuture, JobHandler, JobKind, JobOutcome,
    JobSpec, OutcomeCode, QueueName, RetryPreset, RetrySelection, ValidationError,
};
use storage_provider::{DynamoPort, ItemTombstoneScanAttempt, PrimaryStateScanAttempt};

use super::{SourceCaptureStatus, advance_source_capture};
use crate::{
    constants::SOURCE_CAPTURE_JOB_KIND,
    journal::JournalStore,
    transport::{TcpTransport, TransportError},
};

pub struct SourceCaptureJob<P> {
    port: Arc<P>,
    journal: Arc<JournalStore>,
    codec: Arc<CanonicalJsonCodec>,
    transport: TcpTransport,
    limits: Arc<replication_provider::runtime_configuration::limits::WorkLimits>,
    timeout: Duration,
}

impl<P: DynamoPort> SourceCaptureJob<P> {
    pub fn try_new(
        port: Arc<P>,
        journal: Arc<JournalStore>,
        codec: Arc<CanonicalJsonCodec>,
        transport: TcpTransport,
    ) -> Result<Self, TransportError> {
        let configured = transport.configuration().as_ref().as_ref();
        journal.require_deployment(configured)?;
        let timeout = configured
            .timing
            .request_budget(4)
            .map_err(|_| TransportError::Configuration)?;
        let limits = transport.source_limits();
        Ok(Self {
            port,
            journal,
            codec,
            transport,
            limits,
            timeout,
        })
    }

    pub fn specification(self) -> Result<JobSpec<ControlProposal, Self>, ValidationError> {
        let timeout = self.timeout;
        Ok(JobSpec::new(
            JobKind::try_new(SOURCE_CAPTURE_JOB_KIND)?,
            NonZeroU16::MIN,
            QueueName::try_new(SOURCE_CAPTURE_JOB_KIND)?,
            self,
            |proposal: &ControlProposal| {
                replication_provider::control::source_capture_state(proposal).map_err(|_| {
                    ValidationError::InvalidValue {
                        field: "regional source capture",
                    }
                })?;
                serde_json::to_vec(proposal).map(Into::into).map_err(|_| {
                    ValidationError::InvalidValue {
                        field: "regional source capture",
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
}

impl<P: DynamoPort + 'static> SourceCaptureJob<P>
where for<'native> P::Attempt<'native>:
        PhysicalRecordAttempt + PrimaryStateScanAttempt + ItemTombstoneScanAttempt
{
    pub(crate) async fn advance(
        &self,
        proposal: Arc<ControlProposal>,
    ) -> Result<SourceCaptureStatus, Box<dyn std::error::Error + Send + Sync>> {
        let source = self.transport.reserve_source_budget(self.timeout).await?;
        let capture = self.clone();
        crate::transport::settle_source_with(source, move |source| async move {
            capture.capture_source(proposal, &source).await
        })
        .await
    }

    pub(crate) async fn capture_source(
        &self,
        proposal: Arc<ControlProposal>,
        source: &crate::transport::SourceReservation,
    ) -> Result<SourceCaptureStatus, Box<dyn std::error::Error + Send + Sync>> {
        source.require_live()?;
        Ok(advance_source_capture(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            proposal,
            (Arc::clone(&self.codec), Arc::clone(&self.limits)),
        )
        .await?)
    }
}

impl<P: DynamoPort + 'static> JobHandler for SourceCaptureJob<P>
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
            let proposal = Arc::new(serde_json::from_slice(payload)?);
            let status = self.advance(proposal).await?;
            if matches!(
                status,
                SourceCaptureStatus::BaselineReady
                    | SourceCaptureStatus::Released
                    | SourceCaptureStatus::Cancelled
            ) {
                return Ok(JobOutcome::Complete(OutcomeCode::try_new(
                    "regional-source-capture-complete",
                )?));
            }
            Ok(JobOutcome::Continue {
                not_before: context.wall_now(),
                checkpoint: Default::default(),
            })
        })
    }
}

impl<P> Clone for SourceCaptureJob<P> {
    fn clone(&self) -> Self {
        Self {
            port: Arc::clone(&self.port),
            journal: Arc::clone(&self.journal),
            codec: Arc::clone(&self.codec),
            transport: self.transport.clone(),
            limits: Arc::clone(&self.limits),
            timeout: self.timeout,
        }
    }
}
