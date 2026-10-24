use std::{num::NonZeroU16, sync::Arc, time::Duration};

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec,
    runtime_configuration::{deployment::QualifiedConfiguration, limits::WorkLimits},
};
use scheduled_bg_jobs::{
    EffectMode, EnqueueOptions, ExecutionContext, FairnessClass, FairnessPolicy, HandlerFuture,
    JobCatalog, JobHandle, JobHandler, JobId, JobKind, JobOutcome, JobSpec, QueueName, RetryPreset,
    RetrySelection, Timestamp, ValidationError,
};
use storage_provider::DynamoPort;

use super::{DeliveryExecution, DeliveryScope, plan_delivery, record_delivery};
use crate::{
    constants::DELIVERY_JOB_KIND,
    journal::JournalStore,
    transport::{TcpTransport, TransportError},
};

/// Shared scheduler handler. Job checkpoints never substitute for the source
/// database's atomic acknowledgment records; replacement workers reread them.
pub struct DeliveryJob<P> {
    port: Arc<P>,
    journal: Arc<JournalStore>,
    codec: Arc<CanonicalJsonCodec>,
    configuration: Arc<QualifiedConfiguration>,
    transport: TcpTransport,
    limits: Arc<WorkLimits>,
    seed: JobId,
}

impl<P: DynamoPort + 'static> DeliveryJob<P> {
    pub fn try_new(
        port: Arc<P>,
        journal: Arc<JournalStore>,
        codec: Arc<CanonicalJsonCodec>,
        transport: TcpTransport,
    ) -> Result<Self, TransportError> {
        let configuration = Arc::clone(transport.configuration());
        journal.require_deployment(configuration.as_ref().as_ref())?;
        let limits = transport.source_limits();
        let configured = configuration.as_ref().as_ref();
        let seed = crate::job_recovery::derive_controller_seed(DELIVERY_JOB_KIND, configured)?;
        Ok(Self {
            port,
            journal,
            codec,
            configuration,
            transport,
            limits,
            seed,
        })
    }

    async fn deliver(
        &self,
        scope: DeliveryScope,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>>
    where
        for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
    {
        let source = self
            .transport
            .reserve_source_budget(self.configuration.delivery_budgets().source())
            .await?;
        let transfer = self.clone();
        crate::transport::settle_source(
            source,
            async move { transfer.transfer_delivery(scope).await },
        )
        .await
    }

    async fn transfer_delivery(
        &self,
        scope: DeliveryScope,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>>
    where
        for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
    {
        let Some(import) = plan_delivery(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            scope.clone(),
            (Arc::clone(&self.codec), Arc::clone(&self.limits)),
        )
        .await?
        else {
            return Ok(false);
        };
        let import = Arc::new(import);
        let receipt = Arc::new(
            self.transport
                .destination(&scope.destination)?
                .deliver(Arc::clone(&import))
                .await?,
        );
        record_delivery(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            Arc::new(DeliveryExecution {
                scope,
                import,
                receipt,
                codec: Arc::clone(&self.codec),
                limits: Arc::clone(&self.limits),
            }),
        )
        .await?;
        Ok(true)
    }
}

impl<P: DynamoPort + 'static> DeliveryJob<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub(crate) fn register(
        self,
        catalog: &mut JobCatalog,
        now: Timestamp,
    ) -> Result<JobHandle<()>, ValidationError> {
        let options = EnqueueOptions::new(FairnessClass::low_volume())
            .job_id(self.seed)
            .scheduled_at(now);
        let timeout = self.configuration.delivery_budgets().job();
        let specification = JobSpec::unit(
            JobKind::try_new(DELIVERY_JOB_KIND)?,
            NonZeroU16::MIN,
            QueueName::try_new(DELIVERY_JOB_KIND)?,
            self,
        )
        .policy(
            EffectMode::Idempotent,
            FairnessPolicy::low_volume(),
            RetrySelection::Preset(RetryPreset::Maintenance),
            timeout,
        );
        catalog.register_seeded(specification, (), options, now)
    }
}

impl<P: DynamoPort + 'static> JobHandler for DeliveryJob<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    fn execute<'a>(
        &'a self,
        context: ExecutionContext<'a>,
        payload: &'a [u8],
    ) -> HandlerFuture<'a> {
        let now = context.wall_now();
        crate::job_recovery::recover_controller_turn(
            async move {
                crate::job_recovery::require_unit_payload(payload)?;
                let idle =
                    Duration::from_millis(self.configuration.as_ref().as_ref().timing.retry_min_ms);
                if context.is_cancelled() {
                    return Ok(JobOutcome::Continue {
                        not_before: context.wall_now().checked_add(idle)?,
                        checkpoint: Default::default(),
                    });
                }
                let selection = super::recovery::select_pending_delivery(
                    self.port.as_ref(),
                    Arc::clone(&self.journal),
                )
                .await?;
                if let Some(scope) = selection.scope {
                    self.deliver(scope).await?;
                }
                let not_before = if selection.wrapped {
                    context.wall_now().checked_add(idle)?
                } else {
                    context.wall_now()
                };
                Ok(JobOutcome::Continue {
                    not_before,
                    checkpoint: Default::default(),
                })
            },
            now,
            Duration::from_millis(self.configuration.as_ref().as_ref().timing.retry_max_ms),
        )
    }
}

impl<P> Clone for DeliveryJob<P> {
    fn clone(&self) -> Self {
        Self {
            port: self.port.clone(),
            journal: self.journal.clone(),
            codec: self.codec.clone(),
            configuration: self.configuration.clone(),
            transport: self.transport.clone(),
            limits: self.limits.clone(),
            seed: self.seed,
        }
    }
}
