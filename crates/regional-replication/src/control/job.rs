use std::{num::NonZeroU16, sync::Arc, time::Duration};

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use scheduled_bg_jobs::{
    EffectMode, EnqueueOptions, ExecutionContext, FairnessClass, FairnessPolicy, HandlerFuture,
    JobCatalog, JobHandle, JobHandler, JobId, JobKind, JobSpec, QueueName, RetryPreset,
    RetrySelection, Timestamp, ValidationError,
};
use storage_provider::DynamoPort;

use super::runner::ControlRunner;
use crate::{
    constants::CONTROL_JOB_KIND,
    journal::JournalStore,
    transport::{TcpTransport, TransportError},
};

/// Native pending records own request durability; the shared scheduler seeds
/// one recovery controller instead of separately enqueueing each proposal.
pub struct ControlJob<P> {
    runner: ControlRunner<P>,
    timeout: Duration,
    idle_interval: Duration,
    seed: JobId,
}

impl<P: DynamoPort> ControlJob<P> {
    pub fn try_new(
        port: Arc<P>,
        journal: Arc<JournalStore>,
        codec: Arc<replication_provider::canonical_event_codec::CanonicalJsonCodec>,
        transport: TcpTransport,
    ) -> Result<Self, TransportError> {
        let configured = transport.configuration().as_ref().as_ref();
        journal.require_deployment(configured)?;
        let seed = crate::job_recovery::derive_controller_seed(CONTROL_JOB_KIND, configured)?;
        let idle_interval = Duration::from_millis(configured.timing.retry_max_ms);
        let runner = ControlRunner {
            port,
            journal,
            codec,
            transport,
        };
        let timeout = runner.operation_timeout()?;
        Ok(Self {
            runner,
            timeout,
            idle_interval,
            seed,
        })
    }
}

impl<P: DynamoPort + 'static> ControlJob<P>
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
        let timeout = self.timeout;
        let specification = JobSpec::unit(
            JobKind::try_new(CONTROL_JOB_KIND)?,
            NonZeroU16::MIN,
            QueueName::try_new(CONTROL_JOB_KIND)?,
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

impl<P: DynamoPort + 'static> JobHandler for ControlJob<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    fn execute<'a>(
        &'a self,
        context: ExecutionContext<'a>,
        payload: &'a [u8],
    ) -> HandlerFuture<'a> {
        crate::job_recovery::advance_pending_controller(
            context,
            payload,
            self.idle_interval,
            self.runner.advance_pending(),
        )
    }
}
