use std::{num::NonZeroU16, sync::Arc, time::Duration};

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use scheduled_bg_jobs::{
    EffectMode, EnqueueOptions, ExecutionContext, FairnessClass, FairnessPolicy, HandlerFuture,
    JobCatalog, JobHandle, JobHandler, JobId, JobKind, JobSpec, QueueName, RetryPreset,
    RetrySelection, Timestamp, ValidationError,
};
use storage_provider::{DynamoPort, ItemTombstoneScanAttempt, PrimaryStateScanAttempt};

use super::runner::EnableRunner;
use crate::{
    bootstrap::BootstrapTransferJob,
    constants::ENABLE_JOB_KIND,
    control::runner::ControlRunner,
    journal::JournalStore,
    transport::{TcpTransport, TransportError},
};

/// One recoverable shared controller discovers native pending work. Accepted
/// requests do not depend on a second transaction enqueueing their payload.
pub struct EnableGlobalJob<P> {
    runner: EnableRunner<P>,
    timeout: Duration,
    idle_interval: Duration,
    seed: JobId,
}

impl<P: DynamoPort + 'static> EnableGlobalJob<P> {
    pub fn try_new(
        port: Arc<P>,
        journal: Arc<JournalStore>,
        codec: Arc<CanonicalJsonCodec>,
        transport: TcpTransport,
    ) -> Result<Self, TransportError> {
        let configured = transport.configuration().as_ref().as_ref();
        journal.require_deployment(configured)?;
        let seed = crate::job_recovery::derive_controller_seed(ENABLE_JOB_KIND, configured)?;
        let idle_interval = Duration::from_millis(configured.timing.retry_max_ms);
        let bootstrap = BootstrapTransferJob::try_new(
            Arc::clone(&port),
            Arc::clone(&journal),
            Arc::clone(&codec),
            transport.clone(),
        )?;
        let control = ControlRunner {
            port: Arc::clone(&port),
            journal: Arc::clone(&journal),
            codec,
            transport: transport.clone(),
        };
        let timeout = control.operation_timeout()?;
        Ok(Self {
            runner: EnableRunner {
                port,
                journal,
                transport,
                bootstrap,
                control,
            },
            timeout,
            idle_interval,
            seed,
        })
    }
}

impl<P: DynamoPort + 'static> EnableGlobalJob<P>
where for<'native> P::Attempt<'native>:
        PhysicalRecordAttempt + PrimaryStateScanAttempt + ItemTombstoneScanAttempt
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
            JobKind::try_new(ENABLE_JOB_KIND)?,
            NonZeroU16::MIN,
            QueueName::try_new(ENABLE_JOB_KIND)?,
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

impl<P: DynamoPort + 'static> JobHandler for EnableGlobalJob<P>
where for<'native> P::Attempt<'native>:
        PhysicalRecordAttempt + PrimaryStateScanAttempt + ItemTombstoneScanAttempt
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
