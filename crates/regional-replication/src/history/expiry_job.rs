//! Native deadline settlement on the host's existing shared scheduler and
//! source budget.
use std::{num::NonZeroU16, sync::Arc, time::Duration};

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use scheduled_bg_jobs::{
    EffectMode, EnqueueOptions, ExecutionContext, FairnessClass, FairnessPolicy, HandlerFuture,
    JobCatalog, JobHandle, JobHandler, JobId, JobKind, JobSpec, QueueName, RetryPreset,
    RetrySelection, Timestamp, ValidationError,
};
use storage_common::retry::retry_native_conflicts;
use storage_provider::{DYNAMO_WRITE_RETRY, DynamoPort};
use storage_types::AttemptMode;

use crate::{
    constants::{READER_EXPIRY_IDLE_INTERVAL_MS, READER_EXPIRY_JOB_KIND},
    journal::JournalStore,
    transport::{TcpTransport, TransportError},
};

pub(crate) struct ReaderExpiryJob<P> {
    port: Arc<P>,
    journal: Arc<JournalStore>,
    transport: TcpTransport,
    timeout: Duration,
    seed: JobId,
}

impl<P: DynamoPort> ReaderExpiryJob<P> {
    pub(crate) fn try_new(
        port: Arc<P>,
        journal: Arc<JournalStore>,
        transport: TcpTransport,
    ) -> Result<Self, TransportError> {
        let configured = transport.configuration().as_ref().as_ref();
        journal.require_deployment(configured)?;
        let seed = crate::job_recovery::derive_controller_seed(READER_EXPIRY_JOB_KIND, configured)?;
        let timeout = configured
            .timing
            .request_budget(2)
            .map_err(|_| TransportError::Configuration)?;
        Ok(Self {
            port,
            journal,
            transport,
            timeout,
            seed,
        })
    }
}

impl<P: DynamoPort + 'static> ReaderExpiryJob<P>
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
        let spec = JobSpec::unit(
            JobKind::try_new(READER_EXPIRY_JOB_KIND)?,
            NonZeroU16::MIN,
            QueueName::try_new(READER_EXPIRY_JOB_KIND)?,
            self,
        )
        .policy(
            EffectMode::Idempotent,
            FairnessPolicy::low_volume(),
            RetrySelection::Preset(RetryPreset::Maintenance),
            timeout,
        );
        catalog.register_seeded(spec, (), options, now)
    }

    async fn settle_next(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let source = self.transport.reserve_source().await?;
        let port = Arc::clone(&self.port);
        let journal = Arc::clone(&self.journal);
        crate::transport::settle_source_with(source, move |source| async move {
            let result = retry_native_conflicts(port.as_ref(), DYNAMO_WRITE_RETRY, || {
                let port = Arc::clone(&port);
                let journal = Arc::clone(&journal);
                async move {
                    Arc::clone(&journal)
                        .with_admitted_attempt(
                            (
                                port.as_ref(),
                                storage_provider::native_admission::NativeWork::Recovery,
                            ),
                            crate::journal::NativeAttempt::Items(AttemptMode::ReadWrite),
                            move |attempt, observed_at| {
                                Box::pin(async move {
                                    journal
                                        .settle_next_reader_expiry(attempt, observed_at)
                                        .await
                                })
                            },
                        )
                        .await
                }
            })
            .await;
            drop(source);
            result.map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)
        })
        .await
    }
}

impl<P: DynamoPort + 'static> JobHandler for ReaderExpiryJob<P>
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
            Duration::from_millis(READER_EXPIRY_IDLE_INTERVAL_MS),
            self.settle_next(),
        )
    }
}
