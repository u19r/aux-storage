use std::{num::NonZeroU16, sync::Arc, time::Duration};

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use scheduled_bg_jobs::{
    EffectMode, EnqueueOptions, ExecutionContext, FairnessClass, FairnessPolicy, HandlerFuture,
    JobCatalog, JobHandle, JobHandler, JobId, JobKind, JobSpec, QueueName, RetryPreset,
    RetrySelection, Timestamp, ValidationError,
};
use storage_provider::DynamoPort;
use storage_types::{AttemptMode, StorageResult};

use super::{ArchiveKind, ArchivePublication, ArchiveTurn, CanonicalArchive, VerifiedArchive};
use crate::{
    journal::JournalStore,
    transport::{TcpTransport, TransportError},
};

pub(crate) struct ArchiveJob<P> {
    port: Arc<P>,
    journal: Arc<JournalStore>,
    codec: Arc<CanonicalJsonCodec>,
    transport: TcpTransport,
    archive: Arc<CanonicalArchive>,
    timeout: Duration,
    idle_interval: Duration,
    seed: JobId,
}

impl<P: DynamoPort> ArchiveJob<P> {
    pub(crate) fn try_new(
        port: Arc<P>,
        journal: Arc<JournalStore>,
        codec: Arc<CanonicalJsonCodec>,
        transport: TcpTransport,
    ) -> Result<Self, TransportError> {
        let configured = transport.configuration().as_ref().as_ref();
        journal.require_deployment(configured)?;
        let archive = journal
            .archive
            .get()
            .cloned()
            .ok_or(TransportError::Configuration)?;
        let seed = crate::job_recovery::derive_controller_seed(
            crate::constants::ARCHIVE_JOB_KIND,
            configured,
        )?;
        let timeout = configured
            .timing
            .request_budget(4)
            .map_err(|_| TransportError::Configuration)?;
        let idle_interval = Duration::from_millis(configured.timing.retry_max_ms);
        Ok(Self {
            port,
            journal,
            codec,
            transport,
            archive,
            timeout,
            idle_interval,
            seed,
        })
    }
}

impl<P: DynamoPort + 'static> ArchiveJob<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub(crate) fn register(
        self,
        catalog: &mut JobCatalog,
        now: Timestamp,
    ) -> Result<JobHandle<()>, ValidationError> {
        let timeout = self.timeout;
        let options = EnqueueOptions::new(FairnessClass::low_volume())
            .job_id(self.seed)
            .scheduled_at(now);
        let specification = JobSpec::unit(
            JobKind::try_new(crate::constants::ARCHIVE_JOB_KIND)?,
            NonZeroU16::MIN,
            QueueName::try_new(crate::constants::ARCHIVE_JOB_KIND)?,
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

    async fn advance_archive(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let source = self.transport.reserve_source_budget(self.timeout).await?;
        let job = self.clone();
        crate::transport::settle_source_with(source, move |source| async move {
            source.require_live()?;
            let plan = match job.prepare_archive().await? {
                ArchiveTurn::Export(plan) => plan,
                ArchiveTurn::Advanced => return Ok(true),
                ArchiveTurn::Idle => return Ok(false),
            };
            source.require_live()?;
            let verified = job
                .verify_archive_dependencies(plan.publish(&job.archive).await?, &source)
                .await?;
            source.require_live()?;
            job.publish_archive(Arc::new(verified)).await?;
            Ok(true)
        })
        .await
    }

    async fn verify_archive_dependencies(
        &self,
        mut publication: ArchivePublication,
        source: &crate::transport::SourceReservation,
    ) -> Result<VerifiedArchive, Box<dyn std::error::Error + Send + Sync>> {
        // The closed graph is position -> page -> acceptance -> original: at
        // most three redirects.
        for turn in 0..=3 {
            match publication {
                ArchivePublication::Verified(verified) => return Ok(verified),
                ArchivePublication::RecoverDependency { hold, pointer } => {
                    if turn == 3 {
                        return Err(storage_types::StorageError::internal(
                            "Archive dependency graph exceeds its supported depth",
                        )
                        .into());
                    }
                    source.require_live()?;
                    let plan = self.prepare_dependency_recovery(hold, pointer).await?;
                    source.require_live()?;
                    publication = plan.publish(&self.archive).await?;
                }
            }
        }
        Err(
            storage_types::StorageError::internal("Archive dependency verification did not settle")
                .into(),
        )
    }

    async fn prepare_archive(&self) -> StorageResult<ArchiveTurn> {
        let Some(kind) = self.select_archive_family().await? else {
            return Ok(ArchiveTurn::Advanced);
        };
        storage_common::retry::retry_native_conflicts(
            self.port.as_ref(),
            storage_provider::DYNAMO_WRITE_RETRY,
            || {
                let journal = Arc::clone(&self.journal);
                let codec = Arc::clone(&self.codec);
                let limits = self.transport.source_limits();
                async move {
                    Arc::clone(&journal)
                        .with_admitted_attempt(
                            (
                                self.port.as_ref(),
                                storage_provider::native_admission::NativeWork::Recovery,
                            ),
                            crate::journal::NativeAttempt::Items(AttemptMode::ReadWrite),
                            move |attempt, _| {
                                Box::pin(async move {
                                    journal
                                        .prepare_archive_turn(attempt, kind, (&limits, &codec))
                                        .await
                                })
                            },
                        )
                        .await
                }
            },
        )
        .await
    }

    async fn select_archive_family(&self) -> StorageResult<Option<ArchiveKind>> {
        storage_common::retry::retry_native_conflicts(
            self.port.as_ref(),
            storage_provider::DYNAMO_WRITE_RETRY,
            || {
                let journal = Arc::clone(&self.journal);
                async move {
                    Arc::clone(&journal)
                        .with_admitted_attempt(
                            (
                                self.port.as_ref(),
                                storage_provider::native_admission::NativeWork::Recovery,
                            ),
                            crate::journal::NativeAttempt::Items(AttemptMode::ReadWrite),
                            move |attempt, _| {
                                Box::pin(
                                    async move { journal.select_archive_family(attempt).await },
                                )
                            },
                        )
                        .await
                }
            },
        )
        .await
    }

    async fn prepare_dependency_recovery(
        &self,
        hold: super::ArchiveHold,
        pointer: super::ArchivePointer,
    ) -> StorageResult<super::ArchivePlan> {
        let input = Arc::new((hold, pointer));
        storage_common::retry::retry_native_conflicts(
            self.port.as_ref(),
            storage_provider::DYNAMO_WRITE_RETRY,
            || {
                let journal = Arc::clone(&self.journal);
                let codec = Arc::clone(&self.codec);
                let input = Arc::clone(&input);
                async move {
                    Arc::clone(&journal)
                        .with_admitted_attempt(
                            (
                                self.port.as_ref(),
                                storage_provider::native_admission::NativeWork::Recovery,
                            ),
                            crate::journal::NativeAttempt::Items(AttemptMode::ReadOnly),
                            move |attempt, _| {
                                Box::pin(async move {
                                    journal
                                        .prepare_archive_dependency_recovery(
                                            attempt,
                                            (&input.0, &input.1),
                                            &codec,
                                        )
                                        .await
                                })
                            },
                        )
                        .await
                }
            },
        )
        .await
    }

    async fn publish_archive(&self, verified: Arc<VerifiedArchive>) -> StorageResult<()> {
        storage_common::retry::retry_native_conflicts(
            self.port.as_ref(),
            storage_provider::DYNAMO_WRITE_RETRY,
            || {
                let journal = Arc::clone(&self.journal);
                let verified = Arc::clone(&verified);
                async move {
                    Arc::clone(&journal)
                        .with_admitted_attempt(
                            (
                                self.port.as_ref(),
                                storage_provider::native_admission::NativeWork::Recovery,
                            ),
                            crate::journal::NativeAttempt::Items(AttemptMode::ReadWrite),
                            move |attempt, _| {
                                Box::pin(async move {
                                    journal.publish_archive_pointer(attempt, &verified).await
                                })
                            },
                        )
                        .await
                }
            },
        )
        .await
    }
}

impl<P: DynamoPort + 'static> JobHandler for ArchiveJob<P>
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
            self.advance_archive(),
        )
    }
}

impl<P> Clone for ArchiveJob<P> {
    fn clone(&self) -> Self {
        Self {
            port: Arc::clone(&self.port),
            journal: Arc::clone(&self.journal),
            codec: Arc::clone(&self.codec),
            transport: self.transport.clone(),
            archive: Arc::clone(&self.archive),
            timeout: self.timeout,
            idle_interval: self.idle_interval,
            seed: self.seed,
        }
    }
}
