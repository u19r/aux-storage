//! Genuine scheduler claims shared by native maintenance contracts.

use std::{num::NonZeroU16, sync::Arc, time::Duration};

use bg_job_store::{ScheduledJobStore, ScheduledJobStoreSource};
use scheduled_bg_jobs::{
    EffectMode, EnqueueOptions, FairnessClass, FairnessPolicy, JobCatalog, JobId, JobKind, JobSpec,
    Namespace, OwnerId, QueueName, RetryPreset, RetrySelection, Timestamp,
    runtime::{JobSystem, JobSystemBuilder},
    test_support::{TestProfile, raw::ClaimedJob},
};
use storage_provider::{
    DynamoPort,
    maintenance::{ExpirationHandler, ExpirationPort},
};

use crate::HarnessResult;

fn build_system<P>(
    store: ScheduledJobStore,
    destination: Arc<P>,
    owner: [u8; 16],
    namespace: Namespace,
    payload: bytes::Bytes,
) -> HarnessResult<JobSystem<ScheduledJobStore>>
where
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: ExpirationPort,
{
    let mut catalog = JobCatalog::new();
    let now = Timestamp::from_millis(1_800_000_000_000);
    catalog.register_seeded(
        JobSpec::bytes(
            JobKind::try_new("native-destination-expiration")?,
            NonZeroU16::MIN,
            QueueName::try_new("native-destination-expiration")?,
            ExpirationHandler::try_new(destination, Duration::from_secs(60))?,
        )
        .policy(
            EffectMode::Fenced,
            FairnessPolicy::low_volume(),
            RetrySelection::Preset(RetryPreset::Maintenance),
            Duration::from_secs(30),
        ),
        payload,
        EnqueueOptions::new(FairnessClass::low_volume())
            .job_id(JobId::from([3; 16]))
            .scheduled_at(now),
        now,
    )?;
    Ok(JobSystemBuilder::new(store)
        .catalog(catalog)
        .namespace(namespace)
        .owner(OwnerId::from(owner))
        .scheduler(Arc::new(TestProfile::new()))
        .build()?)
}

pub(crate) async fn prepare_claim<S, P>(
    scheduler: &S,
    destination: Arc<P>,
    successor_destination: Arc<P>,
    namespace: Namespace,
) -> HarnessResult<(
    JobSystem<ScheduledJobStore>,
    JobSystem<ScheduledJobStore>,
    ClaimedJob,
)>
where
    S: ScheduledJobStoreSource,
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: ExpirationPort,
{
    prepare_payload_claim(
        scheduler,
        destination,
        successor_destination,
        namespace,
        bytes::Bytes::from_static(b"null"),
    )
    .await
}

pub(crate) async fn prepare_payload_claim<S, P>(
    scheduler: &S,
    destination: Arc<P>,
    successor_destination: Arc<P>,
    namespace: Namespace,
    payload: bytes::Bytes,
) -> HarnessResult<(
    JobSystem<ScheduledJobStore>,
    JobSystem<ScheduledJobStore>,
    ClaimedJob,
)>
where
    S: ScheduledJobStoreSource,
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: ExpirationPort,
{
    let store = scheduler.scheduled_job_store(super::fixture::scheduled_job_config())?;
    let original = build_system(
        store.clone(),
        destination,
        [1; 16],
        namespace.clone(),
        payload.clone(),
    )?;
    let successor = build_system(store, successor_destination, [2; 16], namespace, payload)?;
    let now = Timestamp::from_millis(1_800_000_000_000);
    original.ensure_seeds(now).await?;
    let first = original.claim(JobId::from([3; 16]), now).await?;
    Ok((original, successor, first))
}

pub(crate) async fn take_over(
    successor: &JobSystem<ScheduledJobStore>,
    first: &ClaimedJob,
) -> HarnessResult<ClaimedJob> {
    let old = first.context().ok_or("first context missing")?;
    let next = old.wall_now().checked_add(Duration::from_secs(31))?;
    let claim = successor.claim(old.job_id(), next).await?;
    let current = claim.context().ok_or("successor context missing")?;
    assert_eq!(old.namespace(), current.namespace());
    assert_eq!(old.job_id(), current.job_id());
    assert_eq!(old.execution_id(), current.execution_id());
    assert_ne!(old.attempt_id(), current.attempt_id());
    assert!(current.fence() > old.fence());
    Ok(claim)
}
