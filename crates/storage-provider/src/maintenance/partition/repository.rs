//! One fenced, bounded topology decision over the initialized store.
use std::{num::NonZeroU64, sync::Arc};

use async_trait::async_trait;
use scheduled_bg_jobs::{JobId, Namespace};
use storage_common::{
    PARTITION_SAMPLE_MAX_AGE_MS, PARTITION_SAMPLE_PAGE_LIMIT,
    partition::{PartitionLoadObservation, PartitionLoadSampleRecord},
    retry::retry_native_conflicts,
};
use storage_types::{
    AttemptMode, BoundedVec, Revision, StorageError, StorageResult, StreamName, TimestampMillis,
    maintenance::{
        MaintenanceKind, StepResult, TaskAdmission, TaskAuthority, TaskProgress, TaskScope,
    },
};

use super::PartitionPort;
use crate::{DYNAMO_WRITE_RETRY, DynamoPort};

#[derive(Clone)]
pub struct PartitionReconciliationRequest {
    pub destination: Namespace,
    pub job: JobId,
    pub fence: NonZeroU64,
    pub captured_at: TimestampMillis,
}

#[async_trait]
pub trait PartitionReconciliationRepository: Send + Sync {
    async fn advance_partition_reconciliation(
        &self,
        request: PartitionReconciliationRequest,
    ) -> StorageResult<StepResult<()>>;
    async fn publish_partition_samples(
        &self,
        stream: StreamName,
        records: BoundedVec<PartitionLoadSampleRecord, 0, { PARTITION_SAMPLE_PAGE_LIMIT as usize }>,
    ) -> StorageResult<()>;
}

#[async_trait]
impl<P: DynamoPort + 'static> PartitionReconciliationRepository for P
where for<'a> P::Attempt<'a>: PartitionPort
{
    async fn advance_partition_reconciliation(
        &self,
        request: PartitionReconciliationRequest,
    ) -> StorageResult<StepResult<()>> {
        retry_native_conflicts(self, DYNAMO_WRITE_RETRY, || {
            let request = request.clone();
            self.with_dynamo_attempt(AttemptMode::ReadWrite, move |port| {
                Box::pin(async move { advance(port, request).await })
            })
        })
        .await
    }

    async fn publish_partition_samples(
        &self,
        stream: StreamName,
        records: BoundedVec<PartitionLoadSampleRecord, 0, { PARTITION_SAMPLE_PAGE_LIMIT as usize }>,
    ) -> StorageResult<()> {
        if records.is_empty() {
            return Ok(());
        }
        let records = Arc::new(records);
        retry_native_conflicts(self, DYNAMO_WRITE_RETRY, || {
            let stream = stream.clone();
            let records = Arc::clone(&records);
            self.with_dynamo_attempt(AttemptMode::ReadWrite, move |port| {
                Box::pin(async move { publish_samples(port, &stream, records.as_slice()).await })
            })
        })
        .await
    }
}

async fn advance<P: PartitionPort>(
    port: &mut P,
    request: PartitionReconciliationRequest,
) -> StorageResult<StepResult<()>> {
    let (identity, durable, fence) = port
        .load_task_admission::<Option<StreamName>>(MaintenanceKind::PartitionReconciliation)
        .await?;
    let authority = TaskAuthority {
        scope: TaskScope {
            destination: request.destination,
            target: identity.clone(),
            work: MaintenanceKind::PartitionReconciliation,
        },
        job: request.job,
        fence: request.fence,
    };
    let revision = match authority.admit(Some(&identity), durable.as_ref()) {
        TaskAdmission::Initialize { .. } => None,
        TaskAdmission::Evaluate {
            expected_revision, ..
        } => Some(expected_revision),
        TaskAdmission::Superseded => return Ok(StepResult::Superseded),
        TaskAdmission::BindingMismatch => {
            // A different namespace or job can reach the same persisted task
            // after configuration changes. Its fence cannot authorize that
            // task.
            return Err(StorageError::validation(
                "partition task belongs to another job",
            ));
        }
    };
    let after = durable.and_then(|progress| progress.state);
    let (next, result) = reconcile_next(port, after, request.captured_at).await?;
    let next_revision = revision
        .unwrap_or(Revision::INITIAL)
        .checked_next()
        .map_err(|_| StorageError::internal("partition task revision overflow"))?;
    port.store_task_progress(
        MaintenanceKind::PartitionReconciliation,
        &fence,
        revision,
        TaskProgress {
            authority,
            revision: next_revision,
            state: next,
        },
    )
    .await?;
    Ok(result)
}

async fn reconcile_next<P: PartitionPort>(
    port: &mut P,
    after: Option<StreamName>,
    now: TimestampMillis,
) -> StorageResult<(Option<StreamName>, StepResult<()>)> {
    let Some(stream) = port.scan_partition_sample_stream(after.as_ref()).await? else {
        return Ok((None, StepResult::Complete));
    };
    let super::PartitionSamplePage {
        family,
        records,
        more,
        consumed,
    } = port.load_partition_samples(&stream).await?;
    let mut observations = observations(records, now);
    match family.filter(|_| !observations.is_empty()) {
        Some(family) => {
            let change = family.reconcile(&mut observations, now.timestamp_millis())?;
            port.store_partition_reconciliation(
                &stream,
                change.write_required.then_some(&change.family),
                consumed,
            )
            .await?;
        }
        None => {
            port.store_partition_reconciliation(&stream, None, consumed)
                .await?
        }
    }
    Ok((
        if more { after } else { Some(stream) },
        StepResult::Advanced(()),
    ))
}

fn observations(
    mut records: Vec<PartitionLoadSampleRecord>,
    now: TimestampMillis,
) -> Vec<PartitionLoadObservation> {
    records.sort_unstable_by_key(|record| {
        (
            record.partition_id,
            std::cmp::Reverse(record.window_start_ms),
        )
    });
    let mut observations: Vec<PartitionLoadObservation> = Vec::new();
    let mut current_window = None;
    let now_millis = now.timestamp_millis();
    for record in records {
        if record.window_start_ms > now_millis
            || now_millis.saturating_sub(record.window_start_ms) > PARTITION_SAMPLE_MAX_AGE_MS
        {
            continue;
        }
        if let Some(last) = observations
            .last_mut()
            .filter(|last| last.partition_id == record.partition_id)
        {
            // Backlogged windows are not one second of throughput. Combine only
            // publishers from the newest observed window for this partition.
            if current_window == Some(record.window_start_ms) {
                last.sample.merge(&record.sample);
            }
        } else {
            current_window = Some(record.window_start_ms);
            observations.push(PartitionLoadObservation {
                partition_id: record.partition_id,
                sample: record.sample,
                queue_empty: false,
            });
        }
    }
    observations
}

async fn publish_samples(
    port: &mut impl PartitionPort,
    stream: &StreamName,
    records: &[PartitionLoadSampleRecord],
) -> StorageResult<()> {
    let family = port.load_partition_family(stream).await?;
    for record in records {
        record.validate_for(&family)?;
    }
    port.store_partition_samples(stream, records).await
}

/// Native admission wraps each attempt, while the shared decisions remain above
/// it.
pub struct AdmittedPartitionRepository<P> {
    port: Arc<P>,
    binding: Arc<crate::native_admission::NativeAdmissionBinding>,
}

impl<P> AdmittedPartitionRepository<P> {
    pub fn new(
        port: Arc<P>,
        binding: Arc<crate::native_admission::NativeAdmissionBinding>,
    ) -> Self {
        Self { port, binding }
    }
}

#[async_trait]
impl<P: DynamoPort> PartitionReconciliationRepository for AdmittedPartitionRepository<P>
where for<'native> P::Attempt<'native>: PartitionPort
{
    async fn advance_partition_reconciliation(
        &self,
        request: PartitionReconciliationRequest,
    ) -> StorageResult<StepResult<()>> {
        retry_native_conflicts(self.port.as_ref(), DYNAMO_WRITE_RETRY, || {
            let port = Arc::clone(&self.port);
            let request = request.clone();
            self.binding.settle(
                crate::native_admission::NativeWork::Background,
                move || async move {
                    port.with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
                        Box::pin(advance(attempt, request))
                    })
                    .await
                },
            )
        })
        .await
    }

    async fn publish_partition_samples(
        &self,
        stream: StreamName,
        records: BoundedVec<PartitionLoadSampleRecord, 0, { PARTITION_SAMPLE_PAGE_LIMIT as usize }>,
    ) -> StorageResult<()> {
        if records.is_empty() {
            return Ok(());
        }
        let records = Arc::new(records);
        retry_native_conflicts(self.port.as_ref(), DYNAMO_WRITE_RETRY, || {
            let port = Arc::clone(&self.port);
            let stream = stream.clone();
            let records = Arc::clone(&records);
            self.binding.settle(
                crate::native_admission::NativeWork::Background,
                move || async move {
                    port.with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
                        Box::pin(async move {
                            publish_samples(attempt, &stream, records.as_slice()).await
                        })
                    })
                    .await
                },
            )
        })
        .await
    }
}
