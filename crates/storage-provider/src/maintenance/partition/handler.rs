use std::{sync::Arc, time::Duration};

use scheduled_bg_jobs::{ExecutionContext, HandlerFuture, JobHandler, JobOutcome};
use storage_types::{StorageError, StorageResult, TimestampMillis, maintenance::StepResult};

use super::{PartitionReconciliationRepository, PartitionReconciliationRequest};

pub struct PartitionReconciliationHandler {
    repository: Arc<dyn PartitionReconciliationRepository>,
    interval: Duration,
}
impl PartitionReconciliationHandler {
    pub fn try_new(
        repository: Arc<dyn PartitionReconciliationRepository>,
        interval: Duration,
    ) -> StorageResult<Self> {
        if interval.is_zero() {
            return Err(StorageError::validation(
                "partition cadence must be positive",
            ));
        }
        Ok(Self {
            repository,
            interval,
        })
    }
}
impl JobHandler for PartitionReconciliationHandler {
    fn execute<'a>(
        &'a self,
        context: ExecutionContext<'a>,
        _payload: &'a [u8],
    ) -> HandlerFuture<'a> {
        let now = context.wall_now();
        let checkpoint = context.checkpoint().unwrap_or_default().to_vec();
        storage_common::maintenance_continuation::defer_unavailable_turn(
            async move {
                if context.is_cancelled() {
                    return Ok(JobOutcome::Cancelled);
                }
                let result = self
                    .repository
                    .advance_partition_reconciliation(PartitionReconciliationRequest {
                        destination: context.namespace().clone(),
                        job: context.job_id(),
                        fence: context.fence(),
                        captured_at: TimestampMillis::from_timestamp(
                            context.wall_now().as_millis(),
                        ),
                    })
                    .await?;
                let delay = if matches!(result, StepResult::Complete) {
                    self.interval
                } else {
                    Duration::ZERO
                };
                Ok(JobOutcome::Continue {
                    not_before: context.wall_now().checked_add(delay)?,
                    checkpoint: Vec::<u8>::new().into(),
                })
            },
            now,
            self.interval,
            checkpoint,
        )
    }
}
