use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use scheduled_bg_jobs::{
    ClaimLeasePolicy, ExecutionContext, HandlerFuture, JobHandler, JobOutcome, Timestamp,
};
use storage_types::{StorageError, StorageResult, TimestampMillis, maintenance::StepResult};

use super::{ExpirationRepository, ExpirationRequest};

const EXPIRATION_PAGE_LIMIT: NonZeroUsize = NonZeroUsize::MIN.saturating_add(255);

pub struct ExpirationHandler {
    repository: Arc<dyn ExpirationRepository>,
    interval: Duration,
}

impl ExpirationHandler {
    pub fn try_new(
        repository: Arc<dyn ExpirationRepository>,
        interval: Duration,
    ) -> StorageResult<Self> {
        if interval.is_zero() {
            return Err(StorageError::validation(
                "expiration maintenance interval must not be zero",
            ));
        }
        Ok(Self {
            repository,
            interval,
        })
    }

    fn outcome(&self, result: StepResult<()>, wall_now: Timestamp) -> StorageResult<JobOutcome> {
        let delay = match result {
            StepResult::Advanced(_) | StepResult::Superseded => Duration::ZERO,
            StepResult::Complete => self.interval,
        };
        let not_before = wall_now
            .checked_add(delay)
            .map_err(|_| StorageError::internal("maintenance cadence overflow"))?;
        Ok(JobOutcome::Continue {
            not_before,
            checkpoint: Vec::<u8>::new().into(),
        })
    }

    fn handler_error(error: StorageError) -> Box<dyn std::error::Error + Send + Sync> {
        Box::new(error)
    }
}

impl JobHandler for ExpirationHandler {
    fn claim_lease_policy(&self) -> ClaimLeasePolicy {
        ClaimLeasePolicy::DriveBoundedNative
    }

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
                let request = ExpirationRequest {
                    destination: context.namespace().clone(),
                    job: context.job_id(),
                    fence: context.fence(),
                    requested_cutoff: TimestampMillis::from_timestamp(
                        context.wall_now().as_millis(),
                    ),
                    candidate_limit: EXPIRATION_PAGE_LIMIT,
                };
                let result = self
                    .repository
                    .advance_expiration(request)
                    .await
                    .map_err(Self::handler_error)?;
                self.outcome(result, context.wall_now())
                    .map_err(Self::handler_error)
            },
            now,
            self.interval,
            checkpoint,
        )
    }
}
