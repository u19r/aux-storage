use std::{sync::Arc, time::Duration};

use scheduled_bg_jobs::{ExecutionContext, HandlerFuture, JobHandler, JobOutcome, Timestamp};
use storage_backfill::GsiPageLimit;
use storage_types::{StorageError, StorageResult, TimestampMillis, maintenance::StepResult};

use super::{GsiBuildRepository, GsiBuildRequest};

pub struct GsiBuildHandler {
    repository: Arc<dyn GsiBuildRepository>,
    interval: Duration,
    page_limit: GsiPageLimit,
}

impl GsiBuildHandler {
    pub fn try_new(
        repository: Arc<dyn GsiBuildRepository>,
        interval: Duration,
        page_limit: GsiPageLimit,
    ) -> StorageResult<Self> {
        if interval.is_zero() {
            return Err(StorageError::validation(
                "GSI build maintenance interval must not be zero",
            ));
        }
        Ok(Self {
            repository,
            interval,
            page_limit,
        })
    }

    fn outcome(&self, result: StepResult<()>, wall_now: Timestamp) -> StorageResult<JobOutcome> {
        let delay = match result {
            StepResult::Advanced(()) | StepResult::Superseded => Duration::ZERO,
            StepResult::Complete => self.interval,
        };
        let not_before = wall_now
            .checked_add(delay)
            .map_err(|_| StorageError::internal("GSI maintenance cadence overflow"))?;
        Ok(JobOutcome::Continue {
            not_before,
            checkpoint: Vec::<u8>::new().into(),
        })
    }

    fn handler_error(error: StorageError) -> Box<dyn std::error::Error + Send + Sync> {
        Box::new(error)
    }
}

impl JobHandler for GsiBuildHandler {
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
                    .advance_gsi_build(GsiBuildRequest {
                        destination: context.namespace().clone(),
                        job: context.job_id(),
                        fence: context.fence(),
                        page_limit: self.page_limit,
                        captured_at: TimestampMillis::from_timestamp(
                            context.wall_now().as_millis(),
                        ),
                    })
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
