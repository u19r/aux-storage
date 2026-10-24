use std::{sync::Arc, time::Duration};

use scheduled_bg_jobs::{ExecutionContext, HandlerFuture, JobHandler, JobOutcome, Timestamp};
use storage_types::{StorageError, StorageResult, TimestampMillis};

use super::{StreamDurationTrimConfig, StreamDurationTrimWorker, StreamRetentionRepository};

pub struct RetentionHandler {
    repository: Arc<dyn StreamRetentionRepository + Send + Sync>,
    interval: Duration,
    page_config: StreamDurationTrimConfig,
}

impl RetentionHandler {
    pub fn try_new(
        repository: Arc<dyn StreamRetentionRepository + Send + Sync>,
        interval: Duration,
        page_config: StreamDurationTrimConfig,
    ) -> StorageResult<Self> {
        if interval.is_zero() {
            return Err(StorageError::validation(
                "stream retention interval must not be zero",
            ));
        }
        Ok(Self {
            repository,
            interval,
            page_config,
        })
    }

    fn outcome(
        &self,
        did_work: bool,
        page_full: bool,
        wall_now: Timestamp,
    ) -> StorageResult<JobOutcome> {
        let delay = if did_work || page_full {
            Duration::ZERO
        } else {
            self.interval
        };
        let not_before = wall_now
            .checked_add(delay)
            .map_err(|_| StorageError::internal("stream retention cadence overflow"))?;
        Ok(JobOutcome::Continue {
            not_before,
            checkpoint: Vec::<u8>::new().into(),
        })
    }

    fn handler_error(error: StorageError) -> Box<dyn std::error::Error + Send + Sync> {
        Box::new(error)
    }
}

impl JobHandler for RetentionHandler {
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
                let wall_now = TimestampMillis::from_timestamp(context.wall_now().as_millis());
                let stats =
                    StreamDurationTrimWorker::new(self.repository.as_ref(), self.page_config)
                        .run_due_page(wall_now, wall_now)
                        .await
                        .map_err(Self::handler_error)?;
                self.outcome(stats.did_work(), stats.marker_page_full, context.wall_now())
                    .map_err(Self::handler_error)
            },
            now,
            self.interval,
            checkpoint,
        )
    }
}
