//! Scheduled bounded reclamation of expired and retired queue messages.

use std::{num::NonZeroU32, sync::Arc, time::Duration};

use scheduled_bg_jobs::{ExecutionContext, HandlerFuture, JobHandler, JobOutcome};
use storage_types::{StorageError, StorageResult, storage_serde};

use crate::{QueueCleanupCursor, QueueMessageCleanupRepository};

pub struct QueueMessageCleanupHandler {
    repository: Arc<dyn QueueMessageCleanupRepository>,
    interval: Duration,
    page_limit: NonZeroU32,
}

impl QueueMessageCleanupHandler {
    pub fn try_new(
        repository: Arc<dyn QueueMessageCleanupRepository>,
        interval: Duration,
        page_limit: NonZeroU32,
    ) -> StorageResult<Self> {
        if interval.is_zero() || page_limit.get() > 100 {
            return Err(StorageError::validation(
                "queue cleanup interval or page limit is invalid",
            ));
        }
        Ok(Self {
            repository,
            interval,
            page_limit,
        })
    }

    fn next_outcome(
        &self,
        next: Option<QueueCleanupCursor>,
        wall_now: scheduled_bg_jobs::Timestamp,
    ) -> StorageResult<JobOutcome> {
        let delay = if next.is_some() {
            Duration::ZERO
        } else {
            self.interval
        };
        let not_before = wall_now
            .checked_add(delay)
            .map_err(|_| StorageError::internal("queue cleanup cadence overflow"))?;
        let checkpoint = next
            .map(|cursor| storage_serde::to_bytes(&cursor))
            .transpose()?
            .unwrap_or_default();
        Ok(JobOutcome::Continue {
            not_before,
            checkpoint: checkpoint.into(),
        })
    }
}

impl JobHandler for QueueMessageCleanupHandler {
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
                let cursor = context
                    .checkpoint()
                    .filter(|bytes| !bytes.is_empty())
                    .map(storage_serde::from_bytes)
                    .transpose()
                    .map_err(handler_error)?;
                let next = self
                    .repository
                    .cleanup_queue_messages_page(cursor, self.page_limit)
                    .await
                    .map_err(handler_error)?;
                self.next_outcome(next, context.wall_now())
                    .map_err(handler_error)
            },
            now,
            self.interval,
            checkpoint,
        )
    }
}

fn handler_error<E: std::error::Error + Send + Sync + 'static>(
    error: E,
) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(error)
}
