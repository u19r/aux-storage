//! Temporary storage unavailability defers recurring work without exhausting
//! retry budgets.
use std::{error::Error, future::Future, time::Duration};

use scheduled_bg_jobs::{HandlerFuture, JobOutcome, Timestamp};
use storage_types::{StorageEnum, StorageError};

pub fn defer_unavailable_turn<'a>(
    turn: impl Future<Output = Result<JobOutcome, Box<dyn Error + Send + Sync>>> + Send + 'a,
    now: Timestamp,
    delay: Duration,
    checkpoint: Vec<u8>,
) -> HandlerFuture<'a> {
    Box::pin(async move {
        match turn.await {
            Err(error)
                if error.downcast_ref::<StorageError>().is_some_and(|error| {
                    matches!(error.as_ref(), StorageEnum::ServiceUnavailable { .. })
                }) =>
            {
                Ok(JobOutcome::Continue {
                    not_before: now.checked_add(delay)?,
                    checkpoint: checkpoint.into(),
                })
            }
            result => result,
        }
    })
}
