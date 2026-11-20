//! Deletion of subscriptions still pending confirmation 48 hours after
//! `Subscribe` (AWS behaviour), one bounded page per scheduled turn.

use std::{error::Error, num::NonZeroU32, sync::Arc, time::Duration};

use scheduled_bg_jobs::{ExecutionContext, HandlerFuture, JobHandler, JobOutcome};
use storage_types::{StorageError, StorageResult, TimestampMillis, storage_serde};

use crate::{
    PendingSubscriptionCursor, PendingSubscriptionPage, PubsubAttempt, PubsubError, PubsubPort,
    PubsubProvider, SubscriptionArn, after,
    constants::{
        PENDING_SUBSCRIPTION_LIFETIME, PENDING_SUBSCRIPTION_SCAN, PUBSUB_PAGE_LIMIT,
        STALE_SUBSCRIPTION_DELETE,
    },
};

/// What one sweep turn did.
pub struct StaleSubscriptionSweep {
    /// The cursor of the next page; `None` after the last page.
    pub next: Option<PendingSubscriptionCursor>,
    /// Subscriptions that this turn deleted.
    pub deleted: usize,
}

struct ScanInput {
    after: Option<PendingSubscriptionCursor>,
    limit: NonZeroU32,
}

struct DeleteInput {
    arn: SubscriptionArn,
    now: TimestampMillis,
}

/// A subscription created at `created_at` is stale at `now` when the pending
/// lifetime has passed.
pub(crate) fn is_stale(created_at: TimestampMillis, now: TimestampMillis) -> bool {
    after(created_at, PENDING_SUBSCRIPTION_LIFETIME) <= now
}

pub(crate) async fn sweep_stale_subscriptions<P: PubsubPort>(
    port: &P,
    cursor: Option<PendingSubscriptionCursor>,
    limit: NonZeroU32,
) -> StorageResult<StaleSubscriptionSweep> {
    let now = storage_types::Clock::now(port);
    let page = port
        .run(
            PENDING_SUBSCRIPTION_SCAN,
            Arc::new(ScanInput {
                after: cursor,
                limit,
            }),
            |attempt, input, _| {
                Box::pin(attempt.read_pending_subscription_page(input.after.as_ref(), input.limit))
            },
        )
        .await?;
    validate_pending_page(&page, limit)?;
    let next = if page.more {
        page.rows.last().map(|row| row.cursor.clone())
    } else {
        None
    };
    let mut deleted = 0;
    for row in page.rows {
        if !is_stale(row.created_at, now) {
            continue;
        }
        let input = Arc::new(DeleteInput {
            arn: row.subscription_arn,
            now,
        });
        let removed = port
            .run(STALE_SUBSCRIPTION_DELETE, input, |attempt, input, _| {
                Box::pin(delete_stale_subscription_in_attempt(
                    attempt, &input.arn, input.now,
                ))
            })
            .await?;
        if removed {
            deleted += 1;
        }
    }
    storage_common::telemetry::record_provider_count(
        "pubsub",
        "stale_subscriptions_deleted",
        deleted,
    );
    Ok(StaleSubscriptionSweep { next, deleted })
}

pub(crate) fn validate_pending_page(
    page: &PendingSubscriptionPage,
    limit: NonZeroU32,
) -> StorageResult<()> {
    if page.rows.len() > limit.get() as usize || page.more && page.rows.is_empty() {
        return Err(StorageError::internal(
            "pending subscription page has invalid bounds",
        ));
    }
    Ok(())
}

/// Delete `arn` when it is still pending and stale at `now`. `true` when this
/// attempt deleted it; a confirmed, missing or younger subscription stays.
pub async fn delete_stale_subscription_in_attempt<A: PubsubAttempt>(
    attempt: &mut A,
    arn: &SubscriptionArn,
    now: TimestampMillis,
) -> StorageResult<bool> {
    let Some(record) = attempt
        .read_subscription(arn, attempt.default_protection())
        .await?
    else {
        return Ok(false);
    };
    if !record.subscription.confirmation.pending_confirmation()
        || !is_stale(record.subscription.created_at, now)
    {
        return Ok(false);
    }
    attempt.delete_subscription(record).await?;
    Ok(true)
}

/// The scheduled job of the sweep: one page per turn; the next page at once
/// while pages remain, else after `interval`.
pub struct StaleSubscriptionSweepHandler {
    provider: Arc<dyn PubsubProvider>,
    interval: Duration,
    page_limit: NonZeroU32,
}

impl StaleSubscriptionSweepHandler {
    pub fn try_new(
        provider: Arc<dyn PubsubProvider>,
        interval: Duration,
        page_limit: NonZeroU32,
    ) -> StorageResult<Self> {
        if interval.is_zero() || page_limit > PUBSUB_PAGE_LIMIT {
            return Err(StorageError::validation(
                "stale subscription sweep interval or page limit is invalid",
            ));
        }
        Ok(Self {
            provider,
            interval,
            page_limit,
        })
    }
}

/// The job outcome of one turn: the next page at once while pages remain
/// (the turn moved past at least one entry, `validate_pending_page`), else
/// after `interval`; the checkpoint holds the next cursor or is empty.
pub(crate) fn sweep_outcome(
    interval: Duration,
    next: Option<PendingSubscriptionCursor>,
    wall_now: scheduled_bg_jobs::Timestamp,
) -> StorageResult<JobOutcome> {
    let delay = if next.is_some() {
        Duration::ZERO
    } else {
        interval
    };
    let not_before = wall_now
        .checked_add(delay)
        .map_err(|_| StorageError::internal("stale subscription sweep cadence overflow"))?;
    let checkpoint = next
        .map(|cursor| storage_serde::to_bytes(&cursor))
        .transpose()?
        .unwrap_or_default();
    Ok(JobOutcome::Continue {
        not_before,
        checkpoint: checkpoint.into(),
    })
}

impl JobHandler for StaleSubscriptionSweepHandler {
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
                    .map_err(storage_handler_error)?;
                let sweep = self
                    .provider
                    .sweep_stale_subscriptions(cursor, self.page_limit)
                    .await
                    .map_err(pubsub_handler_error)?;
                sweep_outcome(self.interval, sweep.next, context.wall_now())
                    .map_err(storage_handler_error)
            },
            now,
            self.interval,
            checkpoint,
        )
    }
}

fn storage_handler_error(error: StorageError) -> Box<dyn Error + Send + Sync> {
    Box::new(error)
}

/// A storage failure stays a `StorageError`, so `defer_unavailable_turn` sees
/// `ServiceUnavailable`.
fn pubsub_handler_error(error: PubsubError) -> Box<dyn Error + Send + Sync> {
    match error {
        PubsubError::Storage(error) => Box::new(error),
        other => Box::new(other),
    }
}
