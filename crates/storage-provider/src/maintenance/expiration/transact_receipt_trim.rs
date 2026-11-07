//! Scheduled reclamation of expired transact-write idempotency receipts.
//!
//! A request-window receipt is dead once its ten-minute idempotency window
//! closes: replay already treats it as absent. Scheduled-job effect receipts
//! outlive the window and are never trimmed here.

use std::{num::NonZeroU32, sync::Arc, time::Duration};

use scheduled_bg_jobs::{ExecutionContext, HandlerFuture, JobHandler, JobOutcome, Timestamp};
use storage_types::{StorageError, StorageResult, TimestampMillis};

use super::{ExpirationPort, ExpirationRepository};
use crate::transact_idempotency::transact_receipt_is_trimmable;

/// One stored receipt, addressed by its token and guarded by its exact bytes.
pub struct TransactReceiptRow {
    pub token: String,
    pub receipt: Vec<u8>,
}

/// Delete the expired receipts of one ordered page and return the token to
/// resume after, or `None` once the table is exhausted.
///
/// The backend deletes a row only while its bytes are unchanged, so a receipt
/// rewritten after the scan survives.
pub async fn trim_transact_receipts_page<P: ExpirationPort>(
    port: &mut P,
    after: Option<&str>,
    limit: NonZeroU32,
    at: TimestampMillis,
) -> StorageResult<Option<String>> {
    let rows = port.scan_transact_receipts(after, limit).await?;
    let next = if rows.len() >= limit.get() as usize {
        rows.last().map(|row| row.token.clone())
    } else {
        None
    };
    let mut expired = Vec::new();
    for row in rows {
        if transact_receipt_is_trimmable(&row.receipt, at)? {
            expired.push(row);
        }
    }
    if !expired.is_empty() {
        port.delete_transact_receipts(&expired).await?;
    }
    Ok(next)
}

const TRIM_PAGE_LIMIT: NonZeroU32 = NonZeroU32::MIN.saturating_add(99);

pub struct TransactReceiptTrimHandler {
    repository: Arc<dyn ExpirationRepository>,
    interval: Duration,
}

impl TransactReceiptTrimHandler {
    pub fn try_new(
        repository: Arc<dyn ExpirationRepository>,
        interval: Duration,
    ) -> StorageResult<Self> {
        if interval.is_zero() {
            return Err(StorageError::validation(
                "transact receipt trim interval must not be zero",
            ));
        }
        Ok(Self {
            repository,
            interval,
        })
    }

    fn outcome(
        next: Option<String>,
        interval: Duration,
        now: Timestamp,
    ) -> StorageResult<JobOutcome> {
        let delay = if next.is_some() {
            Duration::ZERO
        } else {
            interval
        };
        let not_before = now
            .checked_add(delay)
            .map_err(|_| StorageError::internal("transact receipt trim cadence overflow"))?;
        Ok(JobOutcome::Continue {
            not_before,
            checkpoint: next.unwrap_or_default().into_bytes().into(),
        })
    }
}

impl JobHandler for TransactReceiptTrimHandler {
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
                let after = context
                    .checkpoint()
                    .filter(|bytes| !bytes.is_empty())
                    .map(|bytes| String::from_utf8(bytes.to_vec()))
                    .transpose()?;
                let next = self
                    .repository
                    .trim_transact_receipts_page(after, TRIM_PAGE_LIMIT)
                    .await?;
                Ok(Self::outcome(next, self.interval, context.wall_now())?)
            },
            now,
            self.interval,
            checkpoint,
        )
    }
}
