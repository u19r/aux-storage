use std::{
    num::NonZeroU32,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use regional_replication::message::{
    FinishPair, PairTable, RegionalRejection, RegionalRequest, RegionalResponse, ScanPage,
};
use scheduled_bg_jobs::{
    ClaimLeasePolicy, ExecutionContext, HandlerFuture, JobHandler, JobOutcome, OutcomeCode,
};
use storage_provider::{LineageCursor, ScanTurn, SendLimits, SendTurn, SweepTurn};
use storage_types::{PairState, StorageError, StorageResult};

use super::{PairJobCheckpoint, PairJobPayload, RegionalPair};
use crate::constants::{REGIONAL_BACKOFF_CAP, REGIONAL_WARN_EVERY};

/// The pair-job handler of one connection; shares the connection's
/// [`RegionalPair`] with its peer-facing service.
pub(crate) struct RegionalPairHandler {
    pub(crate) pair: Arc<RegionalPair>,
}

pub(crate) enum TurnEnd {
    /// Run again after this delay.
    Again(Duration),
    /// The pair ended here: the job completes.
    Done,
}

impl TurnEnd {
    fn sooner(self, phase: Option<Duration>) -> Self {
        match (self, phase) {
            (Self::Again(send), Some(phase)) => Self::Again(send.min(phase)),
            (end, _) => end,
        }
    }
}

impl RegionalPairHandler {
    pub(crate) fn new(pair: &Arc<RegionalPair>) -> Self {
        Self {
            pair: Arc::clone(pair),
        }
    }

    pub(crate) fn lease(&self) -> Duration {
        self.pair.lease()
    }

    /// `send_interval`, doubling per consecutive failure, capped. Peer and
    /// local failures are counted apart; each count uses this curve.
    fn backoff(&self, failures: u32) -> Duration {
        let doublings = failures.saturating_sub(1).min(16);
        self.pair
            .config
            .send_interval
            .saturating_mul(1 << doublings)
            .min(REGIONAL_BACKOFF_CAP.max(self.pair.config.send_interval))
    }

    /// Counts one failed peer call. Returns the retry delay and, when this
    /// failure is to be logged (the first and every `REGIONAL_WARN_EVERY`-th),
    /// the failure count.
    fn peer_failed(&self, checkpoint: &mut PairJobCheckpoint) -> (Duration, Option<u32>) {
        checkpoint.peer_failures = checkpoint.peer_failures.saturating_add(1);
        let failures = checkpoint.peer_failures;
        (
            self.backoff(failures),
            (failures == 1 || failures.is_multiple_of(REGIONAL_WARN_EVERY)).then_some(failures),
        )
    }

    /// The end of one turn. Every `Err` of a turn is local: peer failures are
    /// answered inside the turn (`peer_failed`). A local failure never changes
    /// `checkpoint.peer_failures`; it has its own per-connection count, backoff
    /// and log line. A successful turn resets the local count.
    pub(crate) fn settle_turn(
        &self,
        job: &PairJobPayload,
        turn: StorageResult<TurnEnd>,
    ) -> TurnEnd {
        match turn {
            Ok(end) => {
                self.pair.local_failures.store(0, Ordering::Relaxed);
                end
            }
            Err(error) => TurnEnd::Again(self.local_failed(job, &error)),
        }
    }

    /// Counts one failed local turn on this connection and returns its retry
    /// delay. Logs the first and every `REGIONAL_WARN_EVERY`-th; a turn handles
    /// item data, so the line prints the attempt failure, or the class and the
    /// variant name, never the error itself.
    fn local_failed(&self, job: &PairJobPayload, error: &StorageError) -> Duration {
        let failures = self
            .pair
            .local_failures
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        if failures == 1 || failures.is_multiple_of(REGIONAL_WARN_EVERY) {
            let class = error.class().as_str();
            match error.attempt_failure() {
                Some(failure) => {
                    tracing::warn!(class, %failure, table = %job.table, failures, "regional pair turn failed locally");
                }
                None => {
                    tracing::warn!(class, kind = error.label(), table = %job.table, failures, "regional pair turn failed locally");
                }
            }
        }
        self.backoff(failures)
    }

    fn limits(&self) -> StorageResult<SendLimits> {
        Ok(SendLimits {
            entries: NonZeroU32::new(
                u32::try_from(self.pair.config.page_entry_limit).unwrap_or(u32::MAX),
            )
            .ok_or_else(|| StorageError::internal("page entry limit is zero"))?,
            bytes: self.pair.config.page_byte_limit,
        })
    }

    pub(crate) fn observe_lag(
        &self,
        job: &PairJobPayload,
        checkpoint: &mut PairJobCheckpoint,
        lag_millis: u64,
    ) {
        self.pair.lag.observe(job.pair, lag_millis);
        let alerting = u128::from(lag_millis) > self.pair.config.lag_alert.as_millis();
        if alerting && !checkpoint.lag_alerting {
            tracing::warn!(event = "regional.lag.alert", table = %job.table, lag_ms = lag_millis, "regional replication lag crossed the alert threshold");
        } else if !alerting && checkpoint.lag_alerting {
            tracing::info!(event = "regional.lag.recovered", table = %job.table, lag_ms = lag_millis, "regional replication lag recovered");
        }
        checkpoint.lag_alerting = alerting;
    }

    /// One send step. REG-10/REG-11 call this from their phase dispatch.
    pub(crate) async fn send_turn(
        &self,
        job: &PairJobPayload,
        checkpoint: &mut PairJobCheckpoint,
    ) -> StorageResult<TurnEnd> {
        let turn = self
            .pair
            .ops
            .next_send_page(&job.table, job.pair, checkpoint.next_lane, self.limits()?)
            .await?;
        let (page, next_lane, caught_up) = match turn {
            SendTurn::PairGone => return Ok(TurnEnd::Done),
            SendTurn::Idle => {
                self.observe_lag(job, checkpoint, 0);
                return Ok(TurnEnd::Again(self.pair.config.send_interval));
            }
            SendTurn::Page {
                page,
                next_lane,
                lag_millis,
                caught_up,
            } => {
                // Before the network call, so an outage is visible.
                self.observe_lag(job, checkpoint, lag_millis);
                (page, next_lane, caught_up)
            }
        };
        let lineage = page.lineage.clone();
        let idle = caught_up && page.entries.is_empty();
        let response = match self.pair.client.call(RegionalRequest::Page(*page)).await {
            Ok(response) => response,
            Err(error) => {
                let (delay, log) = self.peer_failed(checkpoint);
                if let Some(failures) = log {
                    tracing::warn!(%error, table = %job.table, failures, "regional page delivery failed");
                }
                return Ok(TurnEnd::Again(delay));
            }
        };
        if matches!(
            response,
            RegionalResponse::Rejected(RegionalRejection::Unavailable)
        ) {
            let (delay, log) = self.peer_failed(checkpoint);
            if let Some(failures) = log {
                tracing::warn!(table = %job.table, response = ?response, failures, "regional page rejected");
            }
            return Ok(TurnEnd::Again(delay));
        }
        checkpoint.peer_failures = 0;
        match response {
            RegionalResponse::Ack(ack) => {
                let cursor = LineageCursor {
                    through: ack.through,
                    sealed: ack.sealed,
                };
                self.pair
                    .ops
                    .store_send_progress(&job.table, job.pair, lineage, cursor, false)
                    .await?;
                checkpoint.next_lane = next_lane;
                Ok(TurnEnd::Again(if idle {
                    self.pair.config.send_interval
                } else {
                    Duration::ZERO
                }))
            }
            RegionalResponse::Gap { expected } => {
                tracing::warn!(table = %job.table, "regional peer reported a gap; resending from its prefix");
                let cursor = LineageCursor {
                    through: expected,
                    sealed: false,
                };
                self.pair
                    .ops
                    .store_send_progress(&job.table, job.pair, lineage, cursor, true)
                    .await?;
                Ok(TurnEnd::Again(Duration::ZERO))
            }
            other => {
                tracing::warn!(table = %job.table, response = ?other, "regional page rejected");
                Ok(TurnEnd::Again(self.pair.config.send_interval))
            }
        }
    }
}

impl RegionalPairHandler {
    fn page_limit(&self) -> StorageResult<NonZeroU32> {
        Ok(self.limits()?.entries)
    }

    pub(crate) async fn turn(
        &self,
        job: &PairJobPayload,
        checkpoint: &mut PairJobCheckpoint,
    ) -> StorageResult<TurnEnd> {
        let Some(binding) = self.pair.ops.regional_binding(&job.table, job.pair).await? else {
            return Ok(TurnEnd::Done);
        };
        let phase = match binding.state {
            PairState::Seeding => {
                let phase = self.seed_turn(job, checkpoint).await?;
                if !checkpoint.prepared {
                    // Pages before the target exists would only be refused.
                    return Ok(TurnEnd::Again(
                        phase.unwrap_or(self.pair.config.send_interval),
                    ));
                }
                phase
            }
            PairState::Active if !checkpoint.swept => self.sweep_turn(job, checkpoint).await?,
            PairState::Leaving => {
                let send = self.send_turn(job, checkpoint).await?;
                if matches!(send, TurnEnd::Done) {
                    return Ok(TurnEnd::Done);
                }
                return Ok(match (send, self.leave_turn(job, checkpoint).await?) {
                    (TurnEnd::Again(send), TurnEnd::Again(leave)) => {
                        TurnEnd::Again(send.min(leave))
                    }
                    (_, end) => end,
                });
            }
            _ => None, // Joining waits for Activate
        };
        Ok(self.send_turn(job, checkpoint).await?.sooner(phase))
    }

    /// A scan, activate or finish call that was not accepted. Transport failure
    /// and `Unavailable` back off and count; any other response retries at
    /// `send_interval` and ends the failure run.
    fn peer_unaccepted(
        &self,
        job: &PairJobPayload,
        checkpoint: &mut PairJobCheckpoint,
        outcome: Result<RegionalResponse, impl std::fmt::Debug>,
        message: &'static str,
    ) -> Option<Duration> {
        if matches!(
            outcome,
            Err(_) | Ok(RegionalResponse::Rejected(RegionalRejection::Unavailable))
        ) {
            let (delay, log) = self.peer_failed(checkpoint);
            if let Some(failures) = log {
                tracing::warn!(table = %job.table, response = ?outcome.as_ref().ok(), failures, "{message}");
            }
            return Some(delay);
        }
        checkpoint.peer_failures = 0;
        tracing::warn!(table = %job.table, response = ?outcome.as_ref().ok(), "{message}");
        Some(self.pair.config.send_interval)
    }

    /// Source bootstrap: Prepare (idempotent), scan pages, Activate.
    /// `Some(delay)` = phase work remains.
    async fn seed_turn(
        &self,
        job: &PairJobPayload,
        checkpoint: &mut PairJobCheckpoint,
    ) -> StorageResult<Option<Duration>> {
        if !checkpoint.prepared {
            let Some(request) = self.pair.ops.prepare_request(&job.table, job.pair).await? else {
                return Ok(None);
            };
            return Ok(
                match self
                    .pair
                    .client
                    .call(RegionalRequest::Prepare(request))
                    .await
                {
                    Ok(RegionalResponse::Prepared) => {
                        checkpoint.prepared = true;
                        checkpoint.peer_failures = 0;
                        Some(Duration::ZERO)
                    }
                    other => {
                        let (delay, log) = self.peer_failed(checkpoint);
                        if let Some(failures) = log {
                            tracing::warn!(table = %job.table, response = ?other, failures, "regional prepare not accepted");
                        }
                        Some(delay)
                    }
                },
            );
        }
        if !checkpoint.scan_done {
            let (items, next) = match self
                .pair
                .ops
                .next_scan_page(
                    &job.table,
                    job.pair,
                    checkpoint.scan_after.clone(),
                    self.page_limit()?,
                )
                .await?
            {
                ScanTurn::Stopped => return Ok(None),
                ScanTurn::Page { items, next } => (items, next),
            };
            if !items.is_empty() {
                let page = ScanPage {
                    pair: job.pair,
                    table: job.table.clone(),
                    items,
                };
                match self.pair.client.call(RegionalRequest::Scan(page)).await {
                    Ok(RegionalResponse::ScanApplied) => checkpoint.peer_failures = 0,
                    other => {
                        return Ok(self.peer_unaccepted(
                            job,
                            checkpoint,
                            other,
                            "regional scan page not applied",
                        ));
                    }
                }
            }
            checkpoint.scan_done = next.is_none();
            checkpoint.scan_after = next;
            return Ok(Some(Duration::ZERO));
        }
        let target = PairTable {
            pair: job.pair,
            table: job.table.clone(),
        };
        match self
            .pair
            .client
            .call(RegionalRequest::Activate(target))
            .await
        {
            Ok(RegionalResponse::Activated) => {
                checkpoint.peer_failures = 0;
                self.pair
                    .ops
                    .advance_pair_state(&job.table, job.pair, PairState::Seeding, PairState::Active)
                    .await?;
                Ok(Some(Duration::ZERO))
            }
            other => {
                Ok(self.peer_unaccepted(job, checkpoint, other, "regional activate not accepted"))
            }
        }
    }

    /// Leaving region: ask the peer to finish once it has everything up to the
    /// frozen heads.
    async fn leave_turn(
        &self,
        job: &PairJobPayload,
        checkpoint: &mut PairJobCheckpoint,
    ) -> StorageResult<TurnEnd> {
        if checkpoint.leave_last.is_none() {
            // Read in Leaving: no customer or TTL origin entry can follow these
            // heads.
            let Some(heads) = self.pair.ops.history_heads(&job.table, job.pair).await? else {
                return Ok(TurnEnd::Done);
            };
            checkpoint.leave_last = Some(heads);
        }
        let last = checkpoint.leave_last.clone().unwrap_or_default();
        let request = FinishPair {
            pair: job.pair,
            table: job.table.clone(),
            last,
        };
        match self
            .pair
            .client
            .call(RegionalRequest::Finish(request))
            .await
        {
            Ok(
                RegionalResponse::Finished
                | RegionalResponse::Rejected(RegionalRejection::UnknownPair),
            ) => {
                checkpoint.peer_failures = 0;
                self.pair
                    .ops
                    .force_unpair(&job.table, Some(job.pair))
                    .await?;
                Ok(TurnEnd::Done)
            }
            Ok(RegionalResponse::Pending) => {
                checkpoint.peer_failures = 0;
                Ok(TurnEnd::Again(self.pair.config.send_interval))
            }
            other => Ok(TurnEnd::Again(
                self.peer_unaccepted(job, checkpoint, other, "regional finish not accepted")
                    .unwrap_or(self.pair.config.send_interval),
            )),
        }
    }

    async fn sweep_turn(
        &self,
        job: &PairJobPayload,
        checkpoint: &mut PairJobCheckpoint,
    ) -> StorageResult<Option<Duration>> {
        match self
            .pair
            .ops
            .sweep_received_markers(
                &job.table,
                job.pair,
                checkpoint.sweep_after.clone(),
                self.page_limit()?,
            )
            .await?
        {
            SweepTurn::Stopped => Ok(None),
            SweepTurn::Page { next } => {
                checkpoint.swept = next.is_none();
                checkpoint.sweep_after = next;
                Ok(Some(Duration::ZERO))
            }
        }
    }
}

impl JobHandler for RegionalPairHandler {
    fn claim_lease_policy(&self) -> ClaimLeasePolicy {
        ClaimLeasePolicy::DriveBoundedNative
    }

    fn execute<'a>(
        &'a self,
        context: ExecutionContext<'a>,
        payload: &'a [u8],
    ) -> HandlerFuture<'a> {
        Box::pin(async move {
            if context.is_cancelled() {
                return Ok(JobOutcome::Cancelled);
            }
            let job: PairJobPayload = serde_json::from_slice(payload)?;
            let mut checkpoint: PairJobCheckpoint = context
                .checkpoint()
                .filter(|bytes| !bytes.is_empty())
                .map(serde_json::from_slice)
                .transpose()?
                .unwrap_or_default();
            let turn = self.turn(&job, &mut checkpoint).await;
            let end = self.settle_turn(&job, turn);
            let wall_now = context.wall_now();
            match end {
                TurnEnd::Done => {
                    self.pair.lag.end(job.pair);
                    Ok(JobOutcome::Complete(OutcomeCode::try_new("pair-ended")?))
                }
                TurnEnd::Again(delay) => Ok(JobOutcome::Continue {
                    not_before: wall_now
                        .checked_add(delay)
                        .map_err(|_| StorageError::internal("regional cadence overflow"))?,
                    checkpoint: serde_json::to_vec(&checkpoint)?.into(),
                }),
            }
        })
    }
}
