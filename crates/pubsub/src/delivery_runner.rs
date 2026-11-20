//! The SNS delivery loop of one node (W3S plan 5.7; W3 design 4.1 to 4.5).
//!
//! Two tasks. The materializer turns due publish intents into deliveries and
//! sends the destinations it wrote to the deliverer. The deliverer feeds the
//! pure scheduling core with those destinations, sweep pages, claim results
//! and task completions, claims only what the core asks, and attempts every
//! claimed record at once. Nothing here is a safety decision; the fence in
//! the store owns that, so many nodes can run this loop on one database.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use pubsub_provider::{
    ClaimTerms, DeliveryClaimRequest, DestinationAsk, DestinationClaim, DestinationId,
    DestinationKey, PubsubError, PubsubResult, SettleOutcome,
};
use storage_types::TimestampMillis;
use tokio::{
    sync::{Notify, mpsc, watch},
    task::{Id, JoinHandle, JoinSet},
};

use crate::{
    delivery_config::{DeliveryRunnerConfig, SETTLE_MARGIN},
    delivery_scheduler::{BreakerSignal, Completed, DeliveryScheduler, SchedulerConfig},
    delivery_transport::{DeliveryPass, LocalBatchPass},
    manager::PubsubManager,
};

/// Destinations with records written by this node's materializer.
type Written = Vec<(DestinationKey, TimestampMillis)>;

/// Wakes the materializer of this process after a publish commits (design
/// 4.4). Other nodes find the work at their next probe or sweep.
#[derive(Clone, Default)]
pub struct DeliveryWake(Arc<Notify>);

impl DeliveryWake {
    /// Store one wake; a runner that is not waiting takes it at its next wait.
    pub fn notify(&self) {
        self.0.notify_one();
    }

    async fn notified(&self) {
        self.0.notified().await;
    }
}

impl std::fmt::Debug for DeliveryWake {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("DeliveryWake")
    }
}

/// Counters and gauges of one runner. The same values go to the metrics
/// recorder with fixed-cardinality labels (no ARN, URL, or payload).
#[derive(Debug, Default)]
pub struct DeliveryRunnerStats {
    pub oldest_due_age_ms: AtomicI64,
    pub oldest_intent_age_ms: AtomicI64,
    /// Leased records of this node.
    pub in_flight: AtomicUsize,
    pub peak_in_flight: AtomicUsize,
    pub sweeps: AtomicU64,
    pub materialized_chunks: AtomicU64,
    pub claims: AtomicU64,
    pub claimed_records: AtomicU64,
    pub claim_empty: AtomicU64,
    pub delivered: AtomicU64,
    pub retried: AtomicU64,
    pub dead_lettered: AtomicU64,
    pub dropped: AtomicU64,
    pub claim_lost: AtomicU64,
    pub lease_too_short: AtomicU64,
    pub errors: AtomicU64,
    pub breaker_open: AtomicUsize,
}

impl DeliveryRunnerStats {
    fn count(counter: &AtomicU64, amount: usize) {
        counter.fetch_add(amount as u64, Ordering::Relaxed);
    }
}

pub struct DeliveryRunner {
    stop: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
    stats: Arc<DeliveryRunnerStats>,
}

impl DeliveryRunner {
    /// Start both tasks. The settings come from a configuration that
    /// `config::PubsubConfig::validate` accepted (finding 9).
    pub fn start(manager: Arc<PubsubManager>, config: DeliveryRunnerConfig) -> PubsubResult<Self> {
        let (stop, receiver) = watch::channel(false);
        let stats = Arc::new(DeliveryRunnerStats::default());
        // A full channel drops the hint; the sweep finds the work.
        let (written, materialized) = mpsc::channel(256);
        let materializer = tokio::spawn(run_materializer(
            Arc::clone(&manager),
            config,
            written,
            Arc::clone(&stats),
            receiver.clone(),
        ));
        let start = DestinationId::from_bytes(*storage_types::ambient::uuid_v4().as_bytes());
        let deliverer = Deliverer {
            scheduler: DeliveryScheduler::new(SchedulerConfig::from(&config), Some(start)),
            manager,
            config,
            in_flight: JoinSet::new(),
            tasks: HashMap::new(),
            stats: Arc::clone(&stats),
        };
        let deliverer = tokio::spawn(deliverer.run(materialized, receiver));
        Ok(Self {
            stop,
            tasks: vec![materializer, deliverer],
            stats,
        })
    }

    #[must_use]
    pub fn stats(&self) -> Arc<DeliveryRunnerStats> {
        Arc::clone(&self.stats)
    }

    /// Stop claiming, wait for in-flight deliveries (bounded by the request
    /// timeout and the settle margin), and leave the rest to lease expiry.
    pub async fn shutdown(mut self) -> PubsubResult<()> {
        self.stop.send_replace(true);
        let mut failed = false;
        for task in std::mem::take(&mut self.tasks) {
            failed |= task.await.is_err();
        }
        if failed {
            return Err(PubsubError::storage(
                "pubsub delivery runner failed during drainage",
            ));
        }
        Ok(())
    }
}

impl Drop for DeliveryRunner {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}

async fn run_materializer(
    manager: Arc<PubsubManager>,
    config: DeliveryRunnerConfig,
    written: mpsc::Sender<Written>,
    stats: Arc<DeliveryRunnerStats>,
    mut stop: watch::Receiver<bool>,
) {
    let wake = manager.delivery_wake();
    while !*stop.borrow() {
        let wait = match materialize_step(&manager, &config, &stats).await {
            Ok(MaterializeStep::Wrote(destinations)) => {
                let _ = written.try_send(destinations);
                continue;
            }
            Ok(MaterializeStep::Idle(wait)) => wait,
            Err(_) => {
                tracing::warn!("pubsub materializer failed; retrying");
                config.error_backoff
            }
        };
        tokio::select! {
            _ = stop.changed() => return,
            () = wake.notified() => {}
            () = tokio::time::sleep(wait) => {}
        }
    }
}

enum MaterializeStep {
    Wrote(Written),
    Idle(Duration),
}

/// Probe with one read; claim and commit chunks only when an intent is due.
/// The materializer continues while chunks commit, also when the filter
/// excluded every subscription of a chunk (review D3).
async fn materialize_step(
    manager: &PubsubManager,
    config: &DeliveryRunnerConfig,
    stats: &DeliveryRunnerStats,
) -> PubsubResult<MaterializeStep> {
    let due = manager.provider.next_publish_due().await?;
    let now = manager.provider.now();
    let age = overdue_millis(due, now);
    stats.oldest_intent_age_ms.store(age, Ordering::Relaxed);
    metrics::gauge!("pubsub.publish.oldest_intent_age_ms").set(age as f64);
    if !due.is_some_and(|due| due <= now) {
        return Ok(MaterializeStep::Idle(until_due(
            due,
            now,
            config.idle_probe,
        )));
    }
    let report = manager
        .provider
        .materialize_publish_intents(config.materialize_batch.get())
        .await?;
    DeliveryRunnerStats::count(&stats.materialized_chunks, report.chunks);
    if let Some(error) = report.first_error {
        tracing::warn!(
            failed_chunks = report.failed_chunks,
            error_type = error.aws_query_error_type(),
            "pubsub chunks failed to commit; their leases return them"
        );
    }
    if report.chunks > 0 {
        Ok(MaterializeStep::Wrote(report.written))
    } else {
        // Another node holds the intents: probe again later.
        Ok(MaterializeStep::Idle(config.idle_probe))
    }
}

/// What one task did. The task map keeps its destination and slots, so a
/// task that panics still returns them (finding 8).
enum Completion {
    Attempt(PubsubResult<DeliveryPass>),
    /// One batch of records for one queue of this store (review E1).
    Local(PubsubResult<LocalBatchPass>),
}

/// The slots of one running task.
struct TaskSlots {
    destination: DestinationKey,
    records: usize,
    lease_until: TimestampMillis,
}

struct Deliverer {
    manager: Arc<PubsubManager>,
    config: DeliveryRunnerConfig,
    scheduler: DeliveryScheduler,
    in_flight: JoinSet<Completion>,
    tasks: HashMap<Id, TaskSlots>,
    stats: Arc<DeliveryRunnerStats>,
}

impl Deliverer {
    async fn run(
        mut self,
        mut materialized: mpsc::Receiver<Written>,
        mut stop: watch::Receiver<bool>,
    ) {
        while !*stop.borrow() {
            while let Some(done) = self.in_flight.try_join_next_with_id() {
                self.complete(done);
            }
            while let Ok(written) = materialized.try_recv() {
                let now = self.manager.provider.now();
                self.scheduler.on_materialized(&written, now);
            }
            self.sweep().await;
            let now = self.manager.provider.now();
            let rotation = storage_types::ambient::random_u64() as u32;
            if let Some(asks) = self.scheduler.next_claim(now, rotation) {
                self.claim(asks, now).await;
                continue;
            }
            self.record_gauges(now);
            let wait = self
                .scheduler
                .next_wake(now)
                .map_or(self.config.idle_probe, |wake| until(wake, now));
            tokio::select! {
                _ = stop.changed() => break,
                Some(written) = materialized.recv() => {
                    let now = self.manager.provider.now();
                    self.scheduler.on_materialized(&written, now);
                }
                Some(done) = self.in_flight.join_next_with_id(), if !self.in_flight.is_empty() => {
                    self.complete(done);
                }
                () = tokio::time::sleep(wait) => {}
            }
        }
        self.drain().await;
    }

    /// One sweep page when the core asks for it; a failure waits one
    /// interval.
    async fn sweep(&mut self) {
        let now = self.manager.provider.now();
        let Some((cursor, limit)) = self.scheduler.sweep_request(now) else {
            return;
        };
        DeliveryRunnerStats::count(&self.stats.sweeps, 1);
        match self
            .manager
            .provider
            .sweep_destination_heads(cursor, limit)
            .await
        {
            Ok(page) => self.scheduler.on_sweep(&page, now),
            Err(_) => {
                tracing::warn!("pubsub delivery sweep failed; retrying");
                self.scheduler.on_sweep_failed(now);
            }
        }
    }

    /// One claim of the asked destinations; every claimed record starts at
    /// once. A failed claim keeps the destinations' head times (finding 4).
    async fn claim(&mut self, asks: Vec<DestinationAsk>, now: TimestampMillis) {
        DeliveryRunnerStats::count(&self.stats.claims, 1);
        let config = self.manager.delivery_config();
        let request = DeliveryClaimRequest {
            terms: ClaimTerms {
                owner: config.worker_id.clone(),
                now,
                lease_until: config.timing.lease_deadline(now),
            },
            asks: asks.clone(),
        };
        let claims = match self.manager.provider.claim_deliveries(request).await {
            // No claim at all: every conflict retry was lost.
            Ok(claims) if claims.is_empty() => {
                self.scheduler.on_claim_failed(&asks, now);
                Vec::new()
            }
            Ok(claims) => {
                self.scheduler.on_claimed(&claims, now);
                claims
            }
            Err(_) => {
                tracing::warn!("pubsub delivery claim failed; retrying");
                DeliveryRunnerStats::count(&self.stats.errors, 1);
                self.scheduler.on_claim_failed(&asks, now);
                Vec::new()
            }
        };
        let taken: usize = claims.iter().map(|claim| claim.claimed.len()).sum();
        DeliveryRunnerStats::count(&self.stats.claimed_records, taken);
        if taken == 0 {
            DeliveryRunnerStats::count(&self.stats.claim_empty, 1);
        }
        for claim in claims {
            self.spawn_claim(claim);
        }
        self.record_in_flight();
    }

    /// Local records of one queue go in one batch task (one slot per
    /// record); other records go in one task each.
    fn spawn_claim(&mut self, claim: DestinationClaim) {
        let Some(lease_until) = claim
            .claimed
            .iter()
            .filter_map(|claimed| claimed.state.lease.as_ref().map(|lease| lease.until))
            .max()
        else {
            return;
        };
        let (batches, others) = self.manager.split_local(claim.claimed);
        for batch in batches {
            let records = batch.items.len();
            let manager = Arc::clone(&self.manager);
            let handle = self.in_flight.spawn(async move {
                Completion::Local(manager.deliver_local_batch(batch.queue, batch.items).await)
            });
            self.track(handle.id(), &claim.destination, records, lease_until);
        }
        for claimed in others {
            let manager = Arc::clone(&self.manager);
            let handle = self.in_flight.spawn(async move {
                Completion::Attempt(manager.deliver_and_settle(claimed).await)
            });
            self.track(handle.id(), &claim.destination, 1, lease_until);
        }
    }

    fn track(
        &mut self,
        id: Id,
        destination: &DestinationKey,
        records: usize,
        lease_until: TimestampMillis,
    ) {
        self.tasks.insert(
            id,
            TaskSlots {
                destination: destination.clone(),
                records,
                lease_until,
            },
        );
    }

    fn complete(&mut self, done: Result<(Id, Completion), tokio::task::JoinError>) {
        let now = self.manager.provider.now();
        let id = match &done {
            Ok((id, _)) => *id,
            Err(error) => error.id(),
        };
        let Some(slots) = self.tasks.remove(&id) else {
            return;
        };
        let mut completed = Completed {
            destination: slots.destination,
            records: slots.records,
            signal: BreakerSignal::NotAttempted,
            retry_due: None,
            unsettled_until: None,
            jitter: storage_types::ambient::uuid_now_v7().as_u128() as u32,
        };
        match done {
            Ok((_, Completion::Attempt(pass))) => {
                if let Ok(DeliveryPass::Settled { class, report }) = &pass {
                    completed.signal = *class;
                    completed.retry_due = report.retry_due;
                } else {
                    completed.unsettled_until = Some(slots.lease_until);
                }
                self.record_pass(pass);
            }
            Ok((_, Completion::Local(Ok(pass)))) => {
                DeliveryRunnerStats::count(&self.stats.lease_too_short, pass.lease_too_short);
                if pass.lease_too_short > 0 {
                    completed.unsettled_until = Some(slots.lease_until);
                }
                for report in pass.reports {
                    completed.retry_due = completed.retry_due.or(report.retry_due);
                    self.record_pass(Ok(DeliveryPass::Settled {
                        class: BreakerSignal::NotAttempted,
                        report,
                    }));
                }
            }
            Ok((_, Completion::Local(Err(_)))) => {
                completed.unsettled_until = Some(slots.lease_until);
                DeliveryRunnerStats::count(&self.stats.errors, slots.records);
                tracing::warn!("pubsub local batch failed; the leases return the records");
            }
            Err(_) => {
                completed.unsettled_until = Some(slots.lease_until);
                DeliveryRunnerStats::count(&self.stats.errors, slots.records);
                tracing::error!("pubsub delivery task stopped");
            }
        }
        self.scheduler.on_completed(completed, now);
        self.record_in_flight();
    }

    fn record_pass(&self, pass: PubsubResult<DeliveryPass>) {
        let (counter, outcome, reason) = match pass {
            Ok(DeliveryPass::Settled { report, .. }) => {
                let reason = report.failure.map_or("none", |failure| failure.label());
                match report.outcome {
                    SettleOutcome::Delivered => (&self.stats.delivered, "delivered", reason),
                    SettleOutcome::Retried => (&self.stats.retried, "retry", reason),
                    SettleOutcome::DeadLettered => {
                        (&self.stats.dead_lettered, "dead_lettered", reason)
                    }
                    SettleOutcome::Dropped(drop) => (&self.stats.dropped, "dropped", drop.label()),
                    SettleOutcome::ClaimLost => (&self.stats.claim_lost, "claim_lost", reason),
                }
            }
            Ok(DeliveryPass::LeaseTooShort) => {
                (&self.stats.lease_too_short, "lease_too_short", "none")
            }
            Err(_) => {
                tracing::warn!("pubsub delivery settle failed; the lease returns the record");
                (&self.stats.errors, "error", "none")
            }
        };
        DeliveryRunnerStats::count(counter, 1);
        metrics::counter!("pubsub.delivery.outcome", "outcome" => outcome, "reason" => reason)
            .increment(1);
    }

    fn record_in_flight(&self) {
        let leased = self.scheduler.leased();
        self.stats.in_flight.store(leased, Ordering::Relaxed);
        self.stats
            .peak_in_flight
            .fetch_max(leased, Ordering::Relaxed);
        metrics::gauge!("pubsub.delivery.in_flight").set(leased as f64);
    }

    fn record_gauges(&self, now: TimestampMillis) {
        let age = i64::try_from(self.scheduler.oldest_due_age(now).as_millis()).unwrap_or(i64::MAX);
        self.stats.oldest_due_age_ms.store(age, Ordering::Relaxed);
        metrics::gauge!("pubsub.delivery.oldest_due_age_ms").set(age as f64);
        let open = self.scheduler.open_breakers();
        self.stats.breaker_open.store(open, Ordering::Relaxed);
        metrics::gauge!("pubsub.delivery.breaker_open").set(open as f64);
    }

    /// Wait for in-flight deliveries for at most one request and its settle;
    /// the rest end with the task set and their leases return the records.
    async fn drain(&mut self) {
        let bound = self
            .manager
            .delivery_config()
            .timing
            .request_timeout()
            .saturating_add(SETTLE_MARGIN);
        let drained = tokio::time::timeout(bound, async {
            while let Some(done) = self.in_flight.join_next_with_id().await {
                self.complete(done);
            }
        })
        .await;
        if drained.is_err() {
            tracing::warn!("pubsub delivery shutdown left deliveries to lease expiry");
            self.in_flight.abort_all();
        }
    }
}

/// `now - due` in milliseconds when `due` is in the past, else 0.
fn overdue_millis(due: Option<TimestampMillis>, now: TimestampMillis) -> i64 {
    due.map_or(0, |due| {
        now.timestamp_millis()
            .saturating_sub(due.timestamp_millis())
            .max(0)
    })
}

/// The wait until `due`, at most `idle_probe`.
fn until_due(due: Option<TimestampMillis>, now: TimestampMillis, idle_probe: Duration) -> Duration {
    due.map_or(idle_probe, |due| until(due, now).min(idle_probe))
}

/// The wait until `wake`, at least 1 ms.
fn until(wake: TimestampMillis, now: TimestampMillis) -> Duration {
    let millis = wake
        .timestamp_millis()
        .saturating_sub(now.timestamp_millis())
        .max(1);
    Duration::from_millis(u64::try_from(millis).unwrap_or(u64::MAX))
}
