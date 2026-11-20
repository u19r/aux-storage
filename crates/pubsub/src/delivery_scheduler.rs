//! The scheduling core of one node's delivery runner (W3S plan 5.7). Pure:
//! no I/O and no clock; every method takes `now`. It keeps hints only: the
//! destinations with known work, their timers, the per-destination slots
//! and breaker, and the sweep cursor. A lost or wrong hint changes when a
//! record is attempted, never whether (R6): the sweep reads the real index.
//!
//! A claim asks each chosen destination only for its free slots on this
//! node, so every claimed record is attempted at once: there is no waiting
//! list and no deferral write.

use std::{collections::HashMap, num::NonZeroU32, time::Duration};

use pubsub_provider::{
    AskMode, DestinationAsk, DestinationClaim, DestinationId, DestinationKey, DestinationKind,
    SweepCursor, SweepPage, after,
};
use storage_types::TimestampMillis;

use crate::delivery_config::{BreakerConfig, DeliveryRunnerConfig};

/// The pause of a destination whose ask took nothing: a bounded rate of
/// empty claims while another node holds its head.
const EMPTY_CLAIM_PAUSE: Duration = Duration::from_millis(100);

/// What a completed task said about its destination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerSignal {
    /// The destination answered (delivered, or a permanent rejection).
    Answered,
    /// A transient failure.
    Failed,
    /// No attempt reached the destination (lost claim, short lease, local
    /// store work, a panicked task).
    NotAttempted,
}

/// One task that ended: its destination, the records it held, and what the
/// runner learned.
#[derive(Debug, Clone)]
pub struct Completed {
    pub destination: DestinationKey,
    pub records: usize,
    pub signal: BreakerSignal,
    /// The earliest retry time that a settle wrote.
    pub retry_due: Option<TimestampMillis>,
    /// The lease end of records that did not settle (an error or a panic):
    /// they are due again then.
    pub unsettled_until: Option<TimestampMillis>,
    pub jitter: u32,
}

/// Limits of the core, from the runner configuration.
#[derive(Debug, Clone, Copy)]
pub struct SchedulerConfig {
    /// Leased records on this node.
    pub max_in_flight: usize,
    /// Concurrent HTTP attempts per destination.
    pub per_destination: usize,
    /// Records of one local queue batch.
    pub local_batch: usize,
    /// Records of one claim.
    pub claim_batch: usize,
    pub breaker: BreakerConfig,
    pub sweep_page: NonZeroU32,
    pub sweep_interval: Duration,
}

impl From<&DeliveryRunnerConfig> for SchedulerConfig {
    fn from(config: &DeliveryRunnerConfig) -> Self {
        Self {
            max_in_flight: config.max_in_flight.get(),
            per_destination: config.per_destination.get(),
            local_batch: pubsub_provider::LOCAL_BATCH_MAX_RECORDS,
            claim_batch: config.claim_batch.get() as usize,
            breaker: config.breaker,
            sweep_page: config.sweep_page,
            sweep_interval: config.sweep_interval,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Breaker {
    Closed {
        failures: u32,
    },
    Open {
        until: TimestampMillis,
        interval: Duration,
    },
    HalfOpen {
        interval: Duration,
        probing: bool,
    },
}

#[derive(Debug, Clone)]
struct Destination {
    in_flight: usize,
    breaker: Breaker,
    /// The earliest known due time of a record; `None`: no known work.
    head_due: Option<TimestampMillis>,
    /// The retry deadline of the head, from the sweep.
    head_deadline: Option<TimestampMillis>,
    touched: u64,
}

pub struct DeliveryScheduler {
    config: SchedulerConfig,
    destinations: HashMap<DestinationKey, Destination>,
    tick: u64,
    leased: usize,
    sweep: SweepCursor,
    sweep_due: TimestampMillis,
    /// The latest `now` that a method received; eviction compares breaker
    /// times with it.
    now: TimestampMillis,
}

impl DeliveryScheduler {
    /// `start` spreads the nodes' sweep passes over the destination space.
    pub fn new(config: SchedulerConfig, start: Option<DestinationId>) -> Self {
        Self {
            config,
            destinations: HashMap::new(),
            tick: 0,
            leased: 0,
            sweep: SweepCursor::from_start(start),
            sweep_due: TimestampMillis::from_timestamp(0),
            now: TimestampMillis::from_timestamp(0),
        }
    }

    /// Deliveries that this node's materializer wrote.
    pub fn on_materialized(
        &mut self,
        written: &[(DestinationKey, TimestampMillis)],
        now: TimestampMillis,
    ) {
        self.now = self.now.max(now);
        for (destination, due) in written {
            let entry = self.touch(destination);
            entry.head_due = Some(entry.head_due.map_or(*due, |head| head.min(*due)));
        }
    }

    /// One sweep page: each head is the real oldest record of its partition.
    pub fn on_sweep(&mut self, page: &SweepPage, now: TimestampMillis) {
        self.now = self.now.max(now);
        for head in &page.heads {
            let entry = self.touch(&head.destination);
            entry.head_due = Some(head.head_due);
            entry.head_deadline = Some(head.head_retry_deadline);
        }
        self.sweep = page
            .next
            .clone()
            .unwrap_or_else(|| SweepCursor::from_start(None));
        self.sweep_due = after(now, self.config.sweep_interval);
    }

    /// A failed sweep is tried again at the next interval.
    pub fn on_sweep_failed(&mut self, now: TimestampMillis) {
        self.sweep_due = after(now, self.config.sweep_interval);
    }

    /// The asks of the next claim: destinations with due work in rotated
    /// order, each for its free slots; `None` when nothing can be asked.
    pub fn next_claim(
        &mut self,
        now: TimestampMillis,
        rotation: u32,
    ) -> Option<Vec<DestinationAsk>> {
        self.now = self.now.max(now);
        let mut budget = self
            .config
            .claim_batch
            .min(self.config.max_in_flight.saturating_sub(self.leased));
        let mut due: Vec<DestinationKey> = self
            .destinations
            .iter()
            .filter(|(_, entry)| entry.head_due.is_some_and(|head| head <= now))
            .map(|(key, _)| key.clone())
            .collect();
        due.sort();
        if !due.is_empty() {
            let offset = rotation as usize % due.len();
            due.rotate_left(offset);
        }
        let mut asks = Vec::new();
        for key in due {
            if budget == 0 {
                break;
            }
            if let Some(ask) = self.ask(&key, now, budget) {
                budget -= ask.take.get() as usize;
                asks.push(ask);
            }
        }
        (!asks.is_empty()).then_some(asks)
    }

    /// The ask of one destination: closed breaker, all free slots; open
    /// breaker, only records past their deadline (their final attempt, F6);
    /// a breaker whose open interval ended, one probe.
    fn ask(
        &mut self,
        key: &DestinationKey,
        now: TimestampMillis,
        budget: usize,
    ) -> Option<DestinationAsk> {
        let limit = match key.kind() {
            DestinationKind::LocalQueue => self.config.local_batch,
            DestinationKind::Http | DestinationKind::Confirmation => self.config.per_destination,
        };
        let entry = self.destinations.get_mut(key)?;
        let free = limit.saturating_sub(entry.in_flight).min(budget);
        let is_past_deadline = entry.head_deadline.is_some_and(|deadline| deadline <= now);
        let (mode, take, breaker) = match entry.breaker {
            // A failure in the current breaker window stops refills until the
            // records in flight end (W3S review finding 3): a dead
            // destination gets one claim of its slots per window, not one
            // more record per failed completion.
            Breaker::Closed { failures } if failures > 0 && entry.in_flight > 0 => return None,
            Breaker::Closed { .. } => (AskMode::Due, free, entry.breaker),
            Breaker::Open { until, interval } if until <= now => (
                AskMode::Probe,
                free.min(1),
                Breaker::HalfOpen {
                    interval,
                    probing: true,
                },
            ),
            Breaker::HalfOpen {
                interval,
                probing: false,
            } => (
                AskMode::Probe,
                free.min(1),
                Breaker::HalfOpen {
                    interval,
                    probing: true,
                },
            ),
            Breaker::Open { .. } | Breaker::HalfOpen { probing: true, .. } if is_past_deadline => {
                (AskMode::DeadlineOnly, free, entry.breaker)
            }
            Breaker::Open { .. } | Breaker::HalfOpen { .. } => return None,
        };
        let take = NonZeroU32::new(u32::try_from(take).ok()?)?;
        entry.breaker = breaker;
        Some(DestinationAsk {
            destination: key.clone(),
            take,
            mode,
        })
    }

    /// What a claim took: the records hold slots; `next_due` is the
    /// destination's next known work (a timer when it is in the future).
    /// An ask that took nothing (another node holds the records, or a stale
    /// deadline) is not asked again before `EMPTY_CLAIM_PAUSE`.
    pub fn on_claimed(&mut self, claims: &[DestinationClaim], now: TimestampMillis) {
        self.now = self.now.max(now);
        for claim in claims {
            let taken = claim.claimed.len();
            self.leased += taken;
            let entry = self.touch(&claim.destination);
            entry.in_flight += taken;
            entry.head_due = claim.next_due;
            if taken > 0 {
                continue;
            }
            entry.head_due = claim
                .next_due
                .map(|due| due.max(after(now, EMPTY_CLAIM_PAUSE)));
            entry.head_deadline = None;
            if let Breaker::HalfOpen {
                interval,
                probing: true,
            } = entry.breaker
            {
                entry.breaker = Breaker::HalfOpen {
                    interval,
                    probing: false,
                };
            }
        }
    }

    /// A claim that failed or lost every conflict retry says nothing about
    /// the asked destinations (W3S review finding 4): each keeps its known
    /// head time and is paused for `EMPTY_CLAIM_PAUSE`; a probe may be
    /// asked again.
    pub fn on_claim_failed(&mut self, asks: &[DestinationAsk], now: TimestampMillis) {
        self.now = self.now.max(now);
        let pause = after(now, EMPTY_CLAIM_PAUSE);
        for ask in asks {
            let entry = self.touch(&ask.destination);
            entry.head_due = entry.head_due.map(|head| head.max(pause));
            if let Breaker::HalfOpen {
                interval,
                probing: true,
            } = entry.breaker
            {
                entry.breaker = Breaker::HalfOpen {
                    interval,
                    probing: false,
                };
            }
        }
    }

    /// A task ended: return its slots, learn about the destination, and
    /// keep the retry or lease-end time as a timer.
    pub fn on_completed(&mut self, done: Completed, now: TimestampMillis) {
        self.now = self.now.max(now);
        self.leased = self.leased.saturating_sub(done.records);
        let breaker = self.config.breaker;
        let entry = self.touch(&done.destination);
        entry.in_flight = entry.in_flight.saturating_sub(done.records);
        for due in [done.retry_due, done.unsettled_until].into_iter().flatten() {
            entry.head_due = Some(entry.head_due.map_or(due, |head| head.min(due)));
        }
        entry.breaker = next_breaker(entry.breaker, done.signal, &breaker, now, done.jitter);
    }

    /// The next time the runner must act without an event: a sweep, a
    /// timer, or the end of an open interval of a destination with work.
    pub fn next_wake(&self, now: TimestampMillis) -> Option<TimestampMillis> {
        let timers = self.destinations.values().filter_map(|entry| {
            let head = entry.head_due?;
            match entry.breaker {
                // After `until` the destination is asked for a probe as soon
                // as its head is due (W3S review finding 8).
                Breaker::Open { until, .. } if until > now => {
                    let deadline = entry.head_deadline.filter(|deadline| *deadline > now);
                    Some(deadline.map_or(until, |deadline| deadline.min(until)))
                }
                _ => (head > now).then_some(head),
            }
        });
        timers
            .chain([self.sweep_due])
            .filter(|wake| *wake > now)
            .min()
    }

    /// The page of the sweep when one is due.
    pub fn sweep_request(&self, now: TimestampMillis) -> Option<(SweepCursor, NonZeroU32)> {
        (now >= self.sweep_due).then(|| (self.sweep.clone(), self.config.sweep_page))
    }

    /// The largest `now - head_due` over destinations with a closed breaker
    /// and due work; open destinations are reported by the breaker gauge.
    pub fn oldest_due_age(&self, now: TimestampMillis) -> Duration {
        let oldest = self
            .destinations
            .values()
            .filter(|entry| matches!(entry.breaker, Breaker::Closed { .. }))
            .filter_map(|entry| entry.head_due)
            .map(|head| {
                now.timestamp_millis()
                    .saturating_sub(head.timestamp_millis())
            })
            .max()
            .unwrap_or(0);
        Duration::from_millis(u64::try_from(oldest).unwrap_or(0))
    }

    pub fn open_breakers(&self) -> usize {
        self.destinations
            .values()
            .filter(|entry| !matches!(entry.breaker, Breaker::Closed { .. }))
            .count()
    }

    pub fn leased(&self) -> usize {
        self.leased
    }

    /// The entry of a destination, created when it is new. The candidate
    /// set is bounded: the least recently touched entry with nothing in
    /// flight goes; the sweep finds its work again within one pass.
    fn touch(&mut self, key: &DestinationKey) -> &mut Destination {
        self.tick += 1;
        if !self.destinations.contains_key(key)
            && self.destinations.len() >= self.config.breaker.max_destinations.get()
        {
            self.evict_one();
        }
        let tick = self.tick;
        let entry = self.destinations.entry(key.clone()).or_insert(Destination {
            in_flight: 0,
            breaker: Breaker::Closed { failures: 0 },
            head_due: None,
            head_deadline: None,
            touched: tick,
        });
        entry.touched = tick;
        entry
    }

    /// Eviction prefers closed breakers and keeps an open breaker until its
    /// open interval ends (W3S review finding 7), so the sweep cannot make a
    /// dead destination look new; when every idle entry is open, the least
    /// recently touched one goes.
    fn evict_one(&mut self) {
        let now = self.now;
        let now_open = |entry: &Destination| match entry.breaker {
            Breaker::Closed { .. } => false,
            Breaker::Open { until, .. } => until > now,
            Breaker::HalfOpen { .. } => true,
        };
        let oldest = self
            .destinations
            .iter()
            .filter(|(_, entry)| entry.in_flight == 0)
            .min_by_key(|(_, entry)| (now_open(entry), entry.touched))
            .map(|(key, _)| key.clone());
        if let Some(oldest) = oldest {
            self.destinations.remove(&oldest);
        }
    }
}

/// The breaker after one task (design 4.3): 5 consecutive failures open it
/// for 5 s with +-20% jitter; a failed probe doubles the interval up to the
/// maximum; an answer closes it.
fn next_breaker(
    breaker: Breaker,
    signal: BreakerSignal,
    config: &BreakerConfig,
    now: TimestampMillis,
    jitter: u32,
) -> Breaker {
    match (breaker, signal) {
        (_, BreakerSignal::Answered) => Breaker::Closed { failures: 0 },
        (Breaker::HalfOpen { interval, .. }, BreakerSignal::NotAttempted) => Breaker::HalfOpen {
            interval,
            probing: false,
        },
        (breaker, BreakerSignal::NotAttempted) => breaker,
        (Breaker::Closed { failures }, BreakerSignal::Failed) => {
            let failures = failures.saturating_add(1);
            if failures >= config.failures_to_open.get() {
                open(config.open_initial, now, jitter)
            } else {
                Breaker::Closed { failures }
            }
        }
        (Breaker::HalfOpen { interval, .. }, BreakerSignal::Failed) => {
            open(interval.saturating_mul(2).min(config.open_max), now, jitter)
        }
        (breaker @ Breaker::Open { .. }, BreakerSignal::Failed) => breaker,
    }
}

/// Open for `interval` with +-20% jitter.
fn open(interval: Duration, now: TimestampMillis, jitter: u32) -> Breaker {
    let millis = u64::try_from(interval.as_millis()).unwrap_or(u64::MAX);
    let span = millis / 5;
    let offset = u64::from(jitter) % (span.saturating_mul(2) + 1);
    let open_for = Duration::from_millis(millis.saturating_sub(span).saturating_add(offset));
    Breaker::Open {
        until: after(now, open_for),
        interval,
    }
}
