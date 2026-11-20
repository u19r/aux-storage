//! The regional lag gauge of one connection.
use std::{
    collections::HashMap,
    sync::{Mutex, PoisonError},
    time::{Duration, Instant},
};

use storage_types::PairId;

use crate::constants::REGIONAL_LAG_METRIC;

/// Lag of the pairs whose turns ran in this process, published as one gauge
/// labelled by connection id: the largest lag observed within `window`. Turns
/// of one pair can run on any node; a pair whose turns stopped here drops out
/// after `window`. In-process state: a hint for operators, read by nothing.
pub(crate) struct PairLag {
    gauge: metrics::Gauge,
    window: Duration,
    pairs: Mutex<HashMap<PairId, Observed>>,
}

#[derive(Clone, Copy)]
struct Observed {
    lag_millis: u64,
    at: Instant,
}

impl PairLag {
    /// Registers the connection's gauge once (as admission does), so a turn
    /// builds no label.
    pub(crate) fn new(connection_id: &str, window: Duration) -> Self {
        Self {
            gauge: metrics::gauge!(REGIONAL_LAG_METRIC, "connection" => connection_id.to_owned()),
            window,
            pairs: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn observe(&self, pair: PairId, lag_millis: u64) {
        self.observe_at(pair, lag_millis, Instant::now());
    }

    /// The pair's job ended here: its lag leaves the gauge now.
    pub(crate) fn end(&self, pair: PairId) {
        self.end_at(pair, Instant::now());
    }

    pub(crate) fn observe_at(&self, pair: PairId, lag_millis: u64, now: Instant) {
        self.publish(now, |pairs| {
            pairs.insert(
                pair,
                Observed {
                    lag_millis,
                    at: now,
                },
            );
        });
    }

    pub(crate) fn end_at(&self, pair: PairId, now: Instant) {
        self.publish(now, |pairs| {
            pairs.remove(&pair);
        });
    }

    fn publish(&self, now: Instant, change: impl FnOnce(&mut HashMap<PairId, Observed>)) {
        let largest = {
            let mut pairs = self.pairs.lock().unwrap_or_else(PoisonError::into_inner);
            change(&mut pairs);
            pairs.retain(|_, observed| now.saturating_duration_since(observed.at) <= self.window);
            pairs
                .values()
                .map(|observed| observed.lag_millis)
                .max()
                .unwrap_or(0)
        };
        self.gauge.set(largest as f64);
    }
}
