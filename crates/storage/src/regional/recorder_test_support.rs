//! A metrics recorder for tests: keeps every registered gauge by key.
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};

#[derive(Default)]
pub(crate) struct KeyRecorder {
    gauges: Mutex<HashMap<Key, Arc<AtomicU64>>>,
}

impl KeyRecorder {
    /// Labels and current value of every gauge registered under `name`.
    pub(crate) fn gauges_named(&self, name: &str) -> Vec<(Vec<(String, String)>, f64)> {
        self.gauges
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.name() == name)
            .map(|(key, value)| {
                (
                    key.labels()
                        .map(|label| (label.key().to_owned(), label.value().to_owned()))
                        .collect(),
                    f64::from_bits(value.load(Ordering::Relaxed)),
                )
            })
            .collect()
    }
}

impl Recorder for KeyRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, _: &Key, _: &Metadata<'_>) -> Counter {
        Counter::noop()
    }

    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::from_arc(
            self.gauges
                .lock()
                .unwrap()
                .entry(key.clone())
                .or_default()
                .clone(),
        )
    }

    fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
}
