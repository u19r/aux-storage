use std::time::{Duration, Instant};

use crate::admission::{
    admission_transition::constants::SAMPLE_LIMIT, admission_types::AdmissionClass,
};

#[derive(Debug)]
pub(crate) struct Window {
    pub(crate) started_at: Instant,
    pub(crate) samples: [SampleWindow; 3],
    pub(crate) completed: [usize; 3],
    pub(crate) successes: usize,
    pub(crate) peak_in_flight: usize,
    pub(crate) queue_waits: usize,
    pub(crate) queue_nonempty_for: Duration,
    pub(crate) explicit_pressure: bool,
    pub(crate) queue_timeout: bool,
    pub(crate) last_observation_at: Instant,
    pub(crate) queue_was_nonempty: bool,
    pub(crate) emergency_reduction_applied: bool,
}

/// The controller only needs the last bounded sample window. Keeping that
/// window in a fixed ring avoids shifting 127 durations for every completion
/// and keeps percentile evaluation off the allocator.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SampleWindow {
    values: [Duration; SAMPLE_LIMIT],
    start: usize,
    len: usize,
}

impl SampleWindow {
    const fn new() -> Self {
        Self {
            values: [Duration::ZERO; SAMPLE_LIMIT],
            start: 0,
            len: 0,
        }
    }

    fn push(&mut self, latency: Duration) {
        let index = (self.start + self.len) % SAMPLE_LIMIT;
        self.values[index] = latency;
        if self.len == SAMPLE_LIMIT {
            self.start = (self.start + 1) % SAMPLE_LIMIT;
        } else {
            self.len += 1;
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct WindowSignals {
    pub(crate) saturated: bool,
    pub(crate) congested: bool,
    pub(crate) pressure: bool,
    pub(crate) enough_samples: bool,
    pub(crate) gradient: f64,
    pub(crate) goodput: f64,
    pub(crate) now: Instant,
}

impl Window {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            started_at: now,
            samples: [SampleWindow::new(); 3],
            completed: [0; 3],
            successes: 0,
            peak_in_flight: 0,
            queue_waits: 0,
            queue_nonempty_for: Duration::ZERO,
            explicit_pressure: false,
            queue_timeout: false,
            last_observation_at: now,
            queue_was_nonempty: false,
            emergency_reduction_applied: false,
        }
    }

    pub(crate) fn observe_queue(&mut self, now: Instant, queue_depth: usize) {
        // Account for the interval using the state that held during that
        // interval. A transition to a non-empty queue starts the clock at
        // the transition, rather than charging the preceding idle period.
        if self.queue_was_nonempty {
            self.queue_nonempty_for = self
                .queue_nonempty_for
                .saturating_add(now.saturating_duration_since(self.last_observation_at));
        }
        self.last_observation_at = now;
        self.queue_was_nonempty = queue_depth > 0;
    }

    pub(crate) fn add_sample(&mut self, class: AdmissionClass, latency: Duration) {
        let index = class.index();
        self.completed[index] = self.completed[index].saturating_add(1);
        self.samples[index].push(latency);
    }

    pub(crate) fn p90(samples: &SampleWindow) -> Option<f64> {
        if samples.len == 0 {
            return None;
        }
        let mut ordered = samples.values;
        ordered[..samples.len].sort_unstable();
        let index = ((samples.len - 1) * 90) / 100;
        ordered
            .get(index)
            .map(|duration| duration.as_secs_f64() * 1_000.0)
    }
}
