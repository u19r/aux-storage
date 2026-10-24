use std::time::Duration;

pub(super) const SAMPLE_LIMIT: usize = 128;
pub(crate) const WINDOW: Duration = Duration::from_secs(1);
pub(super) const BASELINE_SAMPLE_MINIMUM: usize = 32;
pub(super) const QUEUE_SATURATION_PERCENT: u32 = 20;
pub(super) const LATENCY_BUFFER: f64 = 1.20;
pub(super) const PROBE_PERCENT: usize = 5;
pub(super) const MIN_NORMAL_FACTOR: f64 = 0.80;
pub(super) const MAX_NORMAL_FACTOR: f64 = 0.90;
pub(super) const NORMAL_FACTOR_SCALE: u128 = 1_000_000;
pub(super) const BASELINE_RAISE_INTERVAL: Duration = Duration::from_secs(60);
pub(super) const BASELINE_RAISE_FACTOR: f64 = 1.01;
