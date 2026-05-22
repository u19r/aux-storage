pub(crate) const MAX_CONCURRENT_JOBS: usize = 8;

pub(crate) const FAST_JOB_INTERVAL_THRESHOLD_MS: i64 = 1_000;
pub(crate) const FAST_JOB_LOCK_WINDOW_MS: i64 = 2_000;
pub(crate) const FAST_JOB_LOCK_RENEWAL_THRESHOLD_MS: i64 = 1_000;

pub const ENV_JOBS_MODE: &str = "AUX_JOBS_MODE";
pub(crate) const JOBS_MODE_ALL: &str = "all";
pub(crate) const JOBS_MODE_METRICS_ONLY: &str = "metrics_only";
