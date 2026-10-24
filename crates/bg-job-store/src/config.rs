use std::time::Duration;

/// Execution policy supplied to the native scheduled-job store constructor.
/// Capability construction validates the budget along with native limits.
#[derive(Clone, Copy, Debug)]
pub struct ScheduledJobStoreConfig {
    pub max_transaction_duration: Duration,
}

impl Default for ScheduledJobStoreConfig {
    fn default() -> Self {
        Self::from(super::constants::DEFAULT_TRANSACTION_DURATION)
    }
}

impl From<Duration> for ScheduledJobStoreConfig {
    fn from(max_transaction_duration: Duration) -> Self {
        Self {
            max_transaction_duration,
        }
    }
}
