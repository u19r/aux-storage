use std::time::Duration;

pub(crate) const DELIVERY_IDLE_INTERVAL: Duration = Duration::from_secs(1);
pub(crate) const DELIVERY_ERROR_INTERVAL: Duration = Duration::from_secs(5);
pub(crate) const DELIVERY_BATCH_LIMIT: usize = 100;
