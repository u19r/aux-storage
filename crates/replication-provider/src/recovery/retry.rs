use std::{num::NonZeroU64, time::Duration};
#[derive(Clone, Copy, Debug)]
pub struct RetrySchedule {
    minimum_ms: NonZeroU64,
    maximum_ms: NonZeroU64,
}
impl RetrySchedule {
    pub fn try_new(
        minimum_ms: NonZeroU64,
        maximum_ms: NonZeroU64,
    ) -> Result<Self, replication_types::error::ReplicationProtocolError> {
        validate_retry_bounds(minimum_ms, maximum_ms)?;
        Ok(Self {
            minimum_ms,
            maximum_ms,
        })
    }
    /// Scheduling overflow saturates the delay; protocol counters never use
    /// saturation.
    pub fn delay(self, failures: u32, jitter: u64) -> Duration {
        let cap = self.maximum_ms.get().max(self.minimum_ms.get());
        let ceiling = self
            .minimum_ms
            .get()
            .saturating_mul(1u64.checked_shl(failures).unwrap_or(u64::MAX))
            .min(cap);
        let floor = self.minimum_ms.get().min(ceiling);
        Duration::from_millis(floor + jitter % (ceiling - floor + 1))
    }
}

fn validate_retry_bounds(
    minimum_ms: NonZeroU64,
    maximum_ms: NonZeroU64,
) -> Result<(), replication_types::error::ReplicationProtocolError> {
    if minimum_ms > maximum_ms {
        return Err(
            replication_types::error::ReplicationProtocolError::InvalidRange {
                context: "retry minimum exceeds maximum",
            },
        );
    }
    Ok(())
}
